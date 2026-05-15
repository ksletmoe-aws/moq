//! CMSF demuxer — three layers for consuming CMSF-encoded MoQ broadcasts.
//!
//! - [`parse_cmsf_metadata`]: Pure function extracting timing/keyframe info from moof+mdat.
//! - [`CmsfTrackDemuxer`]: Per-track async wrapper yielding [`CmsfSegment`].
//! - [`CmsfBroadcastDemuxer`]: Broadcast-level coordinator managing catalog + subscriptions.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;

// Re-export shared CMSF types and functions for backward compatibility.
pub use crate::cmsf::{CmsfError, CmsfMetadata, parse_cmsf_metadata, parse_timescale};

/// A demuxed CMSF segment.
#[derive(Clone, Debug)]
pub struct CmsfSegment {
	pub track_name: Arc<str>,
	pub group_id: u64,
	pub media_data: Bytes,
	pub pts: u64,
	pub duration: u64,
	pub timescale: u64,
	pub is_keyframe: bool,
}

/// Why a track ended.
#[derive(Clone, Debug)]
pub enum EndReason {
	CatalogRemoved,
	TrackFinished,
	TrackError(moq_lite::Error),
}

// ---------------------------------------------------------------------------
// Layer 2: Per-Track Demuxer
// ---------------------------------------------------------------------------

/// Per-track CMSF demuxer yielding [`CmsfSegment`] per MoQ Object.
pub struct CmsfTrackDemuxer {
	track: moq_lite::TrackConsumer,
	pub(crate) init_data: Bytes,
	timescale: u64,
	name: Arc<str>,
	current_group: Option<moq_lite::GroupConsumer>,
	end_reason: Option<EndReason>,
	malformed_count: u32,
}

impl CmsfTrackDemuxer {
	pub fn new(track: moq_lite::TrackConsumer, init_data: Bytes, timescale: u64) -> Self {
		let name: Arc<str> = track.name.as_str().into();
		Self {
			track,
			init_data,
			timescale,
			name,
			current_group: None,
			end_reason: None,
			malformed_count: 0,
		}
	}

	/// Create from an MSF catalog track entry.
	pub fn from_msf_track(track: moq_lite::TrackConsumer, msf_track: &moq_msf::Track) -> Result<Self, CmsfError> {
		use base64::Engine;
		let init_b64 = msf_track.init_data.as_deref().ok_or(CmsfError::MissingInitData)?;
		let init_data = base64::engine::general_purpose::STANDARD.decode(init_b64)?;
		let timescale = parse_timescale(&init_data)?;
		Ok(Self::new(track, Bytes::from(init_data), timescale))
	}

	/// Read the next segment. Returns `None` when the track ends.
	pub async fn next(&mut self) -> Result<Option<CmsfSegment>, CmsfError> {
		conducer::wait(|waiter| self.poll_next(waiter)).await
	}

	pub fn poll_next(&mut self, waiter: &conducer::Waiter) -> std::task::Poll<Result<Option<CmsfSegment>, CmsfError>> {
		use std::task::{Poll, ready};

		loop {
			if let Some(group) = &mut self.current_group {
				match group.poll_read_frame(waiter) {
					Poll::Ready(Ok(Some(data))) => {
						let group_id = group.sequence;
						match parse_cmsf_metadata(&data) {
							Ok(meta) => {
								self.malformed_count = 0;
								return Poll::Ready(Ok(Some(CmsfSegment {
									track_name: self.name.clone(),
									group_id,
									media_data: data,
									pts: meta.pts,
									duration: meta.duration,
									timescale: self.timescale,
									is_keyframe: meta.is_keyframe,
								})));
							}
							Err(_) => {
								self.malformed_count += 1;
								if self.malformed_count == 1 {
									tracing::warn!(track = %self.name, "skipping malformed segment");
								}
								if self.malformed_count >= 100 {
									tracing::error!(track = %self.name, "100 consecutive malformed segments, closing track");
									self.end_reason = Some(EndReason::TrackFinished);
									return Poll::Ready(Ok(None));
								}
								continue;
							}
						}
					}
					Poll::Ready(Ok(None)) => {
						self.current_group = None;
						continue;
					}
					Poll::Ready(Err(err)) => {
						tracing::debug!(track = %self.name, %err, "group read error, skipping");
						self.current_group = None;
						continue;
					}
					Poll::Pending => return Poll::Pending,
				}
			}

			match ready!(self.track.poll_recv_group(waiter)) {
				Ok(Some(group)) => {
					self.current_group = Some(group);
				}
				Ok(None) => {
					self.end_reason = Some(EndReason::TrackFinished);
					return Poll::Ready(Ok(None));
				}
				Err(e) => {
					self.end_reason = Some(EndReason::TrackError(e));
					return Poll::Ready(Ok(None));
				}
			}
		}
	}

	pub fn init_data(&self) -> &Bytes {
		&self.init_data
	}

	pub fn end_reason(&self) -> Option<&EndReason> {
		self.end_reason.as_ref()
	}
}

// ---------------------------------------------------------------------------
// Layer 3: Broadcast Demuxer
// ---------------------------------------------------------------------------

struct BroadcastTrackState {
	demuxer: CmsfTrackDemuxer,
	active: bool,
}

/// Broadcast-level CMSF demuxer. Subscribes to MSF catalog, manages track demuxers.
pub struct CmsfBroadcastDemuxer {
	broadcast: moq_lite::BroadcastConsumer,
	catalog_track: moq_lite::TrackConsumer,
	catalog_group: Option<moq_lite::GroupConsumer>,
	tracks: HashMap<Arc<str>, BroadcastTrackState>,
	catalog_closed: bool,
}

impl CmsfBroadcastDemuxer {
	pub fn new(broadcast: moq_lite::BroadcastConsumer) -> Result<Self, CmsfError> {
		let catalog_track = broadcast.subscribe_track(&moq_lite::Track::new("catalog"))?;
		Ok(Self {
			broadcast,
			catalog_track,
			catalog_group: None,
			tracks: HashMap::new(),
			catalog_closed: false,
		})
	}

	/// Wait until the first catalog is received and tracks are subscribed.
	pub async fn ready(&mut self) -> Result<(), CmsfError> {
		conducer::wait(|waiter| self.poll_ready(waiter)).await
	}

	fn poll_ready(&mut self, waiter: &conducer::Waiter) -> std::task::Poll<Result<(), CmsfError>> {
		use std::task::Poll;

		if !self.tracks.is_empty() {
			return Poll::Ready(Ok(()));
		}

		match self.poll_catalog(waiter) {
			Poll::Ready(Ok(Some(catalog))) => {
				self.apply_catalog(&catalog);
				if self.tracks.is_empty() {
					Poll::Ready(Err(CmsfError::NoTracks))
				} else {
					Poll::Ready(Ok(()))
				}
			}
			Poll::Ready(Ok(None)) => {
				self.catalog_closed = true;
				Poll::Ready(Err(CmsfError::NoTracks))
			}
			Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
			Poll::Pending => Poll::Pending,
		}
	}

	/// Read the next segment from any track.
	pub async fn next(&mut self) -> Result<Option<CmsfSegment>, CmsfError> {
		conducer::wait(|waiter| self.poll_next(waiter)).await
	}

	pub fn poll_next(&mut self, waiter: &conducer::Waiter) -> std::task::Poll<Result<Option<CmsfSegment>, CmsfError>> {
		use std::task::Poll;

		// Poll catalog for updates.
		if !self.catalog_closed {
			match self.poll_catalog(waiter) {
				Poll::Ready(Ok(Some(catalog))) => {
					self.apply_catalog(&catalog);
				}
				Poll::Ready(Ok(None)) => {
					self.catalog_closed = true;
				}
				Poll::Ready(Err(_)) => {
					self.catalog_closed = true;
				}
				Poll::Pending => {}
			}
		}

		// Poll all active track demuxers.
		for state in self.tracks.values_mut() {
			if !state.active {
				continue;
			}
			match state.demuxer.poll_next(waiter) {
				Poll::Ready(Ok(Some(seg))) => {
					return Poll::Ready(Ok(Some(seg)));
				}
				Poll::Ready(Ok(None)) => {
					state.active = false;
				}
				Poll::Ready(Err(_)) => {
					state.active = false;
				}
				Poll::Pending => {}
			}
		}

		let all_ended = self.tracks.is_empty() || self.tracks.values().all(|s| !s.active);
		if all_ended && self.catalog_closed {
			return Poll::Ready(Ok(None));
		}

		Poll::Pending
	}

	/// Names of currently active tracks.
	pub fn active_tracks(&self) -> Vec<&str> {
		self.tracks
			.iter()
			.filter(|(_, s)| s.active)
			.map(|(name, _)| name.as_ref())
			.collect()
	}

	/// Init segments for all active tracks, suitable for building a merged fMP4 init.
	pub fn init_segments(&self) -> Vec<&Bytes> {
		self.tracks
			.values()
			.filter(|s| s.active)
			.map(|s| &s.demuxer.init_data)
			.collect()
	}

	/// Poll for a new catalog. Assumes each catalog group contains exactly one frame.
	///
	/// If multiple catalog groups arrive between polls, only the latest is read
	/// (intermediate versions are skipped). This is intentional: the latest catalog
	/// is always authoritative and supersedes prior versions.
	fn poll_catalog(
		&mut self,
		waiter: &conducer::Waiter,
	) -> std::task::Poll<Result<Option<moq_msf::Catalog>, CmsfError>> {
		use std::task::Poll;

		// Get newest group.
		while let Poll::Ready(result) = self.catalog_track.poll_recv_group(waiter) {
			match result {
				Ok(Some(group)) => self.catalog_group = Some(group),
				Ok(None) => {
					if self.catalog_group.is_none() {
						return Poll::Ready(Ok(None));
					}
					break;
				}
				Err(e) => return Poll::Ready(Err(CmsfError::Moq(e))),
			}
		}

		let Some(group) = &mut self.catalog_group else {
			return Poll::Pending;
		};

		if let Poll::Ready(result) = group.poll_read_frame(waiter) {
			self.catalog_group.take();
			match result {
				Ok(Some(data)) => {
					let catalog = serde_json::from_slice::<moq_msf::Catalog>(&data)?;
					return Poll::Ready(Ok(Some(catalog)));
				}
				Ok(None) => {}
				Err(e) => return Poll::Ready(Err(CmsfError::Moq(e))),
			}
		}

		Poll::Pending
	}

	fn apply_catalog(&mut self, catalog: &moq_msf::Catalog) {
		use base64::Engine;

		// Mark tracks not present in the new catalog as inactive.
		for (name, state) in &mut self.tracks {
			if state.active && !catalog.tracks.iter().any(|t| t.name.as_str() == name.as_ref()) {
				state.active = false;
				state.demuxer.end_reason = Some(EndReason::CatalogRemoved);
			}
		}

		// Remove inactive tracks so re-added tracks get a fresh subscription.
		self.tracks.retain(|_, s| s.active);

		for msf_track in &catalog.tracks {
			if msf_track.packaging != moq_msf::Packaging::Cmaf {
				continue;
			}
			if self.tracks.get(msf_track.name.as_str()).is_some_and(|s| s.active) {
				continue;
			}
			let Some(init_b64) = &msf_track.init_data else { continue };
			let Ok(init_data) = base64::engine::general_purpose::STANDARD.decode(init_b64) else {
				continue;
			};
			let Ok(timescale) = parse_timescale(&init_data) else {
				continue;
			};

			let track = moq_lite::Track::new(msf_track.name.as_str());
			let Ok(consumer) = self.broadcast.subscribe_track(&track) else {
				continue;
			};

			let demuxer = CmsfTrackDemuxer::new(consumer, Bytes::from(init_data), timescale);
			let name: Arc<str> = msf_track.name.as_str().into();
			self.tracks.insert(name, BroadcastTrackState { demuxer, active: true });
		}
	}
}

/// Build a merged ftyp + multi-track moov init segment from per-track init segments.
///
/// Each input should be a complete single-track init segment (ftyp + moov) as stored
/// in the MSF catalog's `init_data` field. The output is a single multi-track init
/// segment suitable for fMP4 playback.
pub fn build_merged_init(inits: &[&Bytes]) -> Result<Bytes, CmsfError> {
	use mp4_atom::{DecodeMaybe, Encode};

	let mut traks = Vec::new();
	let mut trexs = Vec::new();
	let mut ftyp_data = None;

	for init in inits {
		let mut cursor = std::io::Cursor::new(init.as_ref());
		while let Some(atom) = mp4_atom::Any::decode_maybe(&mut cursor).map_err(|_| CmsfError::NoMoov)? {
			match atom {
				mp4_atom::Any::Ftyp(f) if ftyp_data.is_none() => ftyp_data = Some(f),
				mp4_atom::Any::Moov(moov) => {
					for trak in moov.trak {
						traks.push(trak);
					}
					if let Some(mvex) = moov.mvex {
						for trex in mvex.trex {
							trexs.push(trex);
						}
					}
				}
				_ => {}
			}
		}
	}

	let ftyp = ftyp_data.ok_or(CmsfError::NoMoov)?;

	// Renumber track IDs sequentially to avoid collisions when merging
	// independently-produced init segments (each may start at track_id=1).
	// Map old→new so trex entries stay paired with their trak.
	let mut id_map = HashMap::new();
	for (i, trak) in traks.iter_mut().enumerate() {
		let new_id = (i + 1) as u32;
		id_map.insert(trak.tkhd.track_id, new_id);
		trak.tkhd.track_id = new_id;
	}
	for trex in &mut trexs {
		if let Some(&new_id) = id_map.get(&trex.track_id) {
			trex.track_id = new_id;
		}
	}

	// mvhd.timescale is the movie-level timescale (not per-track). Use 1000 (ms)
	// which is the conventional value for fragmented MP4 with no movie-level duration.
	let timescale = 1000;

	let moov = mp4_atom::Moov {
		mvhd: mp4_atom::Mvhd {
			timescale,
			..Default::default()
		},
		trak: traks,
		mvex: if trexs.is_empty() {
			None
		} else {
			Some(mp4_atom::Mvex {
				trex: trexs,
				..Default::default()
			})
		},
		..Default::default()
	};

	let mut buf = Vec::new();
	ftyp.encode(&mut buf).map_err(|_| CmsfError::EncodeFailed)?;
	moov.encode(&mut buf).map_err(|_| CmsfError::EncodeFailed)?;
	Ok(Bytes::from(buf))
}

#[cfg(test)]
mod test {
	use super::*;
	use crate::cmsf::test_helpers::{build_init_segment, build_moof_mdat};
	use mp4_atom::Encode;

	#[test]
	fn parse_keyframe() {
		let data = build_moof_mdat(90000, 3000, 0x0200_0000);
		let meta = parse_cmsf_metadata(&data).unwrap();
		assert_eq!(meta.pts, 90000);
		assert_eq!(meta.duration, 3000);
		assert!(meta.is_keyframe);
	}

	#[test]
	fn parse_non_keyframe() {
		let data = build_moof_mdat(93000, 3000, 0x0001_0000);
		let meta = parse_cmsf_metadata(&data).unwrap();
		assert_eq!(meta.pts, 93000);
		assert!(!meta.is_keyframe);
	}

	#[test]
	fn parse_empty_input() {
		assert!(matches!(parse_cmsf_metadata(&[]), Err(CmsfError::NoMoof)));
	}

	#[test]
	fn parse_no_tfdt() {
		let moof = mp4_atom::Moof {
			mfhd: mp4_atom::Mfhd { sequence_number: 1 },
			traf: vec![mp4_atom::Traf {
				tfhd: mp4_atom::Tfhd {
					track_id: 1,
					..Default::default()
				},
				tfdt: None,
				trun: vec![mp4_atom::Trun {
					data_offset: Some(0),
					entries: vec![mp4_atom::TrunEntry {
						size: Some(4),
						duration: Some(1000),
						flags: Some(0x0200_0000),
						..Default::default()
					}],
				}],
				..Default::default()
			}],
		};
		let mut buf = Vec::new();
		moof.encode(&mut buf).unwrap();
		assert!(matches!(parse_cmsf_metadata(&buf), Err(CmsfError::NoTfdt)));
	}

	#[test]
	fn parse_no_trun() {
		let moof = mp4_atom::Moof {
			mfhd: mp4_atom::Mfhd { sequence_number: 1 },
			traf: vec![mp4_atom::Traf {
				tfhd: mp4_atom::Tfhd {
					track_id: 1,
					..Default::default()
				},
				tfdt: Some(mp4_atom::Tfdt {
					base_media_decode_time: 0,
				}),
				trun: vec![],
				..Default::default()
			}],
		};
		let mut buf = Vec::new();
		moof.encode(&mut buf).unwrap();
		assert!(matches!(parse_cmsf_metadata(&buf), Err(CmsfError::NoTrun)));
	}

	#[test]
	fn parse_timescale_valid() {
		let init = build_init_segment(48000);
		assert_eq!(parse_timescale(&init).unwrap(), 48000);
	}

	#[test]
	fn parse_timescale_no_moov() {
		assert!(matches!(parse_timescale(&[]), Err(CmsfError::NoMoov)));
	}

	#[test]
	fn parse_multi_sample_duration() {
		let entries: Vec<mp4_atom::TrunEntry> = (0..4)
			.map(|i| mp4_atom::TrunEntry {
				size: Some(4),
				duration: Some(1000),
				flags: Some(if i == 0 { 0x0200_0000 } else { 0x0001_0000 }),
				..Default::default()
			})
			.collect();
		let moof = mp4_atom::Moof {
			mfhd: mp4_atom::Mfhd { sequence_number: 1 },
			traf: vec![mp4_atom::Traf {
				tfhd: mp4_atom::Tfhd {
					track_id: 1,
					..Default::default()
				},
				tfdt: Some(mp4_atom::Tfdt {
					base_media_decode_time: 0,
				}),
				trun: vec![mp4_atom::Trun {
					data_offset: Some(0),
					entries,
				}],
				..Default::default()
			}],
		};
		let mdat = mp4_atom::Mdat { data: vec![0; 16] };
		let mut buf = Vec::new();
		moof.encode(&mut buf).unwrap();
		mdat.encode(&mut buf).unwrap();
		let meta = parse_cmsf_metadata(&buf).unwrap();
		assert_eq!(meta.duration, 4000);
		assert!(meta.is_keyframe);
	}

	#[test]
	fn parse_default_duration_fallback() {
		let moof = mp4_atom::Moof {
			mfhd: mp4_atom::Mfhd { sequence_number: 1 },
			traf: vec![mp4_atom::Traf {
				tfhd: mp4_atom::Tfhd {
					track_id: 1,
					default_sample_duration: Some(512),
					default_sample_flags: Some(0x0200_0000),
					..Default::default()
				},
				tfdt: Some(mp4_atom::Tfdt {
					base_media_decode_time: 0,
				}),
				trun: vec![mp4_atom::Trun {
					data_offset: Some(0),
					entries: vec![
						mp4_atom::TrunEntry {
							size: Some(4),
							..Default::default()
						},
						mp4_atom::TrunEntry {
							size: Some(4),
							..Default::default()
						},
						mp4_atom::TrunEntry {
							size: Some(4),
							..Default::default()
						},
					],
				}],
				..Default::default()
			}],
		};
		let mdat = mp4_atom::Mdat { data: vec![0; 12] };
		let mut buf = Vec::new();
		moof.encode(&mut buf).unwrap();
		mdat.encode(&mut buf).unwrap();
		let meta = parse_cmsf_metadata(&buf).unwrap();
		assert_eq!(meta.duration, 512 * 3);
		assert!(meta.is_keyframe);
	}
}
