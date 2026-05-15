use anyhow::Context;
use base64::Engine;
use bytes::Bytes;
use hang::catalog::{Audio, AudioConfig, Container, Video, VideoConfig};
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use super::cmsf_types::{
	CmsfAudioTrack, CmsfConfig, CmsfObject, CmsfTrackConfig, CmsfTrackProducer, CmsfVideoTrack, SapType, TrackId,
};
use super::fmp4_parser::TrackKind;

/// Coordinates multi-rendition CMSF-compliant broadcast publishing.
///
/// ## Group Lifecycle
///
/// Video keyframes drive group boundaries. Each video keyframe starts a new moq-lite
/// group. Non-switching audio (no `alt_group`) is aligned to video: a new audio group
/// starts on each video keyframe. Switching audio (with `alt_group`) manages its own
/// groups independently.
///
/// ## Audio Pre-Roll Buffering
///
/// Audio arriving before the first video keyframe is buffered and flushed once the
/// first video keyframe arrives. This ensures subscribers joining at a group boundary
/// get both the video keyframe and corresponding audio.
///
/// ## Explicit Group Mode
///
/// When `CmsfObject::group_id` is `Some(id)`, the track enters "explicit mode" —
/// the caller controls group boundaries (e.g. for dual-pipeline redundancy).
/// Explicit-mode audio bypasses the pre-roll buffer and is not auto-rotated on
/// video keyframes.
///
/// ## SAP Type Derivation
///
/// SAP type is derived from the track's codec configuration rather than stream
/// analysis. H264/VP8 keyframes → Type 1 (IDR), H265/AV1/VP9 → Type 2 (conservative,
/// may be CRA). This is a known deviation from strict spec compliance — an H265
/// stream with only IDR frames will be reported as Type 2.
///
/// Owns its catalog tracks independently of `catalog::Producer`. Publishes an MSF
/// catalog with CMSF-specific metadata and optionally a hang catalog for backward
/// compatibility.
pub struct CmsfBroadcastProducer {
	broadcast: moq_lite::BroadcastProducer,
	pub(crate) msf_track: moq_lite::TrackProducer,
	pub(crate) hang_track: Option<moq_lite::TrackProducer>,
	pub(crate) tracks: Vec<CmsfTrackProducer>,
	video_count: usize,
	audio_count: usize,
	audio_buffer: Vec<(TrackId, CmsfObject)>,
	first_video_group: bool,
	has_video_track: bool,
	catalog_failed: bool,
}

impl CmsfBroadcastProducer {
	/// Create a new CMSF broadcast producer that owns its catalog tracks.
	pub fn new(mut broadcast: moq_lite::BroadcastProducer, config: CmsfConfig) -> anyhow::Result<Self> {
		let msf_track = broadcast.create_track(moq_lite::Track::new("catalog"))?;
		let hang_track = if config.publish_hang {
			Some(broadcast.create_track(moq_lite::Track::new("catalog.json"))?)
		} else {
			None
		};

		Ok(Self {
			broadcast,
			msf_track,
			hang_track,
			tracks: Vec::new(),
			video_count: 0,
			audio_count: 0,
			audio_buffer: Vec::new(),
			first_video_group: false,
			has_video_track: false,
			catalog_failed: false,
		})
	}

	/// Add a video track. Returns a handle for writing chunks.
	pub fn add_video_track(&mut self, track: CmsfVideoTrack) -> anyhow::Result<TrackId> {
		let init_data = base64::engine::general_purpose::STANDARD.encode(&track.init_segment);
		let name = format!("video{}.m4s", self.video_count);
		self.video_count += 1;

		let moq_track = self.broadcast.create_track(moq_lite::Track::new(&name))?;
		let id = TrackId(self.tracks.len());
		let config = CmsfTrackConfig::Video {
			codec: track.codec,
			description: track.description,
			width: track.width,
			height: track.height,
			bitrate: track.bitrate,
			framerate: track.framerate,
			init_data,
		};
		self.tracks.push(CmsfTrackProducer::new(
			TrackKind::Video,
			moq_track,
			track.alt_group,
			config,
			track.timescale,
		));
		self.has_video_track = true;
		self.publish_catalogs();
		Ok(id)
	}

	/// Add an audio track. Returns a handle for writing chunks.
	pub fn add_audio_track(&mut self, track: CmsfAudioTrack) -> anyhow::Result<TrackId> {
		let init_data = base64::engine::general_purpose::STANDARD.encode(&track.init_segment);
		let name = format!("audio{}.m4s", self.audio_count);
		self.audio_count += 1;

		let moq_track = self.broadcast.create_track(moq_lite::Track::new(&name))?;
		let id = TrackId(self.tracks.len());
		let config = CmsfTrackConfig::Audio {
			codec: track.codec,
			description: track.description,
			sample_rate: track.sample_rate,
			channel_count: track.channel_count,
			bitrate: track.bitrate,
			init_data,
		};
		self.tracks.push(CmsfTrackProducer::new(
			TrackKind::Audio,
			moq_track,
			track.alt_group,
			config,
			track.timescale,
		));
		self.publish_catalogs();
		Ok(id)
	}

	/// Write a CMSF Object to the specified track.
	///
	/// Internally parses moof+mdat metadata (PTS, duration, keyframe) and derives
	/// SAP type from the track's codec configuration.
	pub fn write(&mut self, track_id: TrackId, obj: CmsfObject) -> anyhow::Result<()> {
		let meta = crate::cmsf::parse_cmsf_metadata(&obj.data)?;
		let track = self.tracks.get(track_id.0).context("invalid track ID")?;
		let sap_type = if meta.is_keyframe {
			track.sap_type_for_keyframe()
		} else {
			SapType::None
		};
		let kind = track.kind;
		let alt_group = track.alt_group;
		let is_non_switching_audio = kind == TrackKind::Audio && alt_group.is_none();
		let is_explicit = obj.group_id.is_some();

		// Explicit-mode audio bypasses the pre-roll buffer.
		if is_non_switching_audio && is_explicit {
			let track = self.tracks.get_mut(track_id.0).unwrap();
			track.explicit_groups = true;
			if let Some(id) = obj.group_id {
				if let Some(last) = track.last_explicit_group_id {
					anyhow::ensure!(id >= last, "non-monotonic group_id: {id} < {last}");
				}
				if track.group.is_none() || track.last_explicit_group_id != Some(id) {
					track.validate_group_sap(sap_type)?;
					track.start_group(Some(id))?;
				}
				track.last_explicit_group_id = Some(id);
			}
			track.track_object_sap(sap_type);
			track.write_fragment(obj.data)?;
			let _ = track.update_jitter(meta.pts, Some(meta.duration));
			return Ok(());
		}

		// Auto-mode non-switching audio: buffer before first video keyframe.
		if is_non_switching_audio && self.has_video_track && !self.first_video_group {
			self.audio_buffer.push((track_id, obj));
			return Ok(());
		}

		// Auto-mode non-switching audio after first video group.
		if is_non_switching_audio && self.has_video_track {
			let track = self.tracks.get_mut(track_id.0).unwrap();
			if track.group.is_none() {
				track.validate_group_sap(sap_type)?;
				track.start_group(None)?;
			}
			track.track_object_sap(sap_type);
			track.write_fragment(obj.data)?;
			let _ = track.update_jitter(meta.pts, Some(meta.duration));
			return Ok(());
		}

		// Video or switching audio track.
		let is_keyframe = sap_type.is_keyframe();
		let is_video_keyframe = kind == TrackKind::Video && is_keyframe;
		let track = self.tracks.get_mut(track_id.0).unwrap();

		if track.group.is_none() && !is_keyframe {
			anyhow::bail!("first chunk for track must be a keyframe");
		}

		if is_keyframe {
			track.validate_group_sap(sap_type)?;
			if is_explicit {
				track.explicit_groups = true;
				let id = obj.group_id.unwrap();
				if let Some(last) = track.last_explicit_group_id {
					anyhow::ensure!(id >= last, "non-monotonic group_id: {id} < {last}");
				}
				if track.last_explicit_group_id != Some(id) {
					track.start_group(Some(id))?;
				}
				track.last_explicit_group_id = Some(id);
			} else {
				track.start_group(None)?;
			}
		}

		track.track_object_sap(sap_type);
		track.write_fragment(obj.data)?;
		let jitter_changed = track.update_jitter(meta.pts, Some(meta.duration));

		let republish = track.sap_changed() || (jitter_changed && track.jitter.is_some());

		if republish {
			self.publish_catalogs();
		}

		if is_video_keyframe {
			if !self.first_video_group {
				self.first_video_group = true;
				self.flush_audio_buffer()?;
			} else {
				self.start_non_switching_audio_groups()?;
			}
		}

		Ok(())
	}

	/// Build the MSF catalog from current track state.
	pub fn msf_catalog(&self) -> moq_msf::Catalog {
		let tracks = self
			.tracks
			.iter()
			.map(|tp| match &tp.config {
				CmsfTrackConfig::Video {
					codec,
					width,
					height,
					framerate,
					bitrate,
					init_data,
					..
				} => moq_msf::Track {
					name: tp.track.name.clone(),
					packaging: moq_msf::Packaging::Cmaf,
					is_live: true,
					role: Some(moq_msf::Role::Video),
					codec: Some(codec.to_string()),
					width: *width,
					height: *height,
					framerate: *framerate,
					samplerate: None,
					channel_config: None,
					bitrate: *bitrate,
					init_data: Some(init_data.clone()),
					render_group: Some(1),
					alt_group: tp.alt_group,
					max_grp_sap_starting_type: tp.max_grp_sap.to_msf_value(),
					max_obj_sap_starting_type: tp.max_obj_sap.to_msf_value(),
				},
				CmsfTrackConfig::Audio {
					codec,
					sample_rate,
					channel_count,
					bitrate,
					init_data,
					..
				} => moq_msf::Track {
					name: tp.track.name.clone(),
					packaging: moq_msf::Packaging::Cmaf,
					is_live: true,
					role: Some(moq_msf::Role::Audio),
					codec: Some(codec.to_string()),
					width: None,
					height: None,
					framerate: None,
					samplerate: Some(*sample_rate),
					channel_config: Some(channel_count.to_string()),
					bitrate: *bitrate,
					init_data: Some(init_data.clone()),
					render_group: Some(1),
					alt_group: tp.alt_group,
					max_grp_sap_starting_type: tp.max_grp_sap.to_msf_value(),
					max_obj_sap_starting_type: tp.max_obj_sap.to_msf_value(),
				},
			})
			.collect();

		moq_msf::Catalog {
			version: 1,
			generated_at: SystemTime::now()
				.duration_since(UNIX_EPOCH)
				.ok()
				// u128 → u64 truncation is safe until year ~584 million.
				.map(|d| d.as_millis() as u64),
			tracks,
		}
	}

	/// Finish all tracks and catalog tracks.
	pub fn finish(&mut self) -> anyhow::Result<()> {
		for track in &mut self.tracks {
			track.finish()?;
		}
		self.msf_track.finish()?;
		if let Some(hang) = &mut self.hang_track {
			hang.finish()?;
		}
		Ok(())
	}

	fn flush_audio_buffer(&mut self) -> anyhow::Result<()> {
		let buffer = std::mem::take(&mut self.audio_buffer);
		for (tid, obj) in buffer {
			let meta = crate::cmsf::parse_cmsf_metadata(&obj.data)?;
			let track = self.tracks.get_mut(tid.0).context("invalid track ID")?;
			let sap_type = if meta.is_keyframe {
				track.sap_type_for_keyframe()
			} else {
				SapType::None
			};
			if track.group.is_none() {
				track.validate_group_sap(sap_type)?;
				track.start_group(None)?;
			}
			track.track_object_sap(sap_type);
			track.write_fragment(obj.data)?;
			let _ = track.update_jitter(meta.pts, Some(meta.duration));
		}
		Ok(())
	}

	fn start_non_switching_audio_groups(&mut self) -> anyhow::Result<()> {
		for track in &mut self.tracks {
			if track.kind == TrackKind::Audio
				&& track.alt_group.is_none()
				&& track.group.is_some()
				&& !track.explicit_groups
			{
				track.start_group(None)?;
			}
		}
		Ok(())
	}

	fn build_hang_catalog(&self) -> hang::Catalog {
		let mut video_renditions = BTreeMap::new();
		let mut audio_renditions = BTreeMap::new();

		for tp in &self.tracks {
			match &tp.config {
				CmsfTrackConfig::Video {
					codec,
					description,
					width,
					height,
					bitrate,
					framerate,
					init_data,
				} => {
					let init_bytes = base64::engine::general_purpose::STANDARD
						.decode(init_data)
						.unwrap_or_default();
					video_renditions.insert(
						tp.track.name.clone(),
						VideoConfig {
							codec: codec.clone(),
							description: description.clone(),
							coded_width: *width,
							coded_height: *height,
							bitrate: *bitrate,
							framerate: *framerate,
							container: Container::Cmaf {
								init: Bytes::from(init_bytes),
							},
							jitter: tp.jitter_as_time(),
							display_ratio_width: None,
							display_ratio_height: None,
							optimize_for_latency: None,
						},
					);
				}
				CmsfTrackConfig::Audio {
					codec,
					description,
					sample_rate,
					channel_count,
					bitrate,
					init_data,
				} => {
					let init_bytes = base64::engine::general_purpose::STANDARD
						.decode(init_data)
						.unwrap_or_default();
					audio_renditions.insert(
						tp.track.name.clone(),
						AudioConfig {
							codec: codec.clone(),
							description: description.clone(),
							sample_rate: *sample_rate,
							channel_count: *channel_count,
							bitrate: *bitrate,
							container: Container::Cmaf {
								init: Bytes::from(init_bytes),
							},
							jitter: tp.jitter_as_time(),
						},
					);
				}
			}
		}

		hang::Catalog {
			video: Video {
				renditions: video_renditions,
				..Default::default()
			},
			audio: Audio {
				renditions: audio_renditions,
			},
			..Default::default()
		}
	}

	fn publish_catalogs(&mut self) {
		if self.catalog_failed {
			return;
		}

		let msf = self.msf_catalog();
		let Ok(msf_json) = msf.to_string() else {
			tracing::warn!("failed to serialize MSF catalog");
			return;
		};
		let Ok(mut group) = self.msf_track.append_group() else {
			tracing::warn!("catalog track closed, stopping catalog updates");
			self.catalog_failed = true;
			return;
		};
		if group.write_frame(msf_json).is_err() {
			tracing::warn!("failed to write MSF catalog frame");
		}
		let _ = group.finish();

		// NB: can't use `if let Some(t) = &mut self.hang_track` because
		// build_hang_catalog() borrows self.tracks immutably.
		#[allow(clippy::unnecessary_unwrap)]
		if self.hang_track.is_some() {
			let hang = self.build_hang_catalog();
			let Ok(hang_json) = hang.to_string() else {
				tracing::warn!("failed to serialize hang catalog");
				return;
			};
			let hang_track = self.hang_track.as_mut().unwrap();
			let Ok(mut group) = hang_track.append_group() else {
				tracing::warn!("failed to create hang catalog group");
				return;
			};
			if group.write_frame(hang_json).is_err() {
				tracing::warn!("failed to write hang catalog frame");
			}
			let _ = group.finish();
		}
	}
}

#[cfg(test)]
mod test {
	use super::*;
	use hang::catalog::{AudioCodec, H264};

	fn make_producer(config: CmsfConfig) -> CmsfBroadcastProducer {
		let broadcast = moq_lite::Broadcast::new().produce();
		CmsfBroadcastProducer::new(broadcast, config).unwrap()
	}

	fn video_track() -> CmsfVideoTrack {
		CmsfVideoTrack {
			codec: H264 {
				profile: 100,
				constraints: 0,
				level: 31,
				inline: false,
			}
			.into(),
			description: None,
			width: Some(1280),
			height: Some(720),
			bitrate: Some(6_000_000),
			framerate: Some(30.0),
			init_segment: Bytes::from_static(b"video-init"),
			alt_group: Some(1),
			timescale: 90000,
		}
	}

	fn audio_track() -> CmsfAudioTrack {
		CmsfAudioTrack {
			codec: AudioCodec::Opus,
			description: None,
			sample_rate: 48000,
			channel_count: 2,
			bitrate: Some(128_000),
			init_segment: Bytes::from_static(b"audio-init"),
			alt_group: Some(2),
			timescale: 48000,
		}
	}

	// --- Constructor tests ---

	#[test]
	fn test_constructor_creates_msf_track() {
		let p = make_producer(CmsfConfig::default());
		assert_eq!(p.msf_track.name, "catalog");
	}

	#[test]
	fn test_constructor_hang_creates_both() {
		let config = CmsfConfig { publish_hang: true };
		let p = make_producer(config);
		assert!(p.hang_track.is_some());
		assert_eq!(p.hang_track.as_ref().unwrap().name, "catalog.json");
	}

	#[test]
	fn test_constructor_no_hang_by_default() {
		let p = make_producer(CmsfConfig::default());
		assert!(p.hang_track.is_none());
	}

	// --- Track addition tests ---

	#[test]
	fn test_add_video_track_sequential_ids() {
		let mut p = make_producer(CmsfConfig::default());
		let id0 = p.add_video_track(video_track()).unwrap();
		let id1 = p.add_video_track(video_track()).unwrap();
		assert_eq!(id0, TrackId(0));
		assert_eq!(id1, TrackId(1));
	}

	#[test]
	fn test_add_audio_track() {
		let mut p = make_producer(CmsfConfig::default());
		let id = p.add_audio_track(audio_track()).unwrap();
		assert_eq!(id, TrackId(0));
		assert_eq!(p.tracks[0].kind, TrackKind::Audio);
	}

	#[test]
	fn test_track_naming() {
		let mut p = make_producer(CmsfConfig::default());
		p.add_video_track(video_track()).unwrap();
		p.add_video_track(video_track()).unwrap();
		p.add_audio_track(audio_track()).unwrap();
		assert_eq!(p.tracks[0].track.name, "video0.m4s");
		assert_eq!(p.tracks[1].track.name, "video1.m4s");
		assert_eq!(p.tracks[2].track.name, "audio0.m4s");
	}

	#[test]
	fn test_add_video_base64_init_data() {
		let mut p = make_producer(CmsfConfig::default());
		p.add_video_track(video_track()).unwrap();
		match &p.tracks[0].config {
			CmsfTrackConfig::Video { init_data, .. } => {
				let decoded = base64::engine::general_purpose::STANDARD.decode(init_data).unwrap();
				assert_eq!(decoded, b"video-init");
			}
			_ => panic!("expected Video config"),
		}
	}

	// --- MSF catalog tests ---

	#[test]
	fn test_msf_catalog_empty() {
		let p = make_producer(CmsfConfig::default());
		let msf = p.msf_catalog();
		assert_eq!(msf.version, 1);
		assert!(msf.generated_at.is_some());
		assert!(msf.tracks.is_empty());
	}

	#[test]
	fn test_msf_catalog_video_track() {
		let mut p = make_producer(CmsfConfig::default());
		p.add_video_track(video_track()).unwrap();
		let msf = p.msf_catalog();

		assert_eq!(msf.tracks.len(), 1);
		let t = &msf.tracks[0];
		assert_eq!(t.name, "video0.m4s");
		assert_eq!(t.packaging, moq_msf::Packaging::Cmaf);
		assert!(t.is_live);
		assert_eq!(t.role, Some(moq_msf::Role::Video));
		assert_eq!(t.width, Some(1280));
		assert_eq!(t.height, Some(720));
		assert_eq!(t.framerate, Some(30.0));
		assert_eq!(t.bitrate, Some(6_000_000));
		assert_eq!(t.render_group, Some(1));
		assert_eq!(t.alt_group, Some(1));
		let decoded = base64::engine::general_purpose::STANDARD
			.decode(t.init_data.as_ref().unwrap())
			.unwrap();
		assert_eq!(decoded, b"video-init");
	}

	#[test]
	fn test_msf_catalog_audio_track() {
		let mut p = make_producer(CmsfConfig::default());
		p.add_audio_track(audio_track()).unwrap();
		let msf = p.msf_catalog();

		let t = &msf.tracks[0];
		assert_eq!(t.name, "audio0.m4s");
		assert_eq!(t.role, Some(moq_msf::Role::Audio));
		assert_eq!(t.samplerate, Some(48000));
		assert_eq!(t.channel_config, Some("2".into()));
		assert_eq!(t.bitrate, Some(128_000));
		assert_eq!(t.alt_group, Some(2));
	}

	#[test]
	fn test_msf_catalog_sap_before_chunks() {
		let mut p = make_producer(CmsfConfig::default());
		p.add_video_track(video_track()).unwrap();
		let msf = p.msf_catalog();
		let t = &msf.tracks[0];
		assert_eq!(t.max_grp_sap_starting_type, None);
		assert_eq!(t.max_obj_sap_starting_type, None);
	}

	#[test]
	fn test_msf_catalog_sap_after_chunks() {
		let mut p = make_producer(CmsfConfig::default());
		let id = p.add_video_track(video_track()).unwrap();
		p.write(id, cmsf_obj(0, 3003, KF_FLAGS, None)).unwrap();
		let msf = p.msf_catalog();
		let t = &msf.tracks[0];
		assert_eq!(t.max_grp_sap_starting_type, Some(1));
		assert_eq!(t.max_obj_sap_starting_type, Some(1));
	}

	// --- Finish test ---

	#[test]
	fn test_finish() {
		let mut p = make_producer(CmsfConfig::default());
		p.add_video_track(video_track()).unwrap();
		p.finish().unwrap();
	}

	// --- write() method tests ---

	use crate::cmsf::test_helpers::build_moof_mdat;

	const KF_FLAGS: u32 = 0x0200_0000;
	const NON_KF_FLAGS: u32 = 0x0001_0000;

	fn cmsf_obj(dts: u64, duration: u32, flags: u32, group_id: Option<u64>) -> CmsfObject {
		CmsfObject {
			data: Bytes::from(build_moof_mdat(dts, duration, flags)),
			group_id,
		}
	}

	#[test]
	fn test_write_keyframe_starts_group() {
		let mut p = make_producer(CmsfConfig::default());
		let id = p.add_video_track(video_track()).unwrap();
		p.write(id, cmsf_obj(0, 3003, KF_FLAGS, None)).unwrap();
		assert!(p.tracks[0].group.is_some());
	}

	#[test]
	fn test_write_non_keyframe_continues() {
		let mut p = make_producer(CmsfConfig::default());
		let id = p.add_video_track(video_track()).unwrap();
		p.write(id, cmsf_obj(0, 3003, KF_FLAGS, None)).unwrap();
		p.write(id, cmsf_obj(3003, 3003, NON_KF_FLAGS, None)).unwrap();
	}

	#[test]
	fn test_write_non_keyframe_first_fails() {
		let mut p = make_producer(CmsfConfig::default());
		let id = p.add_video_track(video_track()).unwrap();
		let err = p.write(id, cmsf_obj(0, 3003, NON_KF_FLAGS, None)).unwrap_err();
		assert!(err.to_string().contains("keyframe"));
	}

	#[test]
	fn test_write_audio_buffered_before_video_keyframe() {
		let mut p = make_producer(CmsfConfig::default());
		p.add_video_track(video_track()).unwrap();
		let aid = p
			.add_audio_track(CmsfAudioTrack {
				alt_group: None,
				..audio_track()
			})
			.unwrap();
		p.write(aid, cmsf_obj(0, 1024, KF_FLAGS, None)).unwrap();
		assert!(p.tracks[aid.0].group.is_none());
		assert_eq!(p.audio_buffer.len(), 1);
	}

	#[test]
	fn test_write_audio_flushed_on_video_keyframe() {
		let mut p = make_producer(CmsfConfig::default());
		let vid = p.add_video_track(video_track()).unwrap();
		let aid = p
			.add_audio_track(CmsfAudioTrack {
				alt_group: None,
				..audio_track()
			})
			.unwrap();
		p.write(aid, cmsf_obj(0, 1024, KF_FLAGS, None)).unwrap();
		p.write(vid, cmsf_obj(0, 3003, KF_FLAGS, None)).unwrap();
		assert!(p.audio_buffer.is_empty());
		assert!(p.tracks[aid.0].group.is_some());
	}

	#[test]
	fn test_write_explicit_audio_bypasses_buffer() {
		let mut p = make_producer(CmsfConfig::default());
		p.add_video_track(video_track()).unwrap();
		let aid = p
			.add_audio_track(CmsfAudioTrack {
				alt_group: None,
				..audio_track()
			})
			.unwrap();
		p.write(aid, cmsf_obj(0, 1024, KF_FLAGS, Some(1))).unwrap();
		assert!(p.audio_buffer.is_empty());
		assert!(p.tracks[aid.0].group.is_some());
		assert!(p.tracks[aid.0].explicit_groups);
	}

	#[test]
	fn test_write_explicit_audio_not_rotated_on_video_keyframe() {
		let mut p = make_producer(CmsfConfig::default());
		let vid = p.add_video_track(video_track()).unwrap();
		let aid = p
			.add_audio_track(CmsfAudioTrack {
				alt_group: None,
				..audio_track()
			})
			.unwrap();
		// Explicit audio write
		p.write(aid, cmsf_obj(0, 1024, KF_FLAGS, Some(1))).unwrap();
		// First video keyframe
		p.write(vid, cmsf_obj(0, 3003, KF_FLAGS, None)).unwrap();
		// Second video keyframe — should NOT rotate explicit audio
		p.write(vid, cmsf_obj(90000, 3003, KF_FLAGS, None)).unwrap();
		// Audio group_id should still be 1 (not auto-rotated)
		assert_eq!(p.tracks[aid.0].last_explicit_group_id, Some(1));
	}

	#[test]
	fn test_write_malformed_data_returns_error() {
		let mut p = make_producer(CmsfConfig::default());
		let id = p.add_video_track(video_track()).unwrap();
		let result = p.write(
			id,
			CmsfObject {
				data: Bytes::from_static(b"garbage"),
				group_id: None,
			},
		);
		assert!(result.is_err());
	}

	#[test]
	fn test_write_jitter_from_parsed_pts() {
		let mut p = make_producer(CmsfConfig::default());
		let id = p.add_video_track(video_track()).unwrap();
		p.write(id, cmsf_obj(0, 3003, KF_FLAGS, None)).unwrap();
		p.write(id, cmsf_obj(3003, 3003, NON_KF_FLAGS, None)).unwrap();
		p.write(id, cmsf_obj(6006, 3003, NON_KF_FLAGS, None)).unwrap();
		assert_eq!(p.tracks[0].jitter, Some(3003));
	}

	// --- Explicit group_id edge case tests ---

	#[test]
	fn test_write_sequential_explicit_groups() {
		let mut p = make_producer(CmsfConfig::default());
		let id = p.add_video_track(video_track()).unwrap();
		p.write(id, cmsf_obj(0, 3003, KF_FLAGS, Some(1))).unwrap();
		p.write(id, cmsf_obj(90000, 3003, KF_FLAGS, Some(2))).unwrap();
		assert_eq!(p.tracks[0].last_explicit_group_id, Some(2));
		assert!(p.tracks[0].explicit_groups);
	}

	#[test]
	fn test_write_same_group_id_no_new_group() {
		let mut p = make_producer(CmsfConfig::default());
		let id = p.add_video_track(video_track()).unwrap();
		p.write(id, cmsf_obj(0, 3003, KF_FLAGS, Some(1))).unwrap();
		// Same group_id, non-keyframe — continues current group
		p.write(id, cmsf_obj(3003, 3003, NON_KF_FLAGS, Some(1))).unwrap();
		assert_eq!(p.tracks[0].last_explicit_group_id, Some(1));
	}

	#[test]
	fn test_write_none_after_latch_continues_group() {
		let mut p = make_producer(CmsfConfig::default());
		let id = p.add_video_track(video_track()).unwrap();
		p.write(id, cmsf_obj(0, 3003, KF_FLAGS, Some(5))).unwrap();
		// None in explicit mode = continue current group
		p.write(id, cmsf_obj(3003, 3003, NON_KF_FLAGS, None)).unwrap();
		// Then a new explicit group
		p.write(id, cmsf_obj(90000, 3003, KF_FLAGS, Some(7))).unwrap();
		assert_eq!(p.tracks[0].last_explicit_group_id, Some(7));
	}

	#[test]
	fn test_write_non_monotonic_group_id_errors() {
		let mut p = make_producer(CmsfConfig::default());
		let id = p.add_video_track(video_track()).unwrap();
		p.write(id, cmsf_obj(0, 3003, KF_FLAGS, Some(5))).unwrap();
		let err = p.write(id, cmsf_obj(90000, 3003, KF_FLAGS, Some(3))).unwrap_err();
		assert!(err.to_string().contains("non-monotonic"));
	}
}
