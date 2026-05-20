use std::{str::FromStr, sync::Arc};

use bytes::{Buf, Bytes};
use moq_mux::import;

use crate::{Error, Id, NonZeroSlab};

/// Configuration for a track created via the slim CMAF passthrough API.
///
/// The optional fields mirror the nullable pointers in [`crate::moq_track_config`]
/// but live entirely on the Rust side so the C layer can validate inputs before
/// they cross the FFI boundary.
#[derive(Debug, Clone)]
pub struct TrackConfig {
	pub name: String,
	pub codec: String,
	pub init_data: Option<Bytes>,
	pub width: Option<u32>,
	pub height: Option<u32>,
	pub framerate: Option<f64>,
	pub sample_rate: Option<u32>,
	pub channel_count: Option<u32>,
	pub bitrate: Option<u64>,
}

/// A track created via the slim API ([`Publish::track_create`]).
///
/// Holds the [`moq_net::TrackProducer`] plus the currently open group for
/// broadcast aligned (Mode B) writes. Per track groups (Mode A) live in
/// [`Publish::groups`] instead.
struct SlimTrack {
	/// The broadcast this track belongs to. Used by
	/// [`Publish::broadcast_start_group`] to enumerate sibling tracks.
	broadcast_id: Id,
	producer: moq_net::TrackProducer,
	/// The currently active group for Mode B writes. `None` until the first
	/// call to [`Publish::broadcast_start_group`] for this broadcast.
	group: Option<moq_net::GroupProducer>,
	/// The group mode this track has committed to, set on the first
	/// [`Publish::broadcast_start_group`] or [`Publish::group_open`] call.
	/// Once set, the other mode is rejected with [`Error::ModeConflict`].
	mode: Option<GroupMode>,
}

/// Which group control flow a [`SlimTrack`] is using.
///
/// A track must commit to one mode for its lifetime. Mixing the two modes
/// on the same track produces ambiguous group sequencing on the wire.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum GroupMode {
	/// Broadcast level aligned groups driven by [`Publish::broadcast_start_group`].
	Broadcast,
	/// Per track groups driven by [`Publish::group_open`].
	PerTrack,
}

/// A Mode A group opened via [`Publish::group_open`].
///
/// Tracks the owning track so the group can be reaped when the track or
/// its broadcast is closed without an explicit [`Publish::group_close`].
struct SlimGroup {
	producer: moq_net::GroupProducer,
	track_id: Id,
}

#[derive(Default)]
pub struct Publish {
	/// Active broadcast producers for publishing.
	broadcasts: NonZeroSlab<(moq_net::BroadcastProducer, moq_mux::catalog::Producer)>,

	/// Active media encoders/decoders for publishing.
	media: NonZeroSlab<import::Framed>,

	/// Tracks created via the slim API ([`Publish::track_create`]).
	tracks: NonZeroSlab<SlimTrack>,

	/// Groups opened via per track mode ([`Publish::group_open`]).
	groups: NonZeroSlab<SlimGroup>,
}

/// Determine whether a WebCodecs codec string is video. Returns `Some(true)`
/// for video, `Some(false)` for audio, and `None` for unknown codec strings
/// (the caller can then fall back to inspecting the rest of the config).
fn classify_codec(codec: &str) -> Option<bool> {
	if codec.starts_with("avc1.")
		|| codec.starts_with("avc3.")
		|| codec.starts_with("hvc1.")
		|| codec.starts_with("hev1.")
		|| codec == "vp8"
		|| codec.starts_with("vp09.")
		|| codec.starts_with("av01.")
	{
		Some(true)
	} else if codec.starts_with("mp4a.40.") || codec == "opus" {
		Some(false)
	} else {
		None
	}
}

impl Publish {
	pub fn create(&mut self) -> Result<Id, Error> {
		let mut broadcast = moq_net::Broadcast::new().produce();
		let catalog = moq_mux::catalog::Producer::new(&mut broadcast)?;

		let id = self.broadcasts.insert((broadcast, catalog))?;
		Ok(id)
	}

	pub fn get(&self, id: Id) -> Result<&moq_net::BroadcastProducer, Error> {
		self.broadcasts
			.get(id)
			.ok_or(Error::BroadcastNotFound)
			.map(|(broadcast, _)| broadcast)
	}

	pub fn close(&mut self, broadcast: Id) -> Result<(), Error> {
		self.broadcasts.remove(broadcast).ok_or(Error::BroadcastNotFound)?;
		// Collect track ids belonging to this broadcast so we can reap any
		// Mode A groups that were opened against them. We must do this before
		// removing the tracks, since the groups only know their track id.
		let track_ids: Vec<Id> = self
			.tracks
			.iter()
			.filter(|(_, track)| track.broadcast_id == broadcast)
			.map(|(id, _)| id)
			.collect();
		// Drain orphaned Mode A groups whose owning track is going away.
		self.groups.retain(|_, group| !track_ids.contains(&group.track_id));
		// Then drain the tracks themselves. Mode B groups (held inside
		// `SlimTrack::group`) are dropped along with the track.
		self.tracks.retain(|_, track| track.broadcast_id != broadcast);
		Ok(())
	}

	pub fn media_ordered(&mut self, broadcast: Id, format: &str, mut init: &[u8]) -> Result<Id, Error> {
		let (broadcast, catalog) = self.broadcasts.get(broadcast).ok_or(Error::BroadcastNotFound)?;

		let format = import::FramedFormat::from_str(format).map_err(|_| Error::UnknownFormat(format.to_string()))?;
		let decoder = import::Framed::new(broadcast.clone(), catalog.clone(), format, &mut init)
			.map_err(|err| Error::InitFailed(Arc::new(err)))?;

		let id = self.media.insert(decoder)?;
		Ok(id)
	}

	pub fn media_frame(
		&mut self,
		media: Id,
		mut data: &[u8],
		timestamp: hang::container::Timestamp,
	) -> Result<(), Error> {
		let media = self.media.get_mut(media).ok_or(Error::MediaNotFound)?;

		media
			.decode_frame(&mut data, Some(timestamp))
			.map_err(|err| Error::DecodeFailed(Arc::new(err)))?;

		if data.has_remaining() {
			return Err(Error::DecodeFailed(Arc::new(anyhow::anyhow!(
				"buffer was not fully consumed"
			))));
		}

		Ok(())
	}

	pub fn media_close(&mut self, media: Id) -> Result<(), Error> {
		let mut decoder = self.media.remove(media).ok_or(Error::MediaNotFound)?;
		decoder.finish().map_err(|err| Error::DecodeFailed(Arc::new(err)))?;
		Ok(())
	}

	/// Create a track on the given broadcast and update the catalog.
	///
	/// The codec string is parsed to determine whether the track is video or
	/// audio. The init segment, if provided, is stored on the catalog as a
	/// CMAF container so consumers can decode the fragments without inspecting
	/// the wire data.
	pub fn track_create(&mut self, broadcast_id: Id, config: &TrackConfig) -> Result<Id, Error> {
		use hang::catalog::{AudioCodec, AudioConfig, Container, VideoCodec, VideoConfig};

		// Prefer the codec string for classification. For unrecognized codecs
		// (e.g. "flac", "ac-3", future formats) fall back to inspecting which
		// pointer fields the caller populated: `width`/`height` indicate video,
		// `sample_rate`/`channel_count` indicate audio.
		let is_video = match classify_codec(&config.codec) {
			Some(v) => v,
			None => {
				let looks_video = config.width.is_some() || config.height.is_some();
				let looks_audio = config.sample_rate.is_some() || config.channel_count.is_some();
				match (looks_video, looks_audio) {
					(true, false) => true,
					(false, true) => false,
					_ => return Err(Error::UnknownFormat(config.codec.clone())),
				}
			}
		};

		// Validate audio params and parse the codec up front so we don't create
		// a track producer that has to be torn down on a downstream error.
		let container = match config.init_data.as_ref() {
			Some(init) if !init.is_empty() => Container::Cmaf { init: init.clone() },
			_ => Container::Legacy,
		};

		enum Rendition {
			Video(VideoConfig),
			Audio(AudioConfig),
		}

		let rendition = if is_video {
			let codec = VideoCodec::from_str(&config.codec)?;
			Rendition::Video(VideoConfig {
				codec,
				description: None,
				coded_width: config.width,
				coded_height: config.height,
				display_ratio_width: None,
				display_ratio_height: None,
				bitrate: config.bitrate,
				framerate: config.framerate,
				optimize_for_latency: None,
				container,
				jitter: None,
			})
		} else {
			let sample_rate = config.sample_rate.unwrap_or(0);
			let channel_count = config.channel_count.unwrap_or(0);
			if sample_rate == 0 || channel_count == 0 {
				return Err(Error::InitFailed(Arc::new(anyhow::anyhow!(
					"audio track requires non-zero sample_rate and channel_count"
				))));
			}
			let codec = AudioCodec::from_str(&config.codec)?;
			Rendition::Audio(AudioConfig {
				codec,
				sample_rate,
				channel_count,
				bitrate: config.bitrate,
				description: None,
				container,
				jitter: None,
			})
		};

		// All validation has succeeded. Now create the track producer, then
		// update the catalog, then record the SlimTrack.
		let (broadcast, catalog) = self.broadcasts.get_mut(broadcast_id).ok_or(Error::BroadcastNotFound)?;
		let producer = broadcast.create_track(moq_net::Track::new(&config.name))?;

		{
			let mut guard = catalog.lock();
			match rendition {
				Rendition::Video(cfg) => {
					guard.video.renditions.insert(config.name.clone(), cfg);
				}
				Rendition::Audio(cfg) => {
					guard.audio.renditions.insert(config.name.clone(), cfg);
				}
			}
		}

		let id = self.tracks.insert(SlimTrack {
			broadcast_id,
			producer,
			group: None,
			mode: None,
		})?;
		Ok(id)
	}

	/// Start a new group on every slim track in the broadcast with the same
	/// sequence number, finishing any previously open group first.
	///
	/// This is Mode B: the caller drives synchronization across tracks by
	/// asserting that each `sequence` boundary is aligned. Tracks that were
	/// not created via [`Self::track_create`] are unaffected.
	///
	/// # Errors
	/// Returns [`Error::ModeConflict`] if any track in the broadcast has
	/// already been used with [`Self::group_open`] (Mode A). This is checked
	/// before any group is created so the broadcast is not mutated on
	/// rejection.
	///
	/// On other failures (e.g. duplicate sequence or closed track), the
	/// broadcast is left in a partially advanced state. Some tracks may have
	/// new groups while others do not. The caller should treat the broadcast
	/// as unusable after such an error and close it.
	pub fn broadcast_start_group(&mut self, broadcast_id: Id, sequence: u64) -> Result<(), Error> {
		// Verify the broadcast exists. The lookup borrow is released before we
		// iterate `self.tracks`.
		self.broadcasts.get(broadcast_id).ok_or(Error::BroadcastNotFound)?;

		// Pre-validate: reject if any track in this broadcast has committed to
		// per track mode. This must be a separate pass so we don't half advance
		// the broadcast before discovering the conflict.
		for (_, track) in self.tracks.iter() {
			if track.broadcast_id == broadcast_id && track.mode == Some(GroupMode::PerTrack) {
				return Err(Error::ModeConflict);
			}
		}

		for track in self.tracks.values_mut() {
			if track.broadcast_id != broadcast_id {
				continue;
			}
			if let Some(mut prev) = track.group.take() {
				prev.finish()?;
			}
			let group = track.producer.create_group(moq_net::Group { sequence })?;
			track.group = Some(group);
			track.mode = Some(GroupMode::Broadcast);
		}

		Ok(())
	}

	/// Append a frame to the current Mode B group of the given track.
	///
	/// `broadcast_id` is required for symmetry with [`Self::broadcast_start_group`]
	/// and to defend against the caller passing a track from a different
	/// broadcast.
	pub fn broadcast_write(&mut self, broadcast_id: Id, track_id: Id, data: &[u8]) -> Result<(), Error> {
		let track = self.tracks.get_mut(track_id).ok_or(Error::TrackNotFound)?;
		if track.broadcast_id != broadcast_id {
			return Err(Error::TrackNotFound);
		}
		let group = track.group.as_mut().ok_or(Error::NoActiveGroup)?;
		group.write_frame(Bytes::copy_from_slice(data))?;
		Ok(())
	}

	/// Close a slim track. Auto finishes the current Mode B group (if any)
	/// and reaps any Mode A groups still open against this track.
	pub fn track_close(&mut self, broadcast_id: Id, track_id: Id) -> Result<(), Error> {
		let track = self.tracks.get(track_id).ok_or(Error::TrackNotFound)?;
		if track.broadcast_id != broadcast_id {
			return Err(Error::TrackNotFound);
		}
		let mut track = self.tracks.remove(track_id).expect("checked above");
		if let Some(mut group) = track.group.take() {
			group.finish()?;
		}
		// Drain orphaned Mode A groups belonging to this track. Their
		// producers are dropped without an explicit `finish`, mirroring the
		// behavior when their owning broadcast is closed.
		self.groups.retain(|_, group| group.track_id != track_id);
		track.producer.finish()?;
		Ok(())
	}

	/// Open a group on a slim track (Mode A: per track group control).
	///
	/// # Errors
	/// Returns [`Error::ModeConflict`] if the track has already been used with
	/// [`Self::broadcast_start_group`] (Mode B).
	pub fn group_open(&mut self, track_id: Id, sequence: u64) -> Result<Id, Error> {
		let track = self.tracks.get_mut(track_id).ok_or(Error::TrackNotFound)?;
		if track.mode == Some(GroupMode::Broadcast) {
			return Err(Error::ModeConflict);
		}
		let producer = track.producer.create_group(moq_net::Group { sequence })?;
		track.mode = Some(GroupMode::PerTrack);
		let id = self.groups.insert(SlimGroup { producer, track_id })?;
		Ok(id)
	}

	/// Append a frame to a Mode A group.
	pub fn group_write(&mut self, group_id: Id, data: &[u8]) -> Result<(), Error> {
		let slim_group = self.groups.get_mut(group_id).ok_or(Error::GroupNotFound)?;
		slim_group.producer.write_frame(Bytes::copy_from_slice(data))?;
		Ok(())
	}

	/// Finish a Mode A group and free its handle.
	pub fn group_close(&mut self, group_id: Id) -> Result<(), Error> {
		let mut slim_group = self.groups.remove(group_id).ok_or(Error::GroupNotFound)?;
		slim_group.producer.finish()?;
		Ok(())
	}
}
