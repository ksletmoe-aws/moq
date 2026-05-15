use anyhow::Context;
use bytes::Bytes;

use super::fmp4_parser::TrackKind;

/// Stream Access Point type per ISO 14496-12.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum SapType {
	/// Not a random access point.
	None,
	/// Type 1: IDR / clean random access.
	Type1,
	/// Type 2: CRA / open-GOP.
	Type2,
	/// Type 3: Gradual decoding refresh.
	#[allow(dead_code)]
	Type3,
}

impl SapType {
	pub fn is_keyframe(&self) -> bool {
		matches!(self, SapType::Type1 | SapType::Type2)
	}

	pub fn is_valid_group_start(&self) -> bool {
		matches!(self, SapType::Type1 | SapType::Type2)
	}

	/// Convert to the MSF catalog integer representation.
	pub fn to_msf_value(self) -> Option<u32> {
		match self {
			SapType::None => Option::None,
			SapType::Type1 => Some(1),
			SapType::Type2 => Some(2),
			SapType::Type3 => Some(3),
		}
	}
}

/// A CMSF Object — the moof+mdat payload written to a moq-lite group.
///
/// Corresponds to "Object" in CMSF spec §3.3. This is the simplified input
/// to [`CmsfBroadcastProducer::write()`](super::CmsfBroadcastProducer::write). The producer internally parses
/// metadata (PTS, duration, keyframe) from the moof box.
pub struct CmsfObject {
	/// Raw moof+mdat bytes, passed through unchanged to the moq-lite group.
	pub data: Bytes,

	/// Optional group assignment. Monotonically increasing when present.
	///
	/// - `Some(id)`: Producer starts a new moq-lite group when this value changes.
	///   The caller is asserting canonical group boundaries (e.g. from EML).
	///   Applies to ALL track types including audio — the caller is responsible
	///   for coordinating audio/video group boundaries.
	///   Once a track receives `Some(_)`, it enters "explicit mode" for the session.
	///   Sending a value less than the previous `Some` is an error (non-monotonic).
	///
	/// - `None`: Behavior depends on track mode:
	///   - If the track has never received `Some(_)`: Producer auto-assigns groups
	///     based on parsed keyframes. Video: new group on each keyframe.
	///     Non-switching audio: new group aligned to video keyframes.
	///   - If the track is in explicit mode (previously received `Some(_)`):
	///     Continues writing to the current group without starting a new one.
	pub group_id: Option<u64>,
}

/// Lightweight handle returned by `add_video_track`/`add_audio_track`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TrackId(pub usize);

/// Internal per-track producer managing group lifecycle, SAP tracking, and jitter.
pub(crate) struct CmsfTrackProducer {
	pub kind: TrackKind,
	pub track: moq_lite::TrackProducer,
	pub group: Option<moq_lite::GroupProducer>,
	pub alt_group: Option<u32>,
	pub media_group_id: Option<u64>,
	pub max_grp_sap: SapType,
	pub max_obj_sap: SapType,
	pub jitter: Option<u64>,
	pub config: CmsfTrackConfig,
	/// Whether this track uses explicit group IDs from the caller.
	/// Set to true on first `write()` with `group_id: Some(_)`.
	pub explicit_groups: bool,
	/// Last explicit group_id received, for monotonicity checking.
	pub last_explicit_group_id: Option<u64>,
	timescale: u64,
	last_timestamp: Option<u64>,
	sap_dirty: bool,
}

impl CmsfTrackProducer {
	pub fn new(
		kind: TrackKind,
		track: moq_lite::TrackProducer,
		alt_group: Option<u32>,
		config: CmsfTrackConfig,
		timescale: u64,
	) -> Self {
		Self {
			kind,
			track,
			group: None,
			alt_group,
			media_group_id: None,
			max_grp_sap: SapType::None,
			max_obj_sap: SapType::None,
			jitter: None,
			config,
			explicit_groups: false,
			last_explicit_group_id: None,
			timescale,
			last_timestamp: None,
			sap_dirty: false,
		}
	}

	pub fn start_group(&mut self, media_group_id: Option<u64>) -> anyhow::Result<()> {
		if let Some(mut prev) = self.group.take() {
			prev.finish()?;
		}
		self.group = Some(match media_group_id {
			Some(seq) => self.track.create_group(moq_lite::Group { sequence: seq })?,
			None => self.track.append_group()?,
		});
		self.media_group_id = media_group_id;
		Ok(())
	}

	pub fn write_fragment(&mut self, data: Bytes) -> anyhow::Result<()> {
		let group = self.group.as_mut().context("no open group")?;
		group.write_frame(data)?;
		Ok(())
	}

	pub fn validate_group_sap(&mut self, sap_type: SapType) -> anyhow::Result<()> {
		anyhow::ensure!(
			sap_type.is_valid_group_start(),
			"group starts with {:?}, §3.4 requires type 1 or 2",
			sap_type
		);
		let prev = self.max_grp_sap;
		self.max_grp_sap = self.max_grp_sap.max(sap_type);
		if self.max_grp_sap != prev {
			self.sap_dirty = true;
		}
		Ok(())
	}

	pub fn track_object_sap(&mut self, sap_type: SapType) {
		let prev = self.max_obj_sap;
		self.max_obj_sap = self.max_obj_sap.max(sap_type);
		if self.max_obj_sap != prev {
			self.sap_dirty = true;
		}
	}

	/// Update jitter tracking from a new PTS value.
	///
	/// Jitter is the minimum inter-sample PTS delta observed. This represents the
	/// smallest GOP/frame duration, which is what subscribers need to size their
	/// decode buffers. Published in the catalog as milliseconds.
	pub fn update_jitter(&mut self, pts: u64, _duration: Option<u64>) -> bool {
		if let Some(last) = self.last_timestamp
			&& let Some(d) = pts.checked_sub(last)
			&& d > 0 && d < self.jitter.unwrap_or(u64::MAX)
		{
			self.jitter = Some(d);
			self.last_timestamp = Some(pts);
			return true;
		}
		// Only advance last_timestamp for monotonically increasing PTS
		// to avoid corrupting jitter with B-frame reordering.
		if self.last_timestamp.is_none() || pts > self.last_timestamp.unwrap() {
			self.last_timestamp = Some(pts);
		}
		false
	}

	/// Convert jitter from native ticks to milliseconds for catalog publishing.
	pub fn jitter_as_time(&self) -> Option<moq_lite::Time> {
		let j = self.jitter?;
		if self.timescale == 0 {
			return None;
		}
		let ms = (j as u128 * 1000 / self.timescale as u128) as u64;
		moq_lite::Time::new_u64(ms).ok()
	}

	/// Derive the SAP type for a keyframe based on the track's codec.
	///
	/// - Audio → Type1 (always independently decodable)
	/// - Video H264/VP8 → Type1 (IDR only, no open-GOP)
	/// - Video H265/AV1/VP9/Unknown → Type2 (may be CRA/open-GOP, conservative)
	pub fn sap_type_for_keyframe(&self) -> SapType {
		match self.kind {
			TrackKind::Audio => SapType::Type1,
			TrackKind::Video => match &self.config {
				CmsfTrackConfig::Video {
					codec: hang::catalog::VideoCodec::H264(_) | hang::catalog::VideoCodec::VP8,
					..
				} => SapType::Type1,
				_ => SapType::Type2,
			},
		}
	}

	pub fn finish(&mut self) -> anyhow::Result<()> {
		if let Some(mut g) = self.group.take() {
			g.finish()?;
		}
		self.track.finish()?;
		Ok(())
	}

	pub fn sap_changed(&mut self) -> bool {
		let changed = self.sap_dirty;
		self.sap_dirty = false;
		changed
	}
}

/// Configuration for the CMSF broadcast producer.
#[derive(Default)]
pub struct CmsfConfig {
	pub publish_hang: bool,
}

/// Video track input for `CmsfBroadcastProducer::add_video_track`.
pub struct CmsfVideoTrack {
	pub codec: hang::catalog::VideoCodec,
	pub description: Option<Bytes>,
	pub width: Option<u32>,
	pub height: Option<u32>,
	pub bitrate: Option<u64>,
	pub framerate: Option<f64>,
	pub init_segment: Bytes,
	pub alt_group: Option<u32>,
	pub timescale: u64,
}

/// Audio track input for `CmsfBroadcastProducer::add_audio_track`.
pub struct CmsfAudioTrack {
	pub codec: hang::catalog::AudioCodec,
	pub description: Option<Bytes>,
	pub sample_rate: u32,
	pub channel_count: u32,
	pub bitrate: Option<u64>,
	pub init_segment: Bytes,
	pub alt_group: Option<u32>,
	pub timescale: u64,
}

/// Stored track config for building catalogs.
pub(crate) enum CmsfTrackConfig {
	Video {
		codec: hang::catalog::VideoCodec,
		description: Option<Bytes>,
		width: Option<u32>,
		height: Option<u32>,
		bitrate: Option<u64>,
		framerate: Option<f64>,
		init_data: String,
	},
	Audio {
		codec: hang::catalog::AudioCodec,
		description: Option<Bytes>,
		sample_rate: u32,
		channel_count: u32,
		bitrate: Option<u64>,
		init_data: String,
	},
}

#[cfg(test)]
mod test {
	use super::*;
	use std::collections::HashMap;

	#[test]
	fn test_sap_type_ordering() {
		assert!(SapType::None < SapType::Type1);
		assert!(SapType::Type1 < SapType::Type2);
		assert!(SapType::Type2 < SapType::Type3);
	}

	#[test]
	fn test_sap_type_is_keyframe() {
		assert!(!SapType::None.is_keyframe());
		assert!(SapType::Type1.is_keyframe());
		assert!(SapType::Type2.is_keyframe());
		assert!(!SapType::Type3.is_keyframe());
	}

	#[test]
	fn test_sap_type_is_valid_group_start() {
		assert!(!SapType::None.is_valid_group_start());
		assert!(SapType::Type1.is_valid_group_start());
		assert!(SapType::Type2.is_valid_group_start());
		assert!(!SapType::Type3.is_valid_group_start());
	}

	#[test]
	fn test_sap_type_monotonic_max() {
		let mut max = SapType::None;
		max = max.max(SapType::Type1);
		assert_eq!(max, SapType::Type1);
		max = max.max(SapType::Type1);
		assert_eq!(max, SapType::Type1);
		max = max.max(SapType::Type2);
		assert_eq!(max, SapType::Type2);
		max = max.max(SapType::None);
		assert_eq!(max, SapType::Type2);
	}

	#[test]
	fn test_sap_type_to_msf_value() {
		assert_eq!(SapType::None.to_msf_value(), Option::None);
		assert_eq!(SapType::Type1.to_msf_value(), Some(1));
		assert_eq!(SapType::Type2.to_msf_value(), Some(2));
		assert_eq!(SapType::Type3.to_msf_value(), Some(3));
	}

	#[test]
	fn test_cmsf_object_construction() {
		let obj = CmsfObject {
			data: Bytes::from_static(b"test"),
			group_id: Some(42),
		};
		assert_eq!(obj.data, Bytes::from_static(b"test"));
		assert_eq!(obj.group_id, Some(42));
	}

	#[test]
	fn test_track_id_equality_and_hashing() {
		let a = TrackId(0);
		let b = TrackId(0);
		let c = TrackId(1);
		assert_eq!(a, b);
		assert_ne!(a, c);

		let mut map = HashMap::new();
		map.insert(a, "first");
		assert_eq!(map.get(&b), Some(&"first"));
		assert_eq!(map.get(&c), None);
	}

	fn make_track_producer(kind: TrackKind) -> CmsfTrackProducer {
		make_track_producer_with_timescale(kind, 90000)
	}

	fn make_track_producer_with_timescale(kind: TrackKind, timescale: u64) -> CmsfTrackProducer {
		let mut broadcast = moq_lite::Broadcast::new().produce();
		let track = broadcast.create_track(moq_lite::Track::new("test")).unwrap();
		let config = CmsfTrackConfig::Video {
			codec: hang::catalog::H264 {
				profile: 100,
				constraints: 0,
				level: 31,
				inline: false,
			}
			.into(),
			description: None,
			width: None,
			height: None,
			bitrate: None,
			framerate: None,
			init_data: String::new(),
		};
		CmsfTrackProducer::new(kind, track, None, config, timescale)
	}

	#[test]
	fn test_track_producer_start_group() {
		let mut tp = make_track_producer(TrackKind::Video);
		assert!(tp.group.is_none());
		tp.start_group(Some(0)).unwrap();
		assert!(tp.group.is_some());
		assert_eq!(tp.media_group_id, Some(0));
	}

	#[test]
	fn test_track_producer_start_group_finishes_previous() {
		let mut tp = make_track_producer(TrackKind::Video);
		tp.start_group(Some(0)).unwrap();
		tp.write_fragment(Bytes::from_static(b"frame1")).unwrap();
		tp.start_group(Some(1)).unwrap();
		assert_eq!(tp.media_group_id, Some(1));
	}

	#[test]
	fn test_track_producer_write_fragment() {
		let mut tp = make_track_producer(TrackKind::Video);
		tp.start_group(None).unwrap();
		tp.write_fragment(Bytes::from_static(b"data")).unwrap();
	}

	#[test]
	fn test_track_producer_write_fragment_no_group() {
		let mut tp = make_track_producer(TrackKind::Video);
		let err = tp.write_fragment(Bytes::from_static(b"data")).unwrap_err();
		assert!(err.to_string().contains("no open group"));
	}

	#[test]
	fn test_track_producer_validate_group_sap_accepts() {
		let mut tp = make_track_producer(TrackKind::Video);
		tp.validate_group_sap(SapType::Type1).unwrap();
		assert_eq!(tp.max_grp_sap, SapType::Type1);
		tp.validate_group_sap(SapType::Type2).unwrap();
		assert_eq!(tp.max_grp_sap, SapType::Type2);
	}

	#[test]
	fn test_track_producer_validate_group_sap_rejects() {
		let mut tp = make_track_producer(TrackKind::Video);
		let err = tp.validate_group_sap(SapType::None).unwrap_err();
		assert!(err.to_string().contains("§3.4"));
		let err = tp.validate_group_sap(SapType::Type3).unwrap_err();
		assert!(err.to_string().contains("§3.4"));
	}

	#[test]
	fn test_track_producer_sap_monotonic() {
		let mut tp = make_track_producer(TrackKind::Video);
		tp.validate_group_sap(SapType::Type1).unwrap();
		tp.validate_group_sap(SapType::Type2).unwrap();
		tp.validate_group_sap(SapType::Type1).unwrap();
		assert_eq!(tp.max_grp_sap, SapType::Type2);
	}

	#[test]
	fn test_track_producer_object_sap() {
		let mut tp = make_track_producer(TrackKind::Video);
		tp.track_object_sap(SapType::Type1);
		assert_eq!(tp.max_obj_sap, SapType::Type1);
		tp.track_object_sap(SapType::Type2);
		assert_eq!(tp.max_obj_sap, SapType::Type2);
		tp.track_object_sap(SapType::None);
		assert_eq!(tp.max_obj_sap, SapType::Type2);
	}

	#[test]
	fn test_track_producer_sap_changed() {
		let mut tp = make_track_producer(TrackKind::Video);
		assert!(!tp.sap_changed());
		tp.validate_group_sap(SapType::Type1).unwrap();
		assert!(tp.sap_changed());
		assert!(!tp.sap_changed());
		tp.track_object_sap(SapType::Type2);
		assert!(tp.sap_changed());
		assert!(!tp.sap_changed());
	}

	#[test]
	fn test_track_producer_update_jitter() {
		// 90kHz timescale, 29.97fps → 3003 ticks per frame
		let mut tp = make_track_producer(TrackKind::Video);
		assert!(!tp.update_jitter(0, None)); // first PTS, no previous
		assert!(tp.update_jitter(3003, None)); // first jitter computed
		assert_eq!(tp.jitter, Some(3003));
		assert!(!tp.update_jitter(6006, None)); // same interval, no change
		assert_eq!(tp.jitter, Some(3003));
	}

	#[test]
	fn test_jitter_as_time_90khz() {
		let mut tp = make_track_producer_with_timescale(TrackKind::Video, 90000);
		tp.update_jitter(0, None);
		tp.update_jitter(3003, None);
		// 3003 * 1000 / 90000 = 33ms
		let time = tp.jitter_as_time().unwrap();
		assert_eq!(time, moq_lite::Time::new(33));
	}

	#[test]
	fn test_jitter_as_time_48khz() {
		let mut tp = make_track_producer_with_timescale(TrackKind::Audio, 48000);
		tp.update_jitter(0, None);
		tp.update_jitter(1024, None);
		// 1024 * 1000 / 48000 = 21ms
		let time = tp.jitter_as_time().unwrap();
		assert_eq!(time, moq_lite::Time::new(21));
	}

	#[test]
	fn test_jitter_as_time_none_when_no_jitter() {
		let tp = make_track_producer(TrackKind::Video);
		assert_eq!(tp.jitter_as_time(), None);
	}

	#[test]
	fn test_track_producer_finish() {
		let mut tp = make_track_producer(TrackKind::Video);
		tp.start_group(None).unwrap();
		tp.write_fragment(Bytes::from_static(b"data")).unwrap();
		tp.finish().unwrap();
		assert!(tp.group.is_none());
	}

	#[test]
	fn test_cmsf_config_default() {
		let config = CmsfConfig::default();
		assert!(!config.publish_hang);
	}

	#[test]
	fn test_precision_1000_frames_29_97fps() {
		// 29.97fps at 90kHz: frame duration = 3003 ticks (exact integer)
		let mut tp = make_track_producer_with_timescale(TrackKind::Video, 90000);
		for i in 0..1000u64 {
			tp.update_jitter(i * 3003, None);
		}
		// Jitter should be exactly 3003 — no accumulated rounding error
		assert_eq!(tp.jitter, Some(3003));
		assert_eq!(tp.jitter_as_time(), Some(moq_lite::Time::new(33)));
	}

	#[test]
	fn test_precision_48khz_audio() {
		// 48kHz audio with 1024-sample frames
		let mut tp = make_track_producer_with_timescale(TrackKind::Audio, 48000);
		for i in 0..500u64 {
			tp.update_jitter(i * 1024, None);
		}
		assert_eq!(tp.jitter, Some(1024));
		assert_eq!(tp.jitter_as_time(), Some(moq_lite::Time::new(21)));
	}

	#[test]
	fn test_multi_sample_duration() {
		// 4 samples at 3003-tick intervals: PTS 0, 3003, 6006, 9009
		// Duration = last.pts - first.pts = 9009 - 0 = 9009
		let first_pts: u64 = 0;
		let last_pts: u64 = 9009;
		let duration = last_pts.checked_sub(first_pts);
		assert_eq!(duration, Some(9009));
	}

	#[test]
	fn test_jitter_catalog_output() {
		// Verify the full path: native ticks → jitter_as_time() → moq_lite::Time
		let mut tp = make_track_producer_with_timescale(TrackKind::Video, 90000);
		tp.update_jitter(0, None);
		tp.update_jitter(3003, None);
		tp.update_jitter(6006, None);

		let time = tp.jitter_as_time().unwrap();
		// 3003 * 1000 / 90000 = 33
		assert_eq!(time, moq_lite::Time::new(33));

		// Verify this is the exact value that would be written to the hang catalog
		let expected_catalog_jitter: Option<moq_lite::Time> = Some(moq_lite::Time::new(33));
		assert_eq!(tp.jitter_as_time(), expected_catalog_jitter);
	}

	// --- sap_type_for_keyframe tests ---

	fn make_video_producer_with_codec(codec: hang::catalog::VideoCodec) -> CmsfTrackProducer {
		let mut broadcast = moq_lite::Broadcast::new().produce();
		let track = broadcast.create_track(moq_lite::Track::new("test")).unwrap();
		let config = CmsfTrackConfig::Video {
			codec,
			description: None,
			width: None,
			height: None,
			bitrate: None,
			framerate: None,
			init_data: String::new(),
		};
		CmsfTrackProducer::new(TrackKind::Video, track, None, config, 90000)
	}

	#[test]
	fn test_sap_type_audio_always_type1() {
		let tp = make_track_producer(TrackKind::Audio);
		assert_eq!(tp.sap_type_for_keyframe(), SapType::Type1);
	}

	#[test]
	fn test_sap_type_h264_type1() {
		let codec = hang::catalog::H264 {
			profile: 100,
			constraints: 0,
			level: 31,
			inline: false,
		};
		let tp = make_video_producer_with_codec(codec.into());
		assert_eq!(tp.sap_type_for_keyframe(), SapType::Type1);
	}

	#[test]
	fn test_sap_type_vp8_type1() {
		let tp = make_video_producer_with_codec(hang::catalog::VideoCodec::VP8);
		assert_eq!(tp.sap_type_for_keyframe(), SapType::Type1);
	}

	#[test]
	fn test_sap_type_h265_type2() {
		let codec = hang::catalog::H265 {
			in_band: false,
			profile_space: 0,
			profile_idc: 1,
			profile_compatibility_flags: [0x60, 0, 0, 0],
			tier_flag: false,
			level_idc: 120,
			constraint_flags: [0xB0, 0, 0, 0, 0, 0],
		};
		let tp = make_video_producer_with_codec(codec.into());
		assert_eq!(tp.sap_type_for_keyframe(), SapType::Type2);
	}

	#[test]
	fn test_sap_type_av1_type2() {
		let codec = hang::catalog::AV1 {
			profile: 0,
			level: 8,
			bitdepth: 8,
			..Default::default()
		};
		let tp = make_video_producer_with_codec(codec.into());
		assert_eq!(tp.sap_type_for_keyframe(), SapType::Type2);
	}

	#[test]
	fn test_sap_type_vp9_type2() {
		let codec = hang::catalog::VP9 {
			profile: 0,
			level: 31,
			bit_depth: 8,
			chroma_subsampling: 1,
			color_primaries: 1,
			transfer_characteristics: 1,
			matrix_coefficients: 1,
			full_range: false,
		};
		let tp = make_video_producer_with_codec(codec.into());
		assert_eq!(tp.sap_type_for_keyframe(), SapType::Type2);
	}

	#[test]
	fn test_sap_type_unknown_type2() {
		let tp = make_video_producer_with_codec(hang::catalog::VideoCodec::Unknown("custom.codec".into()));
		assert_eq!(tp.sap_type_for_keyframe(), SapType::Type2);
	}
}
