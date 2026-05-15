use anyhow::Context;
use bytes::{Buf, Bytes, BytesMut};
use hang::catalog::{AAC, AV1, AudioCodec, AudioConfig, Container, H264, H265, VP9, VideoCodec, VideoConfig};
use hang::container::Timestamp;
use mp4_atom::{Atom, Moov, Trak};
use std::collections::HashMap;
use tokio::io::AsyncRead;

use super::fmp4_parser::{Fmp4Parser, Fmp4Sink, SampleInfo, TrackKind};

struct Fmp4Track {
	kind: TrackKind,

	track: moq_lite::TrackProducer,
	group: Option<moq_lite::GroupProducer>,

	// The minimum buffer required for the track.
	jitter: Option<Timestamp>,

	// The last timestamp seen for this track.
	last_timestamp: Option<Timestamp>,

	// The minimum duration between frames for this track.
	min_duration: Option<Timestamp>,
}

/// Internal sink state that handles track creation, group writing, and catalog management.
struct Fmp4Inner {
	broadcast: moq_lite::BroadcastProducer,
	catalog: crate::catalog::Producer,
	tracks: HashMap<u32, Fmp4Track>,
	moov: Option<Moov>,
}

impl Fmp4Sink for Fmp4Inner {
	fn on_init(&mut self, track_id: u32, kind: TrackKind, trak: &Trak, moov: &Moov) -> anyhow::Result<()> {
		// Store moov on first init call for container() building.
		if self.moov.is_none() {
			self.moov = Some(moov.clone());
		}

		let mut catalog = self.catalog.clone();
		let mut catalog = catalog.lock();

		let suffix = ".m4s";
		let track = self.broadcast.unique_track(suffix)?;

		match kind {
			TrackKind::Video => {
				let config = self.init_video(trak, moov)?;
				catalog.video.renditions.insert(track.name.clone(), config);
			}
			TrackKind::Audio => {
				let config = self.init_audio(trak, moov)?;
				catalog.audio.renditions.insert(track.name.clone(), config);
			}
		}

		self.tracks.insert(
			track_id,
			Fmp4Track {
				kind,
				track,
				group: None,
				jitter: None,
				last_timestamp: None,
				min_duration: None,
			},
		);

		Ok(())
	}

	fn on_fragment(
		&mut self,
		track_id: u32,
		fragment: Bytes,
		keyframe: bool,
		samples: Vec<SampleInfo>,
	) -> anyhow::Result<()> {
		let track = self.tracks.get_mut(&track_id).context("unknown track")?;

		// Compute jitter from sample timestamps.
		let mut min_timestamp = None;
		let mut max_timestamp = None;

		for sample in &samples {
			let timestamp = Timestamp::from_scale(sample.pts, sample.timescale)?;

			if timestamp >= max_timestamp.unwrap_or(Timestamp::ZERO) {
				max_timestamp = Some(timestamp);
			}
			if timestamp <= min_timestamp.unwrap_or(Timestamp::MAX) {
				min_timestamp = Some(timestamp);
			}

			if let Some(last_timestamp) = track.last_timestamp
				&& let Ok(duration) = timestamp.checked_sub(last_timestamp)
				&& duration < track.min_duration.unwrap_or(Timestamp::MAX)
			{
				track.min_duration = Some(duration);
			}

			track.last_timestamp = Some(timestamp);
		}

		// Write the fragment as a single MoQ frame.
		let mut g = if keyframe {
			if let Some(mut prev) = track.group.take() {
				prev.finish()?;
			}
			track.track.append_group()?
		} else {
			track.group.take().context("no keyframe at start")?
		};

		g.write_frame(fragment)?;
		track.group = Some(g);

		// Update jitter in catalog if improved.
		if let (Some(min), Some(max), Some(min_duration)) = (min_timestamp, max_timestamp, track.min_duration) {
			let jitter = max - min + min_duration;

			if jitter < track.jitter.unwrap_or(Timestamp::MAX) {
				track.jitter = Some(jitter);

				let mut catalog = self.catalog.lock();

				match track.kind {
					TrackKind::Video => {
						let config = catalog
							.video
							.renditions
							.get_mut(&track.track.name)
							.context("missing video config")?;
						config.jitter = Some(jitter.convert()?);
					}
					TrackKind::Audio => {
						let config = catalog
							.audio
							.renditions
							.get_mut(&track.track.name)
							.context("missing audio config")?;
						config.jitter = Some(jitter.convert()?);
					}
				}
			}
		}

		Ok(())
	}
}

impl Fmp4Inner {
	fn container(&self, trak: &Trak, moov: &Moov) -> anyhow::Result<Container> {
		let init = super::fmp4_parser::build_init_segment(trak, moov)?;
		Ok(Container::Cmaf { init })
	}

	fn init_video(&self, trak: &Trak, moov: &Moov) -> anyhow::Result<VideoConfig> {
		let container = self.container(trak, moov)?;
		let stsd = &trak.mdia.minf.stbl.stsd;

		let codec = match stsd.codecs.len() {
			0 => anyhow::bail!("missing codec"),
			1 => &stsd.codecs[0],
			_ => anyhow::bail!("multiple codecs"),
		};

		let config = match codec {
			mp4_atom::Codec::Avc1(avc1) => {
				let avcc = &avc1.avcc;

				let mut description = BytesMut::new();
				avcc.encode_body(&mut description)?;

				VideoConfig {
					coded_width: Some(avc1.visual.width as _),
					coded_height: Some(avc1.visual.height as _),
					codec: H264 {
						profile: avcc.avc_profile_indication,
						constraints: avcc.profile_compatibility,
						level: avcc.avc_level_indication,
						inline: false,
					}
					.into(),
					description: Some(description.freeze()),
					framerate: None,
					bitrate: None,
					display_ratio_width: None,
					display_ratio_height: None,
					optimize_for_latency: None,
					container,
					jitter: None,
				}
			}
			mp4_atom::Codec::Hev1(hev1) => self.init_h265(true, &hev1.hvcc, &hev1.visual, container)?,
			mp4_atom::Codec::Hvc1(hvc1) => self.init_h265(false, &hvc1.hvcc, &hvc1.visual, container)?,
			mp4_atom::Codec::Vp08(vp08) => VideoConfig {
				codec: VideoCodec::VP8,
				description: Default::default(),
				coded_width: Some(vp08.visual.width as _),
				coded_height: Some(vp08.visual.height as _),
				framerate: None,
				bitrate: None,
				display_ratio_width: None,
				display_ratio_height: None,
				optimize_for_latency: None,
				container,
				jitter: None,
			},
			mp4_atom::Codec::Vp09(vp09) => {
				let vpcc = &vp09.vpcc;

				VideoConfig {
					codec: VP9 {
						profile: vpcc.profile,
						level: vpcc.level,
						bit_depth: vpcc.bit_depth,
						color_primaries: vpcc.color_primaries,
						chroma_subsampling: vpcc.chroma_subsampling,
						transfer_characteristics: vpcc.transfer_characteristics,
						matrix_coefficients: vpcc.matrix_coefficients,
						full_range: vpcc.video_full_range_flag,
					}
					.into(),
					description: Default::default(),
					coded_width: Some(vp09.visual.width as _),
					coded_height: Some(vp09.visual.height as _),
					display_ratio_width: None,
					display_ratio_height: None,
					optimize_for_latency: None,
					bitrate: None,
					framerate: None,
					container,
					jitter: None,
				}
			}
			mp4_atom::Codec::Av01(av01) => {
				let av1c = &av01.av1c;

				VideoConfig {
					codec: AV1 {
						profile: av1c.seq_profile,
						level: av1c.seq_level_idx_0,
						bitdepth: match (av1c.seq_tier_0, av1c.high_bitdepth) {
							(true, true) => 12,
							(true, false) => 10,
							(false, true) => 10,
							(false, false) => 8,
						},
						mono_chrome: av1c.monochrome,
						chroma_subsampling_x: av1c.chroma_subsampling_x,
						chroma_subsampling_y: av1c.chroma_subsampling_y,
						chroma_sample_position: av1c.chroma_sample_position,
						..Default::default()
					}
					.into(),
					description: Default::default(),
					coded_width: Some(av01.visual.width as _),
					coded_height: Some(av01.visual.height as _),
					display_ratio_width: None,
					display_ratio_height: None,
					optimize_for_latency: None,
					bitrate: None,
					framerate: None,
					container,
					jitter: None,
				}
			}
			mp4_atom::Codec::Unknown(unknown) => anyhow::bail!("unknown codec: {:?}", unknown),
			unsupported => anyhow::bail!("unsupported codec: {:?}", unsupported),
		};

		Ok(config)
	}

	fn init_h265(
		&self,
		in_band: bool,
		hvcc: &mp4_atom::Hvcc,
		visual: &mp4_atom::Visual,
		container: Container,
	) -> anyhow::Result<VideoConfig> {
		let mut description = BytesMut::new();
		hvcc.encode_body(&mut description)?;

		Ok(VideoConfig {
			codec: H265 {
				in_band,
				profile_space: hvcc.general_profile_space,
				profile_idc: hvcc.general_profile_idc,
				profile_compatibility_flags: hvcc.general_profile_compatibility_flags,
				tier_flag: hvcc.general_tier_flag,
				level_idc: hvcc.general_level_idc,
				constraint_flags: hvcc.general_constraint_indicator_flags,
			}
			.into(),
			description: Some(description.freeze()),
			coded_width: Some(visual.width as _),
			coded_height: Some(visual.height as _),
			bitrate: None,
			framerate: None,
			display_ratio_width: None,
			display_ratio_height: None,
			optimize_for_latency: None,
			container,
			jitter: None,
		})
	}

	fn init_audio(&self, trak: &Trak, moov: &Moov) -> anyhow::Result<AudioConfig> {
		let container = self.container(trak, moov)?;
		let stsd = &trak.mdia.minf.stbl.stsd;

		let codec = match stsd.codecs.len() {
			0 => anyhow::bail!("missing codec"),
			1 => &stsd.codecs[0],
			_ => anyhow::bail!("multiple codecs"),
		};

		let config = match codec {
			mp4_atom::Codec::Mp4a(mp4a) => {
				let desc = &mp4a.esds.es_desc.dec_config;

				if desc.object_type_indication != 0x40 {
					anyhow::bail!("unsupported codec: MPEG2");
				}

				let bitrate = desc.avg_bitrate.max(desc.max_bitrate);
				let profile = desc.dec_specific.profile;
				let sample_rate = mp4a.audio.sample_rate.integer() as u32;
				let channel_count = mp4a.audio.channel_count as u32;

				let description = build_aac_audio_specific_config(profile, sample_rate, channel_count);

				AudioConfig {
					codec: AAC { profile }.into(),
					sample_rate,
					channel_count,
					bitrate: Some(bitrate.into()),
					description: Some(description),
					container,
					jitter: None,
				}
			}
			mp4_atom::Codec::Opus(opus) => AudioConfig {
				codec: AudioCodec::Opus,
				sample_rate: opus.audio.sample_rate.integer() as _,
				channel_count: opus.audio.channel_count as _,
				bitrate: None,
				description: None,
				container,
				jitter: None,
			},
			mp4_atom::Codec::Unknown(unknown) => anyhow::bail!("unknown codec: {:?}", unknown),
			unsupported => anyhow::bail!("unsupported codec: {:?}", unsupported),
		};

		Ok(config)
	}
}

/// Converts fMP4/CMAF files into MoQ broadcast streams using CMAF passthrough.
///
/// This struct processes fragmented MP4 (fMP4) files and transports complete
/// moof+mdat fragments directly as MoQ frames, preserving the CMAF container format.
///
/// ## Supported Codecs
///
/// **Video:**
/// - H.264 (AVC1)
/// - H.265 (HEVC/HEV1/HVC1)
/// - VP8
/// - VP9
/// - AV1
///
/// **Audio:**
/// - AAC (MP4A)
/// - Opus
pub struct Fmp4 {
	parser: Fmp4Parser<Fmp4Inner>,
}

impl Fmp4 {
	/// Create a new CMAF importer that will write to the given broadcast.
	///
	/// The broadcast will be populated with tracks as they're discovered in the fMP4 file.
	pub fn new(broadcast: moq_lite::BroadcastProducer, catalog: crate::catalog::Producer) -> Self {
		let inner = Fmp4Inner {
			broadcast,
			catalog,
			tracks: HashMap::default(),
			moov: None,
		};
		Self {
			parser: Fmp4Parser::new(inner),
		}
	}

	/// Decode from an asynchronous reader.
	pub async fn decode_from<T: AsyncRead + Unpin>(&mut self, reader: &mut T) -> anyhow::Result<()> {
		self.parser.decode_from(reader).await
	}

	/// Decode a buffer of bytes.
	pub fn decode<T: Buf + AsRef<[u8]>>(&mut self, buf: &mut T) -> anyhow::Result<()> {
		self.parser.decode(buf)
	}

	pub fn is_initialized(&self) -> bool {
		self.parser.is_initialized()
	}

	/// Finish all tracks, flushing current groups.
	pub fn finish(&mut self) -> anyhow::Result<()> {
		for track in self.parser.sink.tracks.values_mut() {
			if let Some(mut g) = track.group.take() {
				g.finish()?;
			}
			track.track.finish()?;
		}
		Ok(())
	}
}

impl Drop for Fmp4 {
	fn drop(&mut self) {
		let mut catalog = self.parser.sink.catalog.lock();

		for track in self.parser.sink.tracks.values() {
			match track.kind {
				TrackKind::Video => {
					catalog.video.renditions.remove(&track.track.name);
				}
				TrackKind::Audio => {
					catalog.audio.renditions.remove(&track.track.name);
				}
			}
		}
	}
}

/// Reconstruct the AudioSpecificConfig from parsed fields.
///
/// Layout (ISO 14496-3):
///   audioObjectType      (5 bits)  — the AAC profile (2 = AAC-LC)
///   samplingFreqIndex    (4 bits)  — index into the standard table, or 0xF
///   [samplingFrequency  (24 bits)] — only if index == 0xF
///   channelConfiguration (4 bits)
///
/// For standard sample rates this produces exactly 2 bytes (e.g. 0x12 0x10
/// for AAC-LC / 44100 Hz / stereo).
pub(crate) fn build_aac_audio_specific_config(profile: u8, sample_rate: u32, channels: u32) -> Bytes {
	// audioObjectType is a 5-bit field; mask to prevent shift overflow.
	let profile = profile & 0x1F;

	let freq_index: u8 = match sample_rate {
		96000 => 0,
		88200 => 1,
		64000 => 2,
		48000 => 3,
		44100 => 4,
		32000 => 5,
		24000 => 6,
		22050 => 7,
		16000 => 8,
		12000 => 9,
		11025 => 10,
		8000 => 11,
		7350 => 12,
		_ => 0xF,
	};

	if freq_index != 0xF {
		let b0 = (profile << 3) | (freq_index >> 1);
		let b1 = ((freq_index & 1) << 7) | ((channels as u8 & 0x0F) << 3);
		Bytes::from(vec![b0, b1])
	} else {
		let mut bits: u64 = 0;
		bits |= (profile as u64) << 35;
		bits |= 0xF_u64 << 31;
		bits |= (sample_rate as u64) << 7;
		bits |= ((channels as u64) & 0xF) << 3;
		let all = bits.to_be_bytes();
		Bytes::copy_from_slice(&all[3..8])
	}
}
