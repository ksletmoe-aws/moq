use anyhow::Context;
use bytes::{Buf, Bytes, BytesMut};
use hang::catalog::{AAC, AV1, AudioCodec, H264, H265, VP9, VideoCodec};
use mp4_atom::{Atom, Moov, Trak};
use std::collections::HashMap;
use tokio::io::AsyncRead;

use super::cmsf_broadcast::CmsfBroadcastProducer;
use super::cmsf_types::{CmsfAudioTrack, CmsfConfig, CmsfObject, CmsfVideoTrack, TrackId};
use super::fmp4::build_aac_audio_specific_config;
use super::fmp4_parser::{Fmp4Parser, Fmp4Sink, SampleInfo, TrackKind, build_init_segment};

/// Bridges fMP4 input to CMSF broadcast output.
///
/// Parses fragmented MP4 via `Fmp4Parser`, auto-detects codecs, and feeds
/// `CmsfObject`s to a `CmsfBroadcastProducer`.
pub struct CmsfFmp4Importer {
	parser: Fmp4Parser<CmsfFmp4Consumer>,
}

struct CmsfFmp4Consumer {
	producer: CmsfBroadcastProducer,
	track_map: HashMap<u32, TrackId>,
}

impl Fmp4Sink for CmsfFmp4Consumer {
	fn on_init(&mut self, track_id: u32, kind: TrackKind, trak: &Trak, moov: &Moov) -> anyhow::Result<()> {
		let init_segment = build_init_segment(trak, moov)?;
		let timescale = trak.mdia.mdhd.timescale as u64;

		let id = match kind {
			TrackKind::Video => {
				let (mut track, _is_h264) = build_video_config(trak)?;
				track.init_segment = init_segment;
				track.timescale = timescale;
				self.producer.add_video_track(track)?
			}
			TrackKind::Audio => {
				let mut track = build_audio_config(trak)?;
				track.init_segment = init_segment;
				track.timescale = timescale;
				self.producer.add_audio_track(track)?
			}
		};

		self.track_map.insert(track_id, id);
		Ok(())
	}

	fn on_fragment(
		&mut self,
		track_id: u32,
		fragment: Bytes,
		_keyframe: bool,
		_samples: Vec<SampleInfo>,
	) -> anyhow::Result<()> {
		let &moq_track_id = self.track_map.get(&track_id).context("unknown track")?;
		self.producer.write(
			moq_track_id,
			CmsfObject {
				data: fragment,
				group_id: None,
			},
		)
	}
}

impl CmsfFmp4Importer {
	/// Create a new CMSF fMP4 importer.
	pub fn new(broadcast: moq_lite::BroadcastProducer, config: CmsfConfig) -> anyhow::Result<Self> {
		let producer = CmsfBroadcastProducer::new(broadcast, config)?;
		Ok(Self {
			parser: Fmp4Parser::new(CmsfFmp4Consumer {
				producer,
				track_map: HashMap::new(),
			}),
		})
	}

	pub async fn decode_from<T: AsyncRead + Unpin>(&mut self, reader: &mut T) -> anyhow::Result<()> {
		self.parser.decode_from(reader).await
	}

	pub fn decode<T: Buf + AsRef<[u8]>>(&mut self, buf: &mut T) -> anyhow::Result<()> {
		self.parser.decode(buf)
	}

	pub fn is_initialized(&self) -> bool {
		self.parser.is_initialized()
	}

	pub fn finish(&mut self) -> anyhow::Result<()> {
		self.parser.sink.producer.finish()
	}

	/// Get the current MSF catalog.
	pub fn msf_catalog(&self) -> moq_msf::Catalog {
		self.parser.sink.producer.msf_catalog()
	}
}

/// Build video config from an fMP4 track.
///
/// NOTE: All video tracks are assigned `alt_group: Some(1)` — this assumes a single
/// ABR ladder where all video renditions are switchable. Multi-angle or independent
/// video tracks are not supported via the fMP4 import path.
fn build_video_config(trak: &Trak) -> anyhow::Result<(CmsfVideoTrack, bool)> {
	let stsd = &trak.mdia.minf.stbl.stsd;
	let codec = match stsd.codecs.len() {
		0 => anyhow::bail!("missing codec"),
		1 => &stsd.codecs[0],
		_ => anyhow::bail!("multiple codecs"),
	};

	let (video_codec, description, width, height, is_h264) = match codec {
		mp4_atom::Codec::Avc1(avc1) => {
			let mut desc = BytesMut::new();
			avc1.avcc.encode_body(&mut desc)?;
			(
				H264 {
					profile: avc1.avcc.avc_profile_indication,
					constraints: avc1.avcc.profile_compatibility,
					level: avc1.avcc.avc_level_indication,
					inline: false,
				}
				.into(),
				Some(desc.freeze()),
				avc1.visual.width as u32,
				avc1.visual.height as u32,
				true,
			)
		}
		mp4_atom::Codec::Hev1(hev1) => {
			let (c, d, w, h) = build_h265(true, &hev1.hvcc, &hev1.visual)?;
			(c, Some(d), w, h, false)
		}
		mp4_atom::Codec::Hvc1(hvc1) => {
			let (c, d, w, h) = build_h265(false, &hvc1.hvcc, &hvc1.visual)?;
			(c, Some(d), w, h, false)
		}
		mp4_atom::Codec::Vp08(vp08) => (
			VideoCodec::VP8,
			None,
			vp08.visual.width as u32,
			vp08.visual.height as u32,
			false,
		),
		mp4_atom::Codec::Vp09(vp09) => {
			let vpcc = &vp09.vpcc;
			(
				VP9 {
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
				None,
				vp09.visual.width as u32,
				vp09.visual.height as u32,
				false,
			)
		}
		mp4_atom::Codec::Av01(av01) => {
			let av1c = &av01.av1c;
			(
				AV1 {
					profile: av1c.seq_profile,
					level: av1c.seq_level_idx_0,
					bitdepth: match (av1c.seq_tier_0, av1c.high_bitdepth) {
						(true, true) => 12,
						(true, false) | (false, true) => 10,
						(false, false) => 8,
					},
					mono_chrome: av1c.monochrome,
					chroma_subsampling_x: av1c.chroma_subsampling_x,
					chroma_subsampling_y: av1c.chroma_subsampling_y,
					chroma_sample_position: av1c.chroma_sample_position,
					..Default::default()
				}
				.into(),
				None,
				av01.visual.width as u32,
				av01.visual.height as u32,
				false,
			)
		}
		mp4_atom::Codec::Unknown(unknown) => anyhow::bail!("unknown codec: {:?}", unknown),
		unsupported => anyhow::bail!("unsupported codec: {:?}", unsupported),
	};

	Ok((
		CmsfVideoTrack {
			codec: video_codec,
			description,
			width: Some(width),
			height: Some(height),
			bitrate: None,
			framerate: None,
			init_segment: Bytes::new(),
			alt_group: Some(1),
			timescale: 0,
		},
		is_h264,
	))
}

fn build_h265(
	in_band: bool,
	hvcc: &mp4_atom::Hvcc,
	visual: &mp4_atom::Visual,
) -> anyhow::Result<(VideoCodec, Bytes, u32, u32)> {
	let mut desc = BytesMut::new();
	hvcc.encode_body(&mut desc)?;
	Ok((
		H265 {
			in_band,
			profile_space: hvcc.general_profile_space,
			profile_idc: hvcc.general_profile_idc,
			profile_compatibility_flags: hvcc.general_profile_compatibility_flags,
			tier_flag: hvcc.general_tier_flag,
			level_idc: hvcc.general_level_idc,
			constraint_flags: hvcc.general_constraint_indicator_flags,
		}
		.into(),
		desc.freeze(),
		visual.width as u32,
		visual.height as u32,
	))
}

/// Build audio config from an fMP4 track.
///
/// NOTE: All audio tracks are assigned `alt_group: Some(2)` — this makes them
/// "switching audio" in CMSF terms. Non-switching audio (aligned to video groups)
/// is not triggered via the fMP4 import path.
fn build_audio_config(trak: &Trak) -> anyhow::Result<CmsfAudioTrack> {
	let stsd = &trak.mdia.minf.stbl.stsd;
	let codec = match stsd.codecs.len() {
		0 => anyhow::bail!("missing codec"),
		1 => &stsd.codecs[0],
		_ => anyhow::bail!("multiple codecs"),
	};

	let track = match codec {
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
			CmsfAudioTrack {
				codec: AAC { profile }.into(),
				description: Some(description),
				sample_rate,
				channel_count,
				bitrate: Some(bitrate.into()),
				init_segment: Bytes::new(),
				alt_group: Some(2),
				timescale: 0,
			}
		}
		mp4_atom::Codec::Opus(opus) => CmsfAudioTrack {
			codec: AudioCodec::Opus,
			description: None,
			sample_rate: opus.audio.sample_rate.integer() as _,
			channel_count: opus.audio.channel_count as _,
			bitrate: None,
			init_segment: Bytes::new(),
			alt_group: Some(2),
			timescale: 0,
		},
		mp4_atom::Codec::Unknown(unknown) => anyhow::bail!("unknown codec: {:?}", unknown),
		unsupported => anyhow::bail!("unsupported codec: {:?}", unsupported),
	};

	Ok(track)
}

#[cfg(test)]
mod test {
	use super::*;
	use base64::Engine;

	fn run_cmsf_fmp4(data: &[u8]) -> CmsfFmp4Importer {
		let broadcast = moq_lite::Broadcast::new().produce();
		let mut importer = CmsfFmp4Importer::new(broadcast, CmsfConfig::default()).unwrap();
		let mut buf = bytes::BytesMut::from(data);
		let _ = importer.decode(&mut buf);
		importer
	}

	#[test]
	fn test_bbb_creates_tracks() {
		let data = include_bytes!("test/bbb.mp4");
		let importer = run_cmsf_fmp4(data);
		assert!(importer.is_initialized());
		assert_eq!(importer.parser.sink.track_map.len(), 2);
	}

	#[test]
	fn test_bbb_codec_detection() {
		let data = include_bytes!("test/bbb.mp4");
		let importer = run_cmsf_fmp4(data);
		let msf = importer.msf_catalog();

		let video = msf
			.tracks
			.iter()
			.find(|t| t.role == Some(moq_msf::Role::Video))
			.unwrap();
		assert!(
			video.codec.as_ref().unwrap().starts_with("avc1."),
			"got: {:?}",
			video.codec
		);

		let audio = msf
			.tracks
			.iter()
			.find(|t| t.role == Some(moq_msf::Role::Audio))
			.unwrap();
		assert!(
			audio.codec.as_ref().unwrap().starts_with("mp4a."),
			"got: {:?}",
			audio.codec
		);
	}

	#[test]
	fn test_bbb_alt_group_assignment() {
		let data = include_bytes!("test/bbb.mp4");
		let importer = run_cmsf_fmp4(data);
		let msf = importer.msf_catalog();

		let video = msf
			.tracks
			.iter()
			.find(|t| t.role == Some(moq_msf::Role::Video))
			.unwrap();
		assert_eq!(video.alt_group, Some(1));

		let audio = msf
			.tracks
			.iter()
			.find(|t| t.role == Some(moq_msf::Role::Audio))
			.unwrap();
		assert_eq!(audio.alt_group, Some(2));
	}

	#[test]
	fn test_bbb_init_segments_base64() {
		let data = include_bytes!("test/bbb.mp4");
		let importer = run_cmsf_fmp4(data);
		let msf = importer.msf_catalog();

		for track in &msf.tracks {
			let init_data = track.init_data.as_ref().expect("should have init_data");
			let decoded = base64::engine::general_purpose::STANDARD.decode(init_data).unwrap();
			assert!(!decoded.is_empty());
		}
	}

	#[test]
	fn test_bbb_sap_type1_for_h264() {
		let data = include_bytes!("test/bbb.mp4");
		let importer = run_cmsf_fmp4(data);
		let msf = importer.msf_catalog();

		let video = msf
			.tracks
			.iter()
			.find(|t| t.role == Some(moq_msf::Role::Video))
			.unwrap();
		assert_eq!(video.max_grp_sap_starting_type, Some(1));
		assert_eq!(video.max_obj_sap_starting_type, Some(1));
	}

	#[test]
	fn test_bbb_groups_created() {
		let data = include_bytes!("test/bbb.mp4");
		let importer = run_cmsf_fmp4(data);
		for tp in &importer.parser.sink.producer.tracks {
			assert!(tp.group.is_some(), "track should have an open group");
		}
	}

	#[test]
	fn test_finish() {
		let data = include_bytes!("test/bbb.mp4");
		let mut importer = run_cmsf_fmp4(data);
		importer.finish().unwrap();
	}
}
