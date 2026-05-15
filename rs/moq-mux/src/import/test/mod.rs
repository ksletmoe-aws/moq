use hang::catalog::Container;
use mp4_atom::{Decode, Encode};

fn run_fmp4(data: &[u8]) -> hang::Catalog {
	let mut broadcast = moq_lite::Broadcast::new().produce();
	let catalog = crate::catalog::Producer::new(&mut broadcast).unwrap();

	let mut fmp4 = super::Fmp4::new(broadcast, catalog.clone());

	let mut buf = bytes::BytesMut::from(data);
	// Ignore errors from incomplete/malformed trailing fragments in test files.
	let _ = fmp4.decode(&mut buf);

	catalog.snapshot()
}

fn decode_init(init: &[u8]) -> (mp4_atom::Ftyp, mp4_atom::Moov) {
	let mut cursor = std::io::Cursor::new(init);
	let ftyp = mp4_atom::Ftyp::decode(&mut cursor).expect("invalid ftyp");
	let moov = mp4_atom::Moov::decode(&mut cursor).expect("invalid moov");
	(ftyp, moov)
}

#[test]
fn test_bbb_catalog() {
	let data = include_bytes!("bbb.mp4");
	let catalog = run_fmp4(data);

	assert_eq!(catalog.video.renditions.len(), 1);
	assert_eq!(catalog.audio.renditions.len(), 1);

	let video = catalog.video.renditions.values().next().unwrap();
	assert_eq!(video.codec.to_string(), "avc1.64001f");
	assert_eq!(video.coded_width, Some(1280));
	assert_eq!(video.coded_height, Some(720));
	assert!(matches!(video.container, Container::Cmaf { .. }));

	let audio = catalog.audio.renditions.values().next().unwrap();
	assert_eq!(audio.codec.to_string(), "mp4a.40.2");
	assert_eq!(audio.sample_rate, 44100);
	assert_eq!(audio.channel_count, 2);
	assert!(matches!(audio.container, Container::Cmaf { .. }));
}

#[test]
fn test_bbb_init_roundtrip() {
	let data = include_bytes!("bbb.mp4");
	let catalog = run_fmp4(data);

	let video = catalog.video.renditions.values().next().unwrap();
	let Container::Cmaf { init } = &video.container else {
		panic!("expected Cmaf container");
	};
	let (ftyp, moov) = decode_init(init);
	assert_eq!(ftyp.major_brand, mp4_atom::FourCC::new(b"isom"));
	assert_eq!(moov.trak.len(), 1);
	assert_eq!(moov.trak[0].tkhd.track_id, 1);
	assert_eq!(moov.trak[0].mdia.mdhd.timescale, 24000);
	let mvex = moov.mvex.as_ref().unwrap();
	assert_eq!(mvex.trex.len(), 1);
	assert_eq!(mvex.trex[0].track_id, 1);

	// Verify it round-trips through encode/decode
	let mut buf = Vec::new();
	ftyp.encode(&mut buf).unwrap();
	moov.encode(&mut buf).unwrap();
	let (ftyp2, moov2) = decode_init(&buf);
	assert_eq!(ftyp2.major_brand, mp4_atom::FourCC::new(b"isom"));
	assert_eq!(moov2.trak.len(), 1);

	let audio = catalog.audio.renditions.values().next().unwrap();
	let Container::Cmaf { init } = &audio.container else {
		panic!("expected Cmaf container");
	};
	let (ftyp, moov) = decode_init(init);
	assert_eq!(ftyp.major_brand, mp4_atom::FourCC::new(b"isom"));
	assert_eq!(moov.trak.len(), 1);
	assert_eq!(moov.trak[0].tkhd.track_id, 2);
	assert_eq!(moov.trak[0].mdia.mdhd.timescale, 44100);
	let mvex = moov.mvex.as_ref().unwrap();
	assert_eq!(mvex.trex.len(), 1);
	assert_eq!(mvex.trex[0].track_id, 2);
}

#[test]
fn test_av1_catalog() {
	let data = include_bytes!("av1.mp4");
	let catalog = run_fmp4(data);

	assert_eq!(catalog.video.renditions.len(), 1);
	assert_eq!(catalog.audio.renditions.len(), 0);

	let video = catalog.video.renditions.values().next().unwrap();
	assert!(video.codec.to_string().starts_with("av01."), "codec: {}", video.codec);
	assert!(matches!(video.container, Container::Cmaf { .. }));

	let Container::Cmaf { init } = &video.container else {
		panic!("expected Cmaf container");
	};
	let (ftyp, moov) = decode_init(init);
	assert_eq!(ftyp.major_brand, mp4_atom::FourCC::new(b"isom"));
	assert_eq!(moov.trak.len(), 1);
	let mvex = moov.mvex.as_ref().unwrap();
	assert_eq!(mvex.trex.len(), 1);
	assert_eq!(mvex.trex[0].track_id, moov.trak[0].tkhd.track_id);
}

#[test]
fn test_vp9_catalog() {
	let data = include_bytes!("vp9.mp4");
	let catalog = run_fmp4(data);

	assert_eq!(catalog.video.renditions.len(), 1);
	assert_eq!(catalog.audio.renditions.len(), 0);

	let video = catalog.video.renditions.values().next().unwrap();
	assert!(video.codec.to_string().starts_with("vp09."), "codec: {}", video.codec);
	assert!(matches!(video.container, Container::Cmaf { .. }));

	let Container::Cmaf { init } = &video.container else {
		panic!("expected Cmaf container");
	};
	let (ftyp, moov) = decode_init(init);
	assert_eq!(ftyp.major_brand, mp4_atom::FourCC::new(b"isom"));
	assert_eq!(moov.trak.len(), 1);
	let mvex = moov.mvex.as_ref().unwrap();
	assert_eq!(mvex.trex.len(), 1);
	assert_eq!(mvex.trex[0].track_id, moov.trak[0].tkhd.track_id);
}

// --- Adaptation tests (Step 7) ---

#[test]
fn test_container_cmaf_serde_roundtrip() {
	let original = Container::Cmaf {
		init: bytes::Bytes::from_static(b"hello world init segment"),
	};

	let json = serde_json::to_string(&original).unwrap();
	assert!(json.contains("\"kind\":\"cmaf\""));
	assert!(json.contains("\"init\""));

	let deserialized: Container = serde_json::from_str(&json).unwrap();
	assert_eq!(deserialized, original);
}

#[tokio::test]
async fn test_cmsf_producer_consumer_roundtrip() {
	use crate::export::cmsf::CmsfBroadcastDemuxer;
	use crate::import::cmsf_fmp4::CmsfFmp4Importer;
	use crate::import::cmsf_types::CmsfConfig;

	let data = include_bytes!("bbb.mp4");

	// Producer side: feed bbb.mp4 through CMSF importer.
	let broadcast = moq_lite::Broadcast::new().produce();
	let consumer = broadcast.consume();

	let mut importer = CmsfFmp4Importer::new(broadcast, CmsfConfig::default()).unwrap();
	let mut buf = bytes::BytesMut::from(&data[..]);
	let _ = importer.decode(&mut buf);
	importer.finish().unwrap();

	// Consumer side: create demuxer from the broadcast consumer.
	let mut demuxer = CmsfBroadcastDemuxer::new(consumer).unwrap();

	// Wait for catalog to be received and tracks subscribed.
	demuxer.ready().await.unwrap();

	let tracks = demuxer.active_tracks();
	assert!(tracks.len() >= 2, "expected at least video + audio, got {:?}", tracks);

	// Read segments and verify metadata fidelity.
	let seg = demuxer.next().await.unwrap().expect("should get a segment");
	assert!(!seg.media_data.is_empty());
	assert!(seg.track_name.ends_with(".m4s"));
	assert!(seg.timescale > 0, "timescale should be non-zero");
	assert!(seg.duration > 0, "duration should be non-zero");

	// Verify the first segment of a group is a keyframe.
	if seg.group_id == 0 {
		assert!(seg.is_keyframe, "first segment in first group should be a keyframe");
	}

	// Verify PTS is parseable and consistent with parse_cmsf_metadata.
	let reparsed = crate::cmsf::parse_cmsf_metadata(&seg.media_data).unwrap();
	assert_eq!(seg.pts, reparsed.pts, "PTS should match re-parse");
	assert_eq!(seg.duration, reparsed.duration, "duration should match re-parse");
	assert_eq!(
		seg.is_keyframe, reparsed.is_keyframe,
		"keyframe flag should match re-parse"
	);

	// Read more segments to verify continuity.
	let mut count = 1;
	while let Some(seg) = demuxer.next().await.unwrap() {
		assert!(seg.timescale > 0);
		assert!(!seg.media_data.is_empty());
		count += 1;
		if count >= 10 {
			break;
		}
	}
	assert!(count >= 3, "expected at least 3 segments from bbb.mp4, got {count}");
}

#[tokio::test]
async fn test_cmsf_demuxer_skips_malformed_frames() {
	use crate::export::cmsf::CmsfTrackDemuxer;

	// Create a track with valid init data but feed it a malformed frame.
	let mut broadcast = moq_lite::Broadcast::new().produce();
	let mut track_producer = broadcast.create_track(moq_lite::Track::new("video0.m4s")).unwrap();
	let track_consumer = broadcast
		.consume()
		.subscribe_track(&moq_lite::Track::new("video0.m4s"))
		.unwrap();

	// Build a valid init segment for timescale.
	let init_data = crate::cmsf::test_helpers::build_init_segment(90000);

	let mut demuxer = CmsfTrackDemuxer::new(track_consumer, bytes::Bytes::from(init_data), 90000);

	// Write a valid keyframe followed by garbage.
	let valid = crate::cmsf::test_helpers::build_moof_mdat(0, 3003, 0x0200_0000);
	let mut group = track_producer.append_group().unwrap();
	group.write_frame(bytes::Bytes::from(valid)).unwrap();
	group.write_frame(bytes::Bytes::from_static(b"garbage")).unwrap();
	group.finish().unwrap();
	track_producer.finish().unwrap();

	// Should get the valid frame.
	let seg = demuxer.next().await.unwrap().expect("should get valid segment");
	assert_eq!(seg.pts, 0);
	assert!(seg.is_keyframe);

	// Malformed frame should be skipped, track ends.
	let end = demuxer.next().await.unwrap();
	assert!(end.is_none(), "should end after skipping malformed frame");
}

#[tokio::test]
async fn test_cmsf_demuxer_end_reason_track_finished() {
	use crate::export::cmsf::{CmsfTrackDemuxer, EndReason};

	let mut broadcast = moq_lite::Broadcast::new().produce();
	let mut track_producer = broadcast.create_track(moq_lite::Track::new("audio0.m4s")).unwrap();
	let track_consumer = broadcast
		.consume()
		.subscribe_track(&moq_lite::Track::new("audio0.m4s"))
		.unwrap();

	let init_data = crate::cmsf::test_helpers::build_init_segment(48000);
	let mut demuxer = CmsfTrackDemuxer::new(track_consumer, bytes::Bytes::from(init_data), 48000);

	// Write one frame then finish the track.
	let frame = crate::cmsf::test_helpers::build_moof_mdat(0, 1024, 0x0200_0000);
	let mut group = track_producer.append_group().unwrap();
	group.write_frame(bytes::Bytes::from(frame)).unwrap();
	group.finish().unwrap();
	track_producer.finish().unwrap();

	// Read the frame.
	let seg = demuxer.next().await.unwrap().expect("should get segment");
	assert_eq!(seg.pts, 0);
	assert_eq!(seg.timescale, 48000);

	// Track should end.
	let end = demuxer.next().await.unwrap();
	assert!(end.is_none());
	assert!(matches!(demuxer.end_reason(), Some(EndReason::TrackFinished)));
}
