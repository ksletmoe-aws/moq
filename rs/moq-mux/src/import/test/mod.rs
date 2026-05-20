use hang::catalog::Container;
use mp4_atom::{Decode, Encode};

fn run_fmp4(data: &[u8]) -> hang::Catalog {
	let mut broadcast = moq_net::Broadcast::new().produce();
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

#[test]
fn test_start_group_without_explicit_mode_errors() {
	let mut broadcast = moq_net::Broadcast::new().produce();
	let catalog = crate::catalog::Producer::new(&mut broadcast).unwrap();
	let mut fmp4 = super::Fmp4::new(broadcast, catalog);

	let err = fmp4.start_group(5).expect_err("auto mode should reject start_group");
	assert!(
		err.to_string().contains("explicit group mode"),
		"unexpected error: {}",
		err
	);
}

#[test]
fn test_start_group_before_moov_errors() {
	let mut broadcast = moq_net::Broadcast::new().produce();
	let catalog = crate::catalog::Producer::new(&mut broadcast).unwrap();
	let mut fmp4 = super::Fmp4::new(broadcast, catalog).with_explicit_groups();

	let err = fmp4.start_group(5).expect_err("start_group before moov should error");
	assert!(err.to_string().contains("no tracks"), "unexpected error: {}", err);
}

#[test]
fn test_start_group_creates_groups_with_sequence() {
	let mut broadcast = moq_net::Broadcast::new().produce();
	let consumer = broadcast.consume();
	let catalog = crate::catalog::Producer::new(&mut broadcast).unwrap();
	let mut fmp4 = super::Fmp4::new(broadcast, catalog).with_explicit_groups();

	// Feed bbb.mp4 to populate tracks. ftyp+moov are parsed cleanly; the first
	// mdat then errors in explicit mode (no active group). The error is
	// expected and intentionally ignored. We only need the moov to have been
	// parsed for the rest of the test.
	let data = include_bytes!("bbb.mp4");
	let mut buf = bytes::BytesMut::from(&data[..]);
	let _ = fmp4.decode(&mut buf);
	assert!(fmp4.is_initialized(), "moov should be parsed");

	// The Fmp4 importer creates tracks with unique names "0.m4s" and "1.m4s"
	// (the catalog's two tracks already occupy other names).
	let video = consumer
		.subscribe_track(&moq_net::Track::new("0.m4s"))
		.expect("video track should exist");
	let audio = consumer
		.subscribe_track(&moq_net::Track::new("1.m4s"))
		.expect("audio track should exist");

	// No groups exist yet in explicit mode.
	assert_eq!(video.latest(), None);
	assert_eq!(audio.latest(), None);

	// Start a group with sequence 5 on all tracks.
	fmp4.start_group(5).expect("start_group should succeed");

	assert_eq!(video.latest(), Some(5), "video group should have sequence 5");
	assert_eq!(audio.latest(), Some(5), "audio group should have sequence 5");

	// A second start_group with a higher sequence finishes the previous group
	// and bumps the latest sequence.
	fmp4.start_group(9).expect("second start_group should succeed");
	assert_eq!(video.latest(), Some(9));
	assert_eq!(audio.latest(), Some(9));
}

/// E2E test: publish via the fMP4 importer, subscribe to the MSF catalog track,
/// and verify the resulting `hang::Catalog` matches what the hang catalog would
/// have produced.
///
/// `catalog::Producer` publishes both the hang (`catalog.json`) and MSF (`catalog`)
/// catalog tracks, so subscribing to the MSF one and decoding via `MsfConsumer`
/// exercises the full unified pipeline (hang -> MSF JSON on the wire -> hang).
#[tokio::test]
async fn test_msf_catalog_roundtrip() {
	let mut broadcast = moq_net::Broadcast::new().produce();
	// Take the consumer before adding tracks; subscribe_track is called after the
	// MSF catalog track has been created by `catalog::Producer::new`.
	let consumer = broadcast.consume();
	let catalog = crate::catalog::Producer::new(&mut broadcast).unwrap();
	let mut fmp4 = super::Fmp4::new(broadcast, catalog);

	let data = include_bytes!("bbb.mp4");
	let mut buf = bytes::BytesMut::from(&data[..]);
	// Trailing fragments may error out (e.g. partial mdat); ignore.
	let _ = fmp4.decode(&mut buf);

	let track = consumer
		.subscribe_track(&moq_net::Track::new(moq_msf::DEFAULT_NAME))
		.expect("MSF catalog track should exist");
	let mut msf = crate::catalog::MsfConsumer::new(track);

	let catalog = msf
		.next()
		.await
		.expect("MSF catalog should decode")
		.expect("MSF catalog should be present");

	// Same expectations as `test_bbb_catalog`, ensuring hang -> MSF -> hang preserves
	// codec, geometry, and CMAF init data.
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

/// E2E test for explicit group mode: feed init, call `start_group(42)`, feed a
/// fragment, and verify the resulting group on the consumer side actually carries
/// sequence 42 and contains a frame.
///
/// Complements `test_start_group_creates_groups_with_sequence`, which only checks
/// `latest()` after `start_group` but never feeds a fragment into the resulting
/// group. Here we walk the wire-side group via `next_group` and confirm the
/// fragment landed in the explicitly-numbered group.
#[tokio::test]
async fn test_explicit_group_fragment_lands_in_group() {
	let mut broadcast = moq_net::Broadcast::new().produce();
	let consumer = broadcast.consume();
	let catalog = crate::catalog::Producer::new(&mut broadcast).unwrap();
	let mut fmp4 = super::Fmp4::new(broadcast, catalog).with_explicit_groups();

	// Split bbb.mp4 at the moov boundary so the importer can parse the init
	// segment first and the caller can call `start_group` before any mdat
	// arrives. Without this split the first fragment would error in explicit
	// mode (no active group yet).
	let data = include_bytes!("bbb.mp4");
	let init_end = {
		let mut cursor = std::io::Cursor::new(&data[..]);
		mp4_atom::Ftyp::decode(&mut cursor).expect("invalid ftyp");
		mp4_atom::Moov::decode(&mut cursor).expect("invalid moov");
		cursor.position() as usize
	};

	let mut init_buf = bytes::BytesMut::from(&data[..init_end]);
	fmp4.decode(&mut init_buf).expect("init segment should decode");
	assert!(fmp4.is_initialized(), "moov should be parsed");

	fmp4.start_group(42).expect("start_group should succeed");

	let mut frag_buf = bytes::BytesMut::from(&data[init_end..]);
	// Trailing data may be a partial fragment; ignore the error.
	let _ = fmp4.decode(&mut frag_buf);

	// The video track in bbb.mp4 is published as "0.m4s" by the importer.
	let mut video = consumer
		.subscribe_track(&moq_net::Track::new("0.m4s"))
		.expect("video track should exist");

	assert_eq!(video.latest(), Some(42), "explicit group sequence should be 42");

	let mut group = video
		.next_group()
		.await
		.expect("next_group should not error")
		.expect("a group should be available");
	assert_eq!(group.sequence, 42, "delivered group should have sequence 42");

	let frame = group
		.read_frame()
		.await
		.expect("read_frame should not error")
		.expect("group should contain at least one fragment");
	assert!(!frame.is_empty(), "fragment payload should be non-empty");
}
