//! Integration test: verify that announcing a broadcast and subscribing to a
//! track works end-to-end for every supported protocol version.
//!
//! The server publishes a broadcast containing a track with known data.
//! The client connects, receives the announcement, subscribes to the track,
//! and verifies it receives the correct payload.
//!
//! This covers raw QUIC (moqt://) and WebTransport (https://) transports,
//! exercising every protocol version the library supports.

use moq_native::moq_net::{self, Origin, Track};
use std::time::Duration;

const TIMEOUT: Duration = Duration::from_secs(10);

/// Publish a broadcast on the server, subscribe on the client, and verify
/// the data arrives correctly for the given URL scheme and version configuration.
///
/// `client_version` and `server_version` can differ to test version negotiation.
/// `None` means "support all versions" (empty version vec).
async fn broadcast_test(scheme: &str, client_version: Option<&str>, server_version: Option<&str>) {
	let client_version: Option<moq_net::Version> = client_version.map(|v| v.parse().expect("invalid client version"));
	let server_version: Option<moq_net::Version> = server_version.map(|v| v.parse().expect("invalid server version"));

	// ── publisher (server) ──────────────────────────────────────────
	let pub_origin = Origin::random().produce();
	let mut broadcast = pub_origin.create_broadcast("test").expect("failed to create broadcast");
	let mut track = broadcast
		.create_track(Track::new("video"))
		.expect("failed to create track");

	// Write a group containing a single frame.
	let mut group = track.append_group().expect("failed to append group");
	group.write_frame(b"hello".as_ref()).expect("failed to write frame");
	group.finish().expect("failed to finish group");

	let mut server_config = moq_native::ServerConfig::default();
	server_config.bind = Some("[::]:0".to_string());
	server_config.tls.generate = vec!["localhost".into()];
	if let Some(v) = server_version {
		server_config.version = vec![v];
	}

	let mut server = server_config.init().expect("failed to init server");
	let addr = server.local_addr().expect("failed to get local addr");

	// ── subscriber (client) ─────────────────────────────────────────
	let sub_origin = Origin::random().produce();
	let mut announcements = sub_origin.consume();

	let mut client_config = moq_native::ClientConfig::default();
	client_config.tls.disable_verify = Some(true);
	if let Some(v) = client_version {
		client_config.version = vec![v];
	}

	let client = client_config.init().expect("failed to init client");
	let url: url::Url = format!("{scheme}://localhost:{}", addr.port()).parse().unwrap();

	// ── run server and client concurrently ──────────────────────────
	let server_handle = tokio::spawn(async move {
		let request = server.accept().await.expect("no incoming connection");
		let session = request.with_publish(pub_origin.consume()).ok().await?;

		// Keep producers alive so the subscriber can read data.
		let _broadcast = broadcast;
		let _track = track;

		// Block until the client disconnects.
		let _ = session.closed().await;
		Ok::<_, anyhow::Error>(())
	});

	let client = client.with_consume(sub_origin);
	let session = tokio::time::timeout(TIMEOUT, client.connect(url))
		.await
		.expect("client connect timed out")
		.expect("client connect failed");

	// Wait for the broadcast announcement.
	let (path, bc) = tokio::time::timeout(TIMEOUT, announcements.announced())
		.await
		.expect("announce timed out")
		.expect("origin closed");

	assert_eq!(path.as_str(), "test");
	let bc = bc.expect("expected announce, got unannounce");

	// Subscribe to the track.
	let mut track_sub = bc
		.subscribe_track(&Track::new("video"))
		.expect("subscribe_track failed");

	// Read one group.
	let mut group_sub = tokio::time::timeout(TIMEOUT, track_sub.recv_group())
		.await
		.expect("recv_group timed out")
		.expect("recv_group failed")
		.expect("track closed prematurely");

	// Read one frame and verify the payload.
	let frame = tokio::time::timeout(TIMEOUT, group_sub.read_frame())
		.await
		.expect("read_frame timed out")
		.expect("read_frame failed")
		.expect("group closed prematurely");

	assert_eq!(&*frame, b"hello");

	// Tear down: dropping the session closes the QUIC connection.
	drop(session);
	server_handle
		.await
		.expect("server task panicked")
		.expect("server task failed");
}

// ── Raw QUIC (moqt://) – same version on both sides ─────────────────

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_moq_lite_01() {
	broadcast_test("moqt", Some("moq-lite-01"), Some("moq-lite-01")).await;
}

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_moq_lite_02() {
	broadcast_test("moqt", Some("moq-lite-02"), Some("moq-lite-02")).await;
}

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_moq_lite_03() {
	broadcast_test("moqt", Some("moq-lite-03"), Some("moq-lite-03")).await;
}

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_moq_transport_14() {
	broadcast_test("moqt", Some("moq-transport-14"), Some("moq-transport-14")).await;
}

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_moq_transport_15() {
	broadcast_test("moqt", Some("moq-transport-15"), Some("moq-transport-15")).await;
}

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_moq_transport_16() {
	broadcast_test("moqt", Some("moq-transport-16"), Some("moq-transport-16")).await;
}

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_moq_transport_17() {
	broadcast_test("moqt", Some("moq-transport-17"), Some("moq-transport-17")).await;
}

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_moq_transport_18() {
	broadcast_test("moqt", Some("moq-transport-18"), Some("moq-transport-18")).await;
}

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_moq_transport_19() {
	broadcast_test("moqt", Some("moq-transport-19"), Some("moq-transport-19")).await;
}

// ── Raw QUIC – server supports all versions, client pins one ─────────

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_negotiate_server_all_client_lite_01() {
	broadcast_test("moqt", Some("moq-lite-01"), None).await;
}

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_negotiate_server_all_client_lite_02() {
	broadcast_test("moqt", Some("moq-lite-02"), None).await;
}

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_negotiate_server_all_client_lite_03() {
	broadcast_test("moqt", Some("moq-lite-03"), None).await;
}

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_negotiate_server_all_client_transport_14() {
	broadcast_test("moqt", Some("moq-transport-14"), None).await;
}

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_negotiate_server_all_client_transport_15() {
	broadcast_test("moqt", Some("moq-transport-15"), None).await;
}

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_negotiate_server_all_client_transport_16() {
	broadcast_test("moqt", Some("moq-transport-16"), None).await;
}

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_negotiate_server_all_client_transport_17() {
	broadcast_test("moqt", Some("moq-transport-17"), None).await;
}

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_negotiate_server_all_client_transport_18() {
	broadcast_test("moqt", Some("moq-transport-18"), None).await;
}

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_negotiate_server_all_client_transport_19() {
	broadcast_test("moqt", Some("moq-transport-19"), None).await;
}

// ── Raw QUIC – client supports all versions, server pins one ─────────

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_negotiate_client_all_server_lite_01() {
	broadcast_test("moqt", None, Some("moq-lite-01")).await;
}

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_negotiate_client_all_server_lite_02() {
	broadcast_test("moqt", None, Some("moq-lite-02")).await;
}

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_negotiate_client_all_server_lite_03() {
	broadcast_test("moqt", None, Some("moq-lite-03")).await;
}

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_negotiate_client_all_server_transport_14() {
	broadcast_test("moqt", None, Some("moq-transport-14")).await;
}

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_negotiate_client_all_server_transport_15() {
	broadcast_test("moqt", None, Some("moq-transport-15")).await;
}

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_negotiate_client_all_server_transport_16() {
	broadcast_test("moqt", None, Some("moq-transport-16")).await;
}

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_negotiate_client_all_server_transport_17() {
	broadcast_test("moqt", None, Some("moq-transport-17")).await;
}

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_negotiate_client_all_server_transport_18() {
	broadcast_test("moqt", None, Some("moq-transport-18")).await;
}

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_negotiate_client_all_server_transport_19() {
	broadcast_test("moqt", None, Some("moq-transport-19")).await;
}

// ── WebTransport (https://) – same version on both sides ────────────

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_webtransport() {
	broadcast_test("https", None, None).await;
}

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_webtransport_moq_lite_01() {
	broadcast_test("https", Some("moq-lite-01"), Some("moq-lite-01")).await;
}

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_webtransport_moq_lite_02() {
	broadcast_test("https", Some("moq-lite-02"), Some("moq-lite-02")).await;
}

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_webtransport_moq_lite_03() {
	broadcast_test("https", Some("moq-lite-03"), Some("moq-lite-03")).await;
}

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_webtransport_moq_transport_14() {
	broadcast_test("https", Some("moq-transport-14"), Some("moq-transport-14")).await;
}

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_webtransport_moq_transport_15() {
	broadcast_test("https", Some("moq-transport-15"), Some("moq-transport-15")).await;
}

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_webtransport_moq_transport_16() {
	broadcast_test("https", Some("moq-transport-16"), Some("moq-transport-16")).await;
}

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_webtransport_moq_transport_17() {
	broadcast_test("https", Some("moq-transport-17"), Some("moq-transport-17")).await;
}

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_webtransport_moq_transport_18() {
	broadcast_test("https", Some("moq-transport-18"), Some("moq-transport-18")).await;
}

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_webtransport_moq_transport_19() {
	broadcast_test("https", Some("moq-transport-19"), Some("moq-transport-19")).await;
}

// ── WebTransport – server supports all, client pins one ─────────────

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_webtransport_negotiate_server_all_client_lite_01() {
	broadcast_test("https", Some("moq-lite-01"), None).await;
}

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_webtransport_negotiate_server_all_client_lite_02() {
	broadcast_test("https", Some("moq-lite-02"), None).await;
}

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_webtransport_negotiate_server_all_client_lite_03() {
	broadcast_test("https", Some("moq-lite-03"), None).await;
}

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_webtransport_negotiate_server_all_client_transport_14() {
	broadcast_test("https", Some("moq-transport-14"), None).await;
}

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_webtransport_negotiate_server_all_client_transport_15() {
	broadcast_test("https", Some("moq-transport-15"), None).await;
}

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_webtransport_negotiate_server_all_client_transport_16() {
	broadcast_test("https", Some("moq-transport-16"), None).await;
}

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_webtransport_negotiate_server_all_client_transport_17() {
	broadcast_test("https", Some("moq-transport-17"), None).await;
}

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_webtransport_negotiate_server_all_client_transport_18() {
	broadcast_test("https", Some("moq-transport-18"), None).await;
}

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_webtransport_negotiate_server_all_client_transport_19() {
	broadcast_test("https", Some("moq-transport-19"), None).await;
}

// ── WebTransport – client supports all, server pins one ─────────────

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_webtransport_negotiate_client_all_server_lite_01() {
	broadcast_test("https", None, Some("moq-lite-01")).await;
}

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_webtransport_negotiate_client_all_server_lite_02() {
	broadcast_test("https", None, Some("moq-lite-02")).await;
}

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_webtransport_negotiate_client_all_server_lite_03() {
	broadcast_test("https", None, Some("moq-lite-03")).await;
}

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_webtransport_negotiate_client_all_server_transport_14() {
	broadcast_test("https", None, Some("moq-transport-14")).await;
}

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_webtransport_negotiate_client_all_server_transport_15() {
	broadcast_test("https", None, Some("moq-transport-15")).await;
}

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_webtransport_negotiate_client_all_server_transport_16() {
	broadcast_test("https", None, Some("moq-transport-16")).await;
}

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_webtransport_negotiate_client_all_server_transport_17() {
	broadcast_test("https", None, Some("moq-transport-17")).await;
}

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_webtransport_negotiate_client_all_server_transport_18() {
	broadcast_test("https", None, Some("moq-transport-18")).await;
}

#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_webtransport_negotiate_client_all_server_transport_19() {
	broadcast_test("https", None, Some("moq-transport-19")).await;
}

// ── WebSocket (ws://) ───────────────────────────────────────────────

/// Test WebSocket transport end-to-end.
///
/// The server binds a WebSocket TCP listener on a separate port.
/// The client connects directly via ws://, bypassing QUIC entirely.
#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_websocket() {
	use moq_native::moq_net::{Origin, Track};

	// ── publisher (server) ──────────────────────────────────────────
	let pub_origin = Origin::random().produce();
	let mut broadcast = pub_origin.create_broadcast("test").expect("failed to create broadcast");
	let mut track = broadcast
		.create_track(Track::new("video"))
		.expect("failed to create track");

	let mut group = track.append_group().expect("failed to append group");
	group.write_frame(b"hello".as_ref()).expect("failed to write frame");
	group.finish().expect("failed to finish group");

	// Server with both QUIC (required) and WebSocket listeners.
	let mut server_config = moq_native::ServerConfig::default();
	server_config.bind = Some("[::]:0".to_string());
	server_config.tls.generate = vec!["localhost".into()];

	let ws_listener = moq_native::websocket::Listener::bind("[::]:0".parse().unwrap())
		.await
		.expect("failed to bind WebSocket listener");
	let ws_addr = ws_listener.local_addr().expect("failed to get ws addr");

	let mut server = server_config
		.init()
		.expect("failed to init server")
		.with_websocket(Some(ws_listener));

	// ── subscriber (client) ─────────────────────────────────────────
	let sub_origin = Origin::random().produce();
	let mut announcements = sub_origin.consume();

	let mut client_config = moq_native::ClientConfig::default();
	client_config.tls.disable_verify = Some(true);
	// Disable WebSocket delay so client connects immediately via ws://
	client_config.websocket.delay = None;

	let client = client_config.init().expect("failed to init client");
	let url: url::Url = format!("ws://localhost:{}", ws_addr.port()).parse().unwrap();

	// ── run server and client concurrently ──────────────────────────
	let server_handle = tokio::spawn(async move {
		let request = server.accept().await.expect("no incoming connection");
		assert_eq!(request.transport(), "websocket");
		let session = request.with_publish(pub_origin.consume()).ok().await?;

		let _broadcast = broadcast;
		let _track = track;

		let _ = session.closed().await;
		Ok::<_, anyhow::Error>(())
	});

	let client = client.with_consume(sub_origin);
	let session = tokio::time::timeout(TIMEOUT, client.connect(url))
		.await
		.expect("client connect timed out")
		.expect("client connect failed");

	// Wait for the broadcast announcement.
	let (path, bc) = tokio::time::timeout(TIMEOUT, announcements.announced())
		.await
		.expect("announce timed out")
		.expect("origin closed");

	assert_eq!(path.as_str(), "test");
	let bc = bc.expect("expected announce, got unannounce");

	// Subscribe to the track.
	let mut track_sub = bc
		.subscribe_track(&Track::new("video"))
		.expect("subscribe_track failed");

	// Read one group.
	let mut group_sub = tokio::time::timeout(TIMEOUT, track_sub.recv_group())
		.await
		.expect("recv_group timed out")
		.expect("recv_group failed")
		.expect("track closed prematurely");

	// Read one frame and verify the payload.
	let frame = tokio::time::timeout(TIMEOUT, group_sub.read_frame())
		.await
		.expect("read_frame timed out")
		.expect("read_frame failed")
		.expect("group closed prematurely");

	assert_eq!(&*frame, b"hello");

	drop(session);
	server_handle
		.await
		.expect("server task panicked")
		.expect("server task failed");
}

/// Test WebSocket fallback when QUIC is unavailable.
///
/// The client connects via `http://` to the WebSocket port. QUIC tries to
/// reach that port over UDP and fails (no QUIC listener there). The WebSocket
/// fallback converts `http://` → `ws://` and connects over TCP, succeeding.
#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_websocket_fallback() {
	use moq_native::moq_net::{Origin, Track};

	// ── publisher (server) ──────────────────────────────────────────
	let pub_origin = Origin::random().produce();
	let mut broadcast = pub_origin.create_broadcast("test").expect("failed to create broadcast");
	let mut track = broadcast
		.create_track(Track::new("video"))
		.expect("failed to create track");

	let mut group = track.append_group().expect("failed to append group");
	group.write_frame(b"hello".as_ref()).expect("failed to write frame");
	group.finish().expect("failed to finish group");

	// QUIC binds on its own port; WebSocket on a different port.
	let mut server_config = moq_native::ServerConfig::default();
	server_config.bind = Some("[::]:0".to_string());
	server_config.tls.generate = vec!["localhost".into()];

	let ws_listener = moq_native::websocket::Listener::bind("[::]:0".parse().unwrap())
		.await
		.expect("failed to bind WebSocket listener");
	let ws_addr = ws_listener.local_addr().expect("failed to get ws addr");

	let mut server = server_config
		.init()
		.expect("failed to init server")
		.with_websocket(Some(ws_listener));

	// ── subscriber (client) ─────────────────────────────────────────
	let sub_origin = Origin::random().produce();
	let mut announcements = sub_origin.consume();

	let mut client_config = moq_native::ClientConfig::default();
	client_config.tls.disable_verify = Some(true);
	// No delay. Race QUIC and WebSocket simultaneously.
	client_config.websocket.delay = None;

	let client = client_config.init().expect("failed to init client");

	// Connect via http:// to the WebSocket port.
	// QUIC will try UDP on this port and fail; WebSocket will try ws:// and succeed.
	let url: url::Url = format!("http://localhost:{}", ws_addr.port()).parse().unwrap();

	// ── run server and client concurrently ──────────────────────────
	let server_handle = tokio::spawn(async move {
		let request = server.accept().await.expect("no incoming connection");
		assert_eq!(request.transport(), "websocket");
		let session = request.with_publish(pub_origin.consume()).ok().await?;

		let _broadcast = broadcast;
		let _track = track;

		let _ = session.closed().await;
		Ok::<_, anyhow::Error>(())
	});

	let client = client.with_consume(sub_origin);
	let session = tokio::time::timeout(TIMEOUT, client.connect(url))
		.await
		.expect("client connect timed out")
		.expect("client connect failed");

	// Wait for the broadcast announcement.
	let (path, bc) = tokio::time::timeout(TIMEOUT, announcements.announced())
		.await
		.expect("announce timed out")
		.expect("origin closed");

	assert_eq!(path.as_str(), "test");
	let bc = bc.expect("expected announce, got unannounce");

	// Subscribe to the track.
	let mut track_sub = bc
		.subscribe_track(&Track::new("video"))
		.expect("subscribe_track failed");

	let mut group_sub = tokio::time::timeout(TIMEOUT, track_sub.recv_group())
		.await
		.expect("recv_group timed out")
		.expect("recv_group failed")
		.expect("track closed prematurely");

	let frame = tokio::time::timeout(TIMEOUT, group_sub.read_frame())
		.await
		.expect("read_frame timed out")
		.expect("read_frame failed")
		.expect("group closed prematurely");

	assert_eq!(&*frame, b"hello");

	drop(session);
	server_handle
		.await
		.expect("server task panicked")
		.expect("server task failed");
}

// ── ALPN regression guards ──────────────────────────────────────────

/// The newest moq-lite version both sides advertise by default.
///
/// Bump this whenever [`moq_net::Versions::all`] gains a newer Lite variant
/// so the regression tests below keep tracking "the newest", not a frozen value.
const NEWEST_LITE: &str = "moq-lite-04";

/// Regression guard for the WebSocket ALPN path. Lite02 over WebSocket means
/// the qmux subprotocol negotiation produced a bare `moql` (or no match)
/// instead of `moq-lite-04`, which falls through to legacy SETUP negotiation
/// and picks Lite02. This test fails immediately if that happens.
#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_websocket_uses_newest_version() {
	let pub_origin = Origin::random().produce();
	let mut broadcast = pub_origin.create_broadcast("test").expect("failed to create broadcast");
	let mut track = broadcast
		.create_track(Track::new("video"))
		.expect("failed to create track");
	let mut group = track.append_group().expect("failed to append group");
	group.write_frame(b"hello".as_ref()).expect("failed to write frame");
	group.finish().expect("failed to finish group");

	let mut server_config = moq_native::ServerConfig::default();
	server_config.bind = Some("[::]:0".to_string());
	server_config.tls.generate = vec!["localhost".into()];

	let ws_listener = moq_native::websocket::Listener::bind("[::]:0".parse().unwrap())
		.await
		.expect("failed to bind WebSocket listener");
	let ws_addr = ws_listener.local_addr().expect("failed to get ws addr");

	let mut server = server_config
		.init()
		.expect("failed to init server")
		.with_websocket(Some(ws_listener));

	let sub_origin = Origin::random().produce();
	let mut client_config = moq_native::ClientConfig::default();
	client_config.tls.disable_verify = Some(true);
	client_config.websocket.delay = None;

	let client = client_config.init().expect("failed to init client");
	let url: url::Url = format!("ws://localhost:{}", ws_addr.port()).parse().unwrap();

	let expected_version: moq_net::Version = NEWEST_LITE.parse().expect("invalid version");

	let server_handle = tokio::spawn(async move {
		let request = server.accept().await.expect("no incoming connection");
		assert_eq!(request.transport(), "websocket");
		let session = request.with_publish(pub_origin.consume()).ok().await?;
		assert_eq!(session.version(), expected_version, "server negotiated stale version");
		let _broadcast = broadcast;
		let _track = track;
		let _ = session.closed().await;
		Ok::<_, anyhow::Error>(())
	});

	let client = client.with_consume(sub_origin);
	let session = tokio::time::timeout(TIMEOUT, client.connect(url))
		.await
		.expect("client connect timed out")
		.expect("client connect failed");

	assert_eq!(session.version(), expected_version, "client negotiated stale version");

	drop(session);
	server_handle
		.await
		.expect("server task panicked")
		.expect("server task failed");
}

/// Regression guard for the QUIC vs WebSocket race. With both transports
/// reachable at the same URL, QUIC must win, since it's lower-latency and
/// has direct ALPN negotiation. A WebSocket win here means QUIC silently
/// regressed (and would also tend to drag the version down to Lite02 on
/// older relays). We bind WebSocket TCP and QUIC UDP to the same port,
/// then disable the head start so the race is genuine.
#[tracing_test::traced_test]
#[tokio::test]
async fn broadcast_race_quic_wins() {
	let pub_origin = Origin::random().produce();
	let mut broadcast = pub_origin.create_broadcast("test").expect("failed to create broadcast");
	let mut track = broadcast
		.create_track(Track::new("video"))
		.expect("failed to create track");
	let mut group = track.append_group().expect("failed to append group");
	group.write_frame(b"hello".as_ref()).expect("failed to write frame");
	group.finish().expect("failed to finish group");

	// Bind WebSocket TCP first to pick a random port, then bind QUIC UDP to
	// the same port. UDP and TCP live in separate kernel namespaces, so this
	// works on every supported platform.
	let ws_listener = moq_native::websocket::Listener::bind("[::]:0".parse().unwrap())
		.await
		.expect("failed to bind WebSocket listener");
	let port = ws_listener.local_addr().expect("failed to get ws addr").port();

	let mut server_config = moq_native::ServerConfig::default();
	server_config.bind = Some(format!("[::]:{port}"));
	server_config.tls.generate = vec!["localhost".into()];

	let mut server = server_config
		.init()
		.expect("failed to init server")
		.with_websocket(Some(ws_listener));

	let sub_origin = Origin::random().produce();
	let mut client_config = moq_native::ClientConfig::default();
	client_config.tls.disable_verify = Some(true);
	// Zero head start: QUIC has to win on its own merit, not by penalising WS.
	client_config.websocket.delay = None;

	let client = client_config.init().expect("failed to init client");
	let url: url::Url = format!("https://localhost:{port}").parse().unwrap();

	let expected_version: moq_net::Version = NEWEST_LITE.parse().expect("invalid version");

	let server_handle = tokio::spawn(async move {
		let request = server.accept().await.expect("no incoming connection");
		assert_eq!(
			request.transport(),
			"quic",
			"QUIC lost the race to WebSocket with both reachable",
		);
		let session = request.with_publish(pub_origin.consume()).ok().await?;
		assert_eq!(session.version(), expected_version, "server negotiated stale version");
		let _broadcast = broadcast;
		let _track = track;
		let _ = session.closed().await;
		Ok::<_, anyhow::Error>(())
	});

	let client = client.with_consume(sub_origin);
	let session = tokio::time::timeout(TIMEOUT, client.connect(url))
		.await
		.expect("client connect timed out")
		.expect("client connect failed");

	assert_eq!(session.version(), expected_version, "client negotiated stale version");

	drop(session);
	server_handle
		.await
		.expect("server task panicked")
		.expect("server task failed");
}

#[tracing_test::traced_test]
#[tokio::test]
async fn websocket_unauthorized_handshake_is_explicit() {
	use tokio::io::{AsyncReadExt, AsyncWriteExt};

	let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
		.await
		.expect("failed to bind TCP listener");
	let addr = listener.local_addr().expect("failed to get local addr");

	let server_handle = tokio::spawn(async move {
		let (mut stream, _) = listener.accept().await?;
		let mut buf = [0; 1024];
		let _ = stream.read(&mut buf).await?;
		stream
			.write_all(b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
			.await?;
		Ok::<_, anyhow::Error>(())
	});

	let mut client_config = moq_native::ClientConfig::default();
	client_config.websocket.delay = None;
	let client = client_config.init().expect("failed to init client");
	let url: url::Url = format!("ws://{addr}").parse().unwrap();

	let err = tokio::time::timeout(TIMEOUT, client.connect(url))
		.await
		.expect("client connect timed out");
	let err = expect_connect_err(err);
	assert_connect_error(&err, moq_native::ConnectError::Unauthorized);

	server_handle
		.await
		.expect("server task panicked")
		.expect("server task failed");
}

#[tracing_test::traced_test]
#[tokio::test]
async fn reconnect_stops_on_websocket_unauthorized() {
	use tokio::io::{AsyncReadExt, AsyncWriteExt};

	let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
		.await
		.expect("failed to bind TCP listener");
	let addr = listener.local_addr().expect("failed to get local addr");

	let server_handle = tokio::spawn(async move {
		let (mut stream, _) = listener.accept().await?;
		let mut buf = [0; 1024];
		let _ = stream.read(&mut buf).await?;
		stream
			.write_all(b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
			.await?;
		Ok::<_, anyhow::Error>(())
	});

	let mut client_config = moq_native::ClientConfig::default();
	client_config.websocket.delay = None;
	let client = client_config.init().expect("failed to init client");
	let url: url::Url = format!("ws://{addr}").parse().unwrap();

	let reconnect = client.reconnect(url);
	let err = tokio::time::timeout(TIMEOUT, reconnect.closed())
		.await
		.expect("reconnect close timed out")
		.expect_err("reconnect unexpectedly succeeded");
	assert_connect_error(&err, moq_native::ConnectError::Unauthorized);

	server_handle
		.await
		.expect("server task panicked")
		.expect("server task failed");
}

/// A peer that expresses announce-interest in a prefix the publisher can't serve (e.g. a
/// subscribe-restricted token) must not tear down the whole session. The publisher FINs that
/// announce stream cleanly; the connection and other announce streams keep working.
#[tracing_test::traced_test]
#[tokio::test]
async fn announce_interest_unauthorized_keeps_session_alive() {
	use moq_native::moq_net::{Origin, Track};

	// ── publisher (server): only allowed to announce under "allowed" ──
	let pub_origin = Origin::random().produce();
	let mut broadcast = pub_origin
		.create_broadcast("allowed/test")
		.expect("failed to create broadcast");
	let mut track = broadcast
		.create_track(Track::new("video"))
		.expect("failed to create track");
	let mut group = track.append_group().expect("failed to append group");
	group.write_frame(b"hello".as_ref()).expect("failed to write frame");
	group.finish().expect("failed to finish group");

	let publish = pub_origin
		.consume()
		.scope(&["allowed".into()])
		.expect("failed to scope publish origin");

	let (mut server, addr) = test_server();

	// ── subscriber (client): interested in both "allowed" and "denied" ──
	// "denied" is disjoint from the publisher's scope, so its announce stream is FINed.
	let sub_origin = Origin::random().produce();
	let consume = sub_origin
		.scope(&["allowed".into(), "denied".into()])
		.expect("failed to scope consume origin");
	let mut announcements = consume.consume();

	let client = test_client();
	let url: url::Url = format!("https://localhost:{}", addr.port()).parse().unwrap();

	let server_handle = tokio::spawn(async move {
		let request = server.accept().await.expect("no incoming connection");
		let session = request.with_publish(publish).ok().await?;
		let _broadcast = broadcast;
		let _track = track;
		let _ = session.closed().await;
		Ok::<_, anyhow::Error>(())
	});

	let client = client.with_consume(consume);
	let session = tokio::time::timeout(TIMEOUT, client.connect(url))
		.await
		.expect("client connect timed out")
		.expect("client connect failed");

	// The "allowed" announce stream still delivers even though "denied" was FINed.
	let (path, bc) = tokio::time::timeout(TIMEOUT, announcements.announced())
		.await
		.expect("announce timed out")
		.expect("origin closed");
	assert_eq!(path.as_str(), "allowed/test");
	assert!(bc.is_some(), "expected announce, got unannounce");

	// The unauthorized "denied" interest must not have torn down the session.
	assert!(
		tokio::time::timeout(Duration::from_millis(200), session.closed())
			.await
			.is_err(),
		"session closed after unauthorized announce interest",
	);

	drop(session);
	server_handle
		.await
		.expect("server task panicked")
		.expect("server task failed");
}

/// Reverse of the usual direction: a publish-only client (`with_publish`, no `with_consume`)
/// serving a subscribe-only server (`with_consume`, no `with_publish`). The server is also
/// interested in a disjoint "denied" prefix the client can't serve, so the server's
/// subscriber must survive that FIN and still receive the served broadcast.
#[tracing_test::traced_test]
#[tokio::test]
async fn publish_only_client_to_subscribe_only_server() {
	use moq_native::moq_net::{Origin, Track};

	// ── subscriber (server): interested in both "allowed" and "denied" ──
	let sub_origin = Origin::random().produce();
	let consume = sub_origin
		.scope(&["allowed".into(), "denied".into()])
		.expect("failed to scope consume origin");
	let mut announcements = consume.consume();

	let (mut server, addr) = test_server();
	let url: url::Url = format!("https://localhost:{}", addr.port()).parse().unwrap();

	let server_handle = tokio::spawn(async move {
		let session = server
			.accept()
			.await
			.expect("no incoming connection")
			.with_consume(consume)
			.ok()
			.await?;

		// The client serves "allowed/test"; the "denied" interest is FINed but must not
		// tear down the session.
		let (path, bc) = tokio::time::timeout(TIMEOUT, announcements.announced())
			.await
			.expect("announce timed out")
			.expect("origin closed");
		assert_eq!(path.as_str(), "allowed/test");
		let bc = bc.expect("expected announce, got unannounce");

		let mut track_sub = bc
			.subscribe_track(&Track::new("video"))
			.expect("subscribe_track failed");
		let mut group_sub = tokio::time::timeout(TIMEOUT, track_sub.recv_group())
			.await
			.expect("recv_group timed out")
			.expect("recv_group failed")
			.expect("track closed prematurely");
		let frame = tokio::time::timeout(TIMEOUT, group_sub.read_frame())
			.await
			.expect("read_frame timed out")
			.expect("read_frame failed")
			.expect("group closed prematurely");
		assert_eq!(&*frame, b"hello");

		// The disjoint "denied" interest must not have torn down the session.
		assert!(
			tokio::time::timeout(Duration::from_millis(200), session.closed())
				.await
				.is_err(),
			"server session closed after unauthorized announce interest",
		);

		Ok::<_, anyhow::Error>(())
	});

	// ── publisher (client): only allowed to serve under "allowed" ──
	let pub_origin = Origin::random().produce();
	let mut broadcast = pub_origin
		.create_broadcast("allowed/test")
		.expect("failed to create broadcast");
	let mut track = broadcast
		.create_track(Track::new("video"))
		.expect("failed to create track");
	let mut group = track.append_group().expect("failed to append group");
	group.write_frame(b"hello".as_ref()).expect("failed to write frame");
	group.finish().expect("failed to finish group");

	let publish = pub_origin
		.consume()
		.scope(&["allowed".into()])
		.expect("failed to scope publish origin");

	let session = tokio::time::timeout(TIMEOUT, test_client().with_publish(publish).connect(url))
		.await
		.expect("client connect timed out")
		.expect("client connect failed");

	server_handle
		.await
		.expect("server task panicked")
		.expect("server task failed");

	drop(session);
	drop(track);
	drop(broadcast);
}

/// A test server bound to a free port with a generated localhost certificate.
fn test_server() -> (moq_native::Server, std::net::SocketAddr) {
	let mut config = moq_native::ServerConfig::default();
	config.bind = Some("[::]:0".to_string());
	config.tls.generate = vec!["localhost".into()];
	let server = config.init().expect("failed to init server");
	let addr = server.local_addr().expect("failed to get local addr");
	(server, addr)
}

/// A test client that skips TLS verification (servers use self-signed certs).
fn test_client() -> moq_native::Client {
	let mut config = moq_native::ClientConfig::default();
	config.tls.disable_verify = Some(true);
	config.init().expect("failed to init client")
}

fn assert_connect_error(err: &moq_native::Error, expected: moq_native::ConnectError) {
	assert_eq!(err.connect_error(), Some(expected), "unexpected error: {err}",);
}

fn expect_connect_err(result: moq_native::Result<moq_net::Session>) -> moq_native::Error {
	match result {
		Ok(_) => panic!("client connect unexpectedly succeeded"),
		Err(err) => err,
	}
}

/// Test that a server-side drain via GOAWAY is received by the client.
///
/// The server calls `session.drain().start("")` and the client observes the
/// GOAWAY through `session.goaway()`. Pinned to draft-14 so the test covers
/// the adapter control-stream path (draft-14-16).
#[tracing_test::traced_test]
#[tokio::test]
async fn goaway_drains_peer_moq_transport_14() {
	let pub_origin = Origin::random().produce();
	let mut broadcast = pub_origin.create_broadcast("test").expect("failed to create broadcast");
	let mut track = broadcast
		.create_track(Track::new("video"))
		.expect("failed to create track");
	let mut group = track.append_group().expect("failed to append group");
	group.write_frame(b"hello".as_ref()).expect("failed to write frame");
	group.finish().expect("failed to finish group");

	let mut server_config = moq_native::ServerConfig::default();
	server_config.bind = Some("[::]:0".to_string());
	server_config.tls.generate = vec!["localhost".into()];
	server_config.version = vec!["moq-transport-14".parse().unwrap()];
	let mut server = server_config.init().expect("failed to init server");
	let addr = server.local_addr().expect("failed to get local addr");

	let sub_origin = Origin::random().produce();
	let mut client_config = moq_native::ClientConfig::default();
	client_config.tls.disable_verify = Some(true);
	client_config.version = vec!["moq-transport-14".parse().unwrap()];
	let client = client_config.init().expect("failed to init client");
	let url: url::Url = format!("moqt://localhost:{}", addr.port()).parse().unwrap();

	let server_handle = tokio::spawn(async move {
		let request = server.accept().await.expect("no incoming connection");
		let session = request.with_publish(pub_origin.consume()).ok().await?;
		let _broadcast = broadcast;
		let _track = track;

		// Give the client a moment to establish subscriptions.
		tokio::time::sleep(Duration::from_millis(100)).await;

		// Initiate the drain.
		let draining = session.drain().expect("drain").start("");

		// Wait for the client to disconnect after receiving GOAWAY.
		tokio::time::timeout(TIMEOUT, draining.complete())
			.await
			.expect("drain.complete() timed out");

		Ok::<_, anyhow::Error>(())
	});

	let client = client.with_consume(sub_origin);
	let session = tokio::time::timeout(TIMEOUT, client.connect(url))
		.await
		.expect("client connect timed out")
		.expect("client connect failed");

	// The client should receive the GOAWAY through the public accessor.
	let goaway = tokio::time::timeout(TIMEOUT, session.goaway())
		.await
		.expect("session.goaway() timed out")
		.expect("session closed before GOAWAY");

	// Draft-14 sends an empty URI (None becomes "") and has no timeout.
	assert_eq!(&*goaway.uri, "");
	assert_eq!(goaway.timeout, None);

	// Drop the session to let the server's drain.complete() resolve.
	drop(session);

	server_handle
		.await
		.expect("server task panicked")
		.expect("server task failed");
}

/// Test GOAWAY round-trip with a URI on draft-17 (covers the uni-stream path).
///
/// The server calls `session.drain().start("https://new.example.com")` and the
/// client receives it through `session.goaway()` with the correct URI.
#[tracing_test::traced_test]
#[tokio::test]
async fn goaway_received_with_uri_moq_transport_17() {
	let pub_origin = Origin::random().produce();
	let mut broadcast = pub_origin.create_broadcast("test").expect("failed to create broadcast");
	let mut track = broadcast
		.create_track(Track::new("video"))
		.expect("failed to create track");
	let mut group = track.append_group().expect("failed to append group");
	group.write_frame(b"hello".as_ref()).expect("failed to write frame");
	group.finish().expect("failed to finish group");

	let mut server_config = moq_native::ServerConfig::default();
	server_config.bind = Some("[::]:0".to_string());
	server_config.tls.generate = vec!["localhost".into()];
	server_config.version = vec!["moq-transport-17".parse().unwrap()];
	let mut server = server_config.init().expect("failed to init server");
	let addr = server.local_addr().expect("failed to get local addr");

	let sub_origin = Origin::random().produce();
	let mut client_config = moq_native::ClientConfig::default();
	client_config.tls.disable_verify = Some(true);
	client_config.version = vec!["moq-transport-17".parse().unwrap()];
	let client = client_config.init().expect("failed to init client");
	let url: url::Url = format!("moqt://localhost:{}", addr.port()).parse().unwrap();

	let server_handle = tokio::spawn(async move {
		let request = server.accept().await.expect("no incoming connection");
		let session = request.with_publish(pub_origin.consume()).ok().await?;
		let _broadcast = broadcast;
		let _track = track;

		// Give the client a moment to establish.
		tokio::time::sleep(Duration::from_millis(100)).await;

		let draining = session.drain().expect("drain").start("https://new.example.com");

		tokio::time::timeout(TIMEOUT, draining.complete())
			.await
			.expect("drain.complete() timed out");

		Ok::<_, anyhow::Error>(())
	});

	let client = client.with_consume(sub_origin);
	let session = tokio::time::timeout(TIMEOUT, client.connect(url))
		.await
		.expect("client connect timed out")
		.expect("client connect failed");

	let goaway = tokio::time::timeout(TIMEOUT, session.goaway())
		.await
		.expect("session.goaway() timed out")
		.expect("session closed before GOAWAY");

	assert_eq!(&*goaway.uri, "https://new.example.com");
	// Draft-17 sends timeout=0, which we map to None (zero means no timeout).
	assert_eq!(goaway.timeout, None);

	drop(session);

	server_handle
		.await
		.expect("server task panicked")
		.expect("server task failed");
}

/// Test GOAWAY round-trip on moq-lite-04 (covers the lite publisher receive path).
///
/// This test is currently disabled because the lite-04 GOAWAY path uses a new bidi
/// stream from server to client that races with the QUIC transport state in Quinn.
/// The protocol implementation is exercised by the relay integration tests; unit-level
/// coverage for the bidi GOAWAY decode path is tracked separately.
#[tracing_test::traced_test]
#[tokio::test]
#[ignore = "lite-04 GOAWAY bidi races QUIC transport close in Quinn tests"]
async fn goaway_received_moq_lite_04() {
	let pub_origin = Origin::random().produce();
	let mut broadcast = pub_origin.create_broadcast("test").expect("failed to create broadcast");
	let mut track = broadcast
		.create_track(Track::new("video"))
		.expect("failed to create track");
	let mut group = track.append_group().expect("failed to append group");
	group.write_frame(b"hello".as_ref()).expect("failed to write frame");
	group.finish().expect("failed to finish group");

	let mut server_config = moq_native::ServerConfig::default();
	server_config.bind = Some("[::]:0".to_string());
	server_config.tls.generate = vec!["localhost".into()];
	server_config.version = vec!["moq-lite-04".parse().unwrap()];
	let mut server = server_config.init().expect("failed to init server");
	let addr = server.local_addr().expect("failed to get local addr");

	let mut client_config = moq_native::ClientConfig::default();
	client_config.tls.disable_verify = Some(true);
	client_config.version = vec!["moq-lite-04".parse().unwrap()];
	let client = client_config.init().expect("failed to init client");
	let url: url::Url = format!("moqt://localhost:{}", addr.port()).parse().unwrap();

	let server_handle = tokio::spawn(async move {
		let request = server.accept().await.expect("no incoming connection");
		let session = request.with_publish(pub_origin.consume()).ok().await?;
		let _broadcast = broadcast;
		let _track = track;

		// Give the client a moment to establish.
		tokio::time::sleep(Duration::from_millis(200)).await;

		let draining = session.drain().expect("drain").start("https://new.example.com");

		tokio::time::timeout(TIMEOUT, draining.complete())
			.await
			.expect("drain.complete() timed out");

		Ok::<_, anyhow::Error>(())
	});

	// Connect without consuming to avoid probe/announce streams that race GOAWAY.
	let client = client;
	let session = tokio::time::timeout(TIMEOUT, client.connect(url))
		.await
		.expect("client connect timed out")
		.expect("client connect failed");

	let goaway = tokio::time::timeout(TIMEOUT, session.goaway())
		.await
		.expect("session.goaway() timed out")
		.expect("session closed before GOAWAY");

	assert_eq!(&*goaway.uri, "https://new.example.com");
	// moq-lite has no timeout field.
	assert_eq!(goaway.timeout, None);

	drop(session);

	server_handle
		.await
		.expect("server task panicked")
		.expect("server task failed");
}

// ── GOAWAY gating tests ──────────────────────────────────────────────

/// After a GOAWAY is received, new subscribe attempts fail with GoingAway.
/// Uses moq-transport-17 which has reliable GOAWAY delivery (uni-stream path).
#[tracing_test::traced_test]
#[tokio::test]
async fn goaway_gates_new_subscribe() {
	let version = "moq-transport-17";

	// ── publisher (server) ──────────────────────────────────────────
	let pub_origin = moq_net::Origin::random().produce();
	let mut broadcast = pub_origin.create_broadcast("test").expect("create broadcast");
	let mut track = broadcast
		.create_track(moq_net::Track::new("video"))
		.expect("create track");
	let mut group = track.append_group().expect("append group");
	group.write_frame(b"hello".as_ref()).expect("write frame");
	group.finish().expect("finish group");

	// A second track created after GOAWAY that the client will try to subscribe to.
	let mut track2 = broadcast
		.create_track(moq_net::Track::new("audio"))
		.expect("create track2");
	let mut group2 = track2.append_group().expect("append group2");
	group2.write_frame(b"world".as_ref()).expect("write frame2");
	group2.finish().expect("finish group2");

	let mut server_config = moq_native::ServerConfig::default();
	server_config.bind = Some("[::]:0".to_string());
	server_config.tls.generate = vec!["localhost".into()];
	server_config.version = vec![version.parse().unwrap()];

	let mut server = server_config.init().expect("init server");
	let addr = server.local_addr().expect("local addr");

	// ── subscriber (client) ─────────────────────────────────────────
	let sub_origin = moq_net::Origin::random().produce();
	let mut announcements = sub_origin.consume();

	let mut client_config = moq_native::ClientConfig::default();
	client_config.tls.disable_verify = Some(true);
	client_config.version = vec![version.parse().unwrap()];
	let client = client_config.init().expect("init client");
	let url: url::Url = format!("moqt://localhost:{}", addr.port()).parse().unwrap();

	// ── run ─────────────────────────────────────────────────────────
	let server_handle = tokio::spawn(async move {
		let request = server.accept().await.expect("no incoming connection");
		let session = request.with_publish(pub_origin.consume()).ok().await?;

		// Send GOAWAY after a short delay so the client has time to connect.
		tokio::time::sleep(Duration::from_millis(50)).await;
		session.drain().expect("drain").start("https://new.example.com");

		// Keep producers alive so the subscription can read data.
		let _broadcast = broadcast;
		let _track = track;
		let _track2 = track2;
		let _ = session.closed().await;
		Ok::<_, anyhow::Error>(())
	});

	let client = client.with_consume(sub_origin);
	let session = tokio::time::timeout(TIMEOUT, client.connect(url))
		.await
		.expect("connect timed out")
		.expect("connect failed");

	// Wait for the announcement.
	let (path, bc) = tokio::time::timeout(TIMEOUT, announcements.announced())
		.await
		.expect("announce timed out")
		.expect("origin closed");
	assert_eq!(path.as_str(), "test");
	let bc = bc.expect("expected announce");

	// Wait for GOAWAY to arrive.
	let goaway = tokio::time::timeout(TIMEOUT, session.goaway())
		.await
		.expect("goaway timed out")
		.expect("session closed before GOAWAY");
	assert_eq!(&*goaway.uri, "https://new.example.com");

	// Verify is_going_away reflects the state.
	assert!(session.is_going_away());

	// Attempt a new subscription after GOAWAY: should fail with GoingAway.
	let result = bc.subscribe_track(&moq_net::Track::new("audio"));
	match result {
		Ok(mut track_sub) => {
			// The subscribe_track call itself may succeed (it's a local operation),
			// but the underlying stream open will fail. Read to trigger the error.
			let group_result = tokio::time::timeout(Duration::from_secs(2), track_sub.recv_group()).await;
			match group_result {
				Ok(Err(moq_net::Error::GoingAway)) => {} // Expected
				Ok(Err(err)) => {
					// Accept other errors too: the track may be aborted with GoingAway.
					assert!(
						matches!(err, moq_net::Error::GoingAway | moq_net::Error::Cancel),
						"expected GoingAway or Cancel, got: {err}"
					);
				}
				Ok(Ok(None)) => panic!("track closed without error, expected GoingAway"),
				Ok(Ok(Some(_))) => panic!("received data after GOAWAY, expected GoingAway"),
				Err(_) => panic!("timed out waiting for GoingAway error"),
			}
		}
		Err(err) => {
			// Direct error from subscribe_track is also acceptable.
			assert!(
				matches!(err, moq_net::Error::GoingAway | moq_net::Error::Cancel),
				"expected GoingAway or Cancel, got: {err}"
			);
		}
	}

	drop(session);
	server_handle.await.expect("server panicked").expect("server failed");
}

/// An existing subscription continues to deliver data after GOAWAY is received.
#[tracing_test::traced_test]
#[tokio::test]
async fn goaway_existing_subscription_continues() {
	let version = "moq-transport-17";

	// ── publisher (server) ──────────────────────────────────────────
	let pub_origin = moq_net::Origin::random().produce();
	let mut broadcast = pub_origin.create_broadcast("test").expect("create broadcast");
	let mut track = broadcast
		.create_track(moq_net::Track::new("video"))
		.expect("create track");

	// Write a group before GOAWAY.
	let mut group1 = track.append_group().expect("append group1");
	group1.write_frame(b"before".as_ref()).expect("write frame1");
	group1.finish().expect("finish group1");

	let mut server_config = moq_native::ServerConfig::default();
	server_config.bind = Some("[::]:0".to_string());
	server_config.tls.generate = vec!["localhost".into()];
	server_config.version = vec![version.parse().unwrap()];

	let mut server = server_config.init().expect("init server");
	let addr = server.local_addr().expect("local addr");

	// ── subscriber (client) ─────────────────────────────────────────
	let sub_origin = moq_net::Origin::random().produce();
	let mut announcements = sub_origin.consume();

	let mut client_config = moq_native::ClientConfig::default();
	client_config.tls.disable_verify = Some(true);
	client_config.version = vec![version.parse().unwrap()];
	let client = client_config.init().expect("init client");
	let url: url::Url = format!("moqt://localhost:{}", addr.port()).parse().unwrap();

	// Channel to coordinate GOAWAY timing.
	let (goaway_tx, goaway_rx) = tokio::sync::oneshot::channel::<()>();

	// ── run ─────────────────────────────────────────────────────────
	let server_handle = tokio::spawn(async move {
		let request = server.accept().await.expect("no incoming connection");
		let session = request.with_publish(pub_origin.consume()).ok().await?;

		// Wait for the client to signal it's subscribed before sending GOAWAY.
		let _ = goaway_rx.await;

		// Send GOAWAY.
		session.drain().expect("drain").start("https://new.example.com");

		// Write a second group AFTER GOAWAY. The existing subscription should still receive it.
		tokio::time::sleep(Duration::from_millis(50)).await;
		let mut group2 = track.append_group().expect("append group2");
		group2.write_frame(b"after".as_ref()).expect("write frame2");
		group2.finish().expect("finish group2");

		// Give the client time to read.
		tokio::time::sleep(Duration::from_millis(200)).await;

		let _broadcast = broadcast;
		let _track = track;
		let _ = session.closed().await;
		Ok::<_, anyhow::Error>(())
	});

	let client = client.with_consume(sub_origin);
	let session = tokio::time::timeout(TIMEOUT, client.connect(url))
		.await
		.expect("connect timed out")
		.expect("connect failed");

	// Wait for the announcement.
	let (path, bc) = tokio::time::timeout(TIMEOUT, announcements.announced())
		.await
		.expect("announce timed out")
		.expect("origin closed");
	assert_eq!(path.as_str(), "test");
	let bc = bc.expect("expected announce");

	// Subscribe before GOAWAY.
	let mut track_sub = bc
		.subscribe_track(&moq_net::Track::new("video"))
		.expect("subscribe_track");

	// Read the first group (before GOAWAY).
	let mut group_sub = tokio::time::timeout(TIMEOUT, track_sub.recv_group())
		.await
		.expect("recv_group timed out")
		.expect("recv_group failed")
		.expect("track closed");
	let frame = tokio::time::timeout(TIMEOUT, group_sub.read_frame())
		.await
		.expect("read_frame timed out")
		.expect("read_frame failed")
		.expect("group closed");
	assert_eq!(&*frame, b"before");

	// Signal the server to send GOAWAY now.
	let _ = goaway_tx.send(());

	// Wait for GOAWAY to arrive.
	let goaway = tokio::time::timeout(TIMEOUT, session.goaway())
		.await
		.expect("goaway timed out")
		.expect("session closed before GOAWAY");
	assert_eq!(&*goaway.uri, "https://new.example.com");
	assert!(session.is_going_away());

	// The existing subscription should still receive the second group written after GOAWAY.
	let mut group_sub2 = tokio::time::timeout(TIMEOUT, track_sub.recv_group())
		.await
		.expect("recv_group2 timed out")
		.expect("recv_group2 failed")
		.expect("track closed after GOAWAY");
	let frame2 = tokio::time::timeout(TIMEOUT, group_sub2.read_frame())
		.await
		.expect("read_frame2 timed out")
		.expect("read_frame2 failed")
		.expect("group2 closed");
	assert_eq!(&*frame2, b"after");

	drop(session);
	server_handle.await.expect("server panicked").expect("server failed");
}

// ── Migration tests (Capability 4) ──────────────────────────────────
// Pass 2: T4.6 (timeout) and T4.7 (dedup overlap) need the mock transport
// for deterministic control over timing and stream delivery order.

/// Helper: run a migration happy-path test for a specific protocol version.
///
/// Two servers A and B on ephemeral ports. Client subscribes on A, reads one
/// frame to confirm delivery, then A drains with URI=B. Client observes GOAWAY,
/// calls Session::reconnect, then subscribes on the new session's broadcast and
/// reads a frame from B.
async fn reconnect_happy_path(version: &str) {
	let version_parsed: moq_net::Version = version.parse().expect("invalid version");

	// ── Server A: will drain ──
	let pub_origin_a = Origin::random().produce();
	let mut broadcast_a = pub_origin_a.create_broadcast("test").expect("create broadcast A");
	let mut track_a = broadcast_a.create_track(Track::new("video")).expect("create track A");
	let mut group_a = track_a.append_group().expect("append group A");
	group_a.write_frame(b"from_a".as_ref()).expect("write frame A");
	group_a.finish().expect("finish group A");

	let mut server_a_config = moq_native::ServerConfig::default();
	server_a_config.bind = Some("[::]:0".to_string());
	server_a_config.tls.generate = vec!["localhost".into()];
	server_a_config.version = vec![version_parsed];
	let mut server_a = server_a_config.init().expect("init server A");
	let addr_a = server_a.local_addr().expect("addr A");

	// ── Server B: migration target ──
	let pub_origin_b = Origin::random().produce();
	let mut broadcast_b = pub_origin_b.create_broadcast("test").expect("create broadcast B");
	let mut track_b = broadcast_b.create_track(Track::new("video")).expect("create track B");
	let mut group_b = track_b.append_group().expect("append group B");
	group_b.write_frame(b"from_b".as_ref()).expect("write frame B");
	group_b.finish().expect("finish group B");

	let mut server_b_config = moq_native::ServerConfig::default();
	server_b_config.bind = Some("[::]:0".to_string());
	server_b_config.tls.generate = vec!["localhost".into()];
	server_b_config.version = vec![version_parsed];
	let mut server_b = server_b_config.init().expect("init server B");
	let addr_b = server_b.local_addr().expect("addr B");

	// Coordination: client signals it subscribed, so server A can drain.
	let (subscribed_tx, subscribed_rx) = tokio::sync::oneshot::channel::<()>();

	// ── Server A task: accept, serve, await subscribe signal, drain with URI=B ──
	let server_a_handle = tokio::spawn(async move {
		let request = server_a.accept().await.expect("accept A");
		let session = request.with_publish(pub_origin_a.consume()).ok().await?;
		let _broadcast = broadcast_a;
		let _track = track_a;

		// Wait for client to confirm subscription before draining.
		let _ = subscribed_rx.await;

		// Drain with URI pointing to server B.
		let draining = session
			.drain()
			.expect("drain")
			.start(format!("moqt://localhost:{}", addr_b.port()));
		tokio::time::timeout(TIMEOUT, draining.complete())
			.await
			.expect("drain.complete() timed out");

		Ok::<_, anyhow::Error>(())
	});

	// ── Server B task: accept and serve (keeps session alive) ──
	let server_b_handle = tokio::spawn(async move {
		let request = server_b.accept().await.expect("accept B");
		let session = request.with_publish(pub_origin_b.consume()).ok().await?;
		let _broadcast = broadcast_b;
		let _track = track_b;
		let _ = session.closed().await;
		Ok::<_, anyhow::Error>(())
	});

	// ── Client ──
	let sub_origin = Origin::random().produce();
	let mut announcements = sub_origin.consume();

	let mut client_config = moq_native::ClientConfig::default();
	client_config.tls.disable_verify = Some(true);
	client_config.version = vec![version_parsed];
	let client = client_config.init().expect("init client");

	let url_a: url::Url = format!("moqt://localhost:{}", addr_a.port()).parse().unwrap();
	let client_for_a = client.clone().with_consume(sub_origin.clone());
	let mut session = tokio::time::timeout(TIMEOUT, client_for_a.connect(url_a))
		.await
		.expect("connect A timed out")
		.expect("connect A failed");

	// Wait for the broadcast announcement from A.
	let (path, bc) = tokio::time::timeout(TIMEOUT, announcements.announced())
		.await
		.expect("announce timed out")
		.expect("origin closed");
	assert_eq!(path.as_str(), "test");
	let bc = bc.expect("expected announce");

	// Subscribe to the track on A and read one frame to confirm delivery.
	let mut track_sub = bc.subscribe_track(&Track::new("video")).expect("subscribe_track");
	let mut group_sub = tokio::time::timeout(TIMEOUT, track_sub.recv_group())
		.await
		.expect("recv_group timed out")
		.expect("recv_group failed")
		.expect("track closed");
	let frame = tokio::time::timeout(TIMEOUT, group_sub.read_frame())
		.await
		.expect("read_frame timed out")
		.expect("read_frame failed")
		.expect("group closed");
	assert_eq!(&*frame, b"from_a");

	// Signal server A to drain now that we have confirmed subscription.
	let _ = subscribed_tx.send(());

	// Wait for GOAWAY from server A.
	let goaway = tokio::time::timeout(TIMEOUT, session.goaway())
		.await
		.expect("session.goaway() timed out")
		.expect("session closed before GOAWAY");
	assert!(!goaway.uri.is_empty());

	// Reconnect: the connector establishes a new session to B using the same origin.
	let migrate_client = client.clone();
	let migrate_origin = sub_origin.clone();
	let new_session = tokio::time::timeout(
		TIMEOUT,
		session.reconnect(
			move |uri: &str| {
				let c = migrate_client.clone().with_consume(migrate_origin.clone());
				let url: url::Url = uri.parse().expect("parse reconnect URI");
				async move {
					c.connect(url)
						.await
						.map_err(|e| moq_net::Error::Transport(e.to_string()))
				}
			},
			moq_net::ReconnectOptions::default(),
		),
	)
	.await
	.expect("reconnect timed out")
	.expect("reconnect failed");

	// The old session is now closed. The new session is connected to B.
	// Wait for B's broadcast announcement to appear in our origin.
	// (The old broadcast from A will be unannounced first, then B's announced.)
	loop {
		let (ann_path, ann_bc) = tokio::time::timeout(TIMEOUT, announcements.announced())
			.await
			.expect("announce B timed out")
			.expect("origin closed after reconnect");
		if ann_path.as_str() == "test" {
			if let Some(bc_b) = ann_bc {
				// Got an announce (not unannounce) for "test" from B.
				// Default filter (LatestObject) so we deterministically receive
				// server B's pre-written group 0. NextGroup boundary semantics are
				// covered by subscribe_next_group_filter_moq_transport_17 over
				// the mock (rs/moq-net/tests/goaway.rs).
				let mut track_sub_b = bc_b.subscribe_track(&Track::new("video")).expect("subscribe_track B");
				let mut group_sub_b = tokio::time::timeout(TIMEOUT, track_sub_b.recv_group())
					.await
					.expect("recv_group B timed out")
					.expect("recv_group B failed")
					.expect("track B closed");
				let frame_b = tokio::time::timeout(TIMEOUT, group_sub_b.read_frame())
					.await
					.expect("read_frame B timed out")
					.expect("read_frame B failed")
					.expect("group B closed");
				assert_eq!(&*frame_b, b"from_b");
				break;
			}
			// Got an unannounce for "test" (old A's broadcast closing). Keep looping.
		}
	}

	// Clean up.
	drop(new_session);
	server_a_handle
		.await
		.expect("server A panicked")
		.expect("server A failed");
	server_b_handle
		.await
		.expect("server B panicked")
		.expect("server B failed");
}

/// T4.1: reconnect happy path on moq-transport-17 (uni-stream GOAWAY).
#[tracing_test::traced_test]
#[tokio::test]
async fn reconnect_happy_path_moq_transport_17() {
	reconnect_happy_path("moq-transport-17").await;
}

/// T4.3: reconnect happy path on moq-transport-14 (control-stream GOAWAY).
#[ignore = "flaky under parallel test execution; deterministic mock-based version lives at rs/moq-net/tests/goaway.rs::reconnect_happy_path_moq_transport_14"]
#[tracing_test::traced_test]
#[tokio::test]
async fn reconnect_happy_path_moq_transport_14() {
	reconnect_happy_path("moq-transport-14").await;
}

/// T4.2: reconnect happy path on moq-transport-19 (draft-19 wire, no Request ID).
#[tracing_test::traced_test]
#[tokio::test]
async fn reconnect_happy_path_moq_transport_19() {
	reconnect_happy_path("moq-transport-19").await;
}

/// T4.5: Non-empty URI connects to a different endpoint.
///
/// Server A drains with URI pointing to server B (different port). The connector
/// is verified to connect to B (not A) by checking the frame payload is "from_b".
/// This is structurally identical to T4.1 since the happy path already uses a
/// non-empty URI pointing to a different port, but exists as a named requirement.
#[tracing_test::traced_test]
#[tokio::test]
async fn reconnect_nonempty_uri_connects_new_endpoint() {
	reconnect_happy_path("moq-transport-17").await;
}

/// T4.9: Connector failure leaves the old session alive and usable.
///
/// Server B is NOT running. Client tries to reconnect. Connector returns error.
/// The old session still delivers frames afterward.
#[tracing_test::traced_test]
#[tokio::test]
async fn reconnect_connector_failure_old_session_survives() {
	let version: moq_net::Version = "moq-transport-17".parse().unwrap();

	// ── Server A (no server B) ──
	let pub_origin_a = Origin::random().produce();
	let mut broadcast_a = pub_origin_a.create_broadcast("test").expect("create broadcast A");
	let mut track_a = broadcast_a.create_track(Track::new("video")).expect("create track A");
	let mut group_a = track_a.append_group().expect("append group A");
	group_a.write_frame(b"before_goaway".as_ref()).expect("write frame");
	group_a.finish().expect("finish group");

	let mut server_a_config = moq_native::ServerConfig::default();
	server_a_config.bind = Some("[::]:0".to_string());
	server_a_config.tls.generate = vec!["localhost".into()];
	server_a_config.version = vec![version];
	let mut server_a = server_a_config.init().expect("init server A");
	let addr_a = server_a.local_addr().expect("addr A");

	let (subscribed_tx, subscribed_rx) = tokio::sync::oneshot::channel::<()>();
	let (post_reconnect_tx, post_reconnect_rx) = tokio::sync::oneshot::channel::<()>();

	let server_a_handle = tokio::spawn(async move {
		let request = server_a.accept().await.expect("accept A");
		let session = request.with_publish(pub_origin_a.consume()).ok().await?;
		let _broadcast = broadcast_a;

		// Wait for subscription.
		let _ = subscribed_rx.await;

		// Drain with a URI that no server is listening on.
		session.drain().expect("drain").start("moqt://localhost:1");

		// Wait for the client to attempt reconnection and fail.
		let _ = post_reconnect_rx.await;

		// Write another frame AFTER the failed reconnect to prove the session is alive.
		let mut group2 = track_a.append_group().expect("append group2");
		group2
			.write_frame(b"after_failed_reconnect".as_ref())
			.expect("write frame2");
		group2.finish().expect("finish group2");

		// Keep alive until client disconnects.
		let _ = session.closed().await;
		Ok::<_, anyhow::Error>(())
	});

	// ── Client ──
	let sub_origin = Origin::random().produce();
	let mut announcements = sub_origin.consume();

	let mut client_config = moq_native::ClientConfig::default();
	client_config.tls.disable_verify = Some(true);
	client_config.version = vec![version];
	let client = client_config.init().expect("init client");

	let url_a: url::Url = format!("moqt://localhost:{}", addr_a.port()).parse().unwrap();
	let client_for_a = client.clone().with_consume(sub_origin.clone());
	let mut session = tokio::time::timeout(TIMEOUT, client_for_a.connect(url_a))
		.await
		.expect("connect A timed out")
		.expect("connect A failed");

	// Wait for broadcast.
	let (path, bc) = tokio::time::timeout(TIMEOUT, announcements.announced())
		.await
		.expect("announce timed out")
		.expect("origin closed");
	assert_eq!(path.as_str(), "test");
	let bc = bc.expect("expected announce");

	// Subscribe and read first frame.
	let mut track_sub = bc.subscribe_track(&Track::new("video")).expect("subscribe_track");
	let mut group_sub = tokio::time::timeout(TIMEOUT, track_sub.recv_group())
		.await
		.expect("recv_group timed out")
		.expect("recv_group failed")
		.expect("track closed");
	let frame = tokio::time::timeout(TIMEOUT, group_sub.read_frame())
		.await
		.expect("read_frame timed out")
		.expect("read_frame failed")
		.expect("group closed");
	assert_eq!(&*frame, b"before_goaway");

	// Signal server to drain.
	let _ = subscribed_tx.send(());

	// Wait for GOAWAY.
	let goaway = tokio::time::timeout(TIMEOUT, session.goaway())
		.await
		.expect("session.goaway() timed out")
		.expect("session closed before GOAWAY");
	assert_eq!(&*goaway.uri, "moqt://localhost:1");

	// Attempt reconnect with a connector that fails immediately.
	let result = tokio::time::timeout(
		Duration::from_secs(5),
		session.reconnect(
			|_uri: &str| async move { Err(moq_net::Error::Transport("connection refused".to_string())) },
			moq_net::ReconnectOptions::default(),
		),
	)
	.await
	.expect("reconnect timed out");

	// Reconnect should have failed.
	assert!(result.is_err(), "reconnect should fail when connector fails");

	// The old session should still be alive and delivering.
	// Signal server to write more data.
	let _ = post_reconnect_tx.send(());

	// Read the second frame from the still-alive old session.
	let mut group_sub2 = tokio::time::timeout(TIMEOUT, track_sub.recv_group())
		.await
		.expect("recv_group2 timed out")
		.expect("recv_group2 failed")
		.expect("track closed after failed reconnect");
	let frame2 = tokio::time::timeout(TIMEOUT, group_sub2.read_frame())
		.await
		.expect("read_frame2 timed out")
		.expect("read_frame2 failed")
		.expect("group2 closed");
	assert_eq!(&*frame2, b"after_failed_reconnect");

	drop(session);
	server_a_handle
		.await
		.expect("server A panicked")
		.expect("server A failed");
}

/// T4.10: Multiple tracks all get re-subscribed on the new session after reconnect.
///
/// Client subscribes to 3 tracks on server A. After reconnect to B, all 3 tracks
/// deliver frames from B.
#[tracing_test::traced_test]
#[tokio::test]
async fn reconnect_multiple_tracks_all_resubscribed() {
	let version: moq_net::Version = "moq-transport-17".parse().unwrap();

	// ── Server A ──
	let pub_origin_a = Origin::random().produce();
	let mut broadcast_a = pub_origin_a.create_broadcast("test").expect("create broadcast A");
	let tracks_a: Vec<_> = ["t1", "t2", "t3"]
		.iter()
		.map(|name| {
			let mut track = broadcast_a.create_track(Track::new(*name)).expect("create track A");
			let mut group = track.append_group().expect("append group");
			let payload = format!("a_{name}");
			group.write_frame(payload.into_bytes()).expect("write frame");
			group.finish().expect("finish group");
			track
		})
		.collect();

	let mut server_a_config = moq_native::ServerConfig::default();
	server_a_config.bind = Some("[::]:0".to_string());
	server_a_config.tls.generate = vec!["localhost".into()];
	server_a_config.version = vec![version];
	let mut server_a = server_a_config.init().expect("init server A");
	let addr_a = server_a.local_addr().expect("addr A");

	// ── Server B ──
	let pub_origin_b = Origin::random().produce();
	let mut broadcast_b = pub_origin_b.create_broadcast("test").expect("create broadcast B");
	let tracks_b: Vec<_> = ["t1", "t2", "t3"]
		.iter()
		.map(|name| {
			let mut track = broadcast_b.create_track(Track::new(*name)).expect("create track B");
			let mut group = track.append_group().expect("append group");
			let payload = format!("b_{name}");
			group.write_frame(payload.into_bytes()).expect("write frame");
			group.finish().expect("finish group");
			track
		})
		.collect();

	let mut server_b_config = moq_native::ServerConfig::default();
	server_b_config.bind = Some("[::]:0".to_string());
	server_b_config.tls.generate = vec!["localhost".into()];
	server_b_config.version = vec![version];
	let mut server_b = server_b_config.init().expect("init server B");
	let addr_b = server_b.local_addr().expect("addr B");

	let (subscribed_tx, subscribed_rx) = tokio::sync::oneshot::channel::<()>();

	let server_a_handle = tokio::spawn(async move {
		let request = server_a.accept().await.expect("accept A");
		let session = request.with_publish(pub_origin_a.consume()).ok().await?;
		let _broadcast = broadcast_a;
		let _tracks = tracks_a;

		let _ = subscribed_rx.await;

		let draining = session
			.drain()
			.expect("drain")
			.start(format!("moqt://localhost:{}", addr_b.port()));
		tokio::time::timeout(TIMEOUT, draining.complete())
			.await
			.expect("drain.complete() timed out");
		Ok::<_, anyhow::Error>(())
	});

	let server_b_handle = tokio::spawn(async move {
		let request = server_b.accept().await.expect("accept B");
		let session = request.with_publish(pub_origin_b.consume()).ok().await?;
		let _broadcast = broadcast_b;
		let _tracks = tracks_b;
		let _ = session.closed().await;
		Ok::<_, anyhow::Error>(())
	});

	// ── Client ──
	let sub_origin = Origin::random().produce();
	let mut announcements = sub_origin.consume();

	let mut client_config = moq_native::ClientConfig::default();
	client_config.tls.disable_verify = Some(true);
	client_config.version = vec![version];
	let client = client_config.init().expect("init client");

	let url_a: url::Url = format!("moqt://localhost:{}", addr_a.port()).parse().unwrap();
	let client_for_a = client.clone().with_consume(sub_origin.clone());
	let mut session = tokio::time::timeout(TIMEOUT, client_for_a.connect(url_a))
		.await
		.expect("connect A timed out")
		.expect("connect A failed");

	// Wait for broadcast from A.
	let (path, bc) = tokio::time::timeout(TIMEOUT, announcements.announced())
		.await
		.expect("announce timed out")
		.expect("origin closed");
	assert_eq!(path.as_str(), "test");
	let bc = bc.expect("expected announce from A");

	// Subscribe to all 3 tracks and read one frame each.
	for name in ["t1", "t2", "t3"] {
		let mut track_sub = bc.subscribe_track(&Track::new(name)).expect("subscribe");
		let mut group_sub = tokio::time::timeout(TIMEOUT, track_sub.recv_group())
			.await
			.expect("recv_group timed out")
			.expect("recv_group failed")
			.expect("track closed");
		let frame = tokio::time::timeout(TIMEOUT, group_sub.read_frame())
			.await
			.expect("read_frame timed out")
			.expect("read_frame failed")
			.expect("group closed");
		assert_eq!(&*frame, format!("a_{name}").as_bytes());
	}

	// Signal server A to drain.
	let _ = subscribed_tx.send(());

	// Wait for GOAWAY.
	let goaway = tokio::time::timeout(TIMEOUT, session.goaway())
		.await
		.expect("goaway timed out")
		.expect("session closed before GOAWAY");
	assert!(!goaway.uri.is_empty());

	// Reconnect.
	let reconnect_client = client.clone();
	let reconnect_origin = sub_origin.clone();
	let new_session = tokio::time::timeout(
		TIMEOUT,
		session.reconnect(
			move |uri: &str| {
				let c = reconnect_client.clone().with_consume(reconnect_origin.clone());
				let url: url::Url = uri.parse().expect("parse URI");
				async move {
					c.connect(url)
						.await
						.map_err(|e| moq_net::Error::Transport(e.to_string()))
				}
			},
			moq_net::ReconnectOptions::default(),
		),
	)
	.await
	.expect("reconnect timed out")
	.expect("reconnect failed");

	// Wait for B's broadcast (skip unannounce from A).
	loop {
		let (ann_path, ann_bc) = tokio::time::timeout(TIMEOUT, announcements.announced())
			.await
			.expect("announce B timed out")
			.expect("origin closed");
		if ann_path.as_str() == "test" {
			if let Some(bc_b) = ann_bc {
				// Subscribe to all 3 tracks on B and verify frames.
				for name in ["t1", "t2", "t3"] {
					let mut track_sub = bc_b.subscribe_track(&Track::new(name)).expect("subscribe B");
					let mut group_sub = tokio::time::timeout(TIMEOUT, track_sub.recv_group())
						.await
						.expect("recv_group B timed out")
						.expect("recv_group B failed")
						.expect("track B closed");
					let frame = tokio::time::timeout(TIMEOUT, group_sub.read_frame())
						.await
						.expect("read_frame B timed out")
						.expect("read_frame B failed")
						.expect("group B closed");
					assert_eq!(&*frame, format!("b_{name}").as_bytes());
				}
				break;
			}
		}
	}

	drop(new_session);
	server_a_handle
		.await
		.expect("server A panicked")
		.expect("server A failed");
	server_b_handle
		.await
		.expect("server B panicked")
		.expect("server B failed");
}
