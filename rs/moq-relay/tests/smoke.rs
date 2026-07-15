//! End-to-end smoke test through a real moq-relay.
//!
//! Stands up the relay's actual axum + auth + cluster stack on a free port,
//! connects a publisher and a subscriber via WebSocket, and confirms that
//! a frame round-trips with the newest moq-lite version on both sides. The
//! version assertion is the regression guard for the
//! "axum-only-advertises-bare-`webtransport`" bug that silently downgraded
//! relay clients to moq-lite-02.

use std::{net::TcpListener, sync::atomic::AtomicU64, time::Duration};

use moq_native::moq_net::{self, Origin, Track};
use moq_relay::{AuthConfig, Cluster, ClusterConfig, Connection, PublicConfig, Web, WebConfig, WebState};

const TIMEOUT: Duration = Duration::from_secs(10);

/// The newest moq-lite ALPN both sides should converge on. Derived from
/// `moq_net::ALPNS` so a future bump (e.g. lite-05 promoted out of WIP)
/// doesn't break this test independently of the production negotiation.
/// We filter on the `moq-lite-` prefix specifically; the relay smoke test
/// is asserting lite behavior, not IETF moqt drafts.
fn newest_lite_version() -> moq_net::Version {
	moq_net::ALPNS
		.iter()
		.copied()
		.find(|alpn| alpn.starts_with("moq-lite-"))
		.expect("no moq-lite ALPN in moq_net::ALPNS")
		.parse()
		.expect("parse newest lite ALPN as a Version")
}

async fn build_web(port: u16, ws: bool) -> Web {
	// Crypto provider is process-global; reinstalls after the first one are
	// no-ops, but the test binary may run before any other moq code does.
	let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

	// AuthConfig with public Simple([""]) lets any path through. Simple is
	// deprecated but matches what `simple_public("")` in moq-relay's auth
	// tests uses, and the relay still honors it.
	#[allow(deprecated)]
	let public = PublicConfig::Simple(vec![String::new()]);
	let mut auth_config = AuthConfig::default();
	auth_config.public = Some(public);
	let auth = auth_config
		.init(&moq_native::tls::Client::default())
		.await
		.expect("auth init");

	let cluster = Cluster::new(ClusterConfig::default()).expect("cluster init");

	// moq_native::Server is needed for `tls_info`, even though we never
	// expose HTTPS or QUIC in this test. Binding QUIC to `[::]:0` picks an
	// unused UDP port that we ignore.
	let mut server_config = moq_native::ServerConfig::default();
	server_config.bind = Some("[::]:0".to_string());
	server_config.tls.generate = vec!["localhost".into()];
	let server = server_config.init().expect("server init");

	let mut web_config = WebConfig::default();
	web_config.ws = ws;
	web_config.http.listen = Some(format!("127.0.0.1:{port}").parse().expect("parse listen"));

	Web::new(
		WebState {
			auth,
			cluster,
			tls_info: server.tls_info(),
			conn_id: AtomicU64::new(0),
		},
		web_config,
	)
}

fn free_tcp_port() -> u16 {
	// Pick a free port for HTTP, then immediately drop the probe listener
	// so axum_server can bind it. There's a tiny race window where the
	// kernel could hand the same port to another process, but on localhost
	// in a single-test process it's safe in practice.
	let probe = TcpListener::bind("127.0.0.1:0").expect("bind probe");
	let port = probe.local_addr().expect("local addr").port();
	drop(probe);
	port
}

async fn wait_for_http(port: u16, server_result: &mut tokio::sync::oneshot::Receiver<anyhow::Result<()>>) {
	// Wait for axum_server to bind. A short poll is more reliable than a
	// fixed sleep when CI is slow.
	let deadline = std::time::Instant::now() + Duration::from_secs(5);
	loop {
		if tokio::net::TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
			break;
		}
		match server_result.try_recv() {
			Ok(Ok(())) => panic!("relay web server exited before listening"),
			Ok(Err(err)) => panic!("relay web server failed before listening: {err:#}"),
			Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {}
			Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
				panic!("relay web server task ended before listening")
			}
		}
		if std::time::Instant::now() >= deadline {
			panic!("relay http listener never became ready on port {port}");
		}
		tokio::time::sleep(Duration::from_millis(25)).await;
	}
}

/// The shared bootstrap: stand up a relay listening on `127.0.0.1:<free-port>`
/// with fully public auth, and return the port plus an abort handle for the
/// spawned web server.
async fn spawn_relay() -> (u16, tokio::task::JoinHandle<()>) {
	let port = free_tcp_port();
	let web = build_web(port, true).await;

	let (server_result_tx, mut server_result_rx) = tokio::sync::oneshot::channel();
	let handle = tokio::spawn(async move {
		// `Web::run` only returns on error; in tests we abort it at teardown.
		let _ = server_result_tx.send(web.run().await);
	});

	wait_for_http(port, &mut server_result_rx).await;

	(port, handle)
}

fn client() -> moq_native::Client {
	let mut config = moq_native::ClientConfig::default();
	config.tls.disable_verify = Some(true);
	// Zero head start so the WebSocket path runs immediately.
	config.websocket.delay = None;
	config.init().expect("client init")
}

/// Connect a publisher and a subscriber to a real relay over `ws://`, push
/// one frame end-to-end, and assert both sides see the newest moq-lite ALPN.
/// Regression for the `serve_ws` downgrade to Lite02.
#[tokio::test]
async fn relay_websocket_round_trip_uses_newest_version() {
	let (port, web_handle) = spawn_relay().await;
	let url: url::Url = format!("ws://127.0.0.1:{port}/smoke").parse().expect("parse url");
	let expected_version = newest_lite_version();

	// ── publisher ───────────────────────────────────────────────────
	let pub_origin = Origin::random().produce();
	let mut broadcast = pub_origin.create_broadcast("test").expect("create broadcast");
	let mut track = broadcast.create_track(Track::new("video")).expect("create track");
	let mut group = track.append_group().expect("append group");
	group.write_frame(b"hello".as_ref()).expect("write frame");
	group.finish().expect("finish group");

	let pub_session = tokio::time::timeout(
		TIMEOUT,
		client().with_publish(pub_origin.consume()).connect(url.clone()),
	)
	.await
	.expect("publisher connect timeout")
	.expect("publisher connect failed");
	assert_eq!(
		pub_session.version(),
		expected_version,
		"publisher negotiated stale version"
	);

	// ── subscriber ──────────────────────────────────────────────────
	let sub_origin = Origin::random().produce();
	let mut announcements = sub_origin.consume();

	let sub_session = tokio::time::timeout(TIMEOUT, client().with_consume(sub_origin).connect(url))
		.await
		.expect("subscriber connect timeout")
		.expect("subscriber connect failed");
	assert_eq!(
		sub_session.version(),
		expected_version,
		"subscriber negotiated stale version"
	);

	// ── data path ───────────────────────────────────────────────────
	let (path, bc) = tokio::time::timeout(TIMEOUT, announcements.announced())
		.await
		.expect("announcement timeout")
		.expect("origin closed");
	// Auth root for `/smoke` is "smoke"; the broadcast "test" announces underneath.
	assert_eq!(path.as_str(), "test");
	let bc = bc.expect("expected announce, got unannounce");

	let mut track_sub = bc.subscribe_track(&Track::new("video")).expect("subscribe_track");
	let mut group_sub = tokio::time::timeout(TIMEOUT, track_sub.recv_group())
		.await
		.expect("recv_group timeout")
		.expect("recv_group failed")
		.expect("track closed prematurely");
	let frame = tokio::time::timeout(TIMEOUT, group_sub.read_frame())
		.await
		.expect("read_frame timeout")
		.expect("read_frame failed")
		.expect("group closed prematurely");
	assert_eq!(&*frame, b"hello");

	// Hold the producers until after data is read; dropping them earlier
	// would close the publishing side of the broadcast.
	drop(track);
	drop(broadcast);

	drop(pub_session);
	drop(sub_session);
	web_handle.abort();
}

#[tokio::test]
async fn relay_web_serves_merged_routes() {
	tokio::time::pause();
	let port = free_tcp_port();
	let web = build_web(port, false).await;
	let app = web
		.routes()
		.route("/embedded", axum::routing::get(|| async { "embedded\n" }));

	let (server_result_tx, mut server_result_rx) = tokio::sync::oneshot::channel();
	let handle = tokio::spawn(async move {
		let _ = server_result_tx.send(web.serve(app).await);
	});

	wait_for_http(port, &mut server_result_rx).await;

	let body = reqwest::get(format!("http://127.0.0.1:{port}/embedded"))
		.await
		.expect("fetch embedded route")
		.text()
		.await
		.expect("read embedded response");
	assert_eq!(body, "embedded\n");

	handle.abort();
}

/// A client that dials a bare `host:port` with no path must still get a
/// WebSocket upgrade at the root, not the landing page. The empty path is the
/// root auth scope (same as the internal listener). Regression for the
/// `/{*path}`-only route, which left bare-URL clients (e.g.
/// `moqsink url="https://host:4443"`) with a silently dead WS fallback.
#[tokio::test]
async fn relay_websocket_root_path_upgrades() {
	let (port, web_handle) = spawn_relay().await;
	// No path: the URL is just host:port, so the WS handshake targets "/".
	let url: url::Url = format!("ws://127.0.0.1:{port}").parse().expect("parse url");

	// ── publisher ───────────────────────────────────────────────────
	let pub_origin = Origin::random().produce();
	let mut broadcast = pub_origin.create_broadcast("test").expect("create broadcast");
	let mut track = broadcast.create_track(Track::new("video")).expect("create track");
	let mut group = track.append_group().expect("append group");
	group.write_frame(b"hello".as_ref()).expect("write frame");
	group.finish().expect("finish group");

	let pub_session = tokio::time::timeout(
		TIMEOUT,
		client().with_publish(pub_origin.consume()).connect(url.clone()),
	)
	.await
	.expect("publisher connect timeout")
	.expect("publisher connect failed (root-path WS upgrade)");

	// ── subscriber ──────────────────────────────────────────────────
	let sub_origin = Origin::random().produce();
	let mut announcements = sub_origin.consume();
	let sub_session = tokio::time::timeout(TIMEOUT, client().with_consume(sub_origin).connect(url))
		.await
		.expect("subscriber connect timeout")
		.expect("subscriber connect failed (root-path WS upgrade)");

	// ── data path ───────────────────────────────────────────────────
	// The root auth scope is the empty path, so the broadcast announces at its
	// own name with no prefix.
	let (path, bc) = tokio::time::timeout(TIMEOUT, announcements.announced())
		.await
		.expect("announcement timeout")
		.expect("origin closed");
	assert_eq!(path.as_str(), "test");
	let bc = bc.expect("expected announce, got unannounce");

	let mut track_sub = bc.subscribe_track(&Track::new("video")).expect("subscribe_track");
	let mut group_sub = tokio::time::timeout(TIMEOUT, track_sub.recv_group())
		.await
		.expect("recv_group timeout")
		.expect("recv_group failed")
		.expect("track closed prematurely");
	let frame = tokio::time::timeout(TIMEOUT, group_sub.read_frame())
		.await
		.expect("read_frame timeout")
		.expect("read_frame failed")
		.expect("group closed prematurely");
	assert_eq!(&*frame, b"hello");

	drop(track);
	drop(broadcast);
	drop(pub_session);
	drop(sub_session);
	web_handle.abort();
}

/// Two publish-only clients (each `with_publish`, no `with_consume`) coexist on one relay;
/// a single subscriber sees broadcasts forwarded from both. Verifies that multiple
/// publish-only connections don't interfere with each other or get torn down.
#[tokio::test]
async fn two_publish_only_clients_coexist() {
	let (port, web_handle) = spawn_relay().await;
	let url: url::Url = format!("ws://127.0.0.1:{port}/smoke").parse().expect("parse url");

	// ── two publish-only publishers, each serving a distinct broadcast ──
	let pub_a = Origin::random().produce();
	let mut broadcast_a = pub_a.create_broadcast("alpha").expect("create broadcast a");
	let mut track_a = broadcast_a.create_track(Track::new("video")).expect("create track a");
	track_a
		.append_group()
		.expect("append group a")
		.write_frame(b"a".as_ref())
		.expect("write frame a");

	let pub_b = Origin::random().produce();
	let mut broadcast_b = pub_b.create_broadcast("beta").expect("create broadcast b");
	let mut track_b = broadcast_b.create_track(Track::new("video")).expect("create track b");
	track_b
		.append_group()
		.expect("append group b")
		.write_frame(b"b".as_ref())
		.expect("write frame b");

	let sess_a = tokio::time::timeout(TIMEOUT, client().with_publish(pub_a.consume()).connect(url.clone()))
		.await
		.expect("publisher a connect timeout")
		.expect("publisher a connect failed");
	let sess_b = tokio::time::timeout(TIMEOUT, client().with_publish(pub_b.consume()).connect(url.clone()))
		.await
		.expect("publisher b connect timeout")
		.expect("publisher b connect failed");

	// ── one subscriber should see broadcasts from both publish-only clients ──
	let sub_origin = Origin::random().produce();
	let mut announcements = sub_origin.consume();
	let sub_session = tokio::time::timeout(TIMEOUT, client().with_consume(sub_origin).connect(url))
		.await
		.expect("subscriber connect timeout")
		.expect("subscriber connect failed");

	let mut seen = std::collections::HashSet::new();
	while seen.len() < 2 {
		let (path, bc) = tokio::time::timeout(TIMEOUT, announcements.announced())
			.await
			.expect("announcement timeout")
			.expect("origin closed");
		if bc.is_some() {
			seen.insert(path.as_str().to_owned());
		}
	}
	assert!(
		seen.contains("alpha") && seen.contains("beta"),
		"expected both publish-only broadcasts, saw {seen:?}"
	);

	// Hold producers until announcements are observed.
	drop(track_a);
	drop(broadcast_a);
	drop(track_b);
	drop(broadcast_b);

	drop(sess_a);
	drop(sess_b);
	drop(sub_session);
	web_handle.abort();
}

/// Run the relay's accept loop over a stream-only server (no QUIC), the same path
/// `main.rs` uses. Authenticates through the shared [`Auth`], here with fully
/// public access (`--auth-public ""`) so no-JWT stream clients get the root.
async fn spawn_stream_relay(config: moq_native::ServerConfig) -> tokio::task::JoinHandle<()> {
	let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

	let mut server = config.init().expect("server init");

	// Public Simple([""]) lets any no-JWT stream client through at the root.
	#[allow(deprecated)]
	let public = PublicConfig::Simple(vec![String::new()]);
	let mut auth_config = AuthConfig::default();
	auth_config.public = Some(public);
	let auth = auth_config
		.init(&moq_native::tls::Client::default())
		.await
		.expect("auth init");

	let cluster = Cluster::new(ClusterConfig::default()).expect("cluster init");

	tokio::spawn(async move {
		let mut id = 0;
		while let Some(request) = server.accept().await {
			let conn = Connection {
				id,
				request,
				cluster: cluster.clone(),
				auth: auth.clone(),
				drain_timeout: std::time::Duration::from_secs(moq_relay::DEFAULT_DRAIN_TIMEOUT_SECS),
			};
			id += 1;
			tokio::spawn(async move {
				let _ = conn.run().await;
			});
		}
	})
}

/// Stand up the relay listening only on a plain-TCP qmux `--server-bind` on a
/// free loopback port, with fully public auth (no-JWT => whole root). Returns
/// the port and an abort handle.
async fn spawn_internal_relay() -> (u16, tokio::task::JoinHandle<()>) {
	// Pick a free TCP port, then drop the probe so the listener can bind it.
	let probe = TcpListener::bind("127.0.0.1:0").expect("bind probe");
	let port = probe.local_addr().expect("local addr").port();
	drop(probe);

	// Stream-only: a TCP listener with no `--server-bind`, so no QUIC.
	let mut config = moq_native::ServerConfig::default();
	config.tcp.bind = Some(format!("127.0.0.1:{port}").parse().expect("parse addr"));
	let handle = spawn_stream_relay(config).await;

	let deadline = std::time::Instant::now() + Duration::from_secs(5);
	loop {
		if tokio::net::TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
			break;
		}
		if std::time::Instant::now() >= deadline {
			panic!("internal listener never became ready on port {port}");
		}
		tokio::time::sleep(Duration::from_millis(25)).await;
	}

	(port, handle)
}

/// Connect a publisher and subscriber to a stream `--server-bind` over `tcp://`
/// (plain TCP, no TLS, no JWT) and confirm a frame round-trips. Exercises the
/// qmux-over-TCP transport and no-JWT resolution through public auth.
#[tokio::test]
async fn internal_tcp_round_trip() {
	let (port, handle) = spawn_internal_relay().await;
	// The raw-TCP transport dials host:port only; any URL path is ignored.
	let url: url::Url = format!("tcp://127.0.0.1:{port}").parse().expect("parse url");
	let expected_version = newest_lite_version();

	// ── publisher ───────────────────────────────────────────────────
	let pub_origin = Origin::random().produce();
	let mut broadcast = pub_origin.create_broadcast("test").expect("create broadcast");
	let mut track = broadcast.create_track(Track::new("video")).expect("create track");
	let mut group = track.append_group().expect("append group");
	group.write_frame(b"hello".as_ref()).expect("write frame");
	group.finish().expect("finish group");

	let pub_session = tokio::time::timeout(
		TIMEOUT,
		client().with_publish(pub_origin.consume()).connect(url.clone()),
	)
	.await
	.expect("publisher connect timeout")
	.expect("publisher connect failed");
	assert_eq!(
		pub_session.version(),
		expected_version,
		"publisher should negotiate the newest moq-lite version in-band over TCP"
	);

	// ── subscriber ──────────────────────────────────────────────────
	let sub_origin = Origin::random().produce();
	let mut announcements = sub_origin.consume();
	let sub_session = tokio::time::timeout(TIMEOUT, client().with_consume(sub_origin).connect(url))
		.await
		.expect("subscriber connect timeout")
		.expect("subscriber connect failed");

	// ── data path ───────────────────────────────────────────────────
	// The internal listener grants the empty root, so the broadcast announces
	// at its own name with no path prefix.
	let (path, bc) = tokio::time::timeout(TIMEOUT, announcements.announced())
		.await
		.expect("announcement timeout")
		.expect("origin closed");
	assert_eq!(path.as_str(), "test");
	let bc = bc.expect("expected announce, got unannounce");

	let mut track_sub = bc.subscribe_track(&Track::new("video")).expect("subscribe_track");
	let mut group_sub = tokio::time::timeout(TIMEOUT, track_sub.recv_group())
		.await
		.expect("recv_group timeout")
		.expect("recv_group failed")
		.expect("track closed prematurely");
	let frame = tokio::time::timeout(TIMEOUT, group_sub.read_frame())
		.await
		.expect("read_frame timeout")
		.expect("read_frame failed")
		.expect("group closed prematurely");
	assert_eq!(&*frame, b"hello");

	drop(track);
	drop(broadcast);
	drop(pub_session);
	drop(sub_session);
	handle.abort();
}

/// Stand up a stream `--server-bind` on a Unix socket and return the socket path
/// plus an abort handle.
#[cfg(unix)]
async fn spawn_internal_unix_relay() -> (std::path::PathBuf, tokio::task::JoinHandle<()>) {
	// Keep the path short: macOS caps AF_UNIX paths around 104 bytes, and the
	// system temp dir is long. /tmp is fine on macOS and Linux.
	let path = std::path::PathBuf::from(format!("/tmp/moq-internal-{}.sock", std::process::id()));

	// Stream-only: a Unix listener with no `--server-bind`, so no QUIC.
	let mut config = moq_native::ServerConfig::default();
	config.unix.bind = Some(path.clone());
	let handle = spawn_stream_relay(config).await;

	// Wait for the socket file to appear.
	let deadline = std::time::Instant::now() + Duration::from_secs(5);
	loop {
		if tokio::net::UnixStream::connect(&path).await.is_ok() {
			break;
		}
		if std::time::Instant::now() >= deadline {
			panic!("internal Unix listener never became ready at {}", path.display());
		}
		tokio::time::sleep(Duration::from_millis(25)).await;
	}

	(path, handle)
}

/// Connect over `unix://` (qmux on a Unix socket) and confirm a frame
/// round-trips. Also asserts both sides land on the newest moq-lite version,
/// which proves the in-band ALPN negotiation populated the protocol.
#[cfg(unix)]
#[tokio::test]
async fn internal_unix_round_trip() {
	let (path, handle) = spawn_internal_unix_relay().await;
	// `unix://` + an absolute path yields the triple-slash form the client expects.
	let url: url::Url = format!("unix://{}", path.display()).parse().expect("parse url");
	let expected_version = newest_lite_version();

	// ── publisher ───────────────────────────────────────────────────
	let pub_origin = Origin::random().produce();
	let mut broadcast = pub_origin.create_broadcast("test").expect("create broadcast");
	let mut track = broadcast.create_track(Track::new("video")).expect("create track");
	let mut group = track.append_group().expect("append group");
	group.write_frame(b"hello".as_ref()).expect("write frame");
	group.finish().expect("finish group");

	let pub_session = tokio::time::timeout(
		TIMEOUT,
		client().with_publish(pub_origin.consume()).connect(url.clone()),
	)
	.await
	.expect("publisher connect timeout")
	.expect("publisher connect failed");
	assert_eq!(
		pub_session.version(),
		expected_version,
		"publisher should negotiate the newest moq-lite version in-band over the Unix socket"
	);

	// ── subscriber ──────────────────────────────────────────────────
	let sub_origin = Origin::random().produce();
	let mut announcements = sub_origin.consume();
	let sub_session = tokio::time::timeout(TIMEOUT, client().with_consume(sub_origin).connect(url))
		.await
		.expect("subscriber connect timeout")
		.expect("subscriber connect failed");

	// ── data path ───────────────────────────────────────────────────
	let (announced_path, bc) = tokio::time::timeout(TIMEOUT, announcements.announced())
		.await
		.expect("announcement timeout")
		.expect("origin closed");
	assert_eq!(announced_path.as_str(), "test");
	let bc = bc.expect("expected announce, got unannounce");

	let mut track_sub = bc.subscribe_track(&Track::new("video")).expect("subscribe_track");
	let mut group_sub = tokio::time::timeout(TIMEOUT, track_sub.recv_group())
		.await
		.expect("recv_group timeout")
		.expect("recv_group failed")
		.expect("track closed prematurely");
	let frame = tokio::time::timeout(TIMEOUT, group_sub.read_frame())
		.await
		.expect("read_frame timeout")
		.expect("read_frame failed")
		.expect("group closed prematurely");
	assert_eq!(&*frame, b"hello");

	drop(track);
	drop(broadcast);
	drop(pub_session);
	drop(sub_session);
	handle.abort();
}

/// `/health` is a liveness probe that always returns `200 ok`.
#[tokio::test]
async fn health_endpoint_reports_ok() {
	let (port, web_handle) = spawn_relay().await;

	let resp = tokio::time::timeout(TIMEOUT, reqwest::get(format!("http://127.0.0.1:{port}/health")))
		.await
		.expect("health request timeout")
		.expect("health request failed");

	assert_eq!(resp.status(), reqwest::StatusCode::OK);
	let body = resp.text().await.expect("health body");
	assert_eq!(body, "ok\n");

	web_handle.abort();
}

// ══════════════════════════════════════════════════════════════════════════════
// T5: Relay upstream GOAWAY transparent reconnect
// ══════════════════════════════════════════════════════════════════════════════

/// Helper: spawn a relay whose cluster dials `upstream_url` (TCP), serving
/// downstream connections on its own TCP listener. Returns (downstream port, join handle).
async fn spawn_relay_with_upstream(upstream_url: &str) -> (u16, tokio::task::JoinHandle<()>) {
	let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

	let probe = TcpListener::bind("127.0.0.1:0").expect("bind probe");
	let port = probe.local_addr().expect("local addr").port();
	drop(probe);

	let mut server_config = moq_native::ServerConfig::default();
	server_config.tcp.bind = Some(format!("127.0.0.1:{port}").parse().expect("parse addr"));
	let mut server = server_config.init().expect("server init");

	#[allow(deprecated)]
	let public = PublicConfig::Simple(vec![String::new()]);
	let mut auth_config = AuthConfig::default();
	auth_config.public = Some(public);
	let auth = auth_config
		.init(&moq_native::tls::Client::default())
		.await
		.expect("auth init");

	let mut cluster_config = ClusterConfig::default();
	cluster_config.connect = vec![upstream_url.to_string()];
	let cluster = Cluster::new(cluster_config).expect("cluster init");

	// The relay needs a QUIC client for the cluster dial. We configure it for
	// TCP only (matching the upstream), with TLS verification disabled.
	let mut client_config = moq_native::ClientConfig::default();
	client_config.tls.disable_verify = Some(true);
	client_config.websocket.delay = None;
	let native_client = client_config.init().expect("client init");

	let cluster_with_client = cluster.clone().with_client(native_client);

	let handle = tokio::spawn(async move {
		// Run cluster dialing in the background.
		let cluster_run = cluster_with_client.clone();
		tokio::spawn(async move {
			let _ = cluster_run.run().await;
		});

		// Accept downstream connections.
		let mut id = 0;
		while let Some(request) = server.accept().await {
			let conn = Connection {
				id,
				request,
				cluster: cluster_with_client.clone(),
				auth: auth.clone(),
				drain_timeout: std::time::Duration::from_secs(moq_relay::DEFAULT_DRAIN_TIMEOUT_SECS),
			};
			id += 1;
			tokio::spawn(async move {
				let _ = conn.run().await;
			});
		}
	});

	// Wait for the downstream listener to be ready.
	let deadline = std::time::Instant::now() + Duration::from_secs(5);
	loop {
		if tokio::net::TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
			break;
		}
		if std::time::Instant::now() >= deadline {
			panic!("relay listener never became ready on port {port}");
		}
		tokio::time::sleep(Duration::from_millis(25)).await;
	}

	(port, handle)
}

/// Helper: spawn a TCP server that accepts moq connections on a free port.
/// Returns (port, server). The caller can then accept on the server.
async fn spawn_origin_server() -> (u16, moq_native::Server) {
	let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
	let probe = TcpListener::bind("127.0.0.1:0").expect("bind probe");
	let port = probe.local_addr().expect("local addr").port();
	drop(probe);

	let mut config = moq_native::ServerConfig::default();
	config.tcp.bind = Some(format!("127.0.0.1:{port}").parse().expect("parse addr"));
	let server = config.init().expect("origin server init");
	(port, server)
}

/// T5.1: Seamless continuity across aligned-sibling failover via the track-ended
/// path. Origin A publishes group 0, then sends GOAWAY (with redirect URI to
/// origin B) and its track ends. The relay reconnects to origin B which produces
/// group 1. The downstream subscriber receives group 0 (frame-a1) then group 1
/// (frame-b1) contiguously with no gap, no duplicate, and no subscription error.
/// The GOAWAY is NOT propagated to downstream.
///
/// Precondition: origin B is an aligned sibling that would continue the stream
/// at group sequence 1. The wire SUBSCRIBE from the relay carries start_group=1
/// (the resume position from CHANGE 1) so origin B skips already-delivered groups.
#[tokio::test]
async fn relay_upstream_goaway_transparent_reconnect() {
	let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

	// ── Origin A: will publish group 0, then drain with URI pointing to B ──
	let (port_a, mut server_a) = spawn_origin_server().await;

	// ── Origin B: aligned sibling, accepts the relay's reconnection ──
	let (port_b, mut server_b) = spawn_origin_server().await;

	// ── Relay: dials origin A as an upstream cluster peer ──────────────
	let upstream_url = format!("tcp://127.0.0.1:{port_a}");
	let (relay_port, relay_handle) = spawn_relay_with_upstream(&upstream_url).await;

	// Coordination channels.
	let (relay_connected_tx, relay_connected_rx) = tokio::sync::oneshot::channel::<()>();
	let (drain_tx, drain_rx) = tokio::sync::oneshot::channel::<()>();
	// Origin A waits for origin B to be connected before dropping its track,
	// ensuring the backup broadcast is registered (realistic aligned-sibling
	// timing: the new source joins before the old source's track ends).
	let (origin_b_ready_tx, origin_b_ready_rx) = tokio::sync::oneshot::channel::<()>();

	// ── Origin A task ─────────────────────────────────────────────────
	let origin_a_handle = tokio::spawn(async move {
		let request = tokio::time::timeout(TIMEOUT, server_a.accept())
			.await
			.expect("origin A accept timeout")
			.expect("origin A accept failed");

		let origin_a = Origin::random().produce();
		let mut broadcast = origin_a.create_broadcast("live").expect("create broadcast");
		let mut track = broadcast.create_track(Track::new("video")).expect("create track");

		// Group 0: the pre-GOAWAY content.
		let mut group = track.append_group().expect("append group 0");
		group.write_frame(b"frame-a1".as_ref()).expect("write frame a1");
		group.finish().expect("finish group 0");

		let session = request
			.with_publish(origin_a.consume())
			.ok()
			.await
			.expect("origin A session accept");

		// Signal that the relay connected.
		relay_connected_tx.send(()).expect("signal relay connected");

		// Wait for the test to tell us to drain.
		drain_rx.await.expect("drain signal");

		// Send GOAWAY pointing to origin B.
		let draining = session
			.drain()
			.expect("drain")
			.start(format!("tcp://127.0.0.1:{port_b}"));

		// Wait for origin B to be connected (backup registered) then drop
		// the track to exercise the track-ended failover path. Keep the
		// broadcast alive so the origin model doesn't unannounce "live"
		// (which would race with the backup promotion). Small delay ensures
		// the backup announcement propagates through the relay's origin model.
		origin_b_ready_rx.await.expect("origin B ready signal");
		tokio::time::sleep(Duration::from_millis(200)).await;
		drop(track);

		tokio::time::timeout(Duration::from_secs(30), draining.complete())
			.await
			.expect("drain complete timeout");
		drop(broadcast);
	});

	// ── Origin B task ─────────────────────────────────────────────────
	let (origin_b_connected_tx, origin_b_connected_rx) = tokio::sync::oneshot::channel::<()>();
	let origin_b_handle = tokio::spawn(async move {
		let request = tokio::time::timeout(TIMEOUT, server_b.accept())
			.await
			.expect("origin B accept timeout")
			.expect("origin B accept failed");

		let origin_b = Origin::random().produce();
		let mut broadcast = origin_b.create_broadcast("live").expect("create broadcast");
		let mut track = broadcast.create_track(Track::new("video")).expect("create track");

		// Group 1: aligned sibling continues at sequence 1.
		let mut group = track
			.create_group(moq_net::Group { sequence: 1 })
			.expect("create group 1");
		group.write_frame(b"frame-b1".as_ref()).expect("write frame b1");
		group.finish().expect("finish group 1");

		// Accept the relay connection.
		let _session = request
			.with_publish(origin_b.consume())
			.ok()
			.await
			.expect("origin B session accept");

		origin_b_connected_tx.send(()).expect("signal origin B connected");

		// Signal origin A that the backup is registered.
		let _ = origin_b_ready_tx.send(());

		// Keep alive until test completes.
		tokio::time::sleep(TIMEOUT).await;
		drop(track);
		drop(broadcast);
	});

	// ── Wait for relay to connect to origin A ─────────────────────────
	tokio::time::timeout(TIMEOUT, relay_connected_rx)
		.await
		.expect("relay connect timeout")
		.expect("relay connect signal");

	// Small delay for the relay's cluster session to propagate announcements.
	tokio::time::sleep(Duration::from_millis(200)).await;

	// ── Downstream subscriber ─────────────────────────────────────────
	let relay_url: url::Url = format!("tcp://127.0.0.1:{relay_port}")
		.parse()
		.expect("parse relay url");
	let sub_origin = Origin::random().produce();
	// Force IETF moq-transport for the downstream subscriber: seamless failover
	// is IETF-only, so the relay's IETF publisher must handle the source switch.
	let ietf_client = {
		let mut config = moq_native::ClientConfig::default();
		config.tls.disable_verify = Some(true);
		config.websocket.delay = None;
		config.version = vec!["moq-transport-19".parse().expect("parse ietf version")];
		config.init().expect("ietf client init")
	};
	let sub_session = tokio::time::timeout(TIMEOUT, ietf_client.with_consume(sub_origin.clone()).connect(relay_url))
		.await
		.expect("subscriber connect timeout")
		.expect("subscriber connect failed");

	let mut announcements = sub_origin.consume();

	// Wait for the "live" broadcast to be announced.
	let (path, bc) = tokio::time::timeout(TIMEOUT, announcements.announced())
		.await
		.expect("announcement timeout")
		.expect("origin closed");
	assert_eq!(path.as_str(), "live");
	let bc = bc.expect("expected announce, got unannounce");

	// Subscribe to the video track and read group 0 from origin A.
	let mut track_sub = bc.subscribe_track(&Track::new("video")).expect("subscribe_track");
	let mut group_0 = tokio::time::timeout(TIMEOUT, track_sub.recv_group())
		.await
		.expect("recv_group 0 timeout")
		.expect("recv_group 0 failed")
		.expect("track closed before group 0");
	assert_eq!(group_0.sequence, 0, "first group should be sequence 0");
	let frame_0 = tokio::time::timeout(TIMEOUT, group_0.read_frame())
		.await
		.expect("read_frame 0 timeout")
		.expect("read_frame 0 failed")
		.expect("group 0 closed before frame");
	assert_eq!(&*frame_0, b"frame-a1", "group 0 should contain frame-a1");

	// ── Trigger GOAWAY on origin A ────────────────────────────────────
	drain_tx.send(()).expect("send drain signal");

	// Verify the relay reconnects to origin B (origin B accepts the connection).
	tokio::time::timeout(TIMEOUT, origin_b_connected_rx)
		.await
		.expect("origin B connection timeout (relay did not reconnect)")
		.expect("origin B connection signal");

	// ── Assert seamless delivery: group 1 from origin B arrives ────────
	// The track-ended failover path in the relay publisher must switch to the
	// backup source (origin B) and deliver group 1 without error.
	let mut group_1 = tokio::time::timeout(TIMEOUT, track_sub.recv_group())
		.await
		.expect("recv_group 1 timeout (failover did not deliver group 1)")
		.expect("recv_group 1 failed (subscription errored during failover)")
		.expect("track closed before group 1 (failover dropped the subscription)");
	assert_eq!(
		group_1.sequence, 1,
		"second group should be sequence 1 (contiguous after group 0)"
	);
	let frame_1 = tokio::time::timeout(TIMEOUT, group_1.read_frame())
		.await
		.expect("read_frame 1 timeout")
		.expect("read_frame 1 failed")
		.expect("group 1 closed before frame");
	assert_eq!(&*frame_1, b"frame-b1", "group 1 should contain frame-b1 from origin B");

	// ── Downstream GOAWAY must NOT have fired ─────────────────────────
	// The relay absorbs the GOAWAY and reconnects transparently.
	let goaway_result = tokio::time::timeout(Duration::from_secs(2), sub_session.goaway()).await;
	assert!(
		goaway_result.is_err(),
		"downstream session received GOAWAY (should not have)"
	);

	drop(sub_session);
	origin_a_handle.abort();
	origin_b_handle.abort();
	relay_handle.abort();
}

/// T5.2: After an upstream GOAWAY migration, the downstream session's goaway()
/// never resolves. Simpler variant of T5.1 focused solely on the GOAWAY non-propagation.
#[tokio::test]
async fn relay_upstream_goaway_no_downstream_goaway() {
	let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

	let (port_a, mut server_a) = spawn_origin_server().await;
	let (port_b, mut server_b) = spawn_origin_server().await;

	let upstream_url = format!("tcp://127.0.0.1:{port_a}");
	let (relay_port, relay_handle) = spawn_relay_with_upstream(&upstream_url).await;

	let (relay_connected_tx, relay_connected_rx) = tokio::sync::oneshot::channel::<()>();

	// Origin A: accept, signal, then drain immediately.
	let origin_a_handle = tokio::spawn(async move {
		let request = tokio::time::timeout(TIMEOUT, server_a.accept())
			.await
			.expect("origin A accept timeout")
			.expect("origin A accept");

		let origin_a = Origin::random().produce();
		let _broadcast = origin_a.create_broadcast("test").expect("create broadcast");

		let session = request
			.with_publish(origin_a.consume())
			.ok()
			.await
			.expect("origin A ok");

		relay_connected_tx.send(()).expect("signal");

		// Small delay then drain with URI pointing to B.
		tokio::time::sleep(Duration::from_millis(100)).await;
		let draining = session
			.drain()
			.expect("drain")
			.start(format!("tcp://127.0.0.1:{port_b}"));
		tokio::time::timeout(TIMEOUT, draining.complete())
			.await
			.expect("drain timeout");
	});

	// Origin B: accept and park.
	let origin_b_handle = tokio::spawn(async move {
		let request = tokio::time::timeout(TIMEOUT, server_b.accept())
			.await
			.expect("origin B accept timeout")
			.expect("origin B accept");

		let origin_b = Origin::random().produce();
		let _broadcast = origin_b.create_broadcast("test").expect("create broadcast");

		let _session = request
			.with_publish(origin_b.consume())
			.ok()
			.await
			.expect("origin B ok");

		tokio::time::sleep(TIMEOUT).await;
	});

	// Wait for relay to connect upstream.
	tokio::time::timeout(TIMEOUT, relay_connected_rx)
		.await
		.expect("relay connect timeout")
		.expect("relay connect");

	tokio::time::sleep(Duration::from_millis(200)).await;

	// Downstream subscriber.
	let relay_url: url::Url = format!("tcp://127.0.0.1:{relay_port}").parse().expect("parse url");
	let sub_origin = Origin::random().produce();
	let sub_session = tokio::time::timeout(TIMEOUT, client().with_consume(sub_origin).connect(relay_url))
		.await
		.expect("subscriber connect timeout")
		.expect("subscriber connect");

	// Wait for origin A drain + relay reconnect to origin B to complete.
	tokio::time::sleep(Duration::from_secs(1)).await;

	// Assert downstream goaway() does NOT resolve within 2 seconds.
	let goaway_result = tokio::time::timeout(Duration::from_secs(2), sub_session.goaway()).await;
	assert!(
		goaway_result.is_err(),
		"downstream session received a GOAWAY from the relay (it should not)"
	);

	drop(sub_session);
	origin_a_handle.abort();
	origin_b_handle.abort();
	relay_handle.abort();
}

/// T5.4: Origin A sends GOAWAY with an empty URI. The relay reconnects to the
/// same upstream endpoint. Downstream is uninterrupted.
#[tokio::test]
async fn relay_upstream_goaway_empty_uri() {
	let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

	let (port_a, mut server_a) = spawn_origin_server().await;

	let upstream_url = format!("tcp://127.0.0.1:{port_a}");
	let (relay_port, relay_handle) = spawn_relay_with_upstream(&upstream_url).await;

	let (first_connected_tx, first_connected_rx) = tokio::sync::oneshot::channel::<()>();
	let (drain_tx, drain_rx) = tokio::sync::oneshot::channel::<()>();
	let (second_frame_tx, second_frame_rx) = tokio::sync::oneshot::channel::<()>();

	// Origin A accepts TWO connections: the initial, and the reconnect (empty URI
	// means "same endpoint").
	let origin_handle = tokio::spawn(async move {
		// First connection: publish, then wait for drain signal.
		let request1 = tokio::time::timeout(TIMEOUT, server_a.accept())
			.await
			.expect("origin first accept timeout")
			.expect("origin first accept");

		let origin1 = Origin::random().produce();
		let mut broadcast1 = origin1.create_broadcast("live").expect("create broadcast 1");
		let mut track1 = broadcast1.create_track(Track::new("video")).expect("create track 1");
		let mut group1 = track1.append_group().expect("append group 1");
		group1.write_frame(b"first".as_ref()).expect("write first");
		group1.finish().expect("finish group 1");

		let session1 = request1
			.with_publish(origin1.consume())
			.ok()
			.await
			.expect("origin first ok");

		first_connected_tx.send(()).expect("signal first connected");

		// Wait for test to tell us to drain (after downstream read the first frame).
		drain_rx.await.expect("drain signal");

		let draining = session1.drain().expect("drain").start("");
		tokio::time::timeout(TIMEOUT, draining.complete())
			.await
			.expect("drain timeout");
		drop(track1);
		drop(broadcast1);

		// Second connection (the relay reconnects to us with the same URL).
		let request2 = tokio::time::timeout(TIMEOUT, server_a.accept())
			.await
			.expect("origin second accept timeout")
			.expect("origin second accept");

		let origin2 = Origin::random().produce();
		let mut broadcast2 = origin2.create_broadcast("live").expect("create broadcast 2");
		let mut track2 = broadcast2.create_track(Track::new("video")).expect("create track 2");
		let mut group2 = track2.append_group().expect("append group 2");
		group2.write_frame(b"second".as_ref()).expect("write second");
		group2.finish().expect("finish group 2");

		let _session2 = request2
			.with_publish(origin2.consume())
			.ok()
			.await
			.expect("origin second ok");

		second_frame_tx.send(()).expect("signal second frame");

		// Keep alive.
		tokio::time::sleep(TIMEOUT).await;
		drop(track2);
		drop(broadcast2);
	});

	// Wait for relay to connect the first time.
	tokio::time::timeout(TIMEOUT, first_connected_rx)
		.await
		.expect("first connect timeout")
		.expect("first connect");
	tokio::time::sleep(Duration::from_millis(200)).await;

	// Downstream subscriber.
	let relay_url: url::Url = format!("tcp://127.0.0.1:{relay_port}").parse().expect("parse url");
	let sub_origin = Origin::random().produce();
	let mut announcements = sub_origin.consume();

	let sub_session = tokio::time::timeout(TIMEOUT, client().with_consume(sub_origin).connect(relay_url))
		.await
		.expect("subscriber connect timeout")
		.expect("subscriber connect");

	// Read initial frame from first connection.
	let (path, bc) = tokio::time::timeout(TIMEOUT, announcements.announced())
		.await
		.expect("announcement timeout")
		.expect("origin closed");
	assert_eq!(path.as_str(), "live");
	let bc = bc.expect("expected announce");

	let mut track_sub = bc.subscribe_track(&Track::new("video")).expect("subscribe_track");
	let mut group_sub = tokio::time::timeout(TIMEOUT, track_sub.recv_group())
		.await
		.expect("recv_group timeout")
		.expect("recv_group failed")
		.expect("track closed");
	let frame = tokio::time::timeout(TIMEOUT, group_sub.read_frame())
		.await
		.expect("read_frame timeout")
		.expect("read_frame failed")
		.expect("group closed");
	assert_eq!(&*frame, b"first");

	// Now tell origin A to drain (we have the first frame).
	drain_tx.send(()).expect("send drain signal");

	// Wait for the reconnect and second frame to be available.
	tokio::time::timeout(TIMEOUT, second_frame_rx)
		.await
		.expect("second frame timeout")
		.expect("second frame signal");

	// Read the frame from the second connection (after reconnect).
	// The old track subscription may yield Cancel when origin A closes.
	// In that case, fall through to the re-announcement path.
	let mut got_second = false;

	// Try reading from the existing track subscription.
	if let Ok(Ok(Some(mut new_group))) = tokio::time::timeout(Duration::from_secs(3), track_sub.recv_group()).await {
		if let Ok(Ok(Some(frame))) = tokio::time::timeout(Duration::from_secs(2), new_group.read_frame()).await {
			if &*frame == b"second" {
				got_second = true;
			}
		}
	}
	// Cancel or timeout: fall through to re-announce path.

	if !got_second {
		// The old broadcast was unannounced when origin A's session closed.
		// The re-announce from the second connection carries "live" again.
		let deadline = tokio::time::Instant::now() + TIMEOUT;
		while tokio::time::Instant::now() < deadline {
			match tokio::time::timeout(Duration::from_secs(5), announcements.announced()).await {
				Ok(Some((path, bc))) => {
					if path.as_str() == "live" {
						if let Some(bc) = bc {
							let mut ts = bc.subscribe_track(&Track::new("video")).expect("subscribe_track 2");
							if let Ok(Ok(Some(mut gs))) =
								tokio::time::timeout(Duration::from_secs(3), ts.recv_group()).await
							{
								if let Ok(Ok(Some(f))) =
									tokio::time::timeout(Duration::from_secs(2), gs.read_frame()).await
								{
									if &*f == b"second" {
										got_second = true;
										break;
									}
								}
							}
						}
					}
				}
				_ => break,
			}
		}
	}

	assert!(
		got_second,
		"downstream never received frame after empty-URI GOAWAY reconnect"
	);

	// Downstream GOAWAY must not have fired.
	let goaway_result = tokio::time::timeout(Duration::from_millis(500), sub_session.goaway()).await;
	assert!(goaway_result.is_err(), "downstream received GOAWAY (should not have)");

	drop(sub_session);
	origin_handle.abort();
	relay_handle.abort();
}

// Pass 2 (mock transport): T5.3 relay_upstream_goaway_dedup_at_downstream
// Deferred because verifying exact (group_id, object_id) dedup during the overlap
// window is timing-sensitive on real transports. A mock transport with controlled
// delivery ordering makes this deterministic.

// T5.3 DEFERRED: relay_upstream_goaway_dedup_at_downstream
//
// The test plan's T5.3 criterion asks: "downstream sees each (group_id,
// object_id) exactly once during the two-connection overlap window." This
// assumes the relay merges two upstream sessions into a single downstream
// TrackProducer simultaneously, requiring dedup.
//
// After reading the relay architecture, that scenario does not exist:
//
// 1. `Session::reconnect()` (in moq-net/src/reconnect.rs) closes the old
//    session immediately after the new one is established. There is no
//    sustained overlap where both sessions deliver groups concurrently.
//
// 2. Each upstream session creates its own BroadcastProducer (via
//    `Broadcast::produce()` in lite/subscriber.rs:start_announce). Two
//    sessions publishing the same path yield two distinct BroadcastConsumers
//    routed through Origin's active/backup mechanism, not a single shared
//    TrackProducer.
//
// 3. Origin::publish_broadcast selects ONE active broadcast at a time
//    (shortest hop wins, deterministic hash tiebreak). The backup is
//    promoted only after the active closes. Downstream consumers receive a
//    re-announcement and resubscribe to the new BroadcastConsumer.
//
// 4. `TrackProducer::create_group`'s duplicate HashSet guards against
//    protocol violations within a single session (peer sends same group
//    sequence twice). It is not a cross-session relay dedup mechanism.
//    This primitive is already covered by the T4.7 Part 1 model-level test
//    in `rs/moq-net/tests/goaway.rs::migration_dedup_overlap_window_moq_transport_17`.
//
// Writing a test for a scenario that cannot occur would be misleading.
// The actual relay property during GOAWAY migration is: downstream
// subscribers see a re-announcement, resubscribe to the new broadcast,
// and receive groups from the new upstream with no silent duplication or
// gap. This continuity is covered by T5.1 (transparent reconnect with
// continuous delivery).
//
// Follow-up: if the relay ever gains a fan-in mode where multiple upstream
// sessions feed the same TrackProducer concurrently, T5.3 should be
// revisited.
