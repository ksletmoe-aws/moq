//! Mesh-cluster diamond GOAWAY failover integration test.
//!
//! Exercises a 5-node diamond topology as an automated in-process cargo test
//! with real relay instances and TCP transport. No external processes or scripts.
//!
//! Topology:
//! ```text
//!   TOP (origin server, accepts connections from MID-A and MID-B)
//!     ├── MID-A (mini-relay: subscribes to TOP, serves BOTTOM, sends GOAWAY)
//!     └── MID-B (relay: cluster.connect = [TOP])
//!           ↑
//!   BOTTOM (relay: cluster.connect = [MID-A]) ──reconnects-to──> MID-B
//!     ↓
//!   SUBSCRIBER (downstream verifier)
//! ```
//!
//! MID-A sends GOAWAY with MID-B's URI after the subscriber reads the first
//! group. BOTTOM transparently reconnects to MID-B and the subscriber sees
//! gap-free, duplicate-free group continuity across the diamond failover.
//!
//! This is the automated equivalent of `demo/goaway/run.sh`.

use std::{net::TcpListener, time::Duration};

use moq_native::moq_net::{Group, Origin, Track};
use moq_relay::{AuthConfig, Cluster, ClusterConfig, Connection, PublicConfig};

const TIMEOUT: Duration = Duration::from_secs(30);

fn free_tcp_port() -> u16 {
	let probe = TcpListener::bind("127.0.0.1:0").expect("bind probe");
	let port = probe.local_addr().expect("local addr").port();
	drop(probe);
	port
}

fn client() -> moq_native::Client {
	let mut config = moq_native::ClientConfig::default();
	config.tls.disable_verify = Some(true);
	config.websocket.delay = None;
	config.init().expect("client init")
}

/// Spawn a plain TCP server on a free port. Returns (port, Server).
async fn spawn_origin_server() -> (u16, moq_native::Server) {
	let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
	let port = free_tcp_port();
	let mut config = moq_native::ServerConfig::default();
	config.tcp.bind = Some(format!("127.0.0.1:{port}").parse().expect("parse addr"));
	let server = config.init().expect("origin server init");
	(port, server)
}

/// Spawn a relay with cluster.connect pointing to `upstream_url`.
/// Returns (downstream_port, join_handle).
async fn spawn_relay_with_upstream(upstream_url: &str) -> (u16, tokio::task::JoinHandle<()>) {
	let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

	let port = free_tcp_port();
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

	let mut client_config = moq_native::ClientConfig::default();
	client_config.tls.disable_verify = Some(true);
	client_config.websocket.delay = None;
	let native_client = client_config.init().expect("client init");

	let cluster_with_client = cluster.clone().with_client(native_client);

	let handle = tokio::spawn(async move {
		let cluster_run = cluster_with_client.clone();
		tokio::spawn(async move {
			let _ = cluster_run.run().await;
		});

		let mut id = 0;
		while let Some(request) = server.accept().await {
			let conn = Connection {
				id,
				request,
				cluster: cluster_with_client.clone(),
				auth: auth.clone(),
				drain_timeout: Duration::from_secs(moq_relay::DEFAULT_DRAIN_TIMEOUT_SECS),
			};
			id += 1;
			tokio::spawn(async move {
				let _ = conn.run().await;
			});
		}
	});

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

/// Spawn a mini-relay (MID-A): subscribes to `upstream_url`, serves one
/// downstream session, and sends GOAWAY with `goaway_uri` when signaled.
/// Returns (port, join_handle).
async fn spawn_mid_a_relay(
	upstream_url: &str,
	goaway_uri: String,
	drain_signal: tokio::sync::oneshot::Receiver<()>,
) -> (u16, tokio::task::JoinHandle<()>) {
	let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

	let port = free_tcp_port();
	let origin = Origin::random().produce();

	let mut client_config = moq_native::ClientConfig::default();
	client_config.tls.disable_verify = Some(true);
	client_config.websocket.delay = None;
	let upstream_client = client_config.init().expect("mid-a client init");

	let upstream_url_owned = upstream_url.to_string();
	let origin_for_upstream = origin.clone();

	let mut server_config = moq_native::ServerConfig::default();
	server_config.tcp.bind = Some(format!("127.0.0.1:{port}").parse().expect("parse addr"));
	let mut server = server_config.init().expect("mid-a server init");

	let handle = tokio::spawn(async move {
		let url: url::Url = upstream_url_owned.parse().expect("parse upstream url");
		let _upstream_session = upstream_client
			.with_consume(origin_for_upstream)
			.connect(url)
			.await
			.expect("mid-a upstream connect");

		let origin_consumer = origin.consume();
		let request = server.accept().await.expect("mid-a accept downstream");
		let session = request
			.with_publish(origin_consumer)
			.ok()
			.await
			.expect("mid-a downstream session");

		drain_signal.await.expect("drain signal");

		let drain = session.drain().expect("mid-a drain");
		let draining = drain.start_with_timeout(&*goaway_uri, Duration::from_secs(10));
		draining.complete().await;
	});

	let deadline = std::time::Instant::now() + Duration::from_secs(5);
	loop {
		if tokio::net::TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
			break;
		}
		if std::time::Instant::now() >= deadline {
			panic!("mid-a listener never became ready on port {port}");
		}
		tokio::time::sleep(Duration::from_millis(25)).await;
	}

	(port, handle)
}

/// Diamond cluster GOAWAY failover: TOP -> MID-A/MID-B -> BOTTOM -> subscriber.
///
/// Proves that in a mesh-cluster diamond topology with real TCP transport:
/// 1. Content flows from TOP through MID-A to BOTTOM to the subscriber.
/// 2. When MID-A sends GOAWAY pointing to MID-B, BOTTOM reconnects transparently.
/// 3. The subscriber sees gap-free, duplicate-free group delivery across the failover.
/// 4. The GOAWAY is NOT propagated to the downstream subscriber.
///
/// This is the automated equivalent of `demo/goaway/run.sh`.
#[tokio::test]
async fn cluster_diamond_goaway_seamless_failover() {
	let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

	// ═══════════════════════════════════════════════════════════════════
	// TOP: origin server. Accepts connections from MID-A and MID-B.
	// ═══════════════════════════════════════════════════════════════════
	let (top_port, mut top_server) = spawn_origin_server().await;
	let top_url = format!("tcp://127.0.0.1:{top_port}");

	let (top_both_connected_tx, top_both_connected_rx) = tokio::sync::oneshot::channel::<()>();
	let (write_g1_tx, write_g1_rx) = tokio::sync::oneshot::channel::<()>();

	let top_handle = tokio::spawn(async move {
		let top_origin = Origin::random().produce();
		let mut broadcast = top_origin.create_broadcast("diamond-test").expect("create broadcast");
		let mut track = broadcast.create_track(Track::new("video")).expect("create track");

		// Group 0: pre-failover content.
		let mut g0 = track.create_group(Group { sequence: 0 }).expect("group 0");
		g0.write_frame(b"cluster_g0".as_ref()).expect("write g0");
		g0.finish().expect("finish g0");

		let origin_consumer = top_origin.consume();

		// Accept first connection (from MID-B, since it starts first).
		let request1 = top_server.accept().await.expect("TOP accept 1");
		let _session1 = request1
			.with_publish(origin_consumer.clone())
			.ok()
			.await
			.expect("TOP session 1");

		// Accept second connection (from MID-A).
		let request2 = top_server.accept().await.expect("TOP accept 2");
		let _session2 = request2
			.with_publish(origin_consumer)
			.ok()
			.await
			.expect("TOP session 2");

		top_both_connected_tx.send(()).expect("signal TOP both connected");

		// Wait for signal to write group 1 (after failover is triggered).
		write_g1_rx.await.expect("write g1 signal");

		let mut g1 = track.create_group(Group { sequence: 1 }).expect("group 1");
		g1.write_frame(b"cluster_g1".as_ref()).expect("write g1");
		g1.finish().expect("finish g1");

		// Keep alive.
		tokio::time::sleep(TIMEOUT).await;
		drop(broadcast);
		drop(track);
	});

	// ═══════════════════════════════════════════════════════════════════
	// MID-B: relay clustered to TOP. Stays up throughout the test.
	// Started first so TOP accepts its connection first.
	// ═══════════════════════════════════════════════════════════════════
	let (mid_b_port, mid_b_handle) = spawn_relay_with_upstream(&top_url).await;
	let mid_b_url = format!("tcp://127.0.0.1:{mid_b_port}");

	// ═══════════════════════════════════════════════════════════════════
	// MID-A: mini-relay connecting to TOP, serving BOTTOM.
	// Sends GOAWAY pointing to MID-B when signaled.
	// ═══════════════════════════════════════════════════════════════════
	let (drain_tx, drain_rx) = tokio::sync::oneshot::channel::<()>();
	let (mid_a_port, mid_a_handle) = spawn_mid_a_relay(&top_url, mid_b_url.clone(), drain_rx).await;
	let mid_a_url = format!("tcp://127.0.0.1:{mid_a_port}");

	// Wait for TOP to accept both MID-A and MID-B connections.
	tokio::time::timeout(TIMEOUT, top_both_connected_rx)
		.await
		.expect("TOP both connected timeout")
		.expect("TOP both connected signal");

	// ═══════════════════════════════════════════════════════════════════
	// BOTTOM: relay clustered to MID-A.
	// ═══════════════════════════════════════════════════════════════════
	let (bottom_port, bottom_handle) = spawn_relay_with_upstream(&mid_a_url).await;

	// Wait for the full chain to propagate.
	tokio::time::sleep(Duration::from_secs(1)).await;

	// ═══════════════════════════════════════════════════════════════════
	// SUBSCRIBER: connects to BOTTOM.
	// ═══════════════════════════════════════════════════════════════════
	let bottom_url: url::Url = format!("tcp://127.0.0.1:{bottom_port}")
		.parse()
		.expect("parse bottom url");

	let sub_origin = Origin::random().produce();
	let sub_session = tokio::time::timeout(TIMEOUT, client().with_consume(sub_origin.clone()).connect(bottom_url))
		.await
		.expect("subscriber connect timeout")
		.expect("subscriber connect failed");

	let mut announcements = sub_origin.consume();

	// Wait for the broadcast announcement.
	let (path, bc) = tokio::time::timeout(TIMEOUT, announcements.announced())
		.await
		.expect("announcement timeout")
		.expect("origin closed");
	assert_eq!(path.as_str(), "diamond-test");
	let bc = bc.expect("expected announce, got unannounce");

	// Subscribe.
	let mut track_sub = bc.subscribe_track(&Track::new("video")).expect("subscribe_track");

	// Read group 0 (TOP -> MID-A -> BOTTOM -> subscriber).
	let mut group_0 = tokio::time::timeout(TIMEOUT, track_sub.recv_group())
		.await
		.expect("recv_group 0 timeout")
		.expect("recv_group 0 failed")
		.expect("track closed before group 0");
	assert_eq!(group_0.sequence, 0, "first group should be seq 0");
	let frame_0 = tokio::time::timeout(TIMEOUT, group_0.read_frame())
		.await
		.expect("read_frame 0 timeout")
		.expect("read_frame 0 failed")
		.expect("group 0 closed before frame");
	assert_eq!(&*frame_0, b"cluster_g0", "group 0 payload mismatch");

	// ═══════════════════════════════════════════════════════════════════
	// TRIGGER: MID-A drains, BOTTOM reconnects to MID-B.
	// ═══════════════════════════════════════════════════════════════════
	drain_tx.send(()).expect("send drain signal to MID-A");

	// Signal TOP to write group 1 immediately. No fixed sleep needed:
	// recv_group() below will block (under the 30s timeout) until BOTTOM
	// reconnects to MID-B and the group flows through.
	write_g1_tx.send(()).expect("signal TOP to write g1");

	// ═══════════════════════════════════════════════════════════════════
	// ASSERT: group 1 arrives via the new path (TOP -> MID-B -> BOTTOM).
	// ═══════════════════════════════════════════════════════════════════
	let mut group_1 = tokio::time::timeout(TIMEOUT, track_sub.recv_group())
		.await
		.expect("recv_group 1 timeout (failover did not deliver group 1)")
		.expect("recv_group 1 failed (subscription errored during failover)")
		.expect("track closed before group 1 (failover dropped the subscription)");
	assert_eq!(
		group_1.sequence, 1,
		"second group should be seq 1 (gap-free diamond failover)"
	);
	let frame_1 = tokio::time::timeout(TIMEOUT, group_1.read_frame())
		.await
		.expect("read_frame 1 timeout")
		.expect("read_frame 1 failed")
		.expect("group 1 closed before frame");
	assert_eq!(
		&*frame_1, b"cluster_g1",
		"group 1 should come from TOP via MID-B after failover"
	);

	// ═══════════════════════════════════════════════════════════════════
	// ASSERT: no GOAWAY leaked to downstream.
	// ═══════════════════════════════════════════════════════════════════
	let goaway_result = tokio::time::timeout(Duration::from_secs(2), sub_session.goaway()).await;
	assert!(
		goaway_result.is_err(),
		"downstream subscriber received GOAWAY (relay should absorb it)"
	);

	// Cleanup.
	drop(sub_session);
	top_handle.abort();
	mid_a_handle.abort();
	mid_b_handle.abort();
	bottom_handle.abort();
}
