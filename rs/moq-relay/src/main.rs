use moq_relay::*;

use anyhow::Context;

#[cfg(feature = "jemalloc")]
#[global_allocator]
static ALLOC: moq_native::jemalloc::tikv_jemallocator::Jemalloc = moq_native::jemalloc::tikv_jemallocator::Jemalloc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
	// TODO: It would be nice to remove this and rely on feature flags only.
	// However, some dependency is pulling in `ring` and I don't know why, so meh for now.
	rustls::crypto::aws_lc_rs::default_provider()
		.install_default()
		.expect("failed to install default crypto provider");

	let mut config = Config::load()?;

	config.client.max_streams.get_or_insert(DEFAULT_MAX_STREAMS);
	config.server.max_streams.get_or_insert(DEFAULT_MAX_STREAMS);

	let mtls_enabled = !config.server.tls.root.is_empty();

	#[allow(unused_mut)]
	let mut server = config.server.init()?;
	let client = config.client.clone().init()?;

	// `None` for a stream-only server (no QUIC); any other error is real.
	let addr = match server.local_addr() {
		Ok(addr) => Some(addr),
		Err(moq_native::Error::NoBackend(_)) => None,
		Err(err) => return Err(err).context("failed to resolve the QUIC bind address"),
	};

	#[cfg(feature = "iroh")]
	let (server, client) = {
		let iroh = config.iroh.bind().await?;
		(server.with_iroh(iroh.clone()), client.with_iroh(iroh))
	};

	// Reject configs where neither JWT nor mTLS can authenticate anyone.
	if config.auth.is_empty() {
		anyhow::ensure!(
			mtls_enabled,
			"no auth-key, auth-key-dir, public path, or server tls.root configured; \
			 nobody can authenticate"
		);
		tracing::warn!("no JWT/public auth configured; only mTLS peers will be accepted");
	}

	let auth = if config.auth.is_empty() {
		Auth::default()
	} else {
		config.auth.init(&config.client.tls).await?
	};

	let cluster = Cluster::new(config.cluster)?
		.with_client(client)
		.with_client_tls(config.client.tls.build()?);
	let stats = config.stats.build(cluster.origin.clone());
	let cluster = cluster.with_stats(stats);

	// Create a web server too. mTLS for HTTPS is opt-in via `--web-https-root`.
	let web = Web::new(
		WebState {
			auth: auth.clone(),
			cluster: cluster.clone(),
			tls_info: server.tls_info(),
			conn_id: Default::default(),
		},
		config.web,
	);

	match addr {
		Some(addr) => tracing::info!(%addr, "listening"),
		None => tracing::info!("listening (stream transports only)"),
	}

	#[cfg(unix)]
	// Notify systemd that we're ready after all initialization is complete
	let _ = sd_notify::notify(&[sd_notify::NotifyState::Ready]);

	let drain_timeout = std::time::Duration::from_secs(config.drain_timeout.unwrap_or(DEFAULT_DRAIN_TIMEOUT_SECS));

	#[cfg(feature = "jemalloc")]
	let jemalloc = moq_native::jemalloc::run();
	#[cfg(not(feature = "jemalloc"))]
	let jemalloc = std::future::pending::<anyhow::Result<()>>();

	// Spawn the cluster task so we can abort it once serve() returns (all
	// connections drained). Without this, the infinite reconnect-retry loops
	// inside cluster.run() would prevent the process from exiting on a single
	// SIGTERM.
	let mut cluster_handle = tokio::spawn(cluster.clone().run());

	// serve() completing (Ok or Err) is the normal shutdown trigger: it returns
	// Ok(()) once all connections have drained after SIGTERM.
	//
	// The cluster arm only matches errors. A standalone relay has no peers, so
	// cluster.run() returns Ok(()) immediately; that resolves as Ok(Ok(())) on
	// the JoinHandle, which does NOT match `Ok(Err(_))`, so the branch is
	// effectively disabled and the relay keeps running.
	let result = tokio::select! {
		res = serve(server, cluster, auth, drain_timeout) => res.context("server failed"),
		Err(err) = web.run() => Err(err).context("web server failed"),
		Err(err) = jemalloc => Err(err).context("jemalloc profiler failed"),
		Ok(Err(err)) = &mut cluster_handle => Err(err).context("cluster failed"),
	};

	// Abort the cluster task unconditionally so the process exits on a single
	// SIGTERM even when cluster.run() has infinite reconnect loops.
	cluster_handle.abort();

	result
}

/// Wait for a shutdown signal: SIGINT (Ctrl-C) on any platform, or SIGTERM on
/// unix. Orchestrators (Kubernetes, systemd, Docker) send SIGTERM on shutdown,
/// so both must trigger the graceful drain for zero-downtime deploys to work.
async fn shutdown_signal() {
	#[cfg(unix)]
	{
		let mut sigterm = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
			Ok(sig) => sig,
			Err(err) => {
				tracing::warn!(%err, "failed to install SIGTERM handler; falling back to SIGINT only");
				tokio::signal::ctrl_c().await.ok();
				return;
			}
		};
		tokio::select! {
			_ = tokio::signal::ctrl_c() => {}
			_ = sigterm.recv() => {}
		}
	}

	#[cfg(not(unix))]
	{
		tokio::signal::ctrl_c().await.ok();
	}
}

async fn serve(
	server: moq_native::Server,
	cluster: Cluster,
	auth: Auth,
	drain_timeout: std::time::Duration,
) -> anyhow::Result<()> {
	let mut conn_id = 0;

	// Two-stage shutdown: first signal drains, second signal force-drops.
	let (drain_tx, drain_rx) = tokio::sync::watch::channel(false);
	let (active_tx, mut active_rx) = tokio::sync::mpsc::channel::<()>(1);

	// Register the shutdown signal handler.
	let shutdown_handle = tokio::spawn(async move {
		// First signal: broadcast drain.
		shutdown_signal().await;
		tracing::info!("received shutdown signal, draining connections");
		let _ = drain_tx.send(true);

		// Second signal: force exit.
		shutdown_signal().await;
		tracing::warn!("received second signal, forcing shutdown");
		std::process::exit(0);
	});

	// Disable the built-in ctrl_c handler since we manage signals ourselves.
	let mut server = server.with_ctrl_c_handler(false);
	let mut accept_drain = drain_rx.clone();

	loop {
		tokio::select! {
			request = server.accept() => {
				let Some(request) = request else {
					break;
				};

				let conn = Connection {
					id: conn_id,
					request,
					cluster: cluster.clone(),
					auth: auth.clone(),
					drain_timeout,
				};

				conn_id += 1;
				let drain = drain_rx.clone();
				let _active = active_tx.clone();
				tokio::spawn(async move {
					if let Err(err) = conn.run_with_drain(drain).await {
						tracing::warn!(%err, "connection closed");
					}
					drop(_active);
				});
			}
			_ = accept_drain.changed() => {
				// Stop accepting new connections once drain fires.
				break;
			}
		}
	}

	// Drop the sender; wait for all active connections to finish draining.
	drop(active_tx);
	let _ = active_rx.recv().await;
	shutdown_handle.abort();

	Ok(())
}
