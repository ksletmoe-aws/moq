//! MID-A proxy for the GOAWAY failover demo.
//!
//! Acts as a mini relay: subscribes to TOP (upstream) and serves BOTTOM
//! (downstream). After a configurable delay, sends a targeted GOAWAY to the
//! downstream session, directing it to reconnect to MID-B.
//!
//! This demonstrates the `Session::drain()` API without polluting the
//! production relay with demo-only flags.

use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use moq_net::*;

#[derive(Parser)]
struct Config {
    /// Client config for the upstream (TOP) connection.
    #[command(flatten)]
    client: moq_native::ClientConfig,

    /// Server config for the downstream (BOTTOM) connection.
    #[command(flatten)]
    server: moq_native::ServerConfig,

    /// Log configuration.
    #[command(flatten)]
    log: moq_native::Log,

    /// URI to send in the GOAWAY (where BOTTOM should reconnect, i.e. MID-B).
    #[arg(long)]
    goaway_uri: String,

    /// Delay before sending the first GOAWAY.
    #[arg(long, default_value = "2500ms", value_parser = humantime::parse_duration)]
    goaway_delay: Duration,

    /// Timeout for the peer to disconnect after GOAWAY.
    #[arg(long, default_value = "5s", value_parser = humantime::parse_duration)]
    goaway_timeout: Duration,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::parse();
    config.log.init()?;

    // Shared origin: upstream publishes into it, downstream subscribes from it.
    let origin = Origin::random().produce();

    // Connect upstream to TOP as a consumer.
    let client = config.client.clone().init()?;
    let reconnect = client
        .consume(origin.clone())
        .context("--client-connect is required (upstream TOP URL)")?;

    tracing::info!("MID-A: connected to upstream (TOP)");

    // Start the server for downstream (BOTTOM) connections.
    let mut server = config
        .server
        .clone()
        .init()?
        .with_publish(origin.consume())
        .with_ctrl_c_handler(false);

    tracing::info!(
        goaway_uri = %config.goaway_uri,
        goaway_delay = ?config.goaway_delay,
        "MID-A: server ready, will GOAWAY after delay"
    );

    // Accept exactly one downstream session (BOTTOM), then drain it.
    tokio::select! {
        res = reconnect.closed() => {
            tracing::error!("upstream closed unexpectedly");
            Ok(res?)
        }
        res = run_server(&mut server, &config) => res,
    }
}

async fn run_server(server: &mut moq_native::Server, config: &Config) -> anyhow::Result<()> {
    // Accept the first downstream connection.
    let request = server
        .accept()
        .await
        .context("server stopped before accepting a connection")?;

    tracing::info!("MID-A: accepted downstream session (BOTTOM)");
    let session = request.ok().await?;

    // Wait for the configured delay, then fire GOAWAY.
    tracing::info!(delay = ?config.goaway_delay, "MID-A: waiting before GOAWAY");
    tokio::time::sleep(config.goaway_delay).await;

    tracing::info!(
        uri = %config.goaway_uri,
        timeout = ?config.goaway_timeout,
        "MID-A: sending GOAWAY to downstream"
    );

    let drain = session
        .drain()
        .context("drain already in progress (unexpected)")?;

    let draining = drain.start_with_timeout(&*config.goaway_uri, config.goaway_timeout);
    draining.complete().await;

    tracing::info!("MID-A: drain complete (downstream disconnected or timed out)");

    // Keep serving for a bit so the test can verify the upstream stays alive.
    tokio::time::sleep(Duration::from_secs(2)).await;
    tracing::info!("MID-A: shutting down");
    Ok(())
}
