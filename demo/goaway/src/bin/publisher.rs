//! Synthetic publisher for the GOAWAY failover demo.
//!
//! Emits numbered groups at a steady cadence with deterministic frame content.
//! Each group contains a single frame: `"group={sequence}"`. This makes
//! downstream assertions (contiguous sequence, no gap, no dup) trivial.

use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use moq_net::*;

#[derive(Parser)]
struct Config {
    /// MoQ client configuration (requires --client-connect).
    #[command(flatten)]
    client: moq_native::ClientConfig,

    /// Log configuration.
    #[command(flatten)]
    log: moq_native::Log,

    /// Broadcast name to publish under.
    #[arg(long, default_value = "goaway-test")]
    broadcast: String,

    /// Track name within the broadcast.
    #[arg(long, default_value = "seq")]
    track: String,

    /// Interval between groups.
    #[arg(long, default_value = "200ms", value_parser = humantime::parse_duration)]
    interval: Duration,

    /// Total number of groups to emit (0 = unlimited).
    #[arg(long, default_value = "50")]
    count: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::parse();
    config.log.init()?;

    let client = config.client.init()?;
    let origin = Origin::random().produce();

    let mut broadcast = Broadcast::new().produce();
    let track = broadcast.create_track(Track::new(&config.track))?;
    origin.publish_broadcast(&config.broadcast, broadcast.consume());

    let reconnect = client
        .publish(origin.consume())
        .context("--client-connect is required")?;

    tracing::info!(
        broadcast = %config.broadcast,
        track = %config.track,
        interval = ?config.interval,
        count = config.count,
        "publishing synthetic groups"
    );

    tokio::select! {
        res = reconnect.closed() => {
            tracing::warn!("connection closed unexpectedly");
            Ok(res?)
        }
        res = publish_loop(track, config.interval, config.count) => res,
    }
}

async fn publish_loop(
    mut track: TrackProducer,
    interval: Duration,
    count: u64,
) -> anyhow::Result<()> {
    for seq in 0..count {
        let mut group = track
            .create_group(seq.into())
            .context("failed to create group")?;

        let payload = format!("group={seq}");
        group.write_frame(payload)?;
        group.finish()?;

        tracing::debug!(seq, "published group");
        tokio::time::sleep(interval).await;
    }

    tracing::info!(count, "all groups published; exiting");
    Ok(())
}
