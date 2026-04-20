//! Test publisher that changes its catalog mid-stream.
//!
//! Publishes dummy tracks and updates the catalog on a timer to exercise
//! the relay's long-lived catalog subscription and track lifecycle signaling.
//!
//! Phases (each --interval seconds apart):
//!   1. Publish catalog v1: video0 + audio0
//!   2. Publish catalog v2: video0 + audio0 + video1 (track added)
//!   3. Publish catalog v3: video0 + audio0 (video1 removed from catalog, data still flowing)
//!   4. Stop video1 data (track lifecycle termination)
//!   5. Clean shutdown

use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use hang::catalog::{AudioCodec, AudioConfig, Container, VideoConfig};
use hang::moq_lite;
use moq_lite::Origin;
use url::Url;

#[derive(Parser)]
struct Args {
    /// Relay URL to publish to.
    #[arg(long)]
    url: Url,

    /// Namespace to ANNOUNCE.
    #[arg(long)]
    namespace: String,

    /// Disable TLS verification.
    #[arg(long, default_value_t = false)]
    tls_disable_verify: bool,

    /// Seconds between catalog changes.
    #[arg(long, default_value_t = 10)]
    interval: u64,
}

fn dummy_video(width: u32, height: u32) -> VideoConfig {
    VideoConfig {
        codec: "avc1.64001f".parse().unwrap(),
        description: None,
        coded_width: Some(width),
        coded_height: Some(height),
        display_ratio_width: None,
        display_ratio_height: None,
        bitrate: Some(2_000_000),
        framerate: Some(30.0),
        optimize_for_latency: None,
        container: Container::Legacy,
        jitter: None,
    }
}

fn dummy_audio() -> AudioConfig {
    AudioConfig {
        codec: AudioCodec::Opus,
        sample_rate: 48_000,
        channel_count: 2,
        bitrate: Some(128_000),
        description: None,
        container: Container::Legacy,
        jitter: None,
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    moq_native::Log::new(tracing::Level::INFO).init();
    let args = Args::parse();
    let interval = Duration::from_secs(args.interval);

    // Build client
    let mut config = moq_native::ClientConfig::default();
    if args.tls_disable_verify {
        config.tls.disable_verify = Some(true);
    }
    let client = config.init()?;

    // Create broadcast + catalog
    let mut broadcast = moq_lite::BroadcastProducer::new();
    let mut catalog = moq_mux::CatalogProducer::new(&mut broadcast)?;

    // Initial tracks
    let video0 = broadcast.create_track(moq_lite::Track { name: "video0.hang".into(), priority: 2 })?;
    let audio0 = broadcast.create_track(moq_lite::Track { name: "audio0.hang".into(), priority: 1 })?;

    // Catalog v1: video0 + audio0
    {
        let mut cat = catalog.lock();
        cat.video.renditions.insert("video0.hang".into(), dummy_video(1280, 720));
        cat.audio.renditions.insert("audio0.hang".into(), dummy_audio());
    }
    tracing::info!("phase 1: published catalog v1 (video0 + audio0)");

    // Connect
    let origin = Origin::produce();
    origin.publish_broadcast(&args.namespace, broadcast.consume());
    let session = client
        .with_publish(origin.consume())
        .connect(args.url.clone())
        .await
        .context("failed to connect")?;
    tracing::info!("connected to relay");

    // Start dummy data writers
    let writers = vec![
        spawn_writer(video0),
        spawn_writer(audio0),
    ];

    tracing::info!("waiting {}s before adding video1...", args.interval);
    tokio::time::sleep(interval).await;

    // Phase 2: Add video1
    let video1 = broadcast.create_track(moq_lite::Track { name: "video1.hang".into(), priority: 3 })?;
    {
        let mut cat = catalog.lock();
        cat.video.renditions.insert("video1.hang".into(), dummy_video(640, 360));
    }
    let writer1 = spawn_writer(video1);
    tracing::info!("phase 2: catalog v2 (added video1). Waiting {}s...", args.interval);
    tokio::time::sleep(interval).await;

    // Phase 3: Remove video1 from catalog (data keeps flowing)
    {
        let mut cat = catalog.lock();
        cat.video.renditions.remove("video1.hang");
    }
    tracing::info!("phase 3: catalog v3 (removed video1 from catalog, data still flowing). Waiting {}s...", args.interval);
    tokio::time::sleep(interval).await;

    // Phase 4: Stop video1 data
    drop(writer1);
    tracing::info!("phase 4: stopped video1 data. Waiting {}s...", args.interval);
    tokio::time::sleep(interval).await;

    // Phase 5: Clean shutdown
    drop(writers);
    catalog.finish()?;
    tracing::info!("phase 5: done, closing");
    let _ = session.closed().await;

    Ok(())
}

struct Writer(tokio::task::JoinHandle<()>);
impl Drop for Writer {
    fn drop(&mut self) { self.0.abort(); }
}

fn spawn_writer(mut track: moq_lite::TrackProducer) -> Writer {
    Writer(tokio::spawn(async move {
        loop {
            let Ok(mut group) = track.append_group() else { break };
            let _: Result<(), _> = group.write_frame(bytes::Bytes::from_static(b"dummy"));
            let _ = group.finish();
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }))
}
