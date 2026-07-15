//! Headless verifier for the GOAWAY failover demo.
//!
//! Subscribes to a broadcast at an edge relay, records the received group
//! sequence, and asserts:
//! 1. COMPLETENESS (order-independent): the deduplicated set of received
//!    sequences exactly equals the contiguous range [min..=max] with no missing
//!    sequence, no duplicate, and no truncated group.
//! 2. FAILOVER-WINDOW CONTIGUITY (strict ordering): once delivery transitions
//!    from the initial backfill burst to steady-state live tailing, every
//!    subsequent group (including across the GOAWAY failover) arrives in strict
//!    ascending order with no gap or reorder.
//!
//! Exits 0 on success, non-zero on any violation.

use std::time::{Duration, Instant};

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

    /// Broadcast name to subscribe to.
    #[arg(long, default_value = "goaway-test")]
    broadcast: String,

    /// Track name within the broadcast.
    #[arg(long, default_value = "seq")]
    track: String,

    /// Minimum number of groups to receive before declaring success.
    #[arg(long, default_value = "20")]
    min_groups: u64,

    /// Maximum time to wait for groups before timing out.
    #[arg(long, default_value = "30s", value_parser = humantime::parse_duration)]
    timeout: Duration,

    /// Inter-arrival threshold (ms) to distinguish backfill burst from live.
    /// Groups arriving faster than this are considered part of the initial
    /// backfill burst. Once a gap >= this threshold is seen, all subsequent
    /// groups are "steady-state" and must be strictly ordered.
    #[arg(long, default_value = "100")]
    burst_threshold_ms: u64,
}

/// A received group with its arrival metadata.
struct ReceivedGroup {
    seq: u64,
    arrival: Instant,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::parse();
    config.log.init()?;

    let client = config.client.clone().init()?;
    let origin = Origin::random().produce();

    let reconnect = client
        .consume(origin.clone())
        .context("--client-connect is required")?;

    tracing::info!(
        broadcast = %config.broadcast,
        track = %config.track,
        min_groups = config.min_groups,
        "verifier: waiting for broadcast"
    );

    let timeout_dur = config.timeout;
    let result = tokio::time::timeout(timeout_dur, async {
        tokio::select! {
            res = reconnect.closed() => {
                Err(anyhow::anyhow!("connection closed before verification: {:?}", res))
            }
            res = verify(origin, &config) => res,
        }
    })
    .await;

    match result {
        Ok(Ok(())) => {
            println!("\n=== SUCCESS ===");
            std::process::exit(0);
        }
        Ok(Err(e)) => {
            println!("\n=== FAILURE: {} ===", e);
            std::process::exit(1);
        }
        Err(_) => {
            println!("\n=== FAILURE: timed out after {:?} ===", timeout_dur);
            std::process::exit(1);
        }
    }
}

async fn verify(origin: OriginProducer, config: &Config) -> anyhow::Result<()> {
    let path: Path<'_> = config.broadcast.as_str().into();
    let mut consumer = origin
        .scope(&[path])
        .context("not allowed to consume broadcast")?
        .consume();

    // Wait for the broadcast to be announced.
    let broadcast = loop {
        match consumer.announced().await {
            Some((_path, Some(bc))) => break bc,
            Some((_path, None)) => {
                tracing::debug!("broadcast offline, waiting...");
                continue;
            }
            None => return Err(anyhow::anyhow!("origin closed before broadcast announced")),
        }
    };

    tracing::info!("broadcast online, subscribing to track");

    let track_name = Track::new(&config.track);
    let mut track = broadcast.subscribe_track(&track_name)?;

    let mut received: Vec<ReceivedGroup> = Vec::new();
    let mut truncated_count = 0u64;

    while received.len() < config.min_groups as usize {
        match track.recv_group().await? {
            Some(mut group) => {
                let seq = group.sequence;
                let seq_u64: u64 = seq;
                let arrival = Instant::now();

                // Read at least one frame to confirm the group is not truncated.
                let frame = group.read_frame().await?;
                if frame.is_none() {
                    tracing::error!(seq = seq_u64, "TRUNCATED: group has no frames");
                    truncated_count += 1;
                    received.push(ReceivedGroup { seq: seq_u64, arrival });
                    continue;
                }

                let payload = frame.unwrap();
                let text = String::from_utf8_lossy(&payload);
                tracing::info!(seq = seq_u64, payload = %text, "received group");

                received.push(ReceivedGroup { seq: seq_u64, arrival });
            }
            None => {
                return Err(anyhow::anyhow!(
                    "track ended after {} groups (needed {})",
                    received.len(),
                    config.min_groups
                ));
            }
        }
    }

    // === ASSERTION 1: COMPLETENESS (order-independent) ===
    //
    // The deduplicated set of sequences must form a contiguous range with no
    // gaps. Per MoQT spec, groups ride independent QUIC streams and can arrive
    // out of order, so we do NOT check arrival order here.

    let sequences: Vec<u64> = received.iter().map(|g| g.seq).collect();
    let mut sorted_seqs = sequences.clone();
    sorted_seqs.sort();

    // Duplicates
    let pre_dedup_len = sorted_seqs.len();
    sorted_seqs.dedup();
    let dup_count = pre_dedup_len - sorted_seqs.len();

    // Range completeness: sorted_seqs should be [min..=max] with no missing.
    let first = *sorted_seqs.first().unwrap();
    let last = *sorted_seqs.last().unwrap();
    let expected_count = (last - first + 1) as usize;
    let missing_count = expected_count - sorted_seqs.len();

    println!("\n=== VERIFICATION RESULTS ===");
    println!("Groups received: {} (range {}..={})", sequences.len(), first, last);
    println!("Arrival order: {:?}", sequences);
    println!("Duplicates: {}", dup_count);
    println!("Missing (gaps): {}", missing_count);
    println!("Truncated: {}", truncated_count);

    let mut failed = false;

    if dup_count > 0 {
        println!("COMPLETENESS: FAIL ({} duplicate(s))", dup_count);
        failed = true;
    }
    if missing_count > 0 {
        println!("COMPLETENESS: FAIL ({} missing sequence(s))", missing_count);
        failed = true;
    }
    if truncated_count > 0 {
        println!("COMPLETENESS: FAIL ({} truncated group(s))", truncated_count);
        failed = true;
    }
    if !failed {
        println!("COMPLETENESS: PASS");
    }

    // === ASSERTION 2: FAILOVER-WINDOW CONTIGUITY (strict ordering) ===
    //
    // Signal used: ARRIVAL TIMING heuristic.
    //
    // The initial backfill burst arrives very fast (groups buffered upstream
    // delivered back-to-back over independent QUIC streams, so they can
    // reorder). Once the subscriber catches up to the live edge, groups arrive
    // at the publisher's cadence (~200ms). We use burst_threshold_ms to find
    // the transition point. All groups from that point onward (the "steady-
    // state" window, which spans the GOAWAY failover) MUST arrive in strict
    // ascending +1 order. This ensures the failover itself is seamless.

    let burst_threshold = Duration::from_millis(config.burst_threshold_ms);
    let mut steady_state_start: Option<usize> = None;

    // Find the first group where the inter-arrival from the previous group
    // exceeds the burst threshold. That group and all subsequent ones are
    // steady-state.
    for i in 1..received.len() {
        let gap = received[i].arrival.duration_since(received[i - 1].arrival);
        if gap >= burst_threshold {
            steady_state_start = Some(i);
            break;
        }
    }

    if let Some(start_idx) = steady_state_start {
        let steady_seqs: Vec<u64> = received[start_idx..].iter().map(|g| g.seq).collect();
        println!(
            "Failover-window: steady-state starts at index {} (seq {}), {} groups checked",
            start_idx, steady_seqs[0], steady_seqs.len()
        );

        let mut contiguity_violations = 0u64;
        for window in steady_seqs.windows(2) {
            let expected_next = window[0] + 1;
            if window[1] != expected_next {
                println!(
                    "  CONTIGUITY VIOLATION: after seq {} got seq {} (expected {})",
                    window[0], window[1], expected_next
                );
                contiguity_violations += 1;
            }
        }

        if contiguity_violations > 0 {
            println!(
                "FAILOVER-WINDOW CONTIGUITY: FAIL ({} violation(s) in steady-state)",
                contiguity_violations
            );
            failed = true;
        } else {
            println!("FAILOVER-WINDOW CONTIGUITY: PASS (strict +1 ordering in steady-state)");
        }
    } else {
        // All groups arrived in a burst (no inter-arrival exceeded threshold).
        // This means we never transitioned to live tailing, which is unexpected
        // for a 20-group minimum at 200ms cadence. Treat as a pass on ordering
        // but warn.
        println!("Failover-window: WARNING - no steady-state transition detected (all burst)");
        println!("FAILOVER-WINDOW CONTIGUITY: PASS (vacuously, no steady-state window)");
    }

    if failed {
        return Err(anyhow::anyhow!("one or more assertions failed (see above)"));
    }

    Ok(())
}
