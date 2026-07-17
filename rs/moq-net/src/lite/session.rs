use std::borrow::Cow;

use crate::{
	BandwidthConsumer, BandwidthProducer, Error, OriginConsumer, OriginProducer, StatsHandle,
	coding::Stream,
	lite::SessionInfo,
	session::{GoawayReceivedSignal, GoawaySignal, SourcedPaths},
};

use super::{
	ControlType, Goaway, Publisher, PublisherConfig, Setup, Subscriber, SubscriberConfig, Version, send_setup,
};

#[allow(clippy::too_many_arguments)]
pub fn start<S: web_transport_trait::Session>(
	session: S,
	// The stream used to setup the session, after exchanging setup messages.
	// NOTE: No longer used in draft-03.
	setup: Option<Stream<S, Version>>,
	// We will publish any local broadcasts from this origin.
	publish: Option<OriginConsumer>,
	// We will consume any remote broadcasts, inserting them into this origin.
	subscribe: Option<OriginProducer>,
	// Tier-scoped stats handle. Pass [`StatsHandle::default`] to opt out.
	stats: StatsHandle,
	// The version of the protocol to use.
	version: Version,
	// The SETUP message to advertise on the Setup stream (moq-lite-05+). Ignored on
	// earlier versions, which have no Setup stream.
	our_setup: Setup,
	// Signal to send a GOAWAY frame to the peer.
	goaway: GoawaySignal,
	// Signal to fire when a GOAWAY frame is received from the peer.
	goaway_received: GoawayReceivedSignal,
	// Flag set when GOAWAY is received; checked by subscribers before opening new streams.
	going_away: crate::session::GoingAwayFlag,
	// Shared set of paths the subscriber sources into the origin. The caller retains
	// a clone so Session::begin_failover can read the same set.
	sourced_paths: SourcedPaths,
	// Per-session origin id shared with the Session for origin_id() exposure.
	session_origin: crate::Origin,
) -> Result<Option<BandwidthConsumer>, Error> {
	let recv_bw = BandwidthProducer::new();

	let recv_bw_consumer = match version {
		Version::Lite01 | Version::Lite02 => None,
		_ => Some(recv_bw.consume()),
	};

	let recv_bw_for_sub = match version {
		Version::Lite01 | Version::Lite02 => None,
		_ => Some(recv_bw),
	};

	// Publisher and Subscriber each derive their identity from their own
	// attached origin (publish.info / subscribe.info). This is what gets
	// stamped onto outbound hops and checked against incoming hops, so it
	// must be stable across every session that shares the local origin.
	// Required for cross-session cluster loop detection.
	//
	// Keep a clone of the goaway_received signal in the session task so the
	// channel stays open even if the publisher drops its copy (e.g. transport
	// error races the GOAWAY decode).
	let goaway_received_keepalive = goaway_received.clone();
	let publisher = Publisher::new(PublisherConfig {
		session: session.clone(),
		origin: publish,
		stats: stats.clone(),
		version,
		goaway_received,
		going_away: going_away.clone(),
	});
	let subscriber = Subscriber::new(SubscriberConfig {
		session: session.clone(),
		origin: subscribe,
		recv_bandwidth: recv_bw_for_sub,
		stats,
		version,
		going_away,
		sourced_paths,
		session_origin,
	});

	// moq-lite-05 reintroduced a Setup stream: each endpoint opens one and sends a
	// single SETUP message advertising its optional capabilities.
	if version.has_setup_stream() {
		let session = session.clone();
		web_async::spawn(async move {
			if let Err(err) = send_setup(&session, version, our_setup).await {
				// The peer gates serving on our SETUP, so a failure to send it must
				// tear the session down rather than leave the peer waiting.
				tracing::warn!(%err, "failed to send setup stream");
				session.close(err.to_code(), &err.to_string());
			}
		});
	}

	// Spawn a task that waits for the drain signal and sends GOAWAY.
	// Only Lite04+ supports the GOAWAY control stream.
	if matches!(version, Version::Lite04 | Version::Lite05Wip) {
		let session = session.clone();
		web_async::spawn(async move {
			let payload = crate::session::await_goaway(&goaway).await;

			// moq-lite has no timeout field on the wire; only the URI is sent.
			// The force-close timer (if configured) still applies locally via
			// Draining::complete(), but the peer does not receive a deadline.
			let msg = Goaway {
				uri: Cow::Owned(payload.uri.to_string()),
			};

			// Open a dedicated bidi control stream and send the GOAWAY.
			let mut stream = match crate::coding::Stream::open(&session, version).await {
				Ok(s) => s,
				Err(err) => {
					tracing::warn!(%err, "failed to open goaway stream");
					return;
				}
			};

			if let Err(err) = stream.writer.encode(&ControlType::Goaway).await {
				tracing::warn!(%err, "failed to write goaway type");
				return;
			}
			if let Err(err) = stream.writer.encode(&msg).await {
				tracing::warn!(%err, "failed to write goaway message");
				return;
			}
			let _ = stream.writer.finish();
		});
	}

	web_async::spawn(async move {
		// Hold the signal clone alive for the entire session lifetime so the
		// consumer channel doesn't close prematurely.
		let _goaway_received_keepalive = goaway_received_keepalive;

		let res = tokio::select! {
			Err(res) = run_session(setup) => Err(res),
			res = publisher.run() => res,
			res = subscriber.run() => res,
		};

		match res {
			Err(Error::Transport(_)) => {
				tracing::info!("session terminated");
				session.close(1, "");
			}
			Err(err) => {
				tracing::warn!(%err, "session error");
				session.close(err.to_code(), err.to_string().as_ref());
			}
			_ => {
				tracing::info!("session closed");
				session.close(0, "");
			}
		}
	});

	Ok(recv_bw_consumer)
}

// TODO do something useful with this
async fn run_session<S: web_transport_trait::Session>(stream: Option<Stream<S, Version>>) -> Result<(), Error> {
	if let Some(mut stream) = stream {
		while let Some(_info) = stream.reader.decode_maybe::<SessionInfo>().await? {}
		return Err(Error::Cancel);
	}

	Ok(())
}
