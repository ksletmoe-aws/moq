use std::sync::Arc;
use std::time::Duration;

use crate::{
	Error, OriginConsumer, OriginProducer, StatsHandle,
	coding::{Encode, Reader, Stream, Writer},
	ietf::{self, FetchHeader, RequestId},
	session::{GoawayReceivedSignal, GoawaySignal, SourcedPaths},
	setup,
};

use super::{Control, Message, Publisher, Subscriber, Version, adapter::ControlStreamAdapter};

// Handshake dispatcher: each argument is an independent session parameter, so
// bundling them into a config struct would just add indirection.
#[allow(clippy::too_many_arguments)]
pub fn start<S: web_transport_trait::Session>(
	session: S,
	setup: Option<Stream<S, Version>>,
	request_id_max: Option<RequestId>,
	client: bool,
	publish: Option<OriginConsumer>,
	subscribe: Option<OriginProducer>,
	// Tier-scoped stats handle. Pass [`StatsHandle::default`] to opt out.
	stats: StatsHandle,
	version: Version,
	goaway: GoawaySignal,
	goaway_received: GoawayReceivedSignal,
	going_away: crate::session::GoingAwayFlag,
	// Shared set of paths the subscriber sources into the origin. The caller retains
	// a clone so Session::begin_failover can read the same set.
	sourced_paths: SourcedPaths,
) -> Result<(), Error> {
	web_async::spawn(async move {
		let res = match version {
			Version::Draft14 | Version::Draft15 | Version::Draft16 => {
				let Some(setup) = setup else {
					return session.close(Error::ProtocolViolation.to_code(), "setup stream required");
				};
				let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
				let control = Control::new(request_id_max, client);
				let adapter = ControlStreamAdapter::new(session.clone(), tx, control.clone(), version);

				let publisher = Publisher::new(adapter.clone(), publish, control.clone(), stats.clone(), version);
				let subscriber = Subscriber::new(
					adapter.clone(),
					subscribe,
					control,
					stats,
					version,
					going_away.clone(),
					sourced_paths.clone(),
				);

				// Spawn goaway send task for draft-14-16 (shared control stream).
				let goaway_adapter = adapter.clone();
				let goaway_signal = goaway;
				web_async::spawn(async move {
					let payload = crate::session::await_goaway(&goaway_signal).await;
					let timeout_ms = payload.timeout.map(|d| d.as_millis() as u64).unwrap_or(0);

					goaway_adapter.send_goaway(&payload.uri, timeout_ms, version).await;
				});

				let dispatch_session = adapter.clone();
				let mut sub_ns = subscriber.clone();
				let sub_ns_adapter = adapter.clone();

				tokio::select! {
					Err(err) = adapter.run_with_goaway(setup.reader, setup.writer, rx, goaway_received.clone(), going_away.clone()) => Err::<(), Error>(err),
					Err(err) = run_unis(adapter.clone(), subscriber.clone(), version, goaway_received, going_away) => Err(err),
					Err(err) = run_dispatch(dispatch_session, publisher.clone(), subscriber.clone(), version) => Err(err),
					Err(err) = publisher.run() => Err(err),
					Err(err) = async {
						if !sub_ns.has_origin() {
							return Ok(());
						}
						let stream = match version {
							Version::Draft16 => {
								let (send, recv) = sub_ns_adapter.open_native_bi().await?;
								Stream {
									writer: crate::coding::Writer::new(send, version),
									reader: crate::coding::Reader::new(recv, version),
								}
							}
							_ => Stream::open(&sub_ns_adapter, version).await?,
						};
						if let Err(err) = sub_ns.run_subscribe_namespace(stream).await {
						tracing::warn!(%err, "subscribe_namespace failed, continuing without");
					}
					Ok(())
					} => Err(err),
				}
			}
			_ => {
				// Spawn SETUP sender (keeps stream alive for GOAWAY).
				let mut goaway_signal = goaway;
				web_async::spawn({
					let session = session.clone();
					async move {
						if let Err(err) = run_setup_with_goaway(session, version, &mut goaway_signal).await {
							tracing::warn!(%err, "setup send error");
						}
					}
				});

				let control = Control::new(None, client);
				let publisher = Publisher::new(session.clone(), publish, control.clone(), stats.clone(), version);
				let subscriber = Subscriber::new(
					session.clone(),
					subscribe,
					control,
					stats,
					version,
					going_away.clone(),
					sourced_paths,
				);

				let sub_ns_session = session.clone();
				let mut sub_ns = subscriber.clone();

				tokio::select! {
					Err(err) = run_unis(session.clone(), subscriber.clone(), version, goaway_received, going_away.clone()) => Err(err),
					Err(err) = run_dispatch(session.clone(), publisher.clone(), subscriber.clone(), version) => Err(err),
					Err(err) = publisher.run() => Err(err),
					Err(err) = async {
						if !sub_ns.has_origin() {
							return Ok(());
						}
						let stream = Stream::open(&sub_ns_session, version).await?;
						if let Err(err) = sub_ns.run_subscribe_namespace(stream).await {
							tracing::warn!(%err, "subscribe_namespace failed, continuing without");
						}
						Ok(())
					} => Err(err),
				}
			}
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

	Ok(())
}

/// Send SETUP on a uni stream, then hold it alive for GOAWAY.
///
/// When the goaway signal fires, encodes and sends a GOAWAY message on the
/// same stream before waiting for session close. Draft-17+ uses the SETUP uni
/// stream as the GOAWAY channel.
async fn run_setup_with_goaway<S: web_transport_trait::Session>(
	session: S,
	version: Version,
	goaway: &mut GoawaySignal,
) -> Result<(), Error> {
	let outer_version = crate::Version::Ietf(version);

	let send = session.open_uni().await.map_err(Error::from_transport)?;
	let mut writer: Writer<S::SendStream, crate::Version> = Writer::new(send, outer_version);

	let mut parameters = ietf::Parameters::default();
	parameters.set_bytes(ietf::ParameterBytes::Implementation, b"moq-lite-rs".to_vec());
	let parameters = parameters.encode_bytes(version)?;

	writer.encode(&setup::Setup { parameters }).await?;

	// Wait for either the goaway signal or session close.
	let payload = tokio::select! {
		_ = session.closed() => {
			writer.finish().ok();
			return Ok(());
		}
		payload = crate::session::await_goaway(goaway) => payload,
	};

	// Send the GOAWAY message on the setup stream.
	let timeout_ms = payload.timeout.map(|d| d.as_millis() as u64).unwrap_or(0);
	let goaway_msg = ietf::GoAway {
		new_session_uri: std::borrow::Cow::Owned(payload.uri.to_string()),
		timeout: timeout_ms,
	};

	// Encode as [type_id varint][size u16][body].
	let mut body = bytes::BytesMut::new();
	goaway_msg.encode_msg(&mut body, version)?;
	let type_id: u64 = ietf::GoAway::ID;
	let size: u16 = body
		.len()
		.try_into()
		.map_err(|_| Error::BoundsExceeded(crate::coding::BoundsExceeded))?;

	let mut writer = writer.with_version(version);
	writer.encode(&type_id).await?;
	writer.encode(&size).await?;
	writer.write_all(&mut std::io::Cursor::new(body)).await?;

	// Hold the stream alive until the session closes.
	session.closed().await;
	writer.finish().ok();

	Ok(())
}

/// Accept incoming uni streams and dispatch each to a handler.
///
/// For draft-17+, this also handles the SETUP stream (0x2F00), which then
/// becomes the GOAWAY channel; a decoded GOAWAY is surfaced through the signal.
/// For draft-14-16, all uni streams are group data (GOAWAY arrives on the shared
/// control stream instead), but the signal is still threaded through uniformly.
async fn run_unis<S: web_transport_trait::Session>(
	session: S,
	subscriber: Subscriber<S>,
	version: Version,
	goaway_received: GoawayReceivedSignal,
	going_away: crate::session::GoingAwayFlag,
) -> Result<(), Error> {
	let outer_version = crate::Version::Ietf(version);

	loop {
		let recv = session.accept_uni().await.map_err(Error::from_transport)?;
		let mut reader: Reader<S::RecvStream, crate::Version> = Reader::new(recv, outer_version);
		let kind: u64 = reader.decode_peek().await?;

		// v17+: SETUP arrives on a uni stream, then becomes the GOAWAY channel.
		// We accept it in the background without blocking, since there are no
		// extensions that require waiting on the SETUP before proceeding.
		if kind == setup::SETUP_V17 {
			let signal = goaway_received.clone();
			let flag = going_away.clone();
			web_async::spawn(async move {
				// Decode and discard the unified SETUP message.
				if let Err(err) = reader.decode::<setup::Setup>().await {
					tracing::warn!(%err, "setup decode error");
					return;
				}

				// Monitor for GOAWAY after setup completes.
				if let Err(err) = run_goaway(reader.with_version(version), version, signal, flag).await {
					tracing::warn!(%err, "goaway error");
				}
			});

			continue;
		}

		// Group data — spawn a handler for each stream.
		let mut sub = subscriber.clone();
		web_async::spawn(async move {
			let mut reader = reader.with_version(version);
			if let Err(err) = run_uni_group(&mut sub, &mut reader).await {
				tracing::debug!(%err, "uni stream error");
				reader.abort(&err);
			}
		});
	}
}

async fn run_uni_group<S: web_transport_trait::Session>(
	subscriber: &mut Subscriber<S>,
	stream: &mut Reader<S::RecvStream, Version>,
) -> Result<(), Error> {
	let kind: u64 = stream.decode_peek().await?;

	// SUBGROUP_HEADER type bytes match the form 0b0XX1XXXX (spec §11.4.2):
	// draft-14-17 use 0x10-0x1D and 0x30-0x3D, draft-18 adds 0x40 (FIRST_OBJECT)
	// extending the form to also cover 0x50-0x5D and 0x70-0x7D. Per-version and
	// per-bit validation (e.g., FIRST_OBJECT must be 0 on draft-17) is done in
	// `GroupFlags::decode`.
	if kind <= 0xff && (kind & 0x90) == 0x10 {
		return subscriber.recv_group(stream).await;
	}

	match kind {
		FetchHeader::TYPE => Err(Error::Unsupported),
		_ => Err(Error::UnexpectedStream),
	}
}

/// Accept incoming bidi streams and dispatch to the correct handler based on message type.
async fn run_dispatch<S: web_transport_trait::Session>(
	session: S,
	publisher: Publisher<S>,
	mut subscriber: Subscriber<S>,
	version: Version,
) -> Result<(), Error> {
	loop {
		let mut stream = Stream::accept(&session, version).await?;

		let id: u64 = stream.reader.decode().await?;
		let size: u16 = stream.reader.decode().await?;
		let data = stream.reader.read_exact(size as usize).await?;

		match id {
			// Publisher handles: Subscribe, Fetch, SubscribeNamespace (0x50 modern /
			// 0x11 legacy), TrackStatus
			ietf::Subscribe::ID
			| ietf::Fetch::ID
			| ietf::SubscribeNamespace::ID
			| ietf::SubscribeNamespaceLegacy::ID
			| ietf::TrackStatus::ID => {
				publisher.handle_stream(id, data, stream)?;
			}
			// Subscriber handles: Publish, PublishNamespace
			ietf::Publish::ID | ietf::PublishNamespace::ID => {
				subscriber.handle_stream(id, data, stream)?;
			}
			_ => {
				tracing::warn!(id, "unexpected bidi stream type");
				return Err(Error::UnexpectedStream);
			}
		}
	}
}

/// Block until GOAWAY or stream close.
///
/// The decoded GOAWAY is written to `signal` so the public [`Session::goaway`]
/// accessor resolves, and `going_away` is set so new requests are rejected.
async fn run_goaway<R: web_transport_trait::RecvStream>(
	mut reader: Reader<R, Version>,
	version: Version,
	signal: GoawayReceivedSignal,
	going_away: crate::session::GoingAwayFlag,
) -> Result<(), Error> {
	let id: u64 = match reader.decode_maybe().await? {
		Some(id) => id,
		None => return Ok(()),
	};

	let size: u16 = reader.decode::<u16>().await?;
	let mut data = reader.read_exact(size as usize).await?;

	if id != ietf::GoAway::ID {
		return Err(Error::UnexpectedMessage);
	}

	let msg = ietf::GoAway::decode_msg(&mut data, version)?;
	tracing::debug!(message = ?msg, "received GOAWAY");

	// Set the going-away flag so new requests are rejected immediately.
	going_away.set();

	let timeout = (msg.timeout > 0).then(|| Duration::from_millis(msg.timeout));
	let received = crate::session::GoawayReceived {
		uri: Arc::from(msg.new_session_uri.as_ref()),
		timeout,
	};
	if let Ok(mut state) = signal.write() {
		*state = Some(received);
	}

	Ok(())
}
