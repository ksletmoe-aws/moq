//! Session reconnection after receiving GOAWAY.
//!
//! When a peer sends GOAWAY, the receiver should establish a new session at the
//! indicated URI. This module provides the [`Connector`] trait and
//! [`ReconnectOptions`] struct that drive the reconnection sequence inside
//! [`super::Session::reconnect`].
//!
//! # Sequence
//!
//! 1. Verify a GOAWAY has been received (caller error otherwise).
//! 2. If the GOAWAY carries a non-empty URI, a drain timeout is configured, and
//!    the session has sourced paths, perform a **seamless** handoff: mark paths
//!    as failing over, connect with retries, record the directed successor,
//!    swap sessions, and spawn a background drain task for the old session.
//! 3. Otherwise, perform a **non-seamless** handoff: connect (with retries if
//!    configured), close the old session immediately.
//!
//! # Track re-subscription
//!
//! The new session shares the same [`super::OriginProducer`] as the old one. When
//! the new server announces broadcasts, they flow into the caller's existing
//! [`super::OriginConsumer`]. The caller observes announcements (including
//! re-announcements of the same broadcast path from the new server) and
//! re-subscribes to tracks on the new [`super::BroadcastConsumer`].
//!
//! For a clean group-boundary resume (no duplicate or partial groups), pass
//! [`super::StartPosition::NextGroup`] when re-subscribing on the new session.
//! This tells the publisher to begin delivery at the next group boundary,
//! finishing the current group on the old session and starting fresh on the new
//! one. See [`super::Track::with_start`] for details.
//!
//! # Relay composition
//!
//! A relay achieves seamless downstream playback by calling `reconnect` on its
//! upstream session (pointing at the same origin) and letting its downstream
//! fan-out naturally re-announce. Downstream subscribers keep receiving without
//! observing the GOAWAY; the reconnection is transparent at the relay boundary.

use std::future::Future;

use crate::{Error, Session};

/// Establishes a replacement session during GOAWAY reconnection.
///
/// `moq-net` calls this with the URI from the received GOAWAY message (or an
/// empty string when the GOAWAY URI was empty, meaning "reconnect to the same
/// endpoint"). The implementor dials the endpoint, authenticates, negotiates
/// the MoQ handshake, and returns an established [`Session`].
///
/// # Contract
///
/// The returned [`Session`] **must** share the same [`crate::OriginProducer`] as
/// the old session (by building the client with `.with_consume(origin)`). This is
/// what makes the silent-swap invariant work: the new session's subscriber
/// publishes into the same origin, so downstream consumers see a seamless
/// re-announcement rather than a teardown. A runtime assertion inside
/// [`Session::reconnect`] verifies this for the seamless path and returns an
/// error if the connector violates it.
///
/// # Implementing
///
/// For most callers, a closure works via the blanket implementation:
///
/// ```ignore
/// let new_session = session.reconnect(|uri: &str| async move {
///     let url: url::Url = uri.parse()?;
///     client.connect(url).await.map_err(Into::into)
/// }, ReconnectOptions::default()).await?;
/// ```
///
/// Implement the trait directly when you need a named type (e.g. for FFI or
/// when the connector carries configuration):
///
/// ```ignore
/// struct MyConnector { client: moq_native::Client }
///
/// impl Connector for MyConnector {
///     async fn connect(&self, uri: &str) -> Result<Session, Error> {
///         let url: url::Url = uri.parse().map_err(|_| Error::Version)?;
///         self.client.connect(url).await.map_err(Into::into)
///     }
/// }
/// ```
pub trait Connector: Send + Sync {
	/// Connect to the given URI and return an established session.
	///
	/// The URI is the `new_session_uri` from the received GOAWAY message. When
	/// the GOAWAY URI was empty, the URI passed here is also empty, signaling
	/// that the caller should reconnect to the same endpoint.
	///
	/// The returned session must share the caller's [`crate::OriginProducer`]
	/// (via `.with_consume(origin)` on the client builder). Seamless failover
	/// requires this so the new session publishes into the same origin.
	fn connect(&self, uri: &str) -> impl Future<Output = Result<Session, Error>> + Send;
}

/// Blanket implementation so closures and `Fn` types work as connectors.
impl<F, Fut> Connector for F
where
	F: Fn(&str) -> Fut + Send + Sync,
	Fut: Future<Output = Result<Session, Error>> + Send,
{
	fn connect(&self, uri: &str) -> impl Future<Output = Result<Session, Error>> + Send {
		(self)(uri)
	}
}

/// Options for [`Session::reconnect`](crate::Session::reconnect).
///
/// `#[non_exhaustive]`, so new knobs are non-breaking. Defaults preserve the
/// original non-seamless behavior (close the old session immediately, single
/// connect attempt).
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct ReconnectOptions {
	/// If set, keep the old session alive this long after the replacement is
	/// established so it can drain in-flight groups, then force-close it. This is
	/// what makes the handoff seamless (gap-free) for downstream consumers.
	/// `None` (default) closes the old session immediately once the new one is up.
	///
	/// A timeout carried on the received GOAWAY takes precedence over this value.
	pub drain_timeout: Option<std::time::Duration>,

	/// Additional connect attempts (with exponential backoff) if the initial
	/// connect to the GOAWAY URI fails. `0` (default) tries exactly once.
	pub max_retries: u32,
}

impl ReconnectOptions {
	/// Create default reconnect options (non-seamless: close old immediately, one attempt).
	pub fn new() -> Self {
		Self::default()
	}

	/// Keep the old session alive for `timeout` to drain in-flight groups after the
	/// replacement is up, enabling a seamless handoff.
	///
	/// A timeout carried on the received GOAWAY takes precedence over this value.
	pub fn with_drain_timeout(mut self, timeout: std::time::Duration) -> Self {
		self.drain_timeout = Some(timeout);
		self
	}

	/// Retry the connect up to `retries` extra times with exponential backoff.
	pub fn with_max_retries(mut self, retries: u32) -> Self {
		self.max_retries = retries;
		self
	}
}

/// Drive the reconnection sequence.
///
/// Called by [`Session::reconnect`]; not public because the entry point is on Session.
pub(crate) async fn run_reconnect(
	old_session: &mut Session,
	connector: &impl Connector,
	options: &ReconnectOptions,
) -> Result<Session, Error> {
	// The snapshot is the single source of truth. If it's None, no GOAWAY
	// has been fully received yet (avoids a race with the going_away flag
	// which is set a few instructions before the snapshot is written).
	let goaway = old_session.goaway_received_snapshot().ok_or(Error::NotReady)?;
	let uri: &str = &goaway.uri;

	// The GOAWAY-carried timeout wins over the caller-configured fallback.
	let drain = goaway.timeout.or(options.drain_timeout);

	// Seamless path: non-empty URI, a drain window, and the session has
	// sourced paths to fail over.
	let is_seamless = !uri.is_empty() && drain.is_some();
	let failover_guard = if is_seamless {
		old_session.begin_failover()
	} else {
		None
	};

	// Connect with retries (exponential backoff: 100ms * 2^attempt).
	let new_session = connect_with_retries(connector, uri, options.max_retries).await?;

	if let Some(guard) = failover_guard {
		// Seamless path: the new session must share the origin so the
		// silent-swap invariant keeps the path announced.
		let new_origin_id = new_session.origin_id().ok_or_else(|| {
			Error::Transport(
				"seamless reconnect: new session has no origin (Connector must build with .with_consume(origin))"
					.into(),
			)
		})?;

		// Record the directed successor for each path the old session was
		// sourcing, BEFORE the drain task can close the old session. This
		// ensures the publisher's re-resolve finds the correct successor.
		let origin = old_session.origin().expect("guard implies origin exists");
		let paths = old_session.sourced_paths_snapshot();
		for path in &paths {
			origin.record_failover_successor(path, new_origin_id);
		}

		// Swap: move the new session in, get the old one out. After this,
		// `*old_session` is the new session (the caller's `&mut self`).
		// The returned clone is a read-only convenience value for the caller.
		// Mark it closed so its Drop does not close the shared Arc transport;
		// the in-place-swapped `*old_session` is the real owner.
		let mut ret = new_session.clone();
		ret.closed = true;
		let mut old = std::mem::replace(old_session, new_session);

		// Spawn a drain task: keep the old session alive so its subscriber
		// tasks finish relaying in-flight groups. The failover guard moves
		// into the task so publishers keep re-resolving until the old source
		// has fully drained.
		let drain_timeout = drain.expect("is_seamless implies drain.is_some()");
		tokio::spawn(async move {
			let drain_result = tokio::time::timeout(drain_timeout, old.closed()).await;

			match drain_result {
				Ok(_) => {
					tracing::debug!("old upstream drained cleanly");
				}
				Err(_elapsed) => {
					tracing::warn!(
						timeout = ?drain_timeout,
						"old upstream did not drain in time; force-closing"
					);
					old.close(Error::GoawayTimeout);
				}
			}

			drop(guard);
		});

		Ok(ret)
	} else {
		// Non-seamless path: close the old session immediately.
		// The returned clone is a read-only convenience value for the caller.
		// Mark it closed so its Drop does not close the shared Arc transport;
		// the in-place-swapped `*old_session` is the real owner.
		let mut ret = new_session.clone();
		ret.closed = true;
		let mut old = std::mem::replace(old_session, new_session);
		old.close(Error::Cancel);
		Ok(ret)
	}
}

/// Connect to the GOAWAY URI with exponential backoff retries.
async fn connect_with_retries(connector: &impl Connector, uri: &str, max_retries: u32) -> Result<Session, Error> {
	let mut last_err = None;
	for attempt in 0..=max_retries {
		if attempt > 0 {
			let backoff = std::time::Duration::from_millis(100 * 2u64.pow(attempt - 1));
			tracing::warn!(
				err = ?last_err,
				attempt,
				"reconnect failed; retrying after {backoff:?}"
			);
			tokio::time::sleep(backoff).await;
		}
		match connector.connect(uri).await {
			Ok(session) => return Ok(session),
			Err(err) => last_err = Some(err),
		}
	}
	Err(last_err.expect("at least one attempt was made"))
}
