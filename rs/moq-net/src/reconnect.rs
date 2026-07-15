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
//! 2. Call the [`Connector`] to establish a new session at the GOAWAY URI.
//! 3. Close the old session with NO_ERROR once the new session is established.
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

/// Options for [`Session::reconnect`].
///
/// Empty today. Future additions (timeout override, reconnect-publishers flag)
/// will be non-breaking because this struct is `#[non_exhaustive]`.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct ReconnectOptions {}

impl ReconnectOptions {
	/// Create default reconnect options.
	pub fn new() -> Self {
		Self::default()
	}
}

/// Drive the reconnection sequence.
///
/// Called by [`Session::reconnect`]; not public because the entry point is on Session.
pub(crate) async fn run_reconnect(
	old_session: &mut Session,
	connector: &impl Connector,
	_options: &ReconnectOptions,
) -> Result<Session, Error> {
	// The snapshot is the single source of truth. If it's None, no GOAWAY
	// has been fully received yet (avoids a race with the going_away flag
	// which is set a few instructions before the snapshot is written).
	let goaway = old_session.goaway_received_snapshot().ok_or(Error::NotReady)?;
	let uri: &str = &goaway.uri;

	let new_session = connector.connect(uri).await?;

	old_session.close(Error::Cancel);

	Ok(new_session)
}
