use std::{
	mem::ManuallyDrop,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	task::Poll,
	time::Duration,
};

use web_async::MaybeSendBoxFuture;
use web_transport_trait::Stats;

use crate::{BandwidthConsumer, BandwidthProducer, Error, Version};

// ── Send-path GOAWAY (drain) ────────────────────────────────────────

/// Payload carried by the GOAWAY signal from [`Drain`] to the protocol layer.
#[derive(Clone, Debug)]
pub(crate) struct GoawayPayload {
	/// Redirect URI (empty = reconnect to same endpoint).
	pub uri: Arc<str>,
	/// Timeout before force-close. None means no deadline.
	pub timeout: Option<Duration>,
}

/// The producing half of a GOAWAY signal channel.
///
/// Stored on [`Session`] and consumed by [`Session::drain`] to hand out a
/// [`Drain`] handle. Setting a value on the producer triggers the protocol
/// layer to send the GOAWAY frame to the peer.
pub(crate) type GoawayTrigger = kio::Producer<Option<GoawayPayload>>;

/// The consuming half of a GOAWAY signal channel.
///
/// Passed into the protocol-layer `start()` functions so the spawned send
/// task can await the signal and transmit the GOAWAY frame.
pub(crate) type GoawaySignal = kio::Consumer<Option<GoawayPayload>>;

/// Create a matched trigger/signal pair for the GOAWAY channel.
///
/// The trigger lives on [`Session`]; the signal is forwarded to the protocol
/// layer so it can watch for the drain request.
pub(crate) fn goaway_channel() -> (GoawayTrigger, GoawaySignal) {
	let trigger = GoawayTrigger::new(None);
	let signal = trigger.consume();
	(trigger, signal)
}

/// Wait for the GOAWAY signal to fire, returning the payload to encode.
/// Returns a default (empty URI, no timeout) if the signal channel was
/// dropped (session already closing).
pub(crate) async fn await_goaway(signal: &GoawaySignal) -> GoawayPayload {
	kio::wait(|waiter| {
		match signal.poll(waiter, |state| match &**state {
			Some(v) => Poll::Ready(v.clone()),
			None => Poll::Pending,
		}) {
			Poll::Ready(Ok(v)) => Poll::Ready(v),
			Poll::Ready(Err(_)) => Poll::Ready(GoawayPayload {
				uri: Arc::from(""),
				timeout: None,
			}),
			Poll::Pending => Poll::Pending,
		}
	})
	.await
}

// ── Receive-path GOAWAY ─────────────────────────────────────────────

/// Information from a received GOAWAY message.
///
/// The peer is telling us to reconnect to a new session at the provided URI
/// (or reconnect to the same endpoint if the URI is empty).
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct GoawayReceived {
	/// The URI to reconnect to. Empty means reconnect to the same endpoint.
	pub uri: Arc<str>,
	/// How long before the sender force-closes the session. None if not provided
	/// (moq-lite has no timeout on the wire; IETF draft-14-16 have no timeout).
	pub timeout: Option<Duration>,
}

/// The producing half of the received-GOAWAY channel.
///
/// Passed into protocol-layer `start()` functions so the receive task can
/// signal that a GOAWAY was decoded from the peer.
pub(crate) type GoawayReceivedSignal = kio::Producer<Option<GoawayReceived>>;

/// The consuming half of the received-GOAWAY channel.
///
/// Stored on [`Session`] and polled by [`Session::goaway`].
pub(crate) type GoawayReceivedConsumer = kio::Consumer<Option<GoawayReceived>>;

/// Create a matched signal/consumer pair for the received-GOAWAY channel,
/// plus a shared [`GoingAwayFlag`] that is set alongside the signal.
pub(crate) fn goaway_received_channel() -> (GoawayReceivedSignal, GoawayReceivedConsumer, GoingAwayFlag) {
	let signal = GoawayReceivedSignal::new(None);
	let consumer = signal.consume();
	let flag = GoingAwayFlag::new();
	(signal, consumer, flag)
}

/// A shared boolean flag set when GOAWAY is received from the peer.
///
/// Passed into subscriber tasks so they can cheaply check whether new requests
/// should be rejected without polling the full kio channel.
#[derive(Clone)]
pub(crate) struct GoingAwayFlag(Arc<AtomicBool>);

impl GoingAwayFlag {
	/// Create a new flag, initially `false`.
	pub fn new() -> Self {
		Self(Arc::new(AtomicBool::new(false)))
	}

	/// Mark the session as going away.
	pub fn set(&self) {
		self.0.store(true, Ordering::Release);
	}

	/// Check whether GOAWAY has been received.
	pub fn is_set(&self) -> bool {
		self.0.load(Ordering::Acquire)
	}
}

/// A handle for gracefully draining a session via GOAWAY.
///
/// Obtained from [`Session::drain`]. Call [`start`](Self::start) to send the
/// GOAWAY frame (consuming this handle), which returns a [`Draining`] handle.
/// Then call [`Draining::complete`] to wait for the peer to disconnect.
///
/// The [`Session`] must stay alive until [`Draining::complete`] resolves.
/// Dropping the session while a drain is in progress closes the transport
/// immediately, which prevents the GOAWAY from reaching the peer.
pub struct Drain {
	trigger: GoawayTrigger,
	session: ManuallyDrop<Arc<dyn SessionInner>>,
	draining: ManuallyDrop<Arc<AtomicBool>>,
	/// Set once [`start`](Self::start) fires the GOAWAY. Until then, dropping this
	/// handle releases the drain claim so a later [`Session::drain`] can retry.
	started: bool,
}

/// Active drain waiting for the peer to disconnect.
///
/// Returned by [`Drain::start`] or [`Drain::start_with_timeout`]. Call
/// [`complete`](Self::complete) to block until the peer closes (or the timeout
/// fires).
pub struct Draining {
	session: Arc<dyn SessionInner>,
	timeout: Option<Duration>,
}

impl Drain {
	/// Send the GOAWAY frame to the peer with no deadline.
	///
	/// `uri` is the new session URI the peer should reconnect to. Pass an empty
	/// string to tell the peer to reconnect to the same endpoint (matching the
	/// wire encoding where empty = same endpoint).
	pub fn start(self, uri: impl Into<Arc<str>>) -> Draining {
		self.start_inner(uri, None)
	}

	/// Send the GOAWAY frame with a deadline for the peer to disconnect.
	///
	/// `uri` is the new session URI the peer should reconnect to (empty = same
	/// endpoint). `timeout` is how long the sender waits before force-closing
	/// the session with [`Error::GoawayTimeout`]. The timeout is also encoded
	/// on the wire (for IETF draft-17+) so the peer knows the deadline.
	///
	/// The wire encoding uses 0 to mean "no deadline", so a `Duration::ZERO`
	/// here reaches the peer as "no timeout advertised" even though this side
	/// still force-closes essentially immediately. Pass a non-zero duration for
	/// a deadline the peer can observe.
	pub fn start_with_timeout(self, uri: impl Into<Arc<str>>, timeout: Duration) -> Draining {
		self.start_inner(uri, Some(timeout))
	}

	fn start_inner(mut self, uri: impl Into<Arc<str>>, timeout: Option<Duration>) -> Draining {
		// Mark started so Drop is a no-op, then extract the fields via ManuallyDrop.
		self.started = true;

		let payload = GoawayPayload {
			uri: uri.into(),
			timeout,
		};
		if let Ok(mut state) = self.trigger.write() {
			*state = Some(payload);
		}

		// SAFETY: started is true so Drop won't touch these fields; we take
		// ownership without an extra ref-count bump.
		let session = unsafe { ManuallyDrop::take(&mut self.session) };
		// Drop the draining Arc (it stays true forever once a GOAWAY is sent).
		unsafe { ManuallyDrop::drop(&mut self.draining) };

		Draining { session, timeout }
	}
}

impl Drop for Drain {
	fn drop(&mut self) {
		if self.started {
			// Fields were already taken by start_inner; nothing to drop.
			return;
		}
		// Release the drain claim so a later drain() can retry.
		self.draining.store(false, Ordering::Release);
		// SAFETY: start_inner did not run, so the fields are still valid.
		unsafe {
			ManuallyDrop::drop(&mut self.session);
			ManuallyDrop::drop(&mut self.draining);
		}
	}
}

impl Draining {
	/// Wait for the peer to close the session after receiving the GOAWAY.
	///
	/// If a timeout was provided via [`Drain::start_with_timeout`], the session
	/// is force-closed with [`Error::GoawayTimeout`] when the deadline expires.
	/// If the peer closes before the deadline, the timer is cancelled and this
	/// resolves normally.
	pub async fn complete(self) {
		if let Some(timeout) = self.timeout {
			tokio::select! {
				_ = self.session.closed() => {}
				_ = web_async::time::sleep(timeout) => {
					self.session.close(Error::GoawayTimeout.to_code(), "goaway timeout");
				}
			}
		} else {
			let _ = self.session.closed().await;
		}
	}
}

// ── Per-session sourced-path tracking ────────────────────────────────

/// Shared set of broadcast paths a session's subscriber is currently sourcing
/// into the origin. Updated by the subscriber (lite or ietf) as announcements
/// arrive. Read by [`Session::begin_failover`] to mark only the relevant paths.
pub(crate) type SourcedPaths = Arc<std::sync::Mutex<std::collections::HashSet<crate::PathOwned>>>;

/// Create a new empty sourced-paths set.
pub(crate) fn sourced_paths_new() -> SourcedPaths {
	Arc::new(std::sync::Mutex::new(std::collections::HashSet::new()))
}

/// A MoQ transport session, wrapping a WebTransport connection.
///
/// Created via:
/// - [`crate::Client::connect`] for clients.
/// - [`crate::Server::accept`] for servers.
#[derive(Clone)]
pub struct Session {
	session: Arc<dyn SessionInner>,
	version: Version,
	send_bandwidth: Option<BandwidthConsumer>,
	recv_bandwidth: Option<BandwidthConsumer>,
	goaway: GoawayTrigger,
	goaway_received: GoawayReceivedConsumer,
	going_away: GoingAwayFlag,
	draining: Arc<AtomicBool>,
	closed: bool,
	// Shared set of broadcast paths this session's subscriber is currently
	// sourcing into the origin. Used by `begin_failover()` to mark only the
	// paths belonging to this upstream.
	sourced_paths: SourcedPaths,
	// The origin this session publishes to (subscriber side). Stored so
	// `begin_failover()` can call `begin_failover(path)` on it.
	origin: Option<crate::OriginProducer>,
}

impl Session {
	pub(super) fn new<S: web_transport_trait::Session>(
		session: S,
		version: Version,
		recv_bandwidth: Option<BandwidthConsumer>,
		goaway: GoawayTrigger,
		goaway_received: GoawayReceivedConsumer,
		going_away: GoingAwayFlag,
	) -> Self {
		// Send bandwidth is version-agnostic: it depends on QUIC backend support.
		let send_bandwidth = if session.stats().estimated_send_rate().is_some() {
			let producer = BandwidthProducer::new();
			let consumer = producer.consume();

			let session = session.clone();
			web_async::spawn(async move {
				run_send_bandwidth(&session, producer).await;
			});

			Some(consumer)
		} else {
			None
		};

		Self {
			session: Arc::new(session),
			version,
			send_bandwidth,
			recv_bandwidth,
			goaway,
			goaway_received,
			going_away,
			draining: Arc::new(AtomicBool::new(false)),
			closed: false,
			sourced_paths: sourced_paths_new(),
			origin: None,
		}
	}

	/// Returns the negotiated protocol version.
	pub fn version(&self) -> Version {
		self.version
	}

	/// Returns a consumer for the estimated send bitrate (from the congestion controller).
	///
	/// Returns `None` if the QUIC backend doesn't support bandwidth estimation.
	pub fn send_bandwidth(&self) -> Option<BandwidthConsumer> {
		self.send_bandwidth.clone()
	}

	/// Returns a consumer for the estimated receive bitrate (from PROBE).
	///
	/// Returns `None` if the MoQ version doesn't support PROBE (requires moq-lite-03+).
	pub fn recv_bandwidth(&self) -> Option<BandwidthConsumer> {
		self.recv_bandwidth.clone()
	}

	/// Close the underlying transport session.
	pub fn close(&mut self, err: Error) {
		if self.closed {
			return;
		}
		self.closed = true;
		self.session.close(err.to_code(), err.to_string().as_ref());
	}

	/// Block until the transport session is closed, returning the close reason.
	///
	/// The returned error preserves the wire close code: known codes (e.g. 32 for
	/// [`Error::GoawayTimeout`]) are decoded back to their structured variant, so
	/// callers can pattern-match programmatically rather than parsing strings.
	pub async fn closed(&self) -> Result<(), Error> {
		Err(self.session.closed().await)
	}

	/// Initiate a graceful GOAWAY drain for this session.
	///
	/// Returns a [`Drain`] handle that triggers the GOAWAY and waits for the peer
	/// to disconnect. Call [`Drain::start`] to fire the signal and transition into
	/// the [`Draining`] state.
	///
	/// Returns `None` if a drain is already in progress (only one GOAWAY per
	/// session). The claim is released if the returned [`Drain`] is dropped
	/// before [`Drain::start`], so a caller that bails out can retry later.
	pub fn drain(&self) -> Option<Drain> {
		// Atomically claim the drain so two concurrent callers can't both get a
		// handle. compare_exchange fails if the flag is already set.
		if self
			.draining
			.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
			.is_err()
		{
			return None;
		}
		Some(Drain {
			trigger: self.goaway.clone(),
			session: ManuallyDrop::new(self.session.clone()),
			draining: ManuallyDrop::new(self.draining.clone()),
			started: false,
		})
	}

	/// Wait until a GOAWAY is received from the peer.
	///
	/// Resolves once the remote endpoint signals that this session should reconnect.
	/// Returns the GOAWAY payload (URI to reconnect to, and optional timeout).
	/// Returns `None` if the session closes before a GOAWAY arrives.
	pub async fn goaway(&self) -> Option<GoawayReceived> {
		kio::wait(|waiter| {
			match self.goaway_received.poll(waiter, |state| match &**state {
				Some(v) => Poll::Ready(v.clone()),
				None => Poll::Pending,
			}) {
				Poll::Ready(Ok(v)) => Poll::Ready(Some(v)),
				Poll::Ready(Err(_)) => Poll::Ready(None),
				Poll::Pending => Poll::Pending,
			}
		})
		.await
	}

	/// Returns whether a GOAWAY has been received from the peer.
	///
	/// Once this returns `true`, new subscribe and subscribe-namespace requests
	/// initiated on this session are rejected with [`Error::GoingAway`]. Existing
	/// subscriptions keep flowing until the session closes.
	pub fn is_going_away(&self) -> bool {
		self.going_away.is_set()
	}

	/// Begin a seamless failover for every broadcast path this session is sourcing.
	///
	/// While the returned [`crate::FailoverGuard`] is held, downstream publishers
	/// serving paths from this session will re-resolve at group boundaries instead
	/// of tearing down when the current source ends. This is the session-scoped
	/// complement to [`crate::OriginProducer::begin_failover`] (which targets a
	/// single path): it marks exactly the set of paths this session's subscriber
	/// published into the shared origin.
	///
	/// Returns `None` if this session has no origin (a publish-only session with no
	/// subscriber side) or if no paths are currently sourced.
	pub fn begin_failover(&self) -> Option<crate::FailoverGuard> {
		let origin = self.origin.as_ref()?;
		let paths = self.sourced_paths.lock().expect("sourced_paths poisoned");
		if paths.is_empty() {
			return None;
		}
		let guards: Vec<crate::FailoverGuard> = paths.iter().map(|p| origin.begin_failover(p)).collect();
		Some(crate::FailoverGuard::combined(guards))
	}

	/// Attach the subscriber's shared path tracker and origin to this session.
	///
	/// Called by the lite/ietf session setup code after the subscriber is created.
	/// Required for [`Self::begin_failover`] to know which paths this session sources.
	pub(crate) fn attach_subscriber_state(&mut self, sourced_paths: SourcedPaths, origin: crate::OriginProducer) {
		self.sourced_paths = sourced_paths;
		self.origin = Some(origin);
	}

	/// Establish a new session after receiving a GOAWAY, then close this one.
	///
	/// # What it does
	///
	/// 1. Verifies that a GOAWAY has been received (returns [`Error::NotReady`]
	///    if called before a GOAWAY arrives).
	/// 2. Calls the [`Connector`](crate::Connector) with the GOAWAY URI (or an
	///    empty string if the GOAWAY URI was empty, meaning "same endpoint").
	/// 3. On success, closes this session with NO_ERROR and returns the new
	///    [`Session`].
	///
	/// The new session should share the same [`crate::OriginProducer`] so that
	/// broadcasts announced by the new server flow into the caller's existing
	/// [`crate::OriginConsumer`].
	///
	/// # What the caller must do
	///
	/// Existing [`crate::TrackConsumer`] handles from the old session will close
	/// once this session shuts down. The caller observes the new server's
	/// broadcasts through their existing [`crate::OriginConsumer`] (announce loop)
	/// and re-subscribes to tracks on the new [`crate::BroadcastConsumer`].
	///
	/// For a clean group-boundary resume with no duplicate or partial groups,
	/// re-subscribe using [`crate::StartPosition::NextGroup`]. This tells the
	/// publisher to begin delivery at the next complete group, which is a >=
	/// threshold (safe with non-sequential group IDs).
	///
	/// # Relay composition
	///
	/// A relay achieves seamless downstream playback by calling `reconnect` on its
	/// upstream session (same origin) and letting its downstream fan-out
	/// re-announce. Downstream subscribers keep receiving without seeing a GOAWAY;
	/// the reconnection is transparent at the relay boundary.
	///
	/// # Failure
	///
	/// If the [`Connector`](crate::Connector) fails, this method returns the error
	/// and the old session remains open and usable, so the caller can retry or fall
	/// back.
	pub async fn reconnect(
		&mut self,
		connector: impl crate::reconnect::Connector,
		options: crate::ReconnectOptions,
	) -> Result<Session, Error> {
		crate::reconnect::run_reconnect(self, &connector, &options).await
	}

	/// Non-blocking read of the received GOAWAY, if any.
	pub(crate) fn goaway_received_snapshot(&self) -> Option<GoawayReceived> {
		self.goaway_received.read().clone()
	}
}

impl Drop for Session {
	fn drop(&mut self) {
		if !self.closed {
			if self.draining.load(Ordering::Acquire) {
				tracing::warn!(
					"session dropped while draining; keep the Session alive until Draining::complete() resolves"
				);
			}
			self.session.close(Error::Cancel.to_code(), "dropped");
		}
	}
}

/// Polls the QUIC congestion controller for estimated send rate.
///
/// Exits as soon as the session closes so we don't pin the underlying connection
/// after the wrapping [`Session`] is dropped.
async fn run_send_bandwidth<S: web_transport_trait::Session>(session: &S, producer: BandwidthProducer) {
	tokio::select! {
		_ = session.closed() => {}
		_ = producer.closed() => {}
		_ = run_send_bandwidth_inner(session, &producer) => {}
	}
}

/// Toggles between waiting for a consumer and polling stats while one exists.
/// Returns when the producer channel errors (closed by the consumer side).
async fn run_send_bandwidth_inner<S: web_transport_trait::Session>(session: &S, producer: &BandwidthProducer) {
	const POLL_INTERVAL: Duration = Duration::from_millis(100);

	loop {
		if producer.used().await.is_err() {
			return;
		}

		let mut interval = web_async::time::interval(POLL_INTERVAL);
		loop {
			tokio::select! {
				biased;
				res = producer.unused() => {
					if res.is_err() {
						return;
					}
					// No more consumers, pause polling.
					break;
				}
				_ = interval.tick() => {
					let bitrate = session.stats().estimated_send_rate();
					if producer.set(bitrate).is_err() {
						return;
					}
				}
			}
		}
	}
}

// We use a wrapper type that is dyn-compatible to remove the generic bounds from Session.
trait SessionInner: web_transport_trait::MaybeSend + web_transport_trait::MaybeSync {
	fn close(&self, code: u32, reason: &str);
	fn closed(&self) -> MaybeSendBoxFuture<'_, Error>;
}

impl<S: web_transport_trait::Session> SessionInner for S {
	fn close(&self, code: u32, reason: &str) {
		S::close(self, code, reason);
	}

	fn closed(&self) -> MaybeSendBoxFuture<'_, Error> {
		Box::pin(async move { Error::from_transport(S::closed(self).await) })
	}
}
