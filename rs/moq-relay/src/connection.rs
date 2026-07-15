use crate::{Auth, AuthError, AuthParams, AuthToken, Cluster};

use axum::http;
use moq_native::Request;

/// An error carrying the HTTP status to send when closing the request.
///
/// Used only on the pre-accept auth path so the caller can close once with
/// the right code instead of sprinkling close/return at each failure site.
struct StatusError {
	status: http::StatusCode,
	source: anyhow::Error,
}

impl From<AuthError> for StatusError {
	fn from(err: AuthError) -> Self {
		Self {
			status: (&err).into(),
			source: err.into(),
		}
	}
}

/// An incoming connection that has not yet been authenticated.
///
/// Call [`run`](Self::run) to authenticate the request, wire up
/// publish/subscribe origins, and serve the session until it closes.
pub struct Connection {
	/// A numeric identifier for logging.
	pub id: u64,
	/// The raw QUIC/WebTransport request to accept or reject.
	pub request: Request,
	/// The cluster state used to resolve origins.
	pub cluster: Cluster,
	/// The authenticator used to verify credentials.
	pub auth: Auth,
	/// How long to wait for the peer to close after GOAWAY before force-closing.
	pub drain_timeout: std::time::Duration,
}

impl Connection {
	/// Authenticates and serves this connection until it closes.
	#[tracing::instrument("conn", skip_all, fields(id = self.id))]
	pub async fn run(self) -> anyhow::Result<()> {
		// Hold the sender for the whole call so the drain receiver never fires.
		// Dropping it (e.g. passing `channel(false).1`) makes `changed()` resolve
		// immediately, which would drain the session the instant it connects.
		let (_drain_tx, drain_rx) = tokio::sync::watch::channel(false);
		self.run_with_drain(drain_rx).await
	}

	/// Authenticates and serves this connection, draining on shutdown signal.
	#[tracing::instrument("conn", skip_all, fields(id = self.id))]
	pub async fn run_with_drain(self, mut shutdown: tokio::sync::watch::Receiver<bool>) -> anyhow::Result<()> {
		let token = match self.authenticate().await {
			Ok(token) => token,
			Err(err) => {
				let _ = self.request.close(err.status.as_u16()).await;
				return Err(err.source);
			}
		};

		let publish = self.cluster.publisher(&token);
		let subscribe = self.cluster.subscriber(&token);
		let transport = self.request.transport();

		match (&publish, &subscribe) {
			(Some(publish), Some(subscribe)) => {
				tracing::info!(transport, internal = token.internal, root = %token.root, publish = %publish.allowed().map(|p| p.as_str()).collect::<Vec<_>>().join(","), subscribe = %subscribe.allowed().map(|p| p.as_str()).collect::<Vec<_>>().join(","), "session accepted");
			}
			(Some(publish), None) => {
				tracing::info!(transport, internal = token.internal, root = %token.root, publish = %publish.allowed().map(|p| p.as_str()).collect::<Vec<_>>().join(","), "publisher accepted");
			}
			(None, Some(subscribe)) => {
				tracing::info!(transport, internal = token.internal, root = %token.root, subscribe = %subscribe.allowed().map(|p| p.as_str()).collect::<Vec<_>>().join(","), "subscriber accepted")
			}
			_ => {
				let _ = self.request.close(http::StatusCode::FORBIDDEN.as_u16()).await;
				anyhow::bail!("invalid session; no allowed paths");
			}
		}

		// mTLS-authenticated peers (including other cluster nodes) report through
		// the internal tier so a billing service can rate-differentiate from
		// external traffic. The aggregator is shared; the tier picks which counter
		// set within each level the bumps land in.
		let tier = match token.internal {
			true => moq_net::Tier::Internal,
			false => moq_net::Tier::External,
		};
		let stats = self.cluster.stats.tier(tier);

		// Count this session against its auth root for the whole connection,
		// independent of any data flow, so presence-based billing sees a client
		// that connects to e.g. `/acme` even while idle. Dropped when
		// the connection closes below.
		let _session_stats = stats.session(&token.root);

		// Accept the connection.
		// NOTE: subscribe and publish seem backwards because of how relays work.
		// We publish the tracks the client is allowed to subscribe to.
		// We subscribe to the tracks the client is allowed to publish.
		let mut session = self
			.request
			.with_publish(subscribe)
			.with_consume(publish)
			.with_stats(stats)
			.ok()
			.await?;

		tracing::info!(version = %session.version(), transport, "negotiated");

		// The credential (JWT `exp` or client cert `notAfter`) is only checked at
		// connect time, so hold the session open no longer than the credential is
		// valid. Without an expiry, just wait for the session to close.
		let Some(expires) = token.expires else {
			tokio::select! {
				res = session.closed() => return Ok(res?),
				_ = shutdown.changed() => {
					tracing::info!("draining session");
					if let Some(drain) = session.drain() {
						// moq-transport draft-19 section 3.6: the sender SHOULD close
						// the session after the advertised Timeout.
						drain.start_with_timeout("", self.drain_timeout).complete().await;
					}
					return Ok(());
				}
			}
		};

		let remaining = expires.duration_since(std::time::SystemTime::now()).unwrap_or_default();
		tokio::select! {
			res = tokio::time::timeout(remaining, session.closed()) => {
				match res {
					Ok(res) => Ok(res?),
					Err(_) => {
						tracing::info!("credential expired, closing session");
						session.close(moq_net::Error::Unauthorized);
						Ok(())
					}
				}
			}
			_ = shutdown.changed() => {
				tracing::info!("draining session");
				if let Some(drain) = session.drain() {
					// moq-transport draft-19 section 3.6: the sender SHOULD close
					// the session after the advertised Timeout.
					drain.start_with_timeout("", self.drain_timeout).complete().await;
				}
				Ok(())
			}
		}
	}

	/// Resolve an [`AuthToken`] for this connection. Any failure is returned as a
	/// [`StatusError`] so [`run`] can close the request with the mapped HTTP
	/// status exactly once.
	///
	/// Every transport goes through the same authenticator; only the source of
	/// the path + JWT differs:
	/// - URL-bearing transports (QUIC, WebSocket) take it from the request URL,
	///   and a valid mTLS client certificate (QUIC only) stands in for a JWT,
	///   granting full access within the URL path's root.
	/// - Stream transports (`tcp`/`unix`) take the path + `?jwt=` from the
	///   moq-lite-05 SETUP. A no-JWT connection resolves anonymous/public access
	///   for its path exactly like a tokenless QUIC client (`--auth-public`).
	///   Unix peer-credential gating happens earlier, in the listener.
	async fn authenticate(&self) -> Result<AuthToken, StatusError> {
		let params = match self.request.url() {
			// URL-bearing transports: mTLS (QUIC only) can stand in for a JWT.
			Some(url) => {
				let params = self.auth.params_from_url(url);
				if let Some(identity) = self.request.peer_identity() {
					tracing::debug!("mTLS peer authenticated");
					// Scope the grant to the canonical root. An mTLS publisher dialing a
					// vanity alias lands on the same tree a JWT would; cluster peers dial
					// "/", which the API resolves (typically to an unscoped root). The API
					// also returns the billing tier (defaulting to internal for trusted peers).
					let mut token = self.auth.verify_mtls(&params.path).await?;
					// Close the session when the client certificate expires, mirroring
					// the JWT `exp` handling. Validated once at the TLS handshake otherwise.
					token.expires = identity.expiry();
					return Ok(token);
				}
				params
			}
			// URL-less stream transports: path + `?jwt=` ride the SETUP.
			None => AuthParams::from_path(self.request.path().unwrap_or("")),
		};

		Ok(self.auth.verify(&params).await?)
	}
}
