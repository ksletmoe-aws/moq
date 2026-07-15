//! Test harness that produces a connected `(client, server)` pair of
//! [`moq_net::Session`]s over the in-memory mock transport.
//!
//! The harness runs the full MoQ handshake (Client::connect + Server::accept)
//! over a [`MockSession`] pair, giving tests two live sessions ready for pub/sub
//! without any real QUIC or network I/O.

#![allow(dead_code)]

use moq_net::{Client, OriginConsumer, OriginProducer, Server, Session, Version};

use super::mock::create_mock_session_pair;

/// Options for [`connect_mock`].
pub struct MockConnectOptions {
	/// The MoQ version to negotiate (determines ALPN protocol string).
	pub version: Version,
	/// Origin to publish from the client side.
	pub client_publish: Option<OriginConsumer>,
	/// Origin to consume on the client side.
	pub client_consume: Option<OriginProducer>,
	/// Origin to publish from the server side.
	pub server_publish: Option<OriginConsumer>,
	/// Origin to consume on the server side.
	pub server_consume: Option<OriginProducer>,
}

impl MockConnectOptions {
	/// Create options for the given version with no origins attached.
	pub fn new(version: Version) -> Self {
		Self {
			version,
			client_publish: None,
			client_consume: None,
			server_publish: None,
			server_consume: None,
		}
	}
}

/// Run the MoQ handshake over mock transport, returning connected sessions.
///
/// Both sides negotiate the version via ALPN (the mock reports the protocol
/// string matching the requested version). This mirrors what happens with a real
/// QUIC transport where ALPN selects the wire format before the connection starts.
///
/// # Panics
///
/// Panics if the handshake fails on either side (test-only code).
pub async fn connect_mock(opts: MockConnectOptions) -> (Session, Session) {
	let protocol = opts.version.alpn();

	// SAFETY: The protocol string from Version::alpn() is 'static.
	// We need to leak a &'static str for the mock session since the trait
	// requires Option<&str> with the session's lifetime. Version::alpn()
	// already returns &'static str, so this is fine.
	let (client_transport, server_transport) = create_mock_session_pair(Some(protocol));

	let client_builder = Client::new()
		.with_versions(opts.version.into())
		.with_publish(opts.client_publish)
		.with_consume(opts.client_consume);

	let server_builder = Server::new()
		.with_versions(opts.version.into())
		.with_publish(opts.server_publish)
		.with_consume(opts.server_consume);

	// Run both handshakes concurrently.
	let (client_result, server_result) = tokio::join!(
		client_builder.connect(client_transport),
		server_builder.accept(server_transport)
	);

	let client_session = client_result.expect("client handshake failed");
	let server_session = server_result.expect("server handshake failed");

	(client_session, server_session)
}
