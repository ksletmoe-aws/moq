//! Embeddable MoQ relay for connecting publishers to subscribers.
//!
//! The relay is content-agnostic: it forwards live data without
//! interpreting it, so it works equally well for media, sensor telemetry,
//! or any other stream. Clustering, JWT authentication, WebSocket
//! fallback, and an HTTP API are all included.
//!
//! See `main.rs` for a complete example of how these pieces fit together.

mod auth;
mod cluster;
mod config;
mod connection;
mod http_client;
mod stats;
mod web;
#[cfg(feature = "websocket")]
mod websocket;

/// The relay needs higher stream limits than the library default
/// to handle many concurrent subscriptions across connections.
pub const DEFAULT_MAX_STREAMS: u64 = 10_000;

/// Default GOAWAY drain timeout in seconds. After sending GOAWAY the relay
/// waits this long for the peer to close before force-closing with
/// GOAWAY_TIMEOUT (moq-transport draft-19, section 3.6).
#[doc(hidden)]
pub const DEFAULT_DRAIN_TIMEOUT_SECS: u64 = 10;

pub use auth::*;
pub use cluster::*;
pub use config::*;
pub use connection::*;
pub use stats::*;
pub use web::*;
