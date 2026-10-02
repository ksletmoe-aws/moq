//! The MAX_REQUEST_UPDATES Setup Option (draft-ietf-moq-transport-19 section 10.3.1.7).
//!
//! A request stream (SUBSCRIBE, PUBLISH_NAMESPACE, ...) can be refreshed in place with
//! REQUEST_UPDATE messages. Each is outstanding from when the peer sends it until its
//! REQUEST_OK or REQUEST_ERROR, and MAX_REQUEST_UPDATES caps how many a peer may leave
//! outstanding on one stream at once. We verify a request token asynchronously, so a slow
//! acceptor holds one renewal unanswered while more pile up behind it; this bound is what
//! stops that queue from growing without limit.
//!
//! The option is draft-19+ (Option Type 0x08). Advertising it lets a conforming peer
//! self-limit, so a peer that exceeds the advertised value broke a negotiated rule, which
//! the draft answers with a session close ([`SessionError::TooManyRequestUpdates`]). Drafts
//! below 19 carry no such option, so a peer there agreed to no ceiling: the receiver keeps
//! a generous local guard that ends only the one request rather than fault a peer at
//! session scope for a limit it never saw.
//!
//! [`SessionError::TooManyRequestUpdates`]: crate::SessionError::TooManyRequestUpdates

use super::Version;

/// MAX_REQUEST_UPDATES Setup Option (Option Type 0x08). Even, so the value is a bare varint.
pub const OPTION: u64 = 0x08;

/// The credit we advertise and enforce on drafts that define the option: the most
/// unacknowledged REQUEST_UPDATEs we accept on one request stream, counting the one being
/// verified. One constant drives both the advertisement and the enforcement so the value we
/// promise and the value we hold a peer to cannot drift.
pub const MAX_REQUEST_UPDATES: u64 = 16;

/// Local resource guard on drafts without the option. Generous relative to the negotiated
/// credit, since those drafts negotiate no limit and a conforming peer must never reach it, but
/// still bounded: a control message carries up to a 16-bit length, so this caps the buffered
/// bytes per request stream rather than letting a flood grow without end. Exceeding it ends the
/// one request, never the session.
pub const UNNEGOTIATED_GUARD: usize = 64;

/// Whether this version defines MAX_REQUEST_UPDATES. Draft-19 section 10.3.1.7 added it; the
/// drafts below carry no such Setup Option.
pub fn supported(version: Version) -> bool {
	!matches!(
		version,
		Version::Draft14 | Version::Draft15 | Version::Draft16 | Version::Draft17 | Version::Draft18
	)
}

/// Advertise our MAX_REQUEST_UPDATES credit, on versions that define the option.
pub fn into_setup(params: &mut super::Parameters, version: Version) {
	if supported(version) {
		params.set_varint(super::ParameterVarInt::MaxRequestUpdates, MAX_REQUEST_UPDATES);
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	/// The option rides SETUP only on the drafts that define it; an older peer sees nothing,
	/// which is what keeps the bound a negotiated contract rather than a surprise.
	#[test]
	fn advertised_only_on_draft_19_plus() {
		for version in [Version::Draft19, Version::Draft20, Version::Draft21, Version::Draft22] {
			let mut params = super::super::Parameters::default();
			into_setup(&mut params, version);
			assert_eq!(
				params.get_varint(super::super::ParameterVarInt::MaxRequestUpdates),
				Some(MAX_REQUEST_UPDATES),
				"{version} must advertise MAX_REQUEST_UPDATES"
			);
		}

		for version in [
			Version::Draft14,
			Version::Draft15,
			Version::Draft16,
			Version::Draft17,
			Version::Draft18,
		] {
			let mut params = super::super::Parameters::default();
			into_setup(&mut params, version);
			assert_eq!(
				params.get_varint(super::super::ParameterVarInt::MaxRequestUpdates),
				None,
				"{version} has no MAX_REQUEST_UPDATES option to advertise"
			);
		}
	}
}
