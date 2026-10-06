use std::task::{Context, Poll};

use super::Version;
use crate::{Error, coding::Stream};

pub(super) fn fin_cancels(version: Version) -> bool {
	matches!(
		version,
		Version::Draft14 | Version::Draft15 | Version::Draft16 | Version::Draft17 | Version::Draft18
	)
}

/// Watch abrupt cancellation after the requester has finished sending messages.
pub(super) fn poll_cancel<S: crate::transport::poll::Session>(
	stream: &mut Stream<S, Version>,
	finished: &mut bool,
	version: Version,
	cx: &mut Context<'_>,
) -> Poll<Result<(), Error>> {
	if !*finished {
		match stream.reader.poll_closed(cx) {
			Poll::Ready(Ok(())) if !fin_cancels(version) => *finished = true,
			Poll::Ready(result) => return Poll::Ready(result),
			Poll::Pending => {}
		}
	}
	// Virtual writers on drafts 14-16 are already closed while their control request lives.
	if !matches!(version, Version::Draft14 | Version::Draft15 | Version::Draft16) {
		return stream.writer.poll_closed(cx);
	}
	Poll::Pending
}

/// A framed subscription update, preserving omitted preferences.
pub(super) struct Update {
	pub(super) request_id: super::RequestId,
	pub(super) priority: Option<u8>,
	// The AUTHORIZATION TOKEN carried on this update, captured rather than folded into
	// `unsupported`, so a request-token renewal can be verified (MoQ request-token).
	pub(super) authorization_token: Option<bytes::Bytes>,
	pub(super) unsupported: bool,
}

impl std::fmt::Debug for Update {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("Update")
			.field("request_id", &self.request_id)
			.field("priority", &self.priority)
			.field(
				"authorization_token",
				&super::token::Redacted(&self.authorization_token),
			)
			.field("unsupported", &self.unsupported)
			.finish()
	}
}

impl crate::coding::Decode<Version> for Update {
	fn decode<R: bytes::Buf>(r: &mut R, version: Version) -> Result<Self, crate::coding::DecodeError> {
		use super::{Fill, Filter, Opaque, RequestId};
		use crate::coding::DecodeError;
		if u64::decode(r, version)? != 0x02 {
			return Err(DecodeError::InvalidValue);
		}
		let size = u16::decode(r, version)? as usize;
		if r.remaining() < size {
			return Err(DecodeError::Short);
		}
		let mut data = r.copy_to_bytes(size);
		let result = (|| {
			let request_id = RequestId::decode(&mut data, version)?;
			if version == Version::Draft17 {
				u64::decode(&mut data, version)?;
			}
			decode_params!(&mut data, version,
				0x02 => object_timeout: Option<u64>,
				0x03 => authorization_token: Option<bytes::Bytes>,
				0x06 => subgroup_timeout: Option<u64>,
				0x10 => forward: Option<bool>,
				0x20 => priority: Option<u8>,
				0x21 => filter: Option<Filter>,
				0x23 => fill: Option<Fill>,
				0x25 => subgroup_filter: Vec<Opaque>,
				0x26 => object_filter: Vec<Opaque>,
				0x27 => priority_filter: Vec<Opaque>,
				0x28 => property_filter: Vec<Opaque>,
				0x29 => track_filter: Vec<Opaque>,
				0x32 => new_group: Option<u64>,
			);
			if !data.is_empty() {
				return Err(DecodeError::InvalidValue);
			}
			Ok(Self {
				request_id,
				priority,
				authorization_token,
				unsupported: forward == Some(false)
					|| filter.is_some()
					|| fill.is_some()
					|| object_timeout.is_some()
					|| subgroup_timeout.is_some()
					|| !subgroup_filter.is_empty()
					|| !object_filter.is_empty()
					|| !priority_filter.is_empty()
					|| !property_filter.is_empty()
					|| !track_filter.is_empty()
					|| new_group.is_some(),
			})
		})();
		// The complete frame is present; a short field inside it is malformed.
		result.map_err(|err| {
			if matches!(err, DecodeError::Short) {
				DecodeError::InvalidValue
			} else {
				err
			}
		})
	}
}
