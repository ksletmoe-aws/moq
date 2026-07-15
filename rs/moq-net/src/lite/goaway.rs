use std::borrow::Cow;

use crate::coding::*;

use super::{Message, Version};

/// Sent to gracefully shut down a session and optionally redirect to a new URI.
///
/// Lite04+ only.
#[derive(Clone, Debug)]
pub struct Goaway<'a> {
	pub uri: Cow<'a, str>,
}

impl Message for Goaway<'_> {
	fn decode_msg<R: bytes::Buf>(r: &mut R, version: Version) -> Result<Self, DecodeError> {
		match version {
			Version::Lite01 | Version::Lite02 | Version::Lite03 => {
				return Err(DecodeError::Version);
			}
			_ => {}
		}

		let uri = Cow::<str>::decode(r, version)?;
		Ok(Self { uri })
	}

	fn encode_msg<W: bytes::BufMut>(&self, w: &mut W, version: Version) -> Result<(), EncodeError> {
		match version {
			Version::Lite01 | Version::Lite02 | Version::Lite03 => {
				return Err(EncodeError::Version);
			}
			_ => {}
		}

		self.uri.encode(w, version)?;
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use bytes::BytesMut;

	#[test]
	fn roundtrip_with_uri() {
		let msg = Goaway {
			uri: Cow::Borrowed("https://relay.example/new"),
		};
		let mut buf = BytesMut::new();
		msg.encode_msg(&mut buf, Version::Lite04).unwrap();

		let decoded = Goaway::decode_msg(&mut buf.freeze(), Version::Lite04).unwrap();
		assert_eq!(decoded.uri, "https://relay.example/new");
	}

	#[test]
	fn roundtrip_empty() {
		let msg = Goaway { uri: Cow::Borrowed("") };
		let mut buf = BytesMut::new();
		msg.encode_msg(&mut buf, Version::Lite04).unwrap();

		let decoded = Goaway::decode_msg(&mut buf.freeze(), Version::Lite04).unwrap();
		assert_eq!(decoded.uri, "");
	}

	#[test]
	fn rejected_before_lite04() {
		let msg = Goaway {
			uri: Cow::Borrowed("https://relay.example/new"),
		};
		let mut buf = BytesMut::new();

		// Encoding should fail on Lite03.
		assert!(msg.encode_msg(&mut buf, Version::Lite03).is_err());

		// Even if we have valid bytes, decoding on Lite03 should fail.
		let mut encode_buf = BytesMut::new();
		msg.encode_msg(&mut encode_buf, Version::Lite04).unwrap();
		assert!(Goaway::decode_msg(&mut encode_buf.freeze(), Version::Lite03).is_err());
	}
}
