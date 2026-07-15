//! IETF moq-transport subscribe messages (v14 + v15)

use std::borrow::Cow;

use bytes::Buf;
use num_enum::{IntoPrimitive, TryFromPrimitive};

use crate::{
	Path,
	coding::*,
	ietf::{GroupOrder, Location, Parameters, RequestId},
};

use super::Message;
use super::namespace::{decode_namespace, encode_namespace};

use super::Version;

#[derive(Default, Clone, Copy, Debug, PartialEq, Eq, TryFromPrimitive, IntoPrimitive)]
#[repr(u64)]
pub enum FilterType {
	NextGroup = 0x01,
	#[default]
	LargestObject = 0x2,
	AbsoluteStart = 0x3,
	AbsoluteRange = 0x4,
}

impl Encode<Version> for FilterType {
	fn encode<W: bytes::BufMut>(&self, w: &mut W, version: Version) -> Result<(), EncodeError> {
		u64::from(*self).encode(w, version)?;
		Ok(())
	}
}

impl Decode<Version> for FilterType {
	fn decode<R: bytes::Buf>(r: &mut R, version: Version) -> Result<Self, DecodeError> {
		Self::try_from(u64::decode(r, version)?).map_err(|_| DecodeError::InvalidValue)
	}
}

/// Subscribe message (0x03)
/// Sent by the subscriber to request all future objects for the given track.
#[derive(Clone, Debug)]
pub struct Subscribe<'a> {
	pub request_id: RequestId,
	pub track_namespace: Path<'a>,
	pub track_name: Cow<'a, str>,
	pub subscriber_priority: u8,
	pub group_order: GroupOrder,
	pub filter_type: FilterType,
	/// Start location for `FilterType::AbsoluteStart` and `FilterType::AbsoluteRange`.
	/// Ignored for other filter types.
	pub start_location: Option<Location>,
	/// Absolute end group (inclusive) for `FilterType::AbsoluteRange`. The publisher
	/// delivers groups up to and including this sequence, then finishes the subscription.
	/// `None` when the filter has no end bound.
	pub end_group: Option<u64>,
}

/// Combined filter type + optional start location + optional end group for parameter 0x21.
///
/// The parameter value contains the filter varint followed by a Location
/// (group + object) when the filter is AbsoluteStart or AbsoluteRange.
/// For AbsoluteRange, an End Group Delta varint follows (draft-17+: the wire
/// value is a delta from start_group; internally we store the absolute end group).
#[derive(Clone, Debug, Default)]
struct FilterWithLocation {
	filter_type: FilterType,
	start_location: Option<Location>,
	/// Absolute end group (inclusive). On the wire for draft-17+ this is encoded
	/// as a delta from start_location.group.
	end_group: Option<u64>,
}

impl super::Param for FilterWithLocation {
	fn param_encode<W: bytes::BufMut>(&self, w: &mut W, version: Version) -> Result<(), EncodeError> {
		let sv = match version {
			Version::Draft14 | Version::Draft15 | Version::Draft16 => Version::Draft15,
			_ => version,
		};
		let mut buf = Vec::new();
		self.filter_type.encode(&mut buf, sv)?;
		match self.filter_type {
			FilterType::AbsoluteStart => {
				let loc = self
					.start_location
					.as_ref()
					.unwrap_or(&Location { group: 0, object: 0 });
				loc.group.encode(&mut buf, sv)?;
				loc.object.encode(&mut buf, sv)?;
			}
			FilterType::AbsoluteRange => {
				let loc = self
					.start_location
					.as_ref()
					.unwrap_or(&Location { group: 0, object: 0 });
				loc.group.encode(&mut buf, sv)?;
				loc.object.encode(&mut buf, sv)?;
				// Wire format is End Group Delta (absolute_end - start_group).
				let end_abs = self.end_group.unwrap_or(loc.group);
				let delta = end_abs.saturating_sub(loc.group);
				delta.encode(&mut buf, sv)?;
			}
			_ => {}
		}
		buf.encode(w, version)?;
		Ok(())
	}

	fn param_decode<R: bytes::Buf>(r: &mut R, version: Version) -> Result<Self, DecodeError> {
		let data = Vec::<u8>::decode(r, version)?;
		let mut buf = bytes::Bytes::from(data);
		let sv = match version {
			Version::Draft14 | Version::Draft15 | Version::Draft16 => Version::Draft15,
			_ => version,
		};
		let filter_type = FilterType::decode(&mut buf, sv)?;
		let (start_location, end_group) = match filter_type {
			FilterType::AbsoluteStart if buf.has_remaining() => {
				let group = u64::decode(&mut buf, sv)?;
				let object = u64::decode(&mut buf, sv)?;
				(Some(Location { group, object }), None)
			}
			FilterType::AbsoluteRange if buf.has_remaining() => {
				let group = u64::decode(&mut buf, sv)?;
				let object = u64::decode(&mut buf, sv)?;
				// Wire value is End Group Delta; convert to absolute.
				let end = if buf.has_remaining() {
					let delta = u64::decode(&mut buf, sv)?;
					Some(group.saturating_add(delta))
				} else {
					None
				};
				(Some(Location { group, object }), end)
			}
			_ => (None, None),
		};
		Ok(Self {
			filter_type,
			start_location,
			end_group,
		})
	}
}

impl Message for Subscribe<'_> {
	const ID: u64 = 0x03;

	fn decode_msg<R: bytes::Buf>(r: &mut R, version: Version) -> Result<Self, DecodeError> {
		let request_id = RequestId::decode(r, version)?;
		if version == Version::Draft17 {
			let _required_request_id_delta = u64::decode(r, version)?;
		}
		let track_namespace = decode_namespace(r, version)?;
		let track_name = Cow::<str>::decode(r, version)?;

		match version {
			Version::Draft14 => {
				let subscriber_priority = u8::decode(r, version)?;
				let group_order = GroupOrder::decode(r, version)?;

				let forward = bool::decode(r, version)?;
				if !forward {
					return Err(DecodeError::Unsupported);
				}

				let filter_type = FilterType::decode(r, version)?;
				let (start_location, end_group) = match filter_type {
					FilterType::AbsoluteStart => (Some(Location::decode(r, version)?), None),
					FilterType::AbsoluteRange => {
						let start = Location::decode(r, version)?;
						let end = u64::decode(r, version)?;
						(Some(start), Some(end))
					}
					FilterType::NextGroup | FilterType::LargestObject => (None, None),
				};

				let _params = Parameters::decode(r, version)?;

				Ok(Self {
					request_id,
					track_namespace,
					track_name,
					subscriber_priority,
					group_order,
					filter_type,
					start_location,
					end_group,
				})
			}
			_ => {
				decode_params!(r, version,
					0x10 => forward: Option<bool>,
					0x20 => subscriber_priority: Option<u8>,
					0x21 => filter_with_loc: Option<FilterWithLocation>,
					0x22 => group_order: Option<GroupOrder>,
				);

				if forward == Some(false) {
					return Err(DecodeError::Unsupported);
				}

				let subscriber_priority = subscriber_priority.unwrap_or(128);
				let group_order = group_order.unwrap_or(GroupOrder::Descending);
				let fwl = filter_with_loc.unwrap_or_default();

				Ok(Self {
					request_id,
					track_namespace,
					track_name,
					subscriber_priority,
					group_order,
					filter_type: fwl.filter_type,
					start_location: fwl.start_location,
					end_group: fwl.end_group,
				})
			}
		}
	}

	fn encode_msg<W: bytes::BufMut>(&self, w: &mut W, version: Version) -> Result<(), EncodeError> {
		self.request_id.encode(w, version)?;
		if version == Version::Draft17 {
			0u64.encode(w, version)?; // required_request_id_delta = 0 (draft-17 only, removed in draft-18 per #1615)
		}
		encode_namespace(w, &self.track_namespace, version)?;
		self.track_name.encode(w, version)?;

		match version {
			Version::Draft14 => {
				self.subscriber_priority.encode(w, version)?;
				self.group_order.encode(w, version)?;
				true.encode(w, version)?; // forward

				self.filter_type.encode(w, version)?;
				match self.filter_type {
					FilterType::AbsoluteStart => {
						let loc = self
							.start_location
							.as_ref()
							.unwrap_or(&Location { group: 0, object: 0 });
						loc.encode(w, version)?;
					}
					FilterType::AbsoluteRange => {
						let loc = self
							.start_location
							.as_ref()
							.unwrap_or(&Location { group: 0, object: 0 });
						loc.encode(w, version)?;
						let end = self.end_group.unwrap_or(0);
						end.encode(w, version)?;
					}
					_ => {}
				}
				0u8.encode(w, version)?; // no parameters
			}
			_ => {
				let fwl = FilterWithLocation {
					filter_type: self.filter_type,
					start_location: self.start_location.clone(),
					end_group: self.end_group,
				};
				encode_params!(w, version,
					0x10 => true,
					0x20 => self.subscriber_priority,
					0x21 => fwl,
					0x22 => self.group_order,
				);
			}
		}

		Ok(())
	}
}

/// SubscribeOk message (0x04)
#[derive(Clone, Debug)]
pub struct SubscribeOk {
	pub request_id: Option<RequestId>,
	pub track_alias: u64,
}

impl Message for SubscribeOk {
	const ID: u64 = 0x04;

	fn encode_msg<W: bytes::BufMut>(&self, w: &mut W, version: Version) -> Result<(), EncodeError> {
		if matches!(version, Version::Draft14 | Version::Draft15 | Version::Draft16) {
			self.request_id
				.expect("request_id required for draft14-16")
				.encode(w, version)?;
		} else {
			assert!(self.request_id.is_none(), "request_id must be None for draft17+");
		}
		self.track_alias.encode(w, version)?;

		match version {
			Version::Draft14 => {
				0u64.encode(w, version)?; // expires = 0
				GroupOrder::Descending.encode(w, version)?;
				false.encode(w, version)?; // no content
				0u8.encode(w, version)?; // no parameters
			}
			_ => {
				encode_params!(w, version,
					0x22 => GroupOrder::Descending,
				);
			}
		}

		Ok(())
	}

	fn decode_msg<R: bytes::Buf>(r: &mut R, version: Version) -> Result<Self, DecodeError> {
		let request_id = if matches!(version, Version::Draft14 | Version::Draft15 | Version::Draft16) {
			Some(RequestId::decode(r, version)?)
		} else {
			None
		};
		let track_alias = u64::decode(r, version)?;

		match version {
			Version::Draft14 => {
				let expires = u64::decode(r, version)?;
				if expires != 0 {
					return Err(DecodeError::Unsupported);
				}

				let _group_order = u8::decode(r, version)?;

				if bool::decode(r, version)? {
					let _group = u64::decode(r, version)?;
					let _object = u64::decode(r, version)?;
				}

				let _params = Parameters::decode(r, version)?;
			}
			_ => {
				decode_params!(r, version,
					0x22 => _group_order: Option<GroupOrder>,
				);
				super::properties::skip(r, version)?;
			}
		}

		Ok(Self {
			request_id,
			track_alias,
		})
	}
}

/// SubscribeError message (0x05)
#[derive(Clone, Debug)]
pub struct SubscribeError<'a> {
	pub request_id: RequestId,
	pub error_code: u64,
	pub reason_phrase: Cow<'a, str>,
}

impl Message for SubscribeError<'_> {
	const ID: u64 = 0x05;

	fn encode_msg<W: bytes::BufMut>(&self, w: &mut W, version: Version) -> Result<(), EncodeError> {
		self.request_id.encode(w, version)?;
		self.error_code.encode(w, version)?;
		self.reason_phrase.encode(w, version)?;
		Ok(())
	}

	fn decode_msg<R: bytes::Buf>(r: &mut R, version: Version) -> Result<Self, DecodeError> {
		let request_id = RequestId::decode(r, version)?;
		let error_code = u64::decode(r, version)?;
		let reason_phrase = Cow::<str>::decode(r, version)?;

		Ok(Self {
			request_id,
			error_code,
			reason_phrase,
		})
	}
}

/// Unsubscribe message (0x0a)
#[derive(Clone, Debug)]
pub struct Unsubscribe {
	pub request_id: RequestId,
}

impl Message for Unsubscribe {
	const ID: u64 = 0x0a;

	fn encode_msg<W: bytes::BufMut>(&self, w: &mut W, version: Version) -> Result<(), EncodeError> {
		self.request_id.encode(w, version)?;
		Ok(())
	}

	fn decode_msg<R: bytes::Buf>(r: &mut R, version: Version) -> Result<Self, DecodeError> {
		let request_id = RequestId::decode(r, version)?;
		Ok(Self { request_id })
	}
}

/// SubscribeUpdate message (0x02)
#[derive(Clone, Debug)]
pub struct SubscribeUpdate {
	pub request_id: RequestId,
	pub subscription_request_id: Option<RequestId>,
	pub start_location: Location,
	pub end_group: u64,
	pub subscriber_priority: u8,
	pub forward: bool,
}

impl Message for SubscribeUpdate {
	const ID: u64 = 0x02;

	fn encode_msg<W: bytes::BufMut>(&self, w: &mut W, version: Version) -> Result<(), EncodeError> {
		match version {
			Version::Draft14 => {
				self.request_id.encode(w, version)?;
				self.subscription_request_id
					.expect("subscription_request_id required for draft14")
					.encode(w, version)?;
				self.start_location.encode(w, version)?;
				self.end_group.encode(w, version)?;
				self.subscriber_priority.encode(w, version)?;
				self.forward.encode(w, version)?;
				0u8.encode(w, version)?; // no parameters
			}
			Version::Draft15 | Version::Draft16 => {
				self.request_id.encode(w, version)?;
				self.subscription_request_id
					.expect("subscription_request_id required for draft15-16")
					.encode(w, version)?;
				encode_params!(w, version,
					0x10 => self.forward,
					0x20 => self.subscriber_priority,
					0x21 => FilterType::LargestObject,
				);
			}
			_ => {
				assert!(
					self.subscription_request_id.is_none(),
					"subscription_request_id must be None for draft17+"
				);
				// REQUEST_UPDATE
				self.request_id.encode(w, version)?;
				if matches!(version, Version::Draft17) {
					0u64.encode(w, version)?; // required_request_id_delta = 0 (draft-17 only, removed in draft-18 per #1615)
				}
				encode_params!(w, version,
					0x10 => self.forward,
					0x20 => self.subscriber_priority,
					0x21 => FilterType::LargestObject,
				);
			}
		}

		Ok(())
	}

	fn decode_msg<R: bytes::Buf>(r: &mut R, version: Version) -> Result<Self, DecodeError> {
		match version {
			Version::Draft14 => {
				let request_id = RequestId::decode(r, version)?;
				let subscription_request_id = Some(RequestId::decode(r, version)?);
				let start_location = Location::decode(r, version)?;
				let end_group = u64::decode(r, version)?;
				let subscriber_priority = u8::decode(r, version)?;
				let forward = bool::decode(r, version)?;
				let _parameters = Parameters::decode(r, version)?;

				Ok(Self {
					request_id,
					subscription_request_id,
					start_location,
					end_group,
					subscriber_priority,
					forward,
				})
			}
			Version::Draft15 | Version::Draft16 => {
				let request_id = RequestId::decode(r, version)?;
				let subscription_request_id = Some(RequestId::decode(r, version)?);
				decode_params!(r, version,
					0x10 => forward: Option<bool>,
					0x20 => subscriber_priority: Option<u8>,
					0x21 => _filter_type: Option<FilterType>,
				);

				let subscriber_priority = subscriber_priority.unwrap_or(128);
				let forward = forward.unwrap_or(true);

				Ok(Self {
					request_id,
					subscription_request_id,
					start_location: Location { group: 0, object: 0 },
					end_group: 0,
					subscriber_priority,
					forward,
				})
			}
			_ => {
				// REQUEST_UPDATE
				let request_id = RequestId::decode(r, version)?;
				if matches!(version, Version::Draft17) {
					let _required_request_id_delta = u64::decode(r, version)?;
				}
				decode_params!(r, version,
					0x10 => forward: Option<bool>,
					0x20 => subscriber_priority: Option<u8>,
					0x21 => _filter_type: Option<FilterType>,
				);

				let subscriber_priority = subscriber_priority.unwrap_or(128);
				let forward = forward.unwrap_or(true);

				Ok(Self {
					request_id,
					subscription_request_id: None,
					start_location: Location { group: 0, object: 0 },
					end_group: 0,
					subscriber_priority,
					forward,
				})
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use bytes::BytesMut;

	fn encode_message<M: Message>(msg: &M, version: Version) -> Vec<u8> {
		let mut buf = BytesMut::new();
		msg.encode_msg(&mut buf, version).unwrap();
		buf.to_vec()
	}

	fn decode_message<M: Message>(bytes: &[u8], version: Version) -> Result<M, DecodeError> {
		let mut buf = bytes::Bytes::from(bytes.to_vec());
		M::decode_msg(&mut buf, version)
	}

	#[test]
	fn test_subscribe_round_trip() {
		let msg = Subscribe {
			request_id: RequestId(1),
			track_namespace: Path::new("test"),
			track_name: "video".into(),
			subscriber_priority: 128,
			group_order: GroupOrder::Descending,
			filter_type: FilterType::LargestObject,
			start_location: None,
			end_group: None,
		};

		let encoded = encode_message(&msg, Version::Draft14);
		let decoded: Subscribe = decode_message(&encoded, Version::Draft14).unwrap();

		assert_eq!(decoded.request_id, RequestId(1));
		assert_eq!(decoded.track_namespace.as_str(), "test");
		assert_eq!(decoded.track_name, "video");
		assert_eq!(decoded.subscriber_priority, 128);
	}

	#[test]
	fn test_subscribe_round_trip_v15() {
		let msg = Subscribe {
			request_id: RequestId(1),
			track_namespace: Path::new("test"),
			track_name: "video".into(),
			subscriber_priority: 128,
			group_order: GroupOrder::Descending,
			filter_type: FilterType::LargestObject,
			start_location: None,
			end_group: None,
		};

		let encoded = encode_message(&msg, Version::Draft15);
		let decoded: Subscribe = decode_message(&encoded, Version::Draft15).unwrap();

		assert_eq!(decoded.request_id, RequestId(1));
		assert_eq!(decoded.track_namespace.as_str(), "test");
		assert_eq!(decoded.track_name, "video");
		assert_eq!(decoded.subscriber_priority, 128);
	}

	#[test]
	fn test_subscribe_nested_namespace() {
		let msg = Subscribe {
			request_id: RequestId(100),
			track_namespace: Path::new("conference/room123"),
			track_name: "audio".into(),
			subscriber_priority: 255,
			group_order: GroupOrder::Descending,
			filter_type: FilterType::LargestObject,
			start_location: None,
			end_group: None,
		};

		let encoded = encode_message(&msg, Version::Draft14);
		let decoded: Subscribe = decode_message(&encoded, Version::Draft14).unwrap();

		assert_eq!(decoded.track_namespace.as_str(), "conference/room123");
	}

	#[test]
	fn test_subscribe_ok() {
		let msg = SubscribeOk {
			request_id: Some(RequestId(42)),
			track_alias: 42,
		};

		let encoded = encode_message(&msg, Version::Draft14);
		let decoded: SubscribeOk = decode_message(&encoded, Version::Draft14).unwrap();

		assert_eq!(decoded.request_id, Some(RequestId(42)));
	}

	#[test]
	fn test_subscribe_ok_v15() {
		let msg = SubscribeOk {
			request_id: Some(RequestId(42)),
			track_alias: 42,
		};

		let encoded = encode_message(&msg, Version::Draft15);
		let decoded: SubscribeOk = decode_message(&encoded, Version::Draft15).unwrap();

		assert_eq!(decoded.request_id, Some(RequestId(42)));
		assert_eq!(decoded.track_alias, 42);
	}

	#[test]
	fn test_subscribe_error() {
		let msg = SubscribeError {
			request_id: RequestId(123),
			error_code: 500,
			reason_phrase: "Not found".into(),
		};

		let encoded = encode_message(&msg, Version::Draft14);
		let decoded: SubscribeError = decode_message(&encoded, Version::Draft14).unwrap();

		assert_eq!(decoded.request_id, RequestId(123));
		assert_eq!(decoded.error_code, 500);
		assert_eq!(decoded.reason_phrase, "Not found");
	}

	#[test]
	fn test_unsubscribe() {
		let msg = Unsubscribe {
			request_id: RequestId(999),
		};

		let encoded = encode_message(&msg, Version::Draft14);
		let decoded: Unsubscribe = decode_message(&encoded, Version::Draft14).unwrap();

		assert_eq!(decoded.request_id, RequestId(999));
	}

	#[test]
	fn test_subscribe_rejects_invalid_filter_type() {
		#[rustfmt::skip]
		let invalid_bytes = vec![
			0x01, // subscribe_id
			0x02, // track_alias
			0x01, // namespace length
			0x04, 0x74, 0x65, 0x73, 0x74, // "test"
			0x05, 0x76, 0x69, 0x64, 0x65, 0x6f, // "video"
			0x80, // subscriber_priority
			0x02, // group_order
			0x99, // INVALID filter_type
			0x00, // num_params
		];

		let result: Result<Subscribe, _> = decode_message(&invalid_bytes, Version::Draft14);
		assert!(result.is_err());
	}

	#[test]
	fn test_subscribe_update_v15_round_trip() {
		let msg = SubscribeUpdate {
			request_id: RequestId(10),
			subscription_request_id: Some(RequestId(5)),
			start_location: Location { group: 0, object: 0 },
			end_group: 0,
			subscriber_priority: 200,
			forward: true,
		};

		let encoded = encode_message(&msg, Version::Draft15);
		let decoded: SubscribeUpdate = decode_message(&encoded, Version::Draft15).unwrap();

		assert_eq!(decoded.request_id, RequestId(10));
		assert_eq!(decoded.subscription_request_id, Some(RequestId(5)));
		assert_eq!(decoded.subscriber_priority, 200);
		assert!(decoded.forward);
	}

	#[test]
	fn test_subscribe_update_v14_round_trip() {
		let msg = SubscribeUpdate {
			request_id: RequestId(10),
			subscription_request_id: Some(RequestId(5)),
			start_location: Location { group: 1, object: 2 },
			end_group: 100,
			subscriber_priority: 200,
			forward: true,
		};

		let encoded = encode_message(&msg, Version::Draft14);
		let decoded: SubscribeUpdate = decode_message(&encoded, Version::Draft14).unwrap();

		assert_eq!(decoded.request_id, RequestId(10));
		assert_eq!(decoded.subscription_request_id, Some(RequestId(5)));
		assert_eq!(decoded.start_location, Location { group: 1, object: 2 });
		assert_eq!(decoded.end_group, 100);
		assert_eq!(decoded.subscriber_priority, 200);
		assert!(decoded.forward);
	}

	#[test]
	fn test_subscribe_ok_rejects_non_zero_expires() {
		#[rustfmt::skip]
		let invalid_bytes = vec![
			0x01, // subscribe_id
			0x05, // INVALID: expires = 5
			0x02, // group_order
			0x00, // content_exists
			0x00, // num_params
		];

		let result: Result<SubscribeOk, _> = decode_message(&invalid_bytes, Version::Draft14);
		assert!(result.is_err());
	}

	#[test]
	fn test_subscribe_v17_round_trip() {
		let msg = Subscribe {
			request_id: RequestId(1),
			track_namespace: Path::new("test"),
			track_name: "video".into(),
			subscriber_priority: 128,
			group_order: GroupOrder::Descending,
			filter_type: FilterType::LargestObject,
			start_location: None,
			end_group: None,
		};

		let encoded = encode_message(&msg, Version::Draft17);
		let decoded: Subscribe = decode_message(&encoded, Version::Draft17).unwrap();

		assert_eq!(decoded.request_id, RequestId(1));
		assert_eq!(decoded.track_namespace.as_str(), "test");
		assert_eq!(decoded.track_name, "video");
		assert_eq!(decoded.subscriber_priority, 128);
	}

	#[test]
	fn test_subscribe_ok_v17_round_trip() {
		let msg = SubscribeOk {
			request_id: None,
			track_alias: 42,
		};

		let encoded = encode_message(&msg, Version::Draft17);
		let decoded: SubscribeOk = decode_message(&encoded, Version::Draft17).unwrap();

		assert_eq!(decoded.request_id, None);
		assert_eq!(decoded.track_alias, 42);
	}

	#[test]
	fn test_subscribe_update_v17_round_trip() {
		let msg = SubscribeUpdate {
			request_id: RequestId(10),
			subscription_request_id: None,
			start_location: Location { group: 0, object: 0 },
			end_group: 0,
			subscriber_priority: 200,
			forward: true,
		};

		let encoded = encode_message(&msg, Version::Draft17);
		let decoded: SubscribeUpdate = decode_message(&encoded, Version::Draft17).unwrap();

		assert_eq!(decoded.request_id, RequestId(10));
		assert_eq!(decoded.subscription_request_id, None);
		assert_eq!(decoded.subscriber_priority, 200);
		assert!(decoded.forward);
	}

	#[test]
	fn test_subscribe_v18_round_trip() {
		let msg = Subscribe {
			request_id: RequestId(1),
			track_namespace: Path::new("test"),
			track_name: "video".into(),
			subscriber_priority: 128,
			group_order: GroupOrder::Descending,
			filter_type: FilterType::LargestObject,
			start_location: None,
			end_group: None,
		};

		let encoded = encode_message(&msg, Version::Draft18);
		let decoded: Subscribe = decode_message(&encoded, Version::Draft18).unwrap();

		assert_eq!(decoded.request_id, RequestId(1));
		assert_eq!(decoded.track_namespace.as_str(), "test");
		assert_eq!(decoded.track_name, "video");
		assert_eq!(decoded.subscriber_priority, 128);
	}

	#[test]
	fn test_subscribe_ok_v18_round_trip() {
		let msg = SubscribeOk {
			request_id: None,
			track_alias: 42,
		};

		let encoded = encode_message(&msg, Version::Draft18);
		let decoded: SubscribeOk = decode_message(&encoded, Version::Draft18).unwrap();

		assert_eq!(decoded.request_id, None);
		assert_eq!(decoded.track_alias, 42);
	}

	/// Draft-18 removes the `required_request_id_delta` field (#1615), so the
	/// REQUEST_UPDATE wire format is 1 varint shorter than draft-17.
	#[test]
	fn test_subscribe_update_v18_round_trip() {
		let msg = SubscribeUpdate {
			request_id: RequestId(10),
			subscription_request_id: None,
			start_location: Location { group: 0, object: 0 },
			end_group: 0,
			subscriber_priority: 200,
			forward: true,
		};

		let encoded = encode_message(&msg, Version::Draft18);
		let decoded: SubscribeUpdate = decode_message(&encoded, Version::Draft18).unwrap();

		assert_eq!(decoded.request_id, RequestId(10));
		assert_eq!(decoded.subscription_request_id, None);
		assert_eq!(decoded.subscriber_priority, 200);
		assert!(decoded.forward);
	}

	/// Cross-check: draft-17 emits an extra 0-byte (required_request_id_delta) that
	/// draft-18 does not. So a draft-18 encoding should be exactly 1 byte shorter
	/// than draft-17 for SUBSCRIBE_UPDATE.
	#[test]
	fn test_subscribe_update_v17_v18_size_differs() {
		let v17_msg = SubscribeUpdate {
			request_id: RequestId(10),
			subscription_request_id: None,
			start_location: Location { group: 0, object: 0 },
			end_group: 0,
			subscriber_priority: 200,
			forward: true,
		};
		let v18_msg = SubscribeUpdate { ..v17_msg.clone() };

		let v17 = encode_message(&v17_msg, Version::Draft17);
		let v18 = encode_message(&v18_msg, Version::Draft18);
		assert_eq!(v17.len(), v18.len() + 1);
	}

	#[test]
	fn test_subscribe_next_group_v14_round_trip() {
		let msg = Subscribe {
			request_id: RequestId(5),
			track_namespace: Path::new("live"),
			track_name: "video".into(),
			subscriber_priority: 200,
			group_order: GroupOrder::Descending,
			filter_type: FilterType::NextGroup,
			start_location: None,
			end_group: None,
		};

		let encoded = encode_message(&msg, Version::Draft14);
		let decoded: Subscribe = decode_message(&encoded, Version::Draft14).unwrap();

		assert_eq!(decoded.filter_type, FilterType::NextGroup);
		assert_eq!(decoded.start_location, None);
	}

	#[test]
	fn test_subscribe_next_group_v17_round_trip() {
		let msg = Subscribe {
			request_id: RequestId(5),
			track_namespace: Path::new("live"),
			track_name: "video".into(),
			subscriber_priority: 200,
			group_order: GroupOrder::Descending,
			filter_type: FilterType::NextGroup,
			start_location: None,
			end_group: None,
		};

		let encoded = encode_message(&msg, Version::Draft17);
		let decoded: Subscribe = decode_message(&encoded, Version::Draft17).unwrap();

		assert_eq!(decoded.filter_type, FilterType::NextGroup);
		assert_eq!(decoded.start_location, None);
	}

	#[test]
	fn test_subscribe_next_group_v18_round_trip() {
		let msg = Subscribe {
			request_id: RequestId(5),
			track_namespace: Path::new("live"),
			track_name: "video".into(),
			subscriber_priority: 200,
			group_order: GroupOrder::Descending,
			filter_type: FilterType::NextGroup,
			start_location: None,
			end_group: None,
		};

		let encoded = encode_message(&msg, Version::Draft18);
		let decoded: Subscribe = decode_message(&encoded, Version::Draft18).unwrap();

		assert_eq!(decoded.filter_type, FilterType::NextGroup);
		assert_eq!(decoded.start_location, None);
	}

	#[test]
	fn test_subscribe_absolute_start_v14_round_trip() {
		let msg = Subscribe {
			request_id: RequestId(7),
			track_namespace: Path::new("vod"),
			track_name: "audio".into(),
			subscriber_priority: 128,
			group_order: GroupOrder::Descending,
			filter_type: FilterType::AbsoluteStart,
			start_location: Some(Location { group: 42, object: 3 }),
			end_group: None,
		};

		let encoded = encode_message(&msg, Version::Draft14);
		let decoded: Subscribe = decode_message(&encoded, Version::Draft14).unwrap();

		assert_eq!(decoded.filter_type, FilterType::AbsoluteStart);
		assert_eq!(decoded.start_location, Some(Location { group: 42, object: 3 }));
	}

	#[test]
	fn test_subscribe_absolute_start_v17_round_trip() {
		let msg = Subscribe {
			request_id: RequestId(7),
			track_namespace: Path::new("vod"),
			track_name: "audio".into(),
			subscriber_priority: 128,
			group_order: GroupOrder::Descending,
			filter_type: FilterType::AbsoluteStart,
			start_location: Some(Location { group: 42, object: 3 }),
			end_group: None,
		};

		let encoded = encode_message(&msg, Version::Draft17);
		let decoded: Subscribe = decode_message(&encoded, Version::Draft17).unwrap();

		assert_eq!(decoded.filter_type, FilterType::AbsoluteStart);
		assert_eq!(decoded.start_location, Some(Location { group: 42, object: 3 }));
	}

	#[test]
	fn test_subscribe_absolute_start_v18_round_trip() {
		let msg = Subscribe {
			request_id: RequestId(7),
			track_namespace: Path::new("vod"),
			track_name: "audio".into(),
			subscriber_priority: 128,
			group_order: GroupOrder::Descending,
			filter_type: FilterType::AbsoluteStart,
			start_location: Some(Location { group: 100, object: 0 }),
			end_group: None,
		};

		let encoded = encode_message(&msg, Version::Draft18);
		let decoded: Subscribe = decode_message(&encoded, Version::Draft18).unwrap();

		assert_eq!(decoded.filter_type, FilterType::AbsoluteStart);
		assert_eq!(decoded.start_location, Some(Location { group: 100, object: 0 }));
	}

	#[test]
	fn test_subscribe_absolute_range_v14_round_trip() {
		let msg = Subscribe {
			request_id: RequestId(9),
			track_namespace: Path::new("vod"),
			track_name: "video".into(),
			subscriber_priority: 128,
			group_order: GroupOrder::Descending,
			filter_type: FilterType::AbsoluteRange,
			start_location: Some(Location { group: 1, object: 0 }),
			end_group: Some(5),
		};

		let encoded = encode_message(&msg, Version::Draft14);
		let decoded: Subscribe = decode_message(&encoded, Version::Draft14).unwrap();

		assert_eq!(decoded.filter_type, FilterType::AbsoluteRange);
		assert_eq!(decoded.start_location, Some(Location { group: 1, object: 0 }));
		assert_eq!(decoded.end_group, Some(5));
	}

	#[test]
	fn test_subscribe_absolute_range_v17_round_trip() {
		let msg = Subscribe {
			request_id: RequestId(9),
			track_namespace: Path::new("vod"),
			track_name: "video".into(),
			subscriber_priority: 128,
			group_order: GroupOrder::Descending,
			filter_type: FilterType::AbsoluteRange,
			start_location: Some(Location { group: 1, object: 0 }),
			end_group: Some(2),
		};

		let encoded = encode_message(&msg, Version::Draft17);
		let decoded: Subscribe = decode_message(&encoded, Version::Draft17).unwrap();

		assert_eq!(decoded.filter_type, FilterType::AbsoluteRange);
		assert_eq!(decoded.start_location, Some(Location { group: 1, object: 0 }));
		assert_eq!(decoded.end_group, Some(2));
		// Verify no leftover bytes (simulates publisher's !data.is_empty() check)
	}

	#[test]
	fn test_subscribe_absolute_range_v17_no_leftover() {
		let msg = Subscribe {
			request_id: RequestId(9),
			track_namespace: Path::new("vod"),
			track_name: "video".into(),
			subscriber_priority: 128,
			group_order: GroupOrder::Descending,
			filter_type: FilterType::AbsoluteRange,
			start_location: Some(Location { group: 1, object: 0 }),
			end_group: Some(2),
		};

		// Encode, then decode using the raw Message trait (same path the publisher uses).
		let mut buf = BytesMut::new();
		msg.encode_msg(&mut buf, Version::Draft17).unwrap();
		let mut data = buf.freeze();
		let _decoded = Subscribe::decode_msg(&mut data, Version::Draft17).unwrap();
		assert!(data.is_empty(), "leftover bytes after decode: {} remaining", data.len());
	}
}
