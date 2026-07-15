use crate::coding;

/// A list of possible errors that can occur during the session.
#[derive(thiserror::Error, Debug, Clone)]
#[non_exhaustive]
pub enum Error {
	#[error("transport: {0}")]
	Transport(String),

	#[error(transparent)]
	Decode(#[from] coding::DecodeError),

	// TODO move to a ConnectError
	#[error("unsupported versions")]
	Version,

	/// A required extension was not present
	#[error("extension required")]
	RequiredExtension,

	/// An unexpected stream type was received
	#[error("unexpected stream type")]
	UnexpectedStream,

	#[error(transparent)]
	BoundsExceeded(#[from] coding::BoundsExceeded),

	/// A duplicate ID was used
	// The broadcast/track is a duplicate
	#[error("duplicate")]
	Duplicate,

	// Cancel is returned when there are no more readers.
	#[error("cancelled")]
	Cancel,

	/// It took too long to open or transmit a stream.
	#[error("timeout")]
	Timeout,

	/// The group is older than the latest group and dropped.
	#[error("old")]
	Old,

	// The application closes the stream with a code.
	#[error("app code={0}")]
	App(u16),

	#[error("not found")]
	NotFound,

	/// A broadcast was requested that is neither announced nor served by a dynamic
	/// router, so there is no route to it.
	#[error("unroutable")]
	Unroutable,

	#[error("wrong frame size")]
	WrongSize,

	#[error("protocol violation")]
	ProtocolViolation,

	#[error("unauthorized")]
	Unauthorized,

	#[error("unexpected message")]
	UnexpectedMessage,

	#[error("unsupported")]
	Unsupported,

	#[error(transparent)]
	Encode(#[from] coding::EncodeError),

	#[error("too many parameters")]
	TooManyParameters,

	#[error("invalid role")]
	InvalidRole,

	#[error("unknown ALPN: {0}")]
	UnknownAlpn(String),

	#[error("dropped")]
	Dropped,

	#[error("closed")]
	Closed,

	/// The cached frame has been evicted due to the group size limit.
	#[error("cache full")]
	CacheFull,

	/// A frame declared a payload size larger than the receiver accepts.
	#[error("frame too large")]
	FrameTooLarge,

	/// The session is going away (GOAWAY received); new subscribe and
	/// subscribe-namespace requests are rejected.
	#[error("going away")]
	GoingAway,

	/// The peer did not close the session within the GOAWAY timeout.
	///
	/// Sent as a session-level termination code when the server force-closes
	/// after the advertised GOAWAY deadline expires.
	#[error("goaway timeout")]
	GoawayTimeout,

	/// A precondition for the requested operation was not met.
	///
	/// For example, calling [`Session::reconnect`](crate::Session::reconnect)
	/// before a GOAWAY has been received.
	#[error("not ready")]
	NotReady,

	/// A remote error received via a stream/session reset code.
	#[error("remote error: code={0}")]
	Remote(u32),
}

impl Error {
	/// An integer code that is sent over the wire.
	pub fn to_code(&self) -> u32 {
		match self {
			Self::Cancel => 0,
			Self::RequiredExtension => 1,
			Self::Old => 2,
			Self::Timeout => 3,
			Self::Transport(_) => 4,
			Self::Decode(_) => 5,
			Self::Unauthorized => 6,
			Self::Version => 9,
			Self::UnexpectedStream => 10,
			Self::BoundsExceeded(_) => 11,
			Self::Duplicate => 12,
			Self::NotFound => 13,
			Self::WrongSize => 14,
			Self::ProtocolViolation => 15,
			Self::UnexpectedMessage => 16,
			Self::Unsupported => 17,
			Self::Encode(_) => 18,
			Self::TooManyParameters => 19,
			Self::InvalidRole => 20,
			Self::UnknownAlpn(_) => 21,
			Self::Dropped => 24,
			Self::Closed => 25,
			Self::CacheFull => 26,
			Self::FrameTooLarge => 27,
			// 28 (Decompress) and 29 (TimestampMismatch) are reserved on the dev branch;
			// keep Unroutable at 30 so the wire code is identical across branches.
			Self::Unroutable => 30,
			Self::GoingAway => 31,
			Self::GoawayTimeout => 32,
			Self::NotReady => 33,
			Self::App(app) => *app as u32 + 64,
			Self::Remote(code) => *code,
		}
	}

	/// Reconstruct an [Error] from a wire code.
	///
	/// Payload-free codes round-trip exactly through [`to_code`](Self::to_code).
	/// Codes whose variant carries detail that is not on the wire (4 Transport,
	/// 5 Decode, 18 Encode, 21 UnknownAlpn) stay [`Error::Remote`] rather than
	/// reconstruct a misleading placeholder payload. Unrecognized codes also
	/// become [`Error::Remote`].
	pub fn from_code(code: u32) -> Self {
		match code {
			0 => Self::Cancel,
			1 => Self::RequiredExtension,
			2 => Self::Old,
			3 => Self::Timeout,
			6 => Self::Unauthorized,
			9 => Self::Version,
			10 => Self::UnexpectedStream,
			11 => Self::BoundsExceeded(coding::BoundsExceeded),
			12 => Self::Duplicate,
			13 => Self::NotFound,
			14 => Self::WrongSize,
			15 => Self::ProtocolViolation,
			16 => Self::UnexpectedMessage,
			17 => Self::Unsupported,
			19 => Self::TooManyParameters,
			20 => Self::InvalidRole,
			24 => Self::Dropped,
			25 => Self::Closed,
			26 => Self::CacheFull,
			27 => Self::FrameTooLarge,
			30 => Self::Unroutable,
			31 => Self::GoingAway,
			32 => Self::GoawayTimeout,
			33 => Self::NotReady,
			c if c >= 64 => Self::App((c - 64) as u16),
			// 4, 5, 18, 21 and anything unrecognized: keep the raw code, since the
			// structured variant's payload (string/decode/encode context) is not
			// recoverable from the wire.
			_ => Self::Remote(code),
		}
	}

	/// Convert a transport error into an [Error], decoding session and stream reset codes.
	///
	/// Checks `session_error()` first (connection-level close), then `stream_error()`
	/// (stream-level reset). Known codes are mapped back to their structured variant
	/// via [`from_code`](Self::from_code); unrecognized codes become [`Error::Remote`].
	/// Falls back to [`Error::Transport`] if neither code is present.
	pub fn from_transport(err: impl web_transport_trait::Error) -> Self {
		if let Some((code, _reason)) = err.session_error() {
			return Self::from_code(code);
		}

		if let Some(code) = err.stream_error() {
			return Self::from_code(code);
		}

		Self::Transport(err.to_string())
	}
}

impl web_transport_trait::Error for Error {
	fn session_error(&self) -> Option<(u32, String)> {
		None
	}
}

pub type Result<T> = std::result::Result<T, Error>;
