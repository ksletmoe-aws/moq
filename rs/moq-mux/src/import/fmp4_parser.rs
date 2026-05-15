use anyhow::Context;
use bytes::{Buf, Bytes, BytesMut};
use mp4_atom::{Any, DecodeMaybe, Encode, Mdat, Moof, Moov, Trak};
use tokio::io::{AsyncRead, AsyncReadExt};

/// Build a single-track init segment (ftyp+moov) for the given track.
pub(crate) fn build_init_segment(trak: &Trak, moov: &Moov) -> anyhow::Result<Bytes> {
	let ftyp = mp4_atom::Ftyp {
		major_brand: b"isom".into(),
		minor_version: 0x200,
		compatible_brands: vec![b"isom".into(), b"iso6".into(), b"mp41".into()],
	};

	let track_id = trak.tkhd.track_id;
	let trex = moov
		.mvex
		.as_ref()
		.and_then(|mvex| mvex.trex.iter().find(|trex| trex.track_id == track_id))
		.cloned()
		.unwrap_or(mp4_atom::Trex {
			track_id,
			default_sample_description_index: 1,
			..Default::default()
		});

	let single_moov = Moov {
		mvhd: moov.mvhd.clone(),
		trak: vec![trak.clone()],
		mvex: Some(mp4_atom::Mvex {
			mehd: None,
			trex: vec![trex],
		}),
		meta: None,
		udta: None,
	};

	let mut buf = Vec::new();
	ftyp.encode(&mut buf)?;
	single_moov.encode(&mut buf)?;
	Ok(Bytes::from(buf))
}
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum TrackKind {
	Video,
	Audio,
}

/// Per-sample metadata extracted from trun entries.
#[allow(dead_code)]
pub(crate) struct SampleInfo {
	pub flags: u32,
	pub pts: u64,
	pub duration: u32,
	pub timescale: u64,
	pub keyframe: bool,
}

/// Callback trait for consuming parsed fMP4 events.
///
/// Implement this to receive track initialization and fragment data from the parser.
/// The parser handles the moov/moof/mdat state machine; the sink handles what to do
/// with the parsed results (e.g., create MoQ tracks, write CMSF segments).
pub(crate) trait Fmp4Sink {
	/// Called once per track when the moov box is parsed.
	fn on_init(&mut self, track_id: u32, kind: TrackKind, trak: &Trak, moov: &Moov) -> anyhow::Result<()>;

	/// Called once per track per moof+mdat pair with the per-track fragment bytes.
	fn on_fragment(
		&mut self,
		track_id: u32,
		fragment: Bytes,
		keyframe: bool,
		samples: Vec<SampleInfo>,
	) -> anyhow::Result<()>;
}

/// Reusable fMP4 parser that dispatches events to an [`Fmp4Sink`].
///
/// Handles the moov/moof/mdat state machine and per-track fragment splitting.
/// Consumer-specific logic (track creation, group management, codec detection)
/// is delegated to the sink.
pub(crate) struct Fmp4Parser<S> {
	pub sink: S,
	moov: Option<Moov>,
	moof: Option<Moof>,
	moof_size: usize,
}

impl<S: Fmp4Sink> Fmp4Parser<S> {
	pub fn new(sink: S) -> Self {
		Self {
			sink,
			moov: None,
			moof: None,
			moof_size: 0,
		}
	}

	pub fn is_initialized(&self) -> bool {
		self.moov.is_some()
	}

	/// Decode from an asynchronous reader.
	pub async fn decode_from<T: AsyncRead + Unpin>(&mut self, reader: &mut T) -> anyhow::Result<()> {
		let mut buffer = BytesMut::new();
		while reader.read_buf(&mut buffer).await? > 0 {
			self.decode(&mut buffer)?;
		}
		Ok(())
	}

	/// Decode a buffer of bytes.
	pub fn decode<T: Buf + AsRef<[u8]>>(&mut self, buf: &mut T) -> anyhow::Result<()> {
		let mut cursor = std::io::Cursor::new(buf);
		let mut position = 0;

		while let Some(atom) = Any::decode_maybe(&mut cursor)? {
			let size = cursor.position() as usize - position;
			let raw = &cursor.get_ref().as_ref()[position..position + size];

			match atom {
				Any::Ftyp(_) | Any::Styp(_) => {}
				Any::Moov(moov) => {
					self.init(moov)?;
				}
				Any::Moof(moof) => {
					anyhow::ensure!(self.moof.is_none(), "duplicate moof box");
					self.moof.replace(moof);
					self.moof_size = size;
				}
				Any::Mdat(mdat) => {
					self.extract(mdat, raw)?;
				}
				_ => {}
			}

			position = cursor.position() as usize;
		}

		cursor.into_inner().advance(position);
		Ok(())
	}

	fn init(&mut self, moov: Moov) -> anyhow::Result<()> {
		for trak in &moov.trak {
			let track_id = trak.tkhd.track_id;
			let handler = &trak.mdia.hdlr.handler;

			let kind = match handler.as_ref() {
				b"vide" => TrackKind::Video,
				b"soun" => TrackKind::Audio,
				b"sbtl" => anyhow::bail!("subtitle tracks are not supported"),
				handler => anyhow::bail!("unknown track type: {:?}", handler),
			};

			self.sink.on_init(track_id, kind, trak, &moov)?;
		}

		self.moov = Some(moov);
		Ok(())
	}

	fn extract(&mut self, mdat: Mdat, mdat_raw: &[u8]) -> anyhow::Result<()> {
		let moov = self.moov.as_ref().context("missing moov box")?;
		let moof = self.moof.take().context("missing moof box")?;
		let moof_size = self.moof_size;
		let header_size = mdat_raw.len() - mdat.data.len();

		for traf in &moof.traf {
			let track_id = traf.tfhd.track_id;

			let trak = moov
				.trak
				.iter()
				.find(|trak| trak.tkhd.track_id == track_id)
				.context("unknown track")?;
			let trex = moov
				.mvex
				.as_ref()
				.and_then(|mvex| mvex.trex.iter().find(|trex| trex.track_id == track_id));

			let default_sample_duration = trex.map(|trex| trex.default_sample_duration).unwrap_or_default();
			let default_sample_size = trex.map(|trex| trex.default_sample_size).unwrap_or_default();
			let default_sample_flags = trex.map(|trex| trex.default_sample_flags).unwrap_or_default();

			let handler = &trak.mdia.hdlr.handler;
			let kind = match handler.as_ref() {
				b"vide" => TrackKind::Video,
				b"soun" => TrackKind::Audio,
				handler => anyhow::bail!("unexpected track handler in traf: {:?}", handler),
			};

			let tfdt = traf.tfdt.as_ref().context("missing tfdt box")?;
			let mut dts = tfdt.base_media_decode_time;
			let timescale = trak.mdia.mdhd.timescale as u64;

			let mut offset = traf.tfhd.base_data_offset.unwrap_or_default() as usize;
			let mut track_data_start: Option<usize> = None;

			if traf.trun.is_empty() {
				anyhow::bail!("missing trun box");
			}

			let mut contains_keyframe = false;
			let mut samples = Vec::new();

			for trun in &traf.trun {
				let tfhd = &traf.tfhd;

				if let Some(data_offset) = trun.data_offset {
					let base_offset = tfhd.base_data_offset.unwrap_or_default() as usize;
					let data_offset: usize = data_offset.try_into().context("invalid data offset")?;

					let relative_offset = data_offset
						.checked_sub(moof_size)
						.and_then(|v| v.checked_sub(header_size))
						.context("invalid data offset: underflow")?;

					offset = base_offset
						.checked_add(relative_offset)
						.context("invalid data offset: overflow")?;
				}

				if track_data_start.is_none() {
					track_data_start = Some(offset);
				}

				for entry in &trun.entries {
					let flags = entry
						.flags
						.unwrap_or(tfhd.default_sample_flags.unwrap_or(default_sample_flags));
					let duration = entry
						.duration
						.unwrap_or(tfhd.default_sample_duration.unwrap_or(default_sample_duration));
					let size = entry
						.size
						.unwrap_or(tfhd.default_sample_size.unwrap_or(default_sample_size)) as usize;

					let pts = (dts as i64 + entry.cts.unwrap_or_default() as i64) as u64;

					if offset + size > mdat.data.len() {
						anyhow::bail!("invalid data offset");
					}

					let keyframe = match kind {
						TrackKind::Video => {
							let kf = (flags >> 24) & 0x3 == 0x2;
							let non_sync = (flags >> 16) & 0x1 == 0x1;
							kf && !non_sync
						}
						TrackKind::Audio => true,
					};

					contains_keyframe |= keyframe;

					samples.push(SampleInfo {
						flags,
						pts,
						duration,
						timescale,
						keyframe,
					});

					dts += duration as u64;
					offset += size;
				}
			}

			// Build per-track moof+mdat fragment.
			let single_traf_moof = Moof {
				mfhd: moof.mfhd.clone(),
				traf: vec![traf.clone()],
			};

			let track_data_start = track_data_start.unwrap_or(0);
			let track_data_end = offset;

			anyhow::ensure!(
				track_data_start <= track_data_end && track_data_end <= mdat.data.len(),
				"track sample range {}..{} is out of bounds of mdat (len {})",
				track_data_start,
				track_data_end,
				mdat.data.len()
			);
			let track_mdat_data = &mdat.data[track_data_start..track_data_end];

			let mut adjusted_moof = single_traf_moof;

			// Workaround: mp4_atom's trun encoder drops first_sample_flags when entries
			// lack explicit per-sample flags. Fill in the default so keyframe indication
			// is preserved on re-encode. See: https://github.com/kixelated/mp4-atom/pull/160
			for traf_mut in &mut adjusted_moof.traf {
				let default_flags = traf_mut.tfhd.default_sample_flags.unwrap_or(default_sample_flags);

				traf_mut.tfhd.base_data_offset = None;

				for trun_mut in &mut traf_mut.trun {
					for entry in &mut trun_mut.entries {
						if entry.flags.is_none() {
							entry.flags = Some(default_flags);
						}
					}
					// Reserve the data_offset field; the real value is filled in below.
					trun_mut.data_offset = Some(0);
				}
			}

			// Measure encoded moof size after structural changes.
			let mut moof_buf = Vec::new();
			adjusted_moof.encode(&mut moof_buf)?;
			let new_moof_size = moof_buf.len();

			// Re-encode moof with corrected per-trun data_offset for the per-track fragment.
			let mdat_header_size_new = 8u64;
			let mut cumulative_offset = 0u64;
			for traf_mut in &mut adjusted_moof.traf {
				for trun_mut in &mut traf_mut.trun {
					trun_mut.data_offset =
						Some((new_moof_size as u64 + mdat_header_size_new + cumulative_offset) as i32);

					let trun_data_size: u64 = trun_mut
						.entries
						.iter()
						.map(|e| {
							e.size
								.unwrap_or(traf_mut.tfhd.default_sample_size.unwrap_or(default_sample_size)) as u64
						})
						.sum();
					cumulative_offset += trun_data_size;
				}
			}

			moof_buf.clear();
			adjusted_moof.encode(&mut moof_buf)?;

			let per_track_mdat = Mdat {
				data: track_mdat_data.to_vec(),
			};
			per_track_mdat.encode(&mut moof_buf)?;

			let fragment_bytes = Bytes::from(moof_buf);

			self.sink
				.on_fragment(track_id, fragment_bytes, contains_keyframe, samples)?;
		}

		Ok(())
	}
}

#[cfg(test)]
mod test {
	use super::*;

	struct MockSink {
		inits: Vec<(u32, TrackKind)>,
		fragments: Vec<(u32, Bytes, bool, usize)>,
	}

	impl MockSink {
		fn new() -> Self {
			Self {
				inits: Vec::new(),
				fragments: Vec::new(),
			}
		}
	}

	impl Fmp4Sink for MockSink {
		fn on_init(&mut self, track_id: u32, kind: TrackKind, _trak: &Trak, _moov: &Moov) -> anyhow::Result<()> {
			self.inits.push((track_id, kind));
			Ok(())
		}

		fn on_fragment(
			&mut self,
			track_id: u32,
			fragment: Bytes,
			keyframe: bool,
			samples: Vec<SampleInfo>,
		) -> anyhow::Result<()> {
			self.fragments.push((track_id, fragment, keyframe, samples.len()));
			Ok(())
		}
	}

	#[test]
	fn test_parser_bbb_init() {
		let data = include_bytes!("test/bbb.mp4");
		let mut parser = Fmp4Parser::new(MockSink::new());
		let mut buf = BytesMut::from(&data[..]);
		let _ = parser.decode(&mut buf);

		assert!(parser.is_initialized());

		// bbb.mp4 has track_id=1 (video) and track_id=2 (audio)
		assert_eq!(parser.sink.inits.len(), 2);
		assert_eq!(parser.sink.inits[0], (1, TrackKind::Video));
		assert_eq!(parser.sink.inits[1], (2, TrackKind::Audio));
	}

	#[test]
	fn test_parser_bbb_fragments() {
		let data = include_bytes!("test/bbb.mp4");
		let mut parser = Fmp4Parser::new(MockSink::new());
		let mut buf = BytesMut::from(&data[..]);
		let _ = parser.decode(&mut buf);

		assert!(!parser.sink.fragments.is_empty());

		for (_, fragment, _, sample_count) in &parser.sink.fragments {
			assert!(!fragment.is_empty());
			assert!(*sample_count > 0);
		}

		// First video fragment should be a keyframe
		let first_video = parser.sink.fragments.iter().find(|(id, _, _, _)| *id == 1).unwrap();
		assert!(first_video.2, "first video fragment should be a keyframe");
	}
}
