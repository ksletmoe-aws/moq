//! Shared CMSF types and parsing utilities.
//!
//! Used by both the producer (import) and consumer (export) sides of the CMSF pipeline.

use mp4_atom::DecodeMaybe;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Metadata extracted from a CMSF Object (moof+mdat).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CmsfMetadata {
	/// Presentation timestamp derived from `base_media_decode_time` + first sample's
	/// composition time offset. For single-sample fragments (the CMSF norm), this is
	/// the exact PTS. For multi-sample fragments with B-frames, this is the PTS of
	/// the first sample only.
	pub pts: u64,
	pub duration: u64,
	pub is_keyframe: bool,
}

/// Errors from CMSF parsing operations.
#[derive(Debug, thiserror::Error)]
pub enum CmsfError {
	#[error("no moof box found")]
	NoMoof,
	#[error("no traf in moof")]
	NoTraf,
	#[error("no tfdt in traf")]
	NoTfdt,
	#[error("no trun in traf")]
	NoTrun,
	#[error("mp4 parse error: {0}")]
	Mp4(#[from] mp4_atom::Error),
	#[error("moq error: {0}")]
	Moq(#[from] moq_lite::Error),
	#[error("invalid base64: {0}")]
	Base64(#[from] base64::DecodeError),
	#[error("missing init_data in catalog track")]
	MissingInitData,
	#[error("no moov in init segment")]
	NoMoov,
	#[error("no tracks in moov")]
	NoTracks,
	#[error("catalog parse error: {0}")]
	CatalogParse(#[from] serde_json::Error),
	#[error("mp4 encode error")]
	EncodeFailed,
}

// ---------------------------------------------------------------------------
// Parsing Functions
// ---------------------------------------------------------------------------

/// Parse metadata from a CMSF Object (moof+mdat bytes) without modifying them.
pub fn parse_cmsf_metadata(data: &[u8]) -> Result<CmsfMetadata, CmsfError> {
	let mut cursor = std::io::Cursor::new(data);

	while let Some(atom) = mp4_atom::Any::decode_maybe(&mut cursor)? {
		if let mp4_atom::Any::Moof(moof) = atom {
			let traf = moof.traf.first().ok_or(CmsfError::NoTraf)?;
			let tfdt = traf.tfdt.as_ref().ok_or(CmsfError::NoTfdt)?;

			if traf.trun.is_empty() {
				return Err(CmsfError::NoTrun);
			}

			let default_duration = traf.tfhd.default_sample_duration.unwrap_or(0);
			let default_flags = traf.tfhd.default_sample_flags.unwrap_or(0);

			let mut duration = 0u64;
			let mut first_flags = None;
			let mut first_cts: Option<i32> = None;

			for trun in &traf.trun {
				for entry in &trun.entries {
					duration += entry.duration.unwrap_or(default_duration) as u64;
					if first_flags.is_none() {
						first_flags = Some(entry.flags.unwrap_or(default_flags));
					}
					if first_cts.is_none() {
						first_cts = Some(entry.cts.unwrap_or(0));
					}
				}
			}

			let flags = first_flags.unwrap_or(default_flags);
			let is_keyframe = (flags >> 24) & 0x3 == 0x2 && (flags >> 16) & 0x1 == 0;
			let dts = tfdt.base_media_decode_time;
			let pts = (dts as i64 + first_cts.unwrap_or(0) as i64).max(0) as u64;

			return Ok(CmsfMetadata {
				pts,
				duration,
				is_keyframe,
			});
		}
	}

	Err(CmsfError::NoMoof)
}

/// Parse timescale from a decoded init segment (ftyp+moov bytes).
pub fn parse_timescale(init_data: &[u8]) -> Result<u64, CmsfError> {
	let mut cursor = std::io::Cursor::new(init_data);

	while let Some(atom) = mp4_atom::Any::decode_maybe(&mut cursor)? {
		if let mp4_atom::Any::Moov(moov) = atom {
			let trak = moov.trak.first().ok_or(CmsfError::NoTracks)?;
			return Ok(trak.mdia.mdhd.timescale as u64);
		}
	}

	Err(CmsfError::NoMoov)
}

// ---------------------------------------------------------------------------
// Test Helpers
// ---------------------------------------------------------------------------

#[cfg(test)]
pub(crate) mod test_helpers {
	use mp4_atom::Encode;

	/// Build a minimal valid moof+mdat with the given DTS, sample duration, and flags.
	pub fn build_moof_mdat(dts: u64, duration: u32, flags: u32) -> Vec<u8> {
		let moof = mp4_atom::Moof {
			mfhd: mp4_atom::Mfhd { sequence_number: 1 },
			traf: vec![mp4_atom::Traf {
				tfhd: mp4_atom::Tfhd {
					track_id: 1,
					..Default::default()
				},
				tfdt: Some(mp4_atom::Tfdt {
					base_media_decode_time: dts,
				}),
				trun: vec![mp4_atom::Trun {
					data_offset: Some(0),
					entries: vec![mp4_atom::TrunEntry {
						size: Some(4),
						duration: Some(duration),
						flags: Some(flags),
						..Default::default()
					}],
				}],
				..Default::default()
			}],
		};
		let mdat = mp4_atom::Mdat { data: vec![0; 4] };
		let mut buf = Vec::new();
		moof.encode(&mut buf).unwrap();
		mdat.encode(&mut buf).unwrap();
		buf
	}

	/// Build a minimal valid init segment (ftyp+moov) with the given timescale.
	pub fn build_init_segment(timescale: u32) -> Vec<u8> {
		let ftyp = mp4_atom::Ftyp {
			major_brand: b"isom".into(),
			minor_version: 0x200,
			compatible_brands: vec![b"isom".into()],
		};
		let moov = mp4_atom::Moov {
			mvhd: mp4_atom::Mvhd {
				timescale,
				..Default::default()
			},
			trak: vec![mp4_atom::Trak {
				tkhd: mp4_atom::Tkhd {
					track_id: 1,
					..Default::default()
				},
				mdia: mp4_atom::Mdia {
					mdhd: mp4_atom::Mdhd {
						timescale,
						..Default::default()
					},
					hdlr: mp4_atom::Hdlr {
						handler: b"vide".into(),
						name: "V".into(),
					},
					minf: mp4_atom::Minf {
						vmhd: Some(mp4_atom::Vmhd::default()),
						dinf: mp4_atom::Dinf {
							dref: mp4_atom::Dref { urls: vec![] },
						},
						stbl: mp4_atom::Stbl {
							stsd: mp4_atom::Stsd { codecs: vec![] },
							..Default::default()
						},
						..Default::default()
					},
				},
				..Default::default()
			}],
			..Default::default()
		};
		let mut buf = Vec::new();
		ftyp.encode(&mut buf).unwrap();
		moov.encode(&mut buf).unwrap();
		buf
	}
}
