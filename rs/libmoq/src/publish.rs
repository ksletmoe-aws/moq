use std::{str::FromStr, sync::Arc};

use bytes::{Buf, Bytes};
use moq_mux::import;

use crate::{Error, Id, NonZeroSlab};

#[derive(Default)]
pub struct Publish {
	/// Active broadcast producers for publishing.
	broadcasts: NonZeroSlab<(moq_lite::BroadcastProducer, moq_mux::catalog::Producer)>,

	/// Active media encoders/decoders for publishing.
	media: NonZeroSlab<import::Framed>,

	/// Active CMSF broadcast producers.
	cmsf: NonZeroSlab<import::CmsfBroadcastProducer>,
}

impl Publish {
	pub fn create(&mut self) -> Result<Id, Error> {
		let mut broadcast = moq_lite::Broadcast::new().produce();
		let catalog = moq_mux::catalog::Producer::new(&mut broadcast)?;

		let id = self.broadcasts.insert((broadcast, catalog))?;
		Ok(id)
	}

	pub fn get(&self, id: Id) -> Result<&moq_lite::BroadcastProducer, Error> {
		self.broadcasts
			.get(id)
			.ok_or(Error::BroadcastNotFound)
			.map(|(broadcast, _)| broadcast)
	}

	pub fn close(&mut self, broadcast: Id) -> Result<(), Error> {
		self.broadcasts.remove(broadcast).ok_or(Error::BroadcastNotFound)?;
		Ok(())
	}

	pub fn media_ordered(&mut self, broadcast: Id, format: &str, mut init: &[u8]) -> Result<Id, Error> {
		let (broadcast, catalog) = self.broadcasts.get(broadcast).ok_or(Error::BroadcastNotFound)?;

		let format = import::FramedFormat::from_str(format).map_err(|_| Error::UnknownFormat(format.to_string()))?;
		let decoder = import::Framed::new(broadcast.clone(), catalog.clone(), format, &mut init)
			.map_err(|err| Error::InitFailed(Arc::new(err)))?;

		let id = self.media.insert(decoder)?;
		Ok(id)
	}

	pub fn media_frame(
		&mut self,
		media: Id,
		mut data: &[u8],
		timestamp: hang::container::Timestamp,
	) -> Result<(), Error> {
		let media = self.media.get_mut(media).ok_or(Error::MediaNotFound)?;

		media
			.decode_frame(&mut data, Some(timestamp))
			.map_err(|err| Error::DecodeFailed(Arc::new(err)))?;

		if data.has_remaining() {
			return Err(Error::DecodeFailed(Arc::new(anyhow::anyhow!(
				"buffer was not fully consumed"
			))));
		}

		Ok(())
	}

	pub fn media_close(&mut self, media: Id) -> Result<(), Error> {
		let mut decoder = self.media.remove(media).ok_or(Error::MediaNotFound)?;
		decoder.finish().map_err(|err| Error::DecodeFailed(Arc::new(err)))?;
		Ok(())
	}

	// --- CMSF broadcast producer ---

	pub fn cmsf_create(&mut self, publish_hang: bool) -> Result<(Id, moq_lite::BroadcastConsumer), Error> {
		let broadcast = moq_lite::Broadcast::new().produce();
		let consumer = broadcast.consume();
		let config = import::CmsfConfig { publish_hang };
		let p =
			import::CmsfBroadcastProducer::new(broadcast, config).map_err(|err| Error::InitFailed(Arc::new(err)))?;
		let id = self.cmsf.insert(p)?;
		Ok((id, consumer))
	}

	#[allow(clippy::too_many_arguments)]
	pub fn cmsf_add_video(
		&mut self,
		id: Id,
		codec: &str,
		description: Option<&[u8]>,
		width: u32,
		height: u32,
		bitrate: u64,
		framerate: f64,
		timescale: u64,
		init_segment: &[u8],
		alt_group: u32,
	) -> Result<Id, Error> {
		let p = self.cmsf.get_mut(id).ok_or(Error::BroadcastNotFound)?;
		let codec = hang::catalog::VideoCodec::from_str(codec).map_err(|_| Error::UnknownFormat(codec.to_string()))?;
		let track = import::CmsfVideoTrack {
			codec,
			description: description.map(Bytes::copy_from_slice),
			width: if width == 0 { None } else { Some(width) },
			height: if height == 0 { None } else { Some(height) },
			bitrate: if bitrate == 0 { None } else { Some(bitrate) },
			framerate: if framerate == 0.0 { None } else { Some(framerate) },
			timescale,
			init_segment: Bytes::copy_from_slice(init_segment),
			alt_group: if alt_group == 0 { None } else { Some(alt_group) },
		};
		let tid = p
			.add_video_track(track)
			.map_err(|err| Error::InitFailed(Arc::new(err)))?;
		Id::try_from(tid.0 as u32 + 1)
	}

	#[allow(clippy::too_many_arguments)]
	pub fn cmsf_add_audio(
		&mut self,
		id: Id,
		codec: &str,
		description: Option<&[u8]>,
		sample_rate: u32,
		channel_count: u32,
		bitrate: u64,
		timescale: u64,
		init_segment: &[u8],
		alt_group: u32,
	) -> Result<Id, Error> {
		let p = self.cmsf.get_mut(id).ok_or(Error::BroadcastNotFound)?;
		let codec = hang::catalog::AudioCodec::from_str(codec).map_err(|_| Error::UnknownFormat(codec.to_string()))?;
		let track = import::CmsfAudioTrack {
			codec,
			description: description.map(Bytes::copy_from_slice),
			sample_rate,
			channel_count,
			bitrate: if bitrate == 0 { None } else { Some(bitrate) },
			timescale,
			init_segment: Bytes::copy_from_slice(init_segment),
			alt_group: if alt_group == 0 { None } else { Some(alt_group) },
		};
		let tid = p
			.add_audio_track(track)
			.map_err(|err| Error::InitFailed(Arc::new(err)))?;
		Id::try_from(tid.0 as u32 + 1)
	}

	pub fn cmsf_write(&mut self, id: Id, track_handle: u32, data: &[u8], group_id: u64) -> Result<(), Error> {
		let p = self.cmsf.get_mut(id).ok_or(Error::BroadcastNotFound)?;
		if track_handle == 0 {
			return Err(Error::InvalidId);
		}
		let track_id = import::TrackId((track_handle - 1) as usize);
		let obj = import::CmsfObject {
			data: Bytes::copy_from_slice(data),
			group_id: if group_id == 0 { None } else { Some(group_id) },
		};
		p.write(track_id, obj).map_err(|err| Error::DecodeFailed(Arc::new(err)))
	}

	pub fn cmsf_close(&mut self, id: Id) -> Result<(), Error> {
		let mut p = self.cmsf.remove(id).ok_or(Error::BroadcastNotFound)?;
		p.finish().map_err(|err| Error::DecodeFailed(Arc::new(err)))?;
		Ok(())
	}
}
