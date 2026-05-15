use clap::Subcommand;
use hang::moq_lite;
use moq_mux::import;

#[derive(Subcommand, Clone)]
pub enum PublishFormat {
	Avc3,
	Fmp4,
	Cmsf,
	// NOTE: No aac support because it needs framing.
	Hls {
		/// URL or file path of an HLS playlist to ingest.
		#[arg(long)]
		playlist: String,
	},
}

enum PublishDecoder {
	Avc3(Box<import::Avc3>),
	Fmp4(Box<import::Fmp4>),
	Cmsf(Box<import::CmsfFmp4Importer>),
	Hls(Box<import::Hls>),
}

impl PublishDecoder {
	/// Decode a chunk of bytes from stdin (Avc3, Fmp4, or Cmsf only).
	fn decode_buf(&mut self, buffer: &mut bytes::BytesMut) -> anyhow::Result<()> {
		match self {
			Self::Avc3(d) => d.decode_stream(buffer, None),
			Self::Fmp4(d) => d.decode(buffer),
			Self::Cmsf(d) => d.decode(buffer),
			Self::Hls(_) => unreachable!(),
		}
	}

	/// Signal end-of-input so tracks and catalogs are closed cleanly.
	fn finish(&mut self) -> anyhow::Result<()> {
		match self {
			Self::Cmsf(d) => d.finish(),
			_ => Ok(()),
		}
	}
}

pub struct Publish {
	decoder: PublishDecoder,
	broadcast: moq_lite::BroadcastProducer,
}

impl Publish {
	pub fn new(format: &PublishFormat) -> anyhow::Result<Self> {
		let mut broadcast = moq_lite::Broadcast::new().produce();

		let decoder = match format {
			// CMSF manages its own catalog tracks — skip catalog::Producer.
			PublishFormat::Cmsf => {
				let cmsf = import::CmsfFmp4Importer::new(broadcast.clone(), import::CmsfConfig::default())?;
				PublishDecoder::Cmsf(Box::new(cmsf))
			}
			_ => {
				let catalog = moq_mux::catalog::Producer::new(&mut broadcast)?;
				match format {
					PublishFormat::Avc3 => {
						let avc3 = import::Avc3::new(broadcast.clone(), catalog.clone());
						PublishDecoder::Avc3(Box::new(avc3))
					}
					PublishFormat::Fmp4 => {
						let fmp4 = import::Fmp4::new(broadcast.clone(), catalog.clone());
						PublishDecoder::Fmp4(Box::new(fmp4))
					}
					PublishFormat::Hls { playlist } => {
						let hls = import::Hls::new(
							broadcast.clone(),
							catalog.clone(),
							import::HlsConfig::new(playlist.clone()),
						)?;
						PublishDecoder::Hls(Box::new(hls))
					}
					PublishFormat::Cmsf => unreachable!(),
				}
			}
		};

		Ok(Self { decoder, broadcast })
	}

	pub fn consume(&self) -> moq_lite::BroadcastConsumer {
		self.broadcast.consume()
	}

	pub async fn run(mut self) -> anyhow::Result<()> {
		if let PublishDecoder::Hls(decoder) = &mut self.decoder {
			decoder.init().await?;
			decoder.run().await
		} else {
			let mut stdin = tokio::io::stdin();
			let mut buffer = bytes::BytesMut::new();

			loop {
				let n = tokio::io::AsyncReadExt::read_buf(&mut stdin, &mut buffer).await?;
				if n == 0 {
					self.decoder.finish()?;
					return Ok(());
				}
				self.decoder.decode_buf(&mut buffer)?;
			}
		}
	}
}
