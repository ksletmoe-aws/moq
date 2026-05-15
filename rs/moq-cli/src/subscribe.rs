use std::time::Duration;

use anyhow::Context;
use clap::ValueEnum;
use hang::moq_lite;
use tokio::io::AsyncWriteExt;

#[derive(ValueEnum, Clone, Copy)]
pub enum SubscribeFormat {
	Fmp4,
	/// CMSF-aware fMP4 output (reads MSF catalog instead of hang catalog).
	CmsfFmp4,
}

#[derive(clap::Args, Clone)]
pub struct SubscribeArgs {
	/// The format to write to stdout.
	#[arg(long)]
	pub format: SubscribeFormat,

	/// Maximum latency before skipping groups (e.g. `500ms`, `1s`).
	#[arg(long, default_value = "500ms", value_parser = humantime::parse_duration)]
	pub max_latency: Duration,
}

pub struct Subscribe {
	broadcast: moq_lite::BroadcastConsumer,
	args: SubscribeArgs,
}

impl Subscribe {
	pub fn new(broadcast: moq_lite::BroadcastConsumer, args: SubscribeArgs) -> Self {
		Self { broadcast, args }
	}

	pub async fn run(self) -> anyhow::Result<()> {
		match self.args.format {
			SubscribeFormat::Fmp4 => self.run_fmp4().await,
			SubscribeFormat::CmsfFmp4 => self.run_cmsf_fmp4().await,
		}
	}

	async fn run_fmp4(self) -> anyhow::Result<()> {
		let mut stdout = tokio::io::stdout();

		// Fmp4 subscribes to the catalog internally, builds the merged init segment
		// from the first catalog snapshot, then yields moof+mdat fragments in
		// timestamp order across tracks.
		let mut fmp4 = moq_mux::export::Fmp4::new(self.broadcast)?.with_latency(self.args.max_latency);

		while let Some(chunk) = fmp4.next().await? {
			stdout.write_all(&chunk).await?;
			stdout.flush().await?;
		}

		Ok(())
	}

	async fn run_cmsf_fmp4(self) -> anyhow::Result<()> {
		let mut stdout = tokio::io::stdout();

		if self.args.max_latency != Duration::from_millis(500) {
			anyhow::bail!(
				"--max-latency is not supported for cmsf-fmp4 output (CmsfBroadcastDemuxer \
				 does not yet implement group skipping). Omit --max-latency or use --format fmp4."
			);
		}

		let mut demuxer = moq_mux::export::cmsf::CmsfBroadcastDemuxer::new(self.broadcast)?;

		// Wait for the first catalog and track subscriptions.
		demuxer
			.ready()
			.await
			.context("CMSF demuxer failed waiting for catalog")?;

		// Build merged init segment from per-track init data.
		let inits = demuxer.init_segments();
		let init =
			moq_mux::export::cmsf::build_merged_init(&inits).context("failed to build merged CMSF init segment")?;
		stdout.write_all(&init).await?;
		stdout.flush().await?;

		// Each segment's media_data is already a valid moof+mdat fragment.
		while let Some(seg) = demuxer.next().await.context("failed reading next CMSF segment")? {
			stdout.write_all(&seg.media_data).await?;
			stdout.flush().await?;
		}

		Ok(())
	}
}
