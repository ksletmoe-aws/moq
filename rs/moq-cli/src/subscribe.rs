use std::time::Duration;

use clap::ValueEnum;
use hang::moq_net;
use tokio::io::AsyncWriteExt;

#[derive(ValueEnum, Clone, Copy)]
pub enum SubscribeFormat {
	Fmp4,
}

/// Catalog wire format to subscribe to for track discovery.
#[derive(ValueEnum, Clone, Copy, Default)]
pub enum CatalogFormat {
	/// The hang catalog (`catalog.json`, hang JSON schema).
	#[default]
	Hang,
	/// The MSF catalog (`catalog`, draft-ietf-moq-msf JSON schema).
	Msf,
}

#[derive(clap::Args, Clone)]
pub struct SubscribeArgs {
	/// The format to write to stdout.
	#[arg(long)]
	pub format: SubscribeFormat,

	/// Maximum latency before skipping groups (e.g. `500ms`, `1s`).
	#[arg(long, default_value = "500ms", value_parser = humantime::parse_duration)]
	pub max_latency: Duration,

	/// Catalog format to subscribe to for track discovery.
	#[arg(long, default_value = "hang")]
	pub catalog: CatalogFormat,
}

pub struct Subscribe {
	broadcast: moq_net::BroadcastConsumer,
	args: SubscribeArgs,
}

impl Subscribe {
	pub fn new(broadcast: moq_net::BroadcastConsumer, args: SubscribeArgs) -> Self {
		Self { broadcast, args }
	}

	pub async fn run(self) -> anyhow::Result<()> {
		match self.args.format {
			SubscribeFormat::Fmp4 => self.run_fmp4().await,
		}
	}

	async fn run_fmp4(self) -> anyhow::Result<()> {
		let mut stdout = tokio::io::stdout();

		// Fmp4 subscribes to the catalog internally, builds the merged init segment
		// from the first catalog snapshot, then yields moof+mdat fragments in
		// timestamp order across tracks.
		let mut fmp4 = match self.args.catalog {
			CatalogFormat::Hang => moq_mux::export::Fmp4::new(self.broadcast)?,
			CatalogFormat::Msf => moq_mux::export::Fmp4::new_msf(self.broadcast)?,
		}
		.with_latency(self.args.max_latency);

		while let Some(chunk) = fmp4.next().await? {
			stdout.write_all(&chunk).await?;
			stdout.flush().await?;
		}

		Ok(())
	}
}
