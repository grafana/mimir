// SPDX-License-Identifier: AGPL-3.0-only

use std::net::SocketAddr;

use anyhow::Result;
use clap::Parser;

#[derive(Parser)]
struct Args {
    peer: SocketAddr,
    #[arg(long)]
    cluster_label: String,
    #[arg(long, default_value = "partition-ingesters/ring")]
    instance_key: String,
    #[arg(long, default_value = "ingester-partitions")]
    partition_key: String,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let snapshot = mimir_rust_kafka_ingester::ring_snapshot::fetch(
        args.peer,
        &args.cluster_label,
        &args.instance_key,
        &args.partition_key,
    )?;
    serde_json::to_writer_pretty(std::io::stdout().lock(), &snapshot)?;
    Ok(())
}
