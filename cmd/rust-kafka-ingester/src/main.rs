use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use serde::Serialize;
use tonic::codec::CompressionEncoding;
use tonic::transport::{Identity, Server, ServerTlsConfig};

use mimir_rust_kafka_ingester::consistency::Consistency;
use mimir_rust_kafka_ingester::kafka::{OffsetAt, PartitionClient, StartOffset};
use mimir_rust_kafka_ingester::proto::cortex::ingester_server::IngesterServer;
use mimir_rust_kafka_ingester::record::{DecodedRequest, decode_record};
use mimir_rust_kafka_ingester::segment::SegmentLog;
mod profiling;

#[cfg(all(target_os = "linux", feature = "jemalloc", feature = "mimalloc"))]
compile_error!("enable only one of the jemalloc and mimalloc features");

#[cfg(all(target_os = "linux", feature = "jemalloc"))]
#[global_allocator]
static GLOBAL_ALLOCATOR: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(all(target_os = "linux", feature = "mimalloc"))]
#[global_allocator]
static GLOBAL_ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

use mimir_rust_kafka_ingester::service::IngesterService;
use mimir_rust_kafka_ingester::store::{SnapshotOffset, Store};
use mimir_rust_kafka_ingester::xor;

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Serve(Box<ServeArgs>),
    DecodeRecord {
        #[arg(long)]
        version: u32,
    },
    EncodeXor,
    ServeFixture {
        #[arg(long, default_value = "127.0.0.1:9095")]
        listen: String,
        #[arg(long)]
        profile_listen: Option<String>,
        #[arg(long, default_value_t = 1)]
        version: u32,
        #[arg(long, default_value = "tenant-a")]
        tenant: String,
        #[arg(long)]
        data_dir: Option<PathBuf>,
        #[arg(long)]
        restore_only: bool,
        #[arg(long, default_value_t = 0)]
        offset: i64,
        #[arg(long, default_value_t = 1)]
        timestamp_ms: i64,
    },
}

#[derive(clap::Args)]
struct ServeArgs {
    #[arg(long)]
    brokers: String,
    #[arg(long)]
    topic: String,
    #[arg(long, value_name = "TOPIC=BROKER[,BROKER...]")]
    additional_kafka_cluster: Vec<String>,
    #[arg(long)]
    partition: i32,
    #[arg(long)]
    data_dir: PathBuf,
    #[arg(long, default_value_t = 60_000)]
    segment_sync_interval_ms: u64,
    #[arg(long, default_value = "0.0.0.0:9095")]
    listen: String,
    #[arg(long)]
    profile_listen: Option<String>,
    #[arg(long, default_value = "earliest")]
    start_offset: String,
    #[arg(long, default_value_t = 1200)]
    active_window_seconds: i64,
    #[arg(long)]
    retention_seconds: Option<i64>,
    #[arg(long, default_value_t = 0)]
    read_compartment: i32,
    #[arg(long, default_value_t = 30)]
    consistency_timeout_seconds: u64,
    #[arg(long)]
    sasl_username: Option<String>,
    #[arg(long)]
    sasl_password: Option<String>,
    #[arg(long, default_value = "plain")]
    sasl_mechanism: String,
    #[arg(long)]
    kafka_tls: bool,
    #[arg(long)]
    grpc_tls_cert: Option<String>,
    #[arg(long)]
    grpc_tls_key: Option<String>,
}

#[derive(Serialize)]
struct Summary {
    source: i32,
    series: Vec<SeriesSummary>,
    metadata: Vec<MetadataSummary>,
}

#[derive(Serialize)]
struct SeriesSummary {
    labels: Vec<(String, String)>,
    samples: Vec<(i64, f64)>,
    histogram_timestamps: Vec<i64>,
    exemplars: Vec<ExemplarSummary>,
    created_timestamp: i64,
}

#[derive(Serialize)]
struct ExemplarSummary {
    timestamp: i64,
    value: f64,
    labels: Vec<(String, String)>,
}

#[derive(Serialize)]
struct MetadataSummary {
    metric: String,
    r#type: i32,
    help: String,
    unit: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Serve(args) => serve(*args).await,
        Command::DecodeRecord { version } => {
            let mut bytes = Vec::new();
            std::io::stdin().read_to_end(&mut bytes)?;
            serde_json::to_writer(std::io::stdout(), &summary(decode_record(version, &bytes)?))?;
            Ok(())
        }
        Command::EncodeXor => {
            let samples: Vec<(i64, f64)> = serde_json::from_reader(std::io::stdin())?;
            let encoded = xor::encode(&samples);
            let mut stdout = std::io::stdout().lock();
            for byte in encoded {
                write!(stdout, "{byte:02x}")?;
            }
            Ok(())
        }
        Command::ServeFixture {
            listen,
            profile_listen,
            version,
            tenant,
            data_dir,
            restore_only,
            offset,
            timestamp_ms,
        } => {
            serve_fixture(
                &listen,
                profile_listen.as_deref(),
                version,
                &tenant,
                data_dir.as_deref(),
                restore_only,
                offset,
                timestamp_ms,
            )
            .await
        }
    }
}

async fn serve_fixture(
    listen: &str,
    profile_listen: Option<&str>,
    version: u32,
    tenant: &str,
    data_dir: Option<&std::path::Path>,
    restore_only: bool,
    offset: i64,
    timestamp_ms: i64,
) -> Result<()> {
    let store = Arc::new(Store::default());
    if let Some(data_dir) = data_dir {
        let (mut log, recovered) = SegmentLog::open(data_dir, 0, "fixture", 0, None)?;
        for record in recovered {
            if has_data(&record.request) {
                store.ingest_recovered(&record.tenant, record.request, record.ingested_ms)?;
            }
        }
        if !restore_only {
            let mut bytes = Vec::new();
            std::io::stdin().read_to_end(&mut bytes)?;
            let request = decode_record(version, &bytes)?;
            store.ingest(tenant, request.clone())?;
            log.append(offset, timestamp_ms, tenant, &request)?;
        }
    } else {
        if restore_only {
            bail!("--restore-only requires --data-dir");
        }
        let mut bytes = Vec::new();
        std::io::stdin().read_to_end(&mut bytes)?;
        store.ingest(tenant, decode_record(version, &bytes)?)?;
    }
    if let Some(address) = profile_listen {
        profiling::start(address).await?;
    }
    let address = listen.parse().context("parse listen address")?;
    Server::builder()
        .add_service(IngesterServer::new(IngesterService::new(store)))
        .serve(address)
        .await?;
    Ok(())
}

async fn serve(args: ServeArgs) -> Result<()> {
    let ServeArgs {
        brokers,
        topic,
        additional_kafka_cluster,
        partition,
        data_dir,
        segment_sync_interval_ms,
        listen,
        profile_listen,
        start_offset,
        active_window_seconds,
        retention_seconds,
        read_compartment,
        consistency_timeout_seconds,
        sasl_username,
        sasl_password,
        sasl_mechanism,
        kafka_tls,
        grpc_tls_cert,
        grpc_tls_key,
    } = args;
    let sasl_username = sasl_username.or_else(|| std::env::var("MIMIR_KAFKA_SASL_USERNAME").ok());
    let sasl_password = sasl_password.or_else(|| std::env::var("MIMIR_KAFKA_SASL_PASSWORD").ok());
    if segment_sync_interval_ms == 0 {
        bail!("segment sync interval must be greater than zero");
    }
    let configured_start_offset = match start_offset.as_str() {
        "earliest" => StartOffset::Earliest,
        "latest" => StartOffset::Latest,
        value => StartOffset::At(value.parse().context("parse start offset")?),
    };
    let mut sources = vec![(topic, brokers)];
    for source in &additional_kafka_cluster {
        let (topic, brokers) = source
            .split_once('=')
            .context("additional Kafka cluster must be TOPIC=BROKER[,BROKER...]")?;
        sources.push((topic.to_owned(), brokers.to_owned()));
    }
    let active_window_ms = active_window_seconds.saturating_mul(1000);
    let retention_ms = retention_seconds.map(|seconds| seconds.saturating_mul(1000));
    let chunk_dir = data_dir.join("chunks_head");
    let snapshot_started = Instant::now();
    let mut resumed = None;
    let store = match Store::restore(active_window_ms, retention_ms, &chunk_dir)? {
        Some(restored) if restored.offsets.len() == sources.len() => {
            let logs = sources
                .iter()
                .enumerate()
                .map(|(cluster, (topic, _))| {
                    SegmentLog::open_at_checkpoint(
                        &data_dir,
                        cluster,
                        topic,
                        partition,
                        retention_ms,
                        restored.offsets[cluster].offset,
                    )
                })
                .collect::<Result<Option<Vec<_>>>>()?;
            match logs {
                Some(logs) => {
                    eprintln!(
                        "phase=head_snapshot_restored partition={partition} duration_ms={}",
                        snapshot_started.elapsed().as_millis()
                    );
                    resumed = Some(
                        logs.into_iter()
                            .zip(restored.offsets)
                            .collect::<Vec<_>>()
                            .into_iter(),
                    );
                    restored.store
                }
                None => {
                    eprintln!(
                        "phase=head_snapshot_stale partition={partition} reason=segment_checkpoint_mismatch"
                    );
                    Store::new(active_window_ms, retention_ms, Some(chunk_dir.clone()))?
                }
            }
        }
        Some(_) => {
            eprintln!("phase=head_snapshot_stale partition={partition} reason=kafka_cluster_count");
            Store::new(active_window_ms, retention_ms, Some(chunk_dir.clone()))?
        }
        None => Store::new(active_window_ms, retention_ms, Some(chunk_dir.clone()))?,
    };
    let store = Arc::new(store);
    let consistency = Arc::new(Consistency::new(
        partition,
        read_compartment,
        sources.len(),
        Duration::from_secs(consistency_timeout_seconds),
    ));
    let (fatal_tx, mut fatal_rx) = tokio::sync::watch::channel(false);
    let (shutdown_tx, _) = tokio::sync::watch::channel(false);
    let (warmup_tx, _) = tokio::sync::watch::channel(false);
    let shutdown_requested = Arc::new(AtomicBool::new(false));
    let signal_requested = Arc::clone(&shutdown_requested);
    let signal_sender = shutdown_tx.clone();
    tokio::spawn(async move {
        if let Err(error) = shutdown_signal().await {
            eprintln!("shutdown signal handler failed: {error:#}");
        }
        eprintln!("phase=shutdown_signal_received");
        signal_requested.store(true, Ordering::Relaxed);
        let _ = signal_sender.send(true);
    });
    let mut startup_shutdown_rx = shutdown_tx.subscribe();
    if let Some(address) = profile_listen {
        profiling::start(&address).await?;
    }
    let mut workers = Vec::new();
    let mut replay_ready = Vec::new();
    for (cluster, (topic, brokers)) in sources.into_iter().enumerate() {
        if shutdown_requested.load(Ordering::Relaxed) {
            stop_workers(&shutdown_tx, workers).await?;
            return Ok(());
        }
        eprintln!(
            "phase=disk_recovery_start cluster={cluster} topic={topic} partition={partition} data_dir={}",
            data_dir.display()
        );
        let mut recovered_timestamp = 0;
        let mut recovered_count = 0_u64;
        let mut last_recovery_log = Instant::now();
        let resumed_log = resumed
            .as_mut()
            .map(|logs| logs.next().expect("one log per cluster"));
        let recovered_log = if let Some((log, offset)) = resumed_log {
            recovered_timestamp = offset.timestamp_ms;
            Ok(log)
        } else {
            SegmentLog::open_replaying(
                &data_dir,
                cluster,
                &topic,
                partition,
                retention_seconds.map(|seconds| seconds.saturating_mul(1000)),
                |record| {
                    if shutdown_requested.load(Ordering::Relaxed) {
                        bail!("shutdown requested during segment recovery");
                    }
                    recovered_count += 1;
                    if last_recovery_log.elapsed() >= Duration::from_secs(30) {
                        eprintln!(
                            "phase=disk_recovery_progress cluster={cluster} partition={partition} records={recovered_count} offset={}",
                            record.offset
                        );
                        last_recovery_log = Instant::now();
                    }
                    recovered_timestamp = recovered_timestamp.max(record.kafka_timestamp_ms);
                    if has_data(&record.request) {
                        store
                            .ingest_recovered(&record.tenant, record.request, record.ingested_ms)
                            .with_context(|| {
                                format!(
                                    "restore Kafka cluster {cluster} offset {} from disk",
                                    record.offset
                                )
                            })?;
                    }
                    Ok(())
                },
            )
        };
        let mut segment_log = match recovered_log {
            Ok(log) => log,
            Err(_) if shutdown_requested.load(Ordering::Relaxed) => {
                stop_workers(&shutdown_tx, workers).await?;
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        let persisted_offset = segment_log.last_offset();
        eprintln!(
            "phase=disk_recovery_complete cluster={cluster} partition={partition} records={recovered_count} persisted_offset={persisted_offset:?}"
        );
        let source_start_offset = persisted_offset
            .map(|offset| StartOffset::At(offset.saturating_add(1)))
            .unwrap_or(configured_start_offset);
        let start_offset_label = match source_start_offset {
            StartOffset::Earliest => "earliest".to_owned(),
            StartOffset::Latest => "latest".to_owned(),
            StartOffset::At(offset) => offset.to_string(),
        };
        let partition_client = connect_partition(
            &brokers,
            &topic,
            partition,
            kafka_tls,
            sasl_username.as_deref(),
            sasl_password.as_deref(),
            &sasl_mechanism,
        )
        .await?;
        let latest_offset = partition_client
            .get_offset(OffsetAt::Latest)
            .await
            .context("fetch latest Kafka offset")?;
        eprintln!(
            "phase=kafka_earliest_lookup_start cluster={cluster} topic={topic} partition={partition} latest_offset={latest_offset}"
        );
        let earliest_offset = tokio::time::timeout(
            Duration::from_secs(60),
            partition_client.get_offset(OffsetAt::Earliest),
        )
        .await
        .context("fetch earliest Kafka offset timed out")?
        .context("fetch earliest Kafka offset")?;
        consistency.high_watermark(cluster, latest_offset);
        let replay_target = latest_offset.saturating_sub(1);
        let replay_complete =
            initial_replay_complete(source_start_offset, earliest_offset, latest_offset);
        eprintln!(
            "phase=kafka_replay_start cluster={cluster} topic={topic} partition={partition} start_offset={start_offset_label} earliest_offset={earliest_offset} latest_offset={latest_offset} replay_target={replay_target} already_caught_up={replay_complete}"
        );
        if let Some(offset) = persisted_offset {
            consistency.consumed(
                cluster,
                offset,
                offset.saturating_add(1),
                recovered_timestamp,
            );
        }
        consistency.set_client(cluster, Arc::clone(&partition_client));
        let initial_offset = match source_start_offset {
            StartOffset::Earliest => earliest_offset,
            StartOffset::Latest => latest_offset,
            StartOffset::At(offset) => offset,
        };
        partition_client.assign(initial_offset)?;
        let ingest_store = Arc::clone(&store);
        let ingest_consistency = Arc::clone(&consistency);
        let fatal_tx = fatal_tx.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        let mut warmup_rx = warmup_tx.subscribe();
        let reconnect_username = sasl_username.clone();
        let reconnect_password = sasl_password.clone();
        let reconnect_mechanism = sasl_mechanism.clone();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        replay_ready.push(ready_rx);
        workers.push(tokio::spawn(async move {
            let mut partition_client = partition_client;
            let mut stream = Arc::clone(&partition_client);
            let mut next_offset = initial_offset;
            let mut last_replay_log = Instant::now();
            let mut last_consume_log = Instant::now();
            let mut logged_first_consume = false;
            let mut last_fetch_progress = Instant::now();
            let mut sync_interval =
                tokio::time::interval(Duration::from_millis(segment_sync_interval_ms));
            sync_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut watermark_interval = tokio::time::interval(Duration::from_secs(30));
            watermark_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut ready_tx = Some(ready_tx);
            let mut last_timestamp_ms = recovered_timestamp;
            if replay_complete {
                eprintln!(
                    "phase=kafka_replay_complete cluster={cluster} partition={partition} latest_offset={latest_offset}"
                );
                let _ = ready_tx.take().expect("replay signal available").send(());
                tokio::select! {
                    _ = warmup_rx.changed() => {}
                    _ = shutdown_rx.changed() => return snapshot_offset(&mut segment_log, last_timestamp_ms, cluster, &fatal_tx),
                }
            }
            loop {
                let message = tokio::select! {
                    _ = shutdown_rx.changed() => break,
                    message = tokio::time::timeout_at(tokio::time::Instant::from_std(last_fetch_progress + Duration::from_secs(30)), stream.next()) => {
                        match message {
                            Ok(message) => message,
                            Err(_) => {
                                let latest = tokio::time::timeout(
                                    Duration::from_secs(5),
                                    partition_client.get_offset(OffsetAt::Latest),
                                ).await;
                                if matches!(latest, Ok(Ok(offset)) if offset <= next_offset) {
                                    last_fetch_progress = Instant::now();
                                    continue;
                                }
                                eprintln!("phase=kafka_fetch_timeout cluster={cluster} partition={partition} next_offset={next_offset} latest_offset={latest:?}");
                                let refreshed = tokio::time::timeout(
                                    Duration::from_secs(20),
                                    connect_partition(
                                        &brokers,
                                        &topic,
                                        partition,
                                        kafka_tls,
                                        reconnect_username.as_deref(),
                                        reconnect_password.as_deref(),
                                        &reconnect_mechanism,
                                    ),
                                ).await;
                                match refreshed {
                                    Ok(Ok(client)) => {
                                        match client.assign(next_offset) {
                                            Ok(()) => {
                                                partition_client = client;
                                                ingest_consistency.set_client(cluster, Arc::clone(&partition_client));
                                                eprintln!("phase=kafka_client_reconnected cluster={cluster} partition={partition} next_offset={next_offset}");
                                            }
                                            Err(error) => eprintln!("phase=kafka_client_reconnect_failed cluster={cluster} partition={partition} error={error:#}"),
                                        }
                                    }
                                    Ok(Err(error)) => eprintln!("phase=kafka_client_reconnect_failed cluster={cluster} partition={partition} error={error:#}"),
                                    Err(_) => eprintln!("phase=kafka_client_reconnect_timeout cluster={cluster} partition={partition}"),
                                }
                                stream = Arc::clone(&partition_client);
                                last_fetch_progress = Instant::now();
                                continue;
                            }
                        }
                    },
                    _ = sync_interval.tick() => {
                        if let Err(error) = segment_log.maintain() {
                            eprintln!("Kafka cluster {cluster} persistence maintenance failed: {error:#}");
                            let _ = fatal_tx.send(true);
                            break;
                        }
                        if cluster == 0 {
                            if let Err(error) = ingest_store.prune_expired() {
                                eprintln!("retention pruning failed: {error:#}");
                                let _ = fatal_tx.send(true);
                                break;
                            }
                        }
                        continue;
                    },
                    _ = watermark_interval.tick() => {
                        if let Ok(offset) = partition_client.get_offset(OffsetAt::Latest).await {
                            ingest_consistency.high_watermark(cluster, offset);
                        }
                        continue;
                    }
                };
                let Some(message) = message else {
                    eprintln!("Kafka cluster {cluster} consumer stopped");
                    let _ = fatal_tx.send(true);
                    break;
                };
                let (message, high_watermark) = match message {
                    Ok(message) => message,
                    Err(error) => {
                        eprintln!("Kafka cluster {cluster}: {error}");
                        continue;
                    }
                };
                let record = message.record;
                let timestamp_ms = record.timestamp_ms;
                let (tenant, request, should_ingest) = match record.request {
                    Some(Ok(request)) => (record.tenant, request, true),
                    Some(Err(error)) => {
                        eprintln!(
                            "Kafka cluster {cluster} offset {} decode failed: {error:#}",
                            message.offset
                        );
                        let _ = fatal_tx.send(true);
                        break;
                    }
                    None => (String::new(), empty_request(), false),
                };
                if let Err(error) = ingest_and_append(
                    &ingest_store,
                    &mut segment_log,
                    message.offset,
                    timestamp_ms,
                    &tenant,
                    request,
                    should_ingest,
                ) {
                    eprintln!(
                        "Kafka cluster {cluster} offset {} failed: {error:#}",
                        message.offset
                    );
                    let _ = fatal_tx.send(true);
                    break;
                }
                ingest_consistency.consumed(cluster, message.offset, high_watermark, timestamp_ms);
                last_timestamp_ms = last_timestamp_ms.max(timestamp_ms);
                next_offset = message.offset.saturating_add(1);
                last_fetch_progress = Instant::now();
                if !logged_first_consume || last_consume_log.elapsed() >= Duration::from_secs(60) {
                    eprintln!(
                        "phase=kafka_consume_progress cluster={cluster} partition={partition} offset={} latest_offset={high_watermark} lag={}",
                        message.offset,
                        high_watermark.saturating_sub(next_offset)
                    );
                    last_consume_log = Instant::now();
                    logged_first_consume = true;
                }
                if ready_tx.is_some() && last_replay_log.elapsed() >= Duration::from_secs(30) {
                    eprintln!(
                        "phase=kafka_replay_progress cluster={cluster} partition={partition} offset={} latest_offset={high_watermark} lag={}",
                        message.offset,
                        high_watermark.saturating_sub(message.offset.saturating_add(1))
                    );
                    last_replay_log = Instant::now();
                }
                if message.offset >= replay_target
                    && let Some(ready_tx) = ready_tx.take()
                {
                    eprintln!(
                        "phase=kafka_replay_complete cluster={cluster} partition={partition} offset={} latest_offset={high_watermark}",
                        message.offset
                    );
                    let _ = ready_tx.send(());
                    tokio::select! {
                        _ = warmup_rx.changed() => {}
                        _ = shutdown_rx.changed() => break,
                    }
                }
            }
            snapshot_offset(&mut segment_log, last_timestamp_ms, cluster, &fatal_tx)
        }));
    }

    for ready in replay_ready {
        tokio::select! {
            result = ready => result.context("Kafka consumer stopped before initial replay")?,
            _ = startup_shutdown_rx.changed() => {
                return shutdown_with_snapshot(&store, &shutdown_tx, workers, &fatal_rx, partition).await;
            }
        }
    }
    store.prune_expired()?;
    if shutdown_requested.load(Ordering::Relaxed) {
        return shutdown_with_snapshot(&store, &shutdown_tx, workers, &fatal_rx, partition).await;
    }
    let _ = warmup_tx.send(true);

    let address = listen.parse().context("parse listen address")?;
    let mut server = Server::builder();
    match (grpc_tls_cert, grpc_tls_key) {
        (Some(cert), Some(key)) => {
            server = server.tls_config(ServerTlsConfig::new().identity(Identity::from_pem(
                std::fs::read(cert).context("read gRPC TLS certificate")?,
                std::fs::read(key).context("read gRPC TLS key")?,
            )))?;
        }
        (None, None) => {}
        _ => anyhow::bail!("both gRPC TLS certificate and key are required"),
    }
    let service = IngesterServer::new(IngesterService::with_consistency(
        Arc::clone(&store),
        consistency,
    ))
    .accept_compressed(CompressionEncoding::Gzip);
    let fatal_state = fatal_rx.clone();
    let shutdown_sender = shutdown_tx.clone();
    let mut serve_shutdown_rx = shutdown_tx.subscribe();
    eprintln!("phase=grpc_start partition={partition} address={address}");
    let serve_result = server
        .add_service(service)
        .serve_with_shutdown(address, async move {
            tokio::select! {
                _ = serve_shutdown_rx.changed() => {}
                _ = fatal_rx.changed() => {}
            }
            let _ = shutdown_sender.send(true);
        })
        .await;
    shutdown_with_snapshot(&store, &shutdown_tx, workers, &fatal_state, partition).await?;
    serve_result?;
    if *fatal_state.borrow() {
        bail!("Kafka ingester stopped after a fatal consumer error");
    }
    eprintln!("phase=shutdown_complete partition={partition}");
    Ok(())
}

async fn stop_workers(
    shutdown_tx: &tokio::sync::watch::Sender<bool>,
    workers: Vec<tokio::task::JoinHandle<SnapshotOffset>>,
) -> Result<Vec<SnapshotOffset>> {
    let _ = shutdown_tx.send(true);
    let mut offsets = Vec::with_capacity(workers.len());
    for worker in workers {
        offsets.push(worker.await.context("join Kafka consumer")?);
    }
    Ok(offsets)
}

// Called only once every Kafka cluster finished recovery, so the store covers each log's last offset.
async fn shutdown_with_snapshot(
    store: &Arc<Store>,
    shutdown_tx: &tokio::sync::watch::Sender<bool>,
    workers: Vec<tokio::task::JoinHandle<SnapshotOffset>>,
    fatal_rx: &tokio::sync::watch::Receiver<bool>,
    partition: i32,
) -> Result<()> {
    let offsets = stop_workers(shutdown_tx, workers).await?;
    if *fatal_rx.borrow() {
        eprintln!("phase=head_snapshot_skipped partition={partition} reason=fatal_error");
        return Ok(());
    }
    let started = Instant::now();
    let snapshot_store = Arc::clone(store);
    tokio::task::spawn_blocking(move || snapshot_store.write_snapshot(&offsets))
        .await
        .context("join head snapshot")??;
    eprintln!(
        "phase=head_snapshot_written partition={partition} duration_ms={}",
        started.elapsed().as_millis()
    );
    Ok(())
}

fn snapshot_offset(
    segment_log: &mut SegmentLog,
    timestamp_ms: i64,
    cluster: usize,
    fatal_tx: &tokio::sync::watch::Sender<bool>,
) -> SnapshotOffset {
    if let Err(error) = segment_log.flush() {
        eprintln!("Kafka cluster {cluster} final persistence sync failed: {error:#}");
        let _ = fatal_tx.send(true);
    }
    SnapshotOffset {
        offset: segment_log.last_offset(),
        timestamp_ms,
    }
}

async fn connect_partition(
    brokers: &str,
    topic: &str,
    partition: i32,
    kafka_tls: bool,
    sasl_username: Option<&str>,
    sasl_password: Option<&str>,
    sasl_mechanism: &str,
) -> Result<Arc<PartitionClient>> {
    PartitionClient::connect(
        brokers,
        topic,
        partition,
        kafka_tls,
        sasl_username,
        sasl_password,
        sasl_mechanism,
    )
}

#[cfg(unix)]
async fn shutdown_signal() -> Result<()> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut terminate = signal(SignalKind::terminate()).context("install SIGTERM handler")?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result.context("wait for Ctrl-C"),
        _ = terminate.recv() => Ok(()),
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() -> Result<()> {
    tokio::signal::ctrl_c().await.context("wait for Ctrl-C")
}

fn ingest_and_append(
    store: &Store,
    segment_log: &mut SegmentLog,
    offset: i64,
    timestamp_ms: i64,
    tenant: &str,
    request: DecodedRequest,
    should_ingest: bool,
) -> Result<()> {
    let frame = segment_log.prepare(offset, timestamp_ms, tenant, &request)?;
    if should_ingest {
        store.ingest(tenant, request)?;
    }
    segment_log.append_prepared(frame)
}

fn empty_request() -> DecodedRequest {
    DecodedRequest {
        source: 0,
        series: Vec::new(),
        metadata: Vec::new(),
    }
}

fn has_data(request: &DecodedRequest) -> bool {
    !request.series.is_empty() || !request.metadata.is_empty()
}

fn initial_replay_complete(start: StartOffset, earliest_offset: i64, latest_offset: i64) -> bool {
    match start {
        StartOffset::Latest => true,
        StartOffset::At(offset) => offset >= latest_offset,
        StartOffset::Earliest => earliest_offset >= latest_offset,
    }
}

fn summary(request: DecodedRequest) -> Summary {
    Summary {
        source: request.source,
        series: request
            .series
            .into_iter()
            .map(|series| SeriesSummary {
                labels: series.labels,
                samples: series
                    .samples
                    .into_iter()
                    .map(|sample| (sample.timestamp_ms, sample.value))
                    .collect(),
                histogram_timestamps: series
                    .histograms
                    .into_iter()
                    .map(|histogram| histogram.timestamp)
                    .collect(),
                exemplars: series
                    .exemplars
                    .into_iter()
                    .map(|exemplar| ExemplarSummary {
                        timestamp: exemplar.timestamp_ms,
                        value: exemplar.value,
                        labels: exemplar
                            .labels
                            .into_iter()
                            .map(|pair| {
                                (
                                    String::from_utf8_lossy(&pair.name).into_owned(),
                                    String::from_utf8_lossy(&pair.value).into_owned(),
                                )
                            })
                            .collect(),
                    })
                    .collect(),
                created_timestamp: series.created_timestamp,
            })
            .collect(),
        metadata: request
            .metadata
            .into_iter()
            .map(|metadata| MetadataSummary {
                metric: metadata.metric_family_name,
                r#type: metadata.r#type,
                help: metadata.help,
                unit: metadata.unit,
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mimir_rust_kafka_ingester::proto::cortexpb;
    use mimir_rust_kafka_ingester::record::DecodedSeries;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn empty_partition_is_ready_at_earliest_offset() {
        assert!(initial_replay_complete(StartOffset::Earliest, 42, 42));
        assert!(!initial_replay_complete(StartOffset::Earliest, 41, 42));
        assert!(initial_replay_complete(StartOffset::At(42), 41, 42));
    }

    #[test]
    fn failed_preparation_does_not_checkpoint_kafka_offset() {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let data_dir = std::env::temp_dir().join(format!(
            "mimir-rust-failed-ingest-{}-{suffix}",
            std::process::id()
        ));
        let (mut log, _) = SegmentLog::open(&data_dir, 0, "topic", 0, None).unwrap();
        let store = Store::default();
        let series = DecodedSeries {
            labels: vec![("__name__".into(), "metric".into())],
            samples: vec![cortexpb::Sample {
                timestamp_ms: 1,
                value: 1.0,
            }],
            histograms: Vec::new(),
            exemplars: Vec::new(),
            created_timestamp: 0,
        };
        ingest_and_append(
            &store,
            &mut log,
            1,
            1,
            "tenant",
            DecodedRequest {
                source: 0,
                series: vec![series.clone()],
                metadata: Vec::new(),
            },
            true,
        )
        .unwrap();

        assert!(
            ingest_and_append(
                &store,
                &mut log,
                1,
                2,
                "tenant",
                DecodedRequest {
                    source: 0,
                    series: vec![series],
                    metadata: Vec::new(),
                },
                true,
            )
            .is_err()
        );
        assert_eq!(log.last_offset(), Some(1));
        drop(log);

        let (log, recovered) = SegmentLog::open(&data_dir, 0, "topic", 0, None).unwrap();
        assert_eq!(log.last_offset(), Some(1));
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].offset, 1);
        drop(log);
        std::fs::remove_dir_all(data_dir).unwrap();
    }
}
