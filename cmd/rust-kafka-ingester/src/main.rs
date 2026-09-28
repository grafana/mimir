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
use mimir_rust_kafka_ingester::segment::{CompressedFrame, SegmentLog};
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
    /// With no persisted offset, start at the first Kafka record at most this many seconds old
    /// instead of --start-offset.
    #[arg(long)]
    bootstrap_lookback_seconds: Option<i64>,
    /// Threads that apply records to store shards in parallel; defaults to the available CPUs.
    #[arg(long)]
    ingest_threads: Option<usize>,
    /// Store shards; a restored head snapshot keeps the shard count it was written with.
    #[arg(long, default_value_t = mimir_rust_kafka_ingester::store::DEFAULT_SHARDS)]
    store_shards: usize,
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
        bootstrap_lookback_seconds,
        ingest_threads,
        store_shards,
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
    // Installed before the snapshot restore: an unhandled SIGTERM would kill the process after the
    // one-use snapshot is consumed and force a full segment replay on the next start.
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
    let ingest_threads = ingest_threads
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(1, |threads| threads.get()));
    eprintln!(
        "phase=store_config partition={partition} ingest_threads={ingest_threads} store_shards={store_shards}"
    );
    let (store, mut resumed) = match open_store(
        &data_dir,
        &chunk_dir,
        &sources,
        partition,
        StoreConfig {
            active_window_ms,
            retention_ms,
            shards: store_shards,
            threads: ingest_threads,
        },
        &shutdown_requested,
    )? {
        StartupStore::Resumed { store, logs } => (store, Some(logs.into_iter())),
        StartupStore::Rebuild(store) => (store, None),
        StartupStore::Stopped => return Ok(()),
    };
    let store = Arc::new(store);
    let consistency = Arc::new(Consistency::new(
        partition,
        read_compartment,
        sources.len(),
        Duration::from_secs(consistency_timeout_seconds),
    ));
    let (fatal_tx, mut fatal_rx) = tokio::sync::watch::channel(false);
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
        let segment_log = match recovered_log {
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
        let source_start_offset = match (persisted_offset, bootstrap_lookback_seconds) {
            (Some(offset), _) => StartOffset::At(offset.saturating_add(1)),
            (None, Some(lookback)) => {
                let since = now_ms().saturating_sub(lookback.saturating_mul(1000));
                StartOffset::At(
                    partition_client
                        .offset_for_time(since)
                        .await?
                        .unwrap_or(latest_offset),
                )
            }
            (None, None) => configured_start_offset,
        };
        let start_offset_label = match source_start_offset {
            StartOffset::Earliest => "earliest".to_owned(),
            StartOffset::Latest => "latest".to_owned(),
            StartOffset::At(offset) => offset.to_string(),
        };
        // A fresh start or a gap past Kafka retention restarts the complete-data window at the first
        // consumed record; an older window without a record is replaced conservatively by now.
        let coverage_path = data_dir.join("coverage");
        let coverage_pending =
            persisted_offset.is_none_or(|offset| offset.saturating_add(1) < earliest_offset);
        if !coverage_pending && !coverage_path.exists() {
            raise_coverage(&coverage_path, now_ms())?;
        }
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
            // Decoding and frame compression stay on this task while a dedicated thread applies
            // records to the store and log in order, so replay uses two cores.
            let (apply_tx, applier) = spawn_applier(Applier {
                store: Arc::clone(&ingest_store),
                consistency: Arc::clone(&ingest_consistency),
                segment_log,
                cluster,
                fatal_tx: fatal_tx.clone(),
                coverage_path,
                coverage_pending,
                last_timestamp_ms: recovered_timestamp,
            });
            let mut stopped = false;
            if replay_complete {
                eprintln!(
                    "phase=kafka_replay_complete cluster={cluster} partition={partition} latest_offset={latest_offset}"
                );
                let _ = ready_tx.take().expect("replay signal available").send(());
                tokio::select! {
                    _ = warmup_rx.changed() => {}
                    _ = shutdown_rx.changed() => stopped = true,
                }
            }
            while !stopped {
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
                        if apply_tx.send(Apply::Maintain).await.is_err() {
                            break;
                        }
                        if next_offset > initial_offset {
                            if let Err(error) = partition_client.commit(next_offset) {
                                eprintln!("phase=kafka_commit_failed cluster={cluster} partition={partition} error={error:#}");
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
                let frame = match SegmentLog::frame(message.offset, timestamp_ms, &tenant, &request)
                    .and_then(|frame| frame.compress())
                {
                    Ok(frame) => frame,
                    Err(error) => {
                        eprintln!(
                            "Kafka cluster {cluster} offset {} failed: {error:#}",
                            message.offset
                        );
                        let _ = fatal_tx.send(true);
                        break;
                    }
                };
                let command = Apply::Record {
                    offset: message.offset,
                    timestamp_ms,
                    high_watermark,
                    tenant,
                    request: should_ingest.then_some(request),
                    frame,
                };
                if apply_tx.send(command).await.is_err() {
                    break;
                }
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
            drop(apply_tx);
            tokio::task::spawn_blocking(move || applier.join())
                .await
                .expect("join applier task")
                .expect("applier thread panicked")
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

enum StartupStore {
    /// Restored from the head snapshot, with each cluster's log opened at its checkpoint.
    Resumed {
        store: Store,
        logs: Vec<(SegmentLog, SnapshotOffset)>,
    },
    /// Empty store to rebuild from the segment logs.
    Rebuild(Store),
    /// Shutdown arrived during the restore; the snapshot was written back.
    Stopped,
}

#[derive(Clone, Copy)]
struct StoreConfig {
    active_window_ms: i64,
    retention_ms: Option<i64>,
    shards: usize,
    threads: usize,
}

fn open_store(
    data_dir: &std::path::Path,
    chunk_dir: &std::path::Path,
    sources: &[(String, String)],
    partition: i32,
    config: StoreConfig,
    shutdown_requested: &AtomicBool,
) -> Result<StartupStore> {
    let StoreConfig {
        active_window_ms,
        retention_ms,
        shards,
        threads,
    } = config;
    let started = Instant::now();
    let rebuild = || -> Result<StartupStore> {
        Ok(StartupStore::Rebuild(Store::with_shards(
            active_window_ms,
            retention_ms,
            Some(chunk_dir.to_path_buf()),
            shards,
            threads,
        )?))
    };
    let Some(restored) = Store::restore(active_window_ms, retention_ms, chunk_dir, threads)? else {
        return rebuild();
    };
    if restored.offsets.len() != sources.len() {
        eprintln!("phase=head_snapshot_stale partition={partition} reason=kafka_cluster_count");
        return rebuild();
    }
    let logs = sources
        .iter()
        .enumerate()
        .map(|(cluster, (topic, _))| {
            SegmentLog::open_at_checkpoint(
                data_dir,
                cluster,
                topic,
                partition,
                retention_ms,
                restored.offsets[cluster].offset,
            )
        })
        .collect::<Result<Option<Vec<_>>>>()?;
    let Some(logs) = logs else {
        eprintln!(
            "phase=head_snapshot_stale partition={partition} reason=segment_checkpoint_mismatch"
        );
        return rebuild();
    };
    eprintln!(
        "phase=head_snapshot_restored partition={partition} duration_ms={}",
        started.elapsed().as_millis()
    );
    // The snapshot is consumed on read, so a shutdown before ingestion starts must write it back.
    if shutdown_requested.load(Ordering::Relaxed) {
        restored.store.write_snapshot(&restored.offsets)?;
        eprintln!(
            "phase=head_snapshot_written partition={partition} reason=shutdown_during_restore"
        );
        return Ok(StartupStore::Stopped);
    }
    Ok(StartupStore::Resumed {
        store: restored.store,
        logs: logs.into_iter().zip(restored.offsets).collect(),
    })
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

// Rejects a duplicate or old offset before touching the store, so the store never holds data the
// segment log does not.
fn ingest_and_append(
    store: &Store,
    segment_log: &mut SegmentLog,
    tenant: &str,
    request: Option<DecodedRequest>,
    frame: CompressedFrame,
) -> Result<()> {
    segment_log.check_offset(frame.offset())?;
    if let Some(request) = request {
        store.ingest(tenant, request)?;
    }
    segment_log.append_compressed(frame)
}

enum Apply {
    Record {
        offset: i64,
        timestamp_ms: i64,
        high_watermark: i64,
        tenant: String,
        request: Option<DecodedRequest>,
        frame: CompressedFrame,
    },
    Maintain,
}

struct Applier {
    store: Arc<Store>,
    consistency: Arc<Consistency>,
    segment_log: SegmentLog,
    cluster: usize,
    fatal_tx: tokio::sync::watch::Sender<bool>,
    coverage_path: PathBuf,
    coverage_pending: bool,
    last_timestamp_ms: i64,
}

impl Applier {
    fn apply(&mut self, command: Apply) -> Result<()> {
        match command {
            Apply::Record {
                offset,
                timestamp_ms,
                high_watermark,
                tenant,
                request,
                frame,
            } => {
                ingest_and_append(&self.store, &mut self.segment_log, &tenant, request, frame)
                    .with_context(|| format!("apply offset {offset}"))?;
                self.consistency
                    .consumed(self.cluster, offset, high_watermark, timestamp_ms);
                self.last_timestamp_ms = self.last_timestamp_ms.max(timestamp_ms);
                if self.coverage_pending {
                    raise_coverage(&self.coverage_path, timestamp_ms)?;
                    self.coverage_pending = false;
                }
            }
            Apply::Maintain => {
                self.segment_log.maintain()?;
                if self.cluster == 0 {
                    self.store.prune_expired()?;
                }
            }
        }
        Ok(())
    }
}

fn spawn_applier(
    mut applier: Applier,
) -> (
    tokio::sync::mpsc::Sender<Apply>,
    std::thread::JoinHandle<SnapshotOffset>,
) {
    let (apply_tx, mut apply_rx) = tokio::sync::mpsc::channel(64);
    let handle = std::thread::Builder::new()
        .name(format!("apply-cluster-{}", applier.cluster))
        .spawn(move || {
            while let Some(command) = apply_rx.blocking_recv() {
                if let Err(error) = applier.apply(command) {
                    eprintln!("Kafka cluster {} failed: {error:#}", applier.cluster);
                    let _ = applier.fatal_tx.send(true);
                    break;
                }
            }
            snapshot_offset(
                &mut applier.segment_log,
                applier.last_timestamp_ms,
                applier.cluster,
                &applier.fatal_tx,
            )
        })
        .expect("spawn applier thread");
    (apply_tx, handle)
}

/// Records, in Unix milliseconds, the time since which this ingester holds complete data. It only
/// ever moves forward so a gap is never hidden by an older value.
fn raise_coverage(path: &std::path::Path, since_ms: i64) -> Result<()> {
    let current = std::fs::read_to_string(path)
        .ok()
        .and_then(|contents| contents.trim().parse::<i64>().ok());
    if current.is_some_and(|current| current >= since_ms) {
        return Ok(());
    }
    let temporary = path.with_extension("tmp");
    std::fs::write(&temporary, since_ms.to_string())
        .with_context(|| format!("write coverage {}", temporary.display()))?;
    std::fs::rename(&temporary, path).with_context(|| format!("record coverage {}", path.display()))
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() as i64)
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
    fn coverage_only_moves_forward() {
        let path = std::env::temp_dir().join(format!("mimir-rust-coverage-{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        raise_coverage(&path, 100).unwrap();
        raise_coverage(&path, 50).unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "100");
        raise_coverage(&path, 200).unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "200");
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn empty_partition_is_ready_at_earliest_offset() {
        assert!(initial_replay_complete(StartOffset::Earliest, 42, 42));
        assert!(!initial_replay_complete(StartOffset::Earliest, 41, 42));
        assert!(initial_replay_complete(StartOffset::At(42), 41, 42));
    }

    #[test]
    fn shutdown_during_restore_keeps_the_head_snapshot() {
        let data_dir = std::env::temp_dir().join(format!(
            "mimir-rust-restore-shutdown-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let chunk_dir = data_dir.join("chunks_head");
        let sources = vec![("topic".to_owned(), "broker".to_owned())];
        let (mut log, _) = SegmentLog::open(&data_dir, 0, "topic", 0, None).unwrap();
        let request = DecodedRequest {
            source: 0,
            series: vec![DecodedSeries {
                labels: vec![("__name__".into(), "metric".into())],
                samples: vec![cortexpb::Sample {
                    timestamp_ms: 1,
                    value: 1.0,
                }],
                histograms: Vec::new(),
                exemplars: Vec::new(),
                created_timestamp: 0,
            }],
            metadata: Vec::new(),
        };
        let store = Store::with_shards(1000, None, Some(chunk_dir.clone()), 4, 2).unwrap();
        let frame = SegmentLog::frame(5, 1, "tenant", &request)
            .unwrap()
            .compress()
            .unwrap();
        ingest_and_append(&store, &mut log, "tenant", Some(request), frame).unwrap();
        log.flush().unwrap();
        drop(log);
        let offsets = [SnapshotOffset {
            offset: Some(5),
            timestamp_ms: 1,
        }];
        store.write_snapshot(&offsets).unwrap();
        drop(store);

        let open = |shutdown: bool| {
            open_store(
                &data_dir,
                &chunk_dir,
                &sources,
                0,
                StoreConfig {
                    active_window_ms: 1000,
                    retention_ms: None,
                    shards: 4,
                    threads: 2,
                },
                &AtomicBool::new(shutdown),
            )
            .unwrap()
        };
        assert!(matches!(open(true), StartupStore::Stopped));
        assert!(chunk_dir.join("snapshot").exists());
        match open(false) {
            StartupStore::Resumed { store, logs } => {
                assert_eq!(store.num_series("tenant"), 1);
                assert_eq!(logs[0].0.last_offset(), Some(5));
                assert_eq!(logs[0].1, offsets[0]);
            }
            _ => panic!("expected the snapshot to resume"),
        }
        assert!(matches!(open(false), StartupStore::Rebuild(_)));
        std::fs::remove_dir_all(data_dir).unwrap();
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
        let request = |series: DecodedSeries| DecodedRequest {
            source: 0,
            series: vec![series],
            metadata: Vec::new(),
        };
        let frame = |offset, request: &DecodedRequest| {
            SegmentLog::frame(offset, offset, "tenant", request)
                .unwrap()
                .compress()
                .unwrap()
        };
        let first = request(series.clone());
        ingest_and_append(
            &store,
            &mut log,
            "tenant",
            Some(first.clone()),
            frame(1, &first),
        )
        .unwrap();

        let duplicate = request(series);
        assert!(
            ingest_and_append(
                &store,
                &mut log,
                "tenant",
                Some(duplicate.clone()),
                frame(1, &duplicate)
            )
            .is_err()
        );
        assert_eq!(store.num_series("tenant"), 1);
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
