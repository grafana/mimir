// SPDX-License-Identifier: AGPL-3.0-only

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, ensure};
use axum::http::{Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router, routing::any};
use clap::Args;
use prometheus::IntGauge;
use tokio::sync::watch;

use crate::limits::{Overrides, parse_duration_ms};
use crate::ring_snapshot::{
    self, INSTANCE_ACTIVE, INSTANCE_LEFT, Instance, OWNER_ACTIVE, OWNER_DELETED, Owner,
    PARTITION_ACTIVE, PARTITION_INACTIVE, PARTITION_PENDING, Partition, RingSnapshot,
};
use crate::ring_tokens::{DerivedTokens, PartitionTokens};
use crate::store::Store;

const HEARTBEAT_PERIOD: Duration = Duration::from_secs(5);
const MAX_HEARTBEAT_AGE: i64 = 60;
const SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(8);

#[derive(Args, Clone)]
pub struct NativeRingArgs {
    /// Go memberlist peers, each as HOST:PORT. Enables the shared-ring client.
    #[arg(long = "ring-peer", value_delimiter = ',')]
    peers: Vec<String>,
    #[arg(long = "ring-cluster-label", requires = "peers")]
    label: Option<String>,
    #[arg(long = "ring-instance-id", requires = "peers")]
    instance_id: Option<String>,
    #[arg(long = "ring-advertise-address", requires = "peers")]
    advertise_address: Option<SocketAddr>,
    #[arg(long = "ring-zone", requires = "peers")]
    zone: Option<String>,
    /// KV key used by the Go ingesters' instance ring.
    #[arg(long = "ring-instance-key", default_value = "partition-ingesters/ring")]
    instance_key: String,
    /// KV key used by the Go ingesters' Kafka partition ring.
    #[arg(long = "ring-partition-key", default_value = "ingester-partitions")]
    partition_key: String,
    #[arg(long = "ring-min-coverage", default_value = "0s")]
    min_coverage: String,
    #[arg(long = "ingester.ring.unregister-on-shutdown", default_value_t = true, action = clap::ArgAction::Set)]
    unregister_on_shutdown: bool,
}

pub struct NativeRing {
    peers: Vec<String>,
    label: String,
    id: String,
    addr: String,
    zone: String,
    instance_key: String,
    partition_key: String,
    partition: i32,
    coverage: PathBuf,
    minimum_coverage_ms: i64,
    marker: PathBuf,
    unregister_on_shutdown: bool,
    prepared: AtomicBool,
    status: RwLock<Status>,
    ready_metric: IntGauge,
    prepared_metric: IntGauge,
}

#[derive(Default)]
struct Status {
    ready: bool,
    partition: Option<Partition>,
}

impl NativeRing {
    pub fn new(args: NativeRingArgs, partition: i32, data_dir: &Path) -> Result<Option<Arc<Self>>> {
        if args.peers.is_empty() {
            return Ok(None);
        }
        let label = args
            .label
            .filter(|s| !s.is_empty())
            .context("native ring requires --ring-cluster-label")?;
        ensure!(label.len() <= 255, "memberlist label is too long");
        let id = args
            .instance_id
            .filter(|s| !s.is_empty())
            .context("native ring requires --ring-instance-id")?;
        let addr = args
            .advertise_address
            .context("native ring requires --ring-advertise-address")?;
        ensure!(
            !addr.ip().is_unspecified() && addr.port() != 0,
            "ring advertise address must be routable"
        );
        let zone = args
            .zone
            .filter(|s| !s.is_empty())
            .context("native ring requires --ring-zone")?;
        ensure!(
            partition >= 0,
            "native ring requires a non-negative partition ID"
        );
        ensure!(
            !args.instance_key.is_empty()
                && !args.partition_key.is_empty()
                && args.instance_key != args.partition_key,
            "native ring requires different non-empty ring keys"
        );
        let marker = data_dir.join("shutdown-requested.txt");
        let prepared = match marker.metadata() {
            Ok(metadata) => metadata.is_file(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
            Err(error) => return Err(error).context("read ring shutdown marker"),
        };
        let minimum_coverage_ms = parse_duration_ms(&args.min_coverage)?;
        ensure!(
            minimum_coverage_ms >= 0,
            "ring minimum coverage must be non-negative"
        );
        let ready_metric = IntGauge::new(
            "rust_ingester_ring_ready",
            "Whether the Rust ingester is visible in both rings and serving reads.",
        )?;
        let prepared_metric = IntGauge::new(
            "cortex_ingester_prepare_shutdown_requested",
            "Whether the ingester is prepared for shutdown.",
        )?;
        prepared_metric.set(i64::from(prepared));
        Ok(Some(Arc::new(Self {
            peers: args.peers,
            label,
            id,
            addr: addr.to_string(),
            zone,
            instance_key: args.instance_key,
            partition_key: args.partition_key,
            partition,
            coverage: data_dir.join("coverage"),
            minimum_coverage_ms,
            marker,
            unregister_on_shutdown: args.unregister_on_shutdown,
            prepared: AtomicBool::new(prepared),
            status: RwLock::new(Status::default()),
            ready_metric,
            prepared_metric,
        })))
    }

    pub fn register_metrics(&self, registry: &prometheus::Registry) -> Result<()> {
        registry.register(Box::new(self.ready_metric.clone()))?;
        registry.register(Box::new(self.prepared_metric.clone()))?;
        Ok(())
    }

    pub fn routes(self: &Arc<Self>) -> Router {
        Router::new()
            .route("/ready", any(ready))
            .route("/ingester/prepare-shutdown", any(prepare_shutdown))
            .route(
                "/ingester/prepare-partition-downscale",
                any(prepare_downscale),
            )
            .with_state(self.clone())
    }

    pub async fn wait_for_partition(
        self: &Arc<Self>,
        overrides: &Overrides,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<bool> {
        let wait = async {
            loop {
                match self.snapshot(None).await {
                    Ok((_, snapshot)) => {
                        overrides.set_active_partitions(active_count(&snapshot));
                        if partition(&snapshot, self.partition).is_some() {
                            return Ok::<_, anyhow::Error>(());
                        }
                    }
                    Err(error) => eprintln!("phase=ring_startup_poll_error error={error:#}"),
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        };
        tokio::select! {
            _ = shutdown.wait_for(|stopping| *stopping) => Ok(false),
            result = tokio::time::timeout(Duration::from_secs(30), wait) => {
                result.context("waiting for the shared Kafka partition in memberlist")??;
                Ok(true)
            }
        }
    }

    pub async fn run(
        self: Arc<Self>,
        store: Arc<Store>,
        serving: Arc<AtomicBool>,
        mut shutdown: watch::Receiver<bool>,
        fatal: watch::Receiver<bool>,
        owned_series: bool,
    ) {
        let mut peer = None;
        let mut registration = None;
        let mut ownership = Ownership::default();
        let mut ticker = tokio::time::interval(HEARTBEAT_PERIOD);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                _ = shutdown.wait_for(|stopping| *stopping) => break,
                _ = ticker.tick() => {}
            }
            match self.snapshot(peer).await {
                Ok((address, snapshot)) => {
                    peer = Some(address);
                    store
                        .overrides()
                        .set_active_partitions(active_count(&snapshot));
                    let owns_ranges = !owned_series
                        || match ownership.refresh(&snapshot, &store, self.partition) {
                            Ok(()) => true,
                            Err(error) => {
                                eprintln!("phase=ring_tokens_error error={error:#}");
                                false
                            }
                        };
                    if *shutdown.borrow() {
                        break;
                    }
                    let should_serve = serving.load(Ordering::Acquire) && owns_ranges;
                    if let Err(error) = self
                        .reconcile(address, &snapshot, should_serve, &mut registration)
                        .await
                    {
                        self.set_status(false, None);
                        eprintln!("phase=ring_publish_error error={error:#}");
                    }
                }
                Err(error) => {
                    self.set_status(false, None);
                    eprintln!("phase=ring_poll_error error={error:#}");
                }
            }
        }

        self.set_status(false, None);
        if let Some(registered_at) = registration {
            let remove_owner = self.prepared.load(Ordering::Acquire) || *fatal.borrow();
            let remove_instance = remove_owner || self.unregister_on_shutdown;
            if remove_instance || remove_owner {
                match self.snapshot(peer).await {
                    Ok((address, snapshot)) => {
                        if let Err(error) = self
                            .withdraw(
                                address,
                                &snapshot,
                                registered_at,
                                remove_instance,
                                remove_owner,
                            )
                            .await
                        {
                            eprintln!("phase=ring_withdraw_error error={error:#}");
                        }
                    }
                    Err(error) => eprintln!("phase=ring_withdraw_error error={error:#}"),
                }
            }
        }
    }

    async fn reconcile(
        self: &Arc<Self>,
        peer: SocketAddr,
        snapshot: &RingSnapshot,
        serving: bool,
        registration: &mut Option<i64>,
    ) -> Result<()> {
        let part = partition(snapshot, self.partition);
        let now = unix_ms();
        if !serving || !self.has_coverage(now) || part.is_none() {
            self.set_status(false, None);
            if let Some(registered_at) = *registration {
                self.withdraw(peer, snapshot, registered_at, true, true)
                    .await?;
                *registration = None;
            }
            return Ok(());
        }
        let seen_instance = snapshot.instances.get(&self.id);
        let seen_owner = snapshot.owners.get(&self.id);
        let ready = seen_instance.is_some_and(|instance| {
            instance.state == INSTANCE_ACTIVE
                && instance.addr == self.addr
                && instance.zone == self.zone
                && now / 1000 - instance.timestamp < MAX_HEARTBEAT_AGE
        }) && seen_owner.is_some_and(|owner| {
            owner.state == OWNER_ACTIVE && owner.owned_partition == self.partition
        });
        let observed_partition = if ready { part.cloned() } else { None };
        let registered_at = *registration.get_or_insert_with(|| {
            seen_instance
                .filter(|instance| instance.state != INSTANCE_LEFT)
                .map_or(now / 1000, |instance| instance.registered_timestamp)
        });
        let instance = self.instance(now / 1000, registered_at, INSTANCE_ACTIVE);
        let owner = (!seen_owner.is_some_and(|owner| {
            owner.state == OWNER_ACTIVE && owner.owned_partition == self.partition
        }))
        .then_some(Owner {
            owned_partition: self.partition,
            state: OWNER_ACTIVE,
            updated_timestamp: now / 1000,
        });
        self.publish(peer, Some(instance), owner).await?;
        self.set_status(ready, observed_partition);
        Ok(())
    }

    async fn withdraw(
        self: &Arc<Self>,
        peer: SocketAddr,
        snapshot: &RingSnapshot,
        registered_at: i64,
        remove_instance: bool,
        remove_owner: bool,
    ) -> Result<()> {
        // Equal timestamps favor tombstones in dskit. Preserve a greater observed
        // timestamp for our own address if the wall clock moved backwards.
        let now = unix_ms() / 1000;
        if snapshot
            .instances
            .get(&self.id)
            .is_some_and(|instance| instance.state == INSTANCE_ACTIVE && instance.addr != self.addr)
        {
            return Ok(());
        }
        let instance = remove_instance.then(|| {
            let timestamp = snapshot
                .instances
                .get(&self.id)
                .filter(|instance| instance.addr == self.addr && instance.timestamp <= now + 600)
                .map_or(now, |instance| now.max(instance.timestamp));
            self.instance(timestamp, registered_at, INSTANCE_LEFT)
        });
        let owner = remove_owner.then(|| Owner {
            owned_partition: self.partition,
            state: OWNER_DELETED,
            updated_timestamp: snapshot
                .owners
                .get(&self.id)
                .filter(|owner| {
                    owner.owned_partition == self.partition && owner.updated_timestamp <= now + 600
                })
                .map_or(now, |owner| now.max(owner.updated_timestamp)),
        });
        self.publish(peer, instance, owner).await
    }

    fn instance(&self, timestamp: i64, registered_timestamp: i64, state: i32) -> Instance {
        Instance {
            addr: self.addr.clone(),
            timestamp,
            state,
            zone: self.zone.clone(),
            registered_timestamp,
            id: self.id.clone(),
        }
    }

    fn has_coverage(&self, now: i64) -> bool {
        self.minimum_coverage_ms == 0
            || std::fs::read_to_string(&self.coverage)
                .ok()
                .and_then(|text| text.trim().parse::<i64>().ok())
                .is_some_and(|since| now.saturating_sub(since) >= self.minimum_coverage_ms)
    }

    fn set_status(&self, ready: bool, partition: Option<Partition>) {
        *self.status.write().unwrap() = Status { ready, partition };
        self.ready_metric.set(i64::from(ready));
    }

    async fn snapshot(
        self: &Arc<Self>,
        previous: Option<SocketAddr>,
    ) -> Result<(SocketAddr, RingSnapshot)> {
        tokio::time::timeout(SNAPSHOT_TIMEOUT, self.try_peers(previous))
            .await
            .context("timed out fetching shared ring snapshot")?
    }

    async fn try_peers(
        self: &Arc<Self>,
        previous: Option<SocketAddr>,
    ) -> Result<(SocketAddr, RingSnapshot)> {
        if let Some(address) = previous
            && let Ok(snapshot) = self.fetch(address).await
        {
            return Ok((address, snapshot));
        }
        let mut last_error = None;
        for name in &self.peers {
            let addresses =
                match tokio::time::timeout(Duration::from_secs(2), tokio::net::lookup_host(name))
                    .await
                {
                    Ok(Ok(addresses)) => addresses,
                    Ok(Err(error)) => {
                        last_error = Some(
                            anyhow::Error::new(error)
                                .context(format!("resolve memberlist peer {name}")),
                        );
                        continue;
                    }
                    Err(error) => {
                        last_error = Some(
                            anyhow::Error::new(error)
                                .context(format!("resolve memberlist peer {name}")),
                        );
                        continue;
                    }
                };
            for address in addresses.filter(|address| Some(*address) != previous) {
                match self.fetch(address).await {
                    Ok(snapshot) => return Ok((address, snapshot)),
                    Err(error) => last_error = Some(error),
                }
            }
        }
        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("no reachable Go memberlist peer")))
    }

    async fn fetch(self: &Arc<Self>, peer: SocketAddr) -> Result<RingSnapshot> {
        let ring = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            ring_snapshot::fetch(peer, &ring.label, &ring.instance_key, &ring.partition_key)
        })
        .await?
    }

    async fn publish(
        self: &Arc<Self>,
        peer: SocketAddr,
        instance: Option<Instance>,
        owner: Option<Owner>,
    ) -> Result<()> {
        let ring = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            ring_snapshot::publish(
                peer,
                &ring.label,
                &ring.instance_key,
                instance.as_ref(),
                &ring.partition_key,
                owner.as_ref().map(|owner| (ring.id.as_str(), owner)),
            )
        })
        .await?
    }

    fn prepare(&self) -> Result<()> {
        if self.prepared.load(Ordering::Acquire) {
            return Ok(());
        }
        // Persist the decision before returning to the rollout operator.
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&self.marker)?;
        file.sync_all()?;
        File::open(self.marker.parent().context("missing marker directory")?)?.sync_all()?;
        self.prepared.store(true, Ordering::Release);
        self.prepared_metric.set(1);
        Ok(())
    }
}

#[derive(Default)]
struct Ownership {
    derived: DerivedTokens,
    partitions: Option<HashMap<i32, Partition>>,
    tenant_shards: HashMap<String, i64>,
    tokens: Option<PartitionTokens>,
}

impl Ownership {
    fn refresh(&mut self, snapshot: &RingSnapshot, store: &Store, partition: i32) -> Result<()> {
        let changed = self.partitions.as_ref() != Some(&snapshot.partitions);
        if changed {
            self.tokens = Some(PartitionTokens::new(
                &snapshot.partitions,
                &mut self.derived,
            )?);
            self.partitions = Some(snapshot.partitions.clone());
        }
        let tenant_shards: HashMap<_, _> = store
            .tenant_ids()
            .into_iter()
            .map(|tenant| {
                let shard = store
                    .overrides()
                    .tenant(&tenant)
                    .limits
                    .ingestion_partitions_tenant_shard_size;
                (tenant, shard)
            })
            .collect();
        if changed || tenant_shards != self.tenant_shards {
            let tokens = self
                .tokens
                .as_ref()
                .context("missing partition token view")?;
            store.set_owned_ranges(
                tenant_shards
                    .iter()
                    .map(|(tenant, &shard)| {
                        (
                            tenant.clone(),
                            tokens.owned_ranges(tenant, shard, partition),
                        )
                    })
                    .collect(),
            );
            self.tenant_shards = tenant_shards;
        }
        Ok(())
    }
}

fn partition(snapshot: &RingSnapshot, id: i32) -> Option<&Partition> {
    snapshot.partitions.get(&id).filter(|p| {
        matches!(
            p.state,
            PARTITION_PENDING | PARTITION_ACTIVE | PARTITION_INACTIVE
        )
    })
}

fn active_count(snapshot: &RingSnapshot) -> u64 {
    snapshot
        .partitions
        .values()
        .filter(|p| p.state == PARTITION_ACTIVE)
        .count() as u64
}

fn unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

async fn ready(
    axum::extract::State(ring): axum::extract::State<Arc<NativeRing>>,
    method: Method,
) -> StatusCode {
    if method != Method::GET && method != Method::HEAD {
        return StatusCode::METHOD_NOT_ALLOWED;
    }
    if ring.status.read().unwrap().ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn prepare_shutdown(
    axum::extract::State(ring): axum::extract::State<Arc<NativeRing>>,
    method: Method,
) -> Response {
    match method {
        Method::GET => {
            let text = if ring.prepared.load(Ordering::Acquire) {
                "set\n"
            } else {
                "unset\n"
            };
            text.into_response()
        }
        Method::POST => match tokio::task::spawn_blocking(move || ring.prepare()).await {
            Ok(Ok(())) => StatusCode::NO_CONTENT.into_response(),
            error => {
                eprintln!("phase=ring_prepare_shutdown_error error={error:?}");
                StatusCode::INTERNAL_SERVER_ERROR.into_response()
            }
        },
        _ => StatusCode::METHOD_NOT_ALLOWED.into_response(),
    }
}

async fn prepare_downscale(
    axum::extract::State(ring): axum::extract::State<Arc<NativeRing>>,
    method: Method,
) -> Response {
    let status = ring.status.read().unwrap();
    let Some(partition) = &status.partition else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let inactive = partition.state == PARTITION_INACTIVE;
    match method {
        Method::GET => {}
        Method::POST if inactive => {}
        Method::DELETE if !inactive => {}
        Method::POST | Method::DELETE => {
            return (
                StatusCode::CONFLICT,
                "partition states in the shared ring belong to the Go ingesters",
            )
                .into_response();
        }
        _ => return StatusCode::METHOD_NOT_ALLOWED.into_response(),
    }
    let timestamp = if inactive {
        partition.state_timestamp
    } else {
        0
    };
    Json(serde_json::json!({ "timestamp": timestamp })).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use serde::Deserialize;
    use tower::ServiceExt;

    #[derive(Deserialize)]
    struct ShardCase {
        tenant: String,
        size: i64,
        ranges: Option<Vec<u32>>,
    }

    #[tokio::test]
    #[ignore = "requires the Go peers started by TestRustSharedRingLifecycleWithGoPeers"]
    async fn mixed_go_lifecycle_and_ownership() -> Result<()> {
        let peer = std::env::var("MIMIR_TEST_RING_PEER")?;
        let replica = std::env::var("MIMIR_TEST_RING_REPLICA")?;
        let data_dir = PathBuf::from(std::env::var_os("MIMIR_TEST_RING_DATA_DIR").unwrap());
        let ring = NativeRing::new(
            NativeRingArgs {
                peers: vec![peer.clone(), replica.clone()],
                label: Some("rust-shared-ring-test".into()),
                instance_id: Some("rust-ingester".into()),
                advertise_address: Some("127.0.0.1:19095".parse()?),
                zone: Some("zone-b".into()),
                instance_key: "partition-ingesters/ring".into(),
                partition_key: "ingester-partitions".into(),
                min_coverage: "1m".into(),
                unregister_on_shutdown: true,
            },
            17,
            &data_dir,
        )?
        .unwrap();
        let mut derived = DerivedTokens::default();
        let snapshot = ring.fetch(peer.parse()?).await?;
        let tokens = PartitionTokens::new(&snapshot.partitions, &mut derived)?;
        let cases: Vec<ShardCase> =
            serde_json::from_slice(&std::fs::read(data_dir.join("ranges.json"))?)?;
        for case in cases {
            assert_eq!(
                tokens.owned_ranges(&case.tenant, case.size, ring.partition),
                case.ranges,
                "shuffle shard for {} at size {}",
                case.tenant,
                case.size,
            );
        }

        let (shutdown, stop) = watch::channel(false);
        let (_fail, fatal) = watch::channel(false);
        let store = Arc::new(Store::with_shards(1000, None, None, 1, 1)?);
        assert!(
            ring.wait_for_partition(store.overrides(), stop.clone())
                .await?
        );
        let serving = Arc::new(AtomicBool::new(false));
        let service = tokio::spawn(ring.clone().run(store, serving.clone(), stop, fatal, true));
        assert_eq!(
            request(&ring, Method::GET, "/ready").await.0,
            StatusCode::SERVICE_UNAVAILABLE
        );
        serving.store(true, Ordering::Release);
        // Coverage can still be too short after replay, so the node must stay out of the rings.
        tokio::time::sleep(Duration::from_millis(150)).await;
        let before_coverage = ring.fetch(peer.parse()?).await?;
        assert!(!before_coverage.instances.contains_key(&ring.id));
        assert!(!before_coverage.owners.contains_key(&ring.id));
        std::fs::write(&ring.coverage, (unix_ms() - 120_000).to_string())?;

        let downscale = "/ingester/prepare-partition-downscale";
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                if request(&ring, Method::GET, "/ready").await.0 == StatusCode::OK {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await?;
        // The Go test switches the partition after observing the next heartbeat.
        let inactive = tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                match request(&ring, Method::POST, downscale).await {
                    (StatusCode::OK, body) => break body,
                    (StatusCode::CONFLICT, _) => {}
                    response => panic!("unexpected downscale response {response:?}"),
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await?;
        let inactive: serde_json::Value = serde_json::from_str(&inactive)?;
        assert!(
            inactive["timestamp"]
                .as_i64()
                .is_some_and(|timestamp| timestamp > 0)
        );
        assert_eq!(
            request(&ring, Method::DELETE, downscale).await.0,
            StatusCode::CONFLICT
        );

        let prepare = "/ingester/prepare-shutdown";
        assert_eq!(request(&ring, Method::GET, prepare).await.1, "unset\n");
        assert_eq!(
            request(&ring, Method::POST, prepare).await.0,
            StatusCode::NO_CONTENT
        );
        assert_eq!(request(&ring, Method::GET, prepare).await.1, "set\n");
        assert!(ring.marker.is_file());
        shutdown.send(true)?;
        service.await?;
        tokio::time::timeout(Duration::from_secs(8), async {
            let replica = replica.parse()?;
            loop {
                let snapshot = ring.fetch(replica).await?;
                if snapshot
                    .instances
                    .get(&ring.id)
                    .is_some_and(|instance| instance.state == INSTANCE_LEFT)
                    && snapshot
                        .owners
                        .get(&ring.id)
                        .is_some_and(|owner| owner.state == OWNER_DELETED)
                {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await??;
        Ok(())
    }

    async fn request(ring: &Arc<NativeRing>, method: Method, uri: &str) -> (StatusCode, String) {
        let response = ring
            .routes()
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .method(method)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        (
            status,
            String::from_utf8(to_bytes(response.into_body(), 4096).await.unwrap().to_vec())
                .unwrap(),
        )
    }
}
