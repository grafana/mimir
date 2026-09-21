# Nautilus / readcache dev stack

End-to-end docker-compose stack for the experimental readcache /
nautilus rebalancer pipeline. The plan that scopes this is at
`/Users/davidgrant/.cursor/plans/readcache_phase_2_7a953324.plan.md`
in the editor; the canonical contract this stack exercises is:

- writes for two Nautilus tenants land on the `nautilus_ingest` Kafka
  topic instead of `mimir-ingest`;
- the `readcache-1` and `readcache-2` pods consume from
  `nautilus_ingest` (no ingester involvement);
- reads for each Nautilus tenant are routed to the readcache pod
  currently owning the queried partition;
- each tenant receives an independent full uint32 hash-space assignment
  that the rebalancer spreads across physical Kafka partitions.

## Topology

| service               | role                                          |
| --------------------- | --------------------------------------------- |
| `distributor-1`       | accepts writes, routes per tenant             |
| `ingester-1`          | vanilla ingester for the production tenant    |
| `readcache-1`         | consumes partitions assigned by the rebalancer |
| `readcache-2`         | consumes partitions assigned by the rebalancer |
| `nautilus-rebalancer` | rebalances readcache ownership, persists logs |
| `block-builder-*`     | converts Kafka segments to TSDB blocks        |
| `query-frontend`      | PromQL entrypoint                             |
| `querier`             | scatter/gather over ingesters + store-gateway |
| `store-gateway`       | reads compacted blocks from MinIO             |
| `compactor`           | compacts blocks                               |
| `kafka_{1,2,3}`       | KRaft Kafka cluster                           |
| `minio`               | S3-compatible blocks store                    |
| `grafana`             | UI on http://localhost:3000                   |
| `tempo`               | OTel traces                                   |
| `redpanda_console`    | Kafka UI on http://localhost:8090             |

Stateful services use named volumes, so ordinary container restarts retain
their state while `compose-down.sh` (`docker compose down -v`) provides a
genuinely clean bootstrap environment.

## Iteration loop (for the agent)

```sh
# First-time setup.
./compose-up.sh -d

# Iterate on Go code: rebuild and bounce only the Mimir containers.
./compose-update-mimir.sh
./verify.sh

# On failure, inspect:
docker compose -f docker-compose.yml logs --tail=200 readcache-1
docker compose -f docker-compose.yml logs --tail=200 nautilus-rebalancer
docker volume inspect mimir-nautilus_nautilus-rebalancer-data
```

`verify.sh` prints `PASS:` / `FAIL:` lines so the agent can grep:

```
PASS: distributor is ready (http://localhost:8000)
PASS: nautilus-tenant query returned the expected sample
PASS: nautilus-tenant-b query returned the expected sample
PASS: both nautilus tenants have valid multi-partition assignments
PASS: post-rebalance writes remained tenant-isolated
PASS: default-tenant query returned the expected sample
PASS: all verify steps succeeded
```

The smoke test pushes tenant-distinct samples through two Nautilus tenants,
drives both tenants with multi-series load, and reads the rebalancer's durable
assignment log to assert:

- `nautilus-tenant` and `nautilus-tenant-b` (per `config/runtime.yaml`,
  `nautilus_ingest_routing=nautilus-only` +
  `readcache_read_routing=nautilus-only`) flows entirely through
  readcache; the ingester never sees the data;
- each tenant independently tiles `[0, MaxUint32]` and uses at least two
  physical Kafka partitions after rebalancing;
- writes issued after that rebalance remain queryable and tenant-isolated.
- `default-tenant` (both routing knobs `disabled`) flows through the
  production ingester path.

## Per-tenant runtime config

`config/runtime.yaml` configures the two routing knobs in lockstep.
Flipping only one yields a half-broken tenant whose writes land on
one topic while reads ask the other; the limits validator rejects
this misalignment, so editing one alone will reject the runtime
config on reload.

## Caveats / known follow-ups

- Readcache takes a per-partition TSDB mutex around Kafka-driven appends,
  `ApplyConfig`, and head compaction so parallel ingest (`ingest_storage.kafka.ingestion-concurrency-max` > 0) cannot race the Prometheus head the way the ingester avoids with `acquireAppendLock`.
- Verification waits up to three minutes for bootstrap discovery and a
  subsequent load-bearing slicer round. Override this with
  `REBALANCE_RETRY_BUDGET` on slower machines.
- Tempo trace assertions are intentionally omitted; the dev stack is
  hermetic enough without depending on Tempo's reliability for the
  smoke test.
