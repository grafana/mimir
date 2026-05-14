# Nautilus / readcache dev stack

End-to-end docker-compose stack for the experimental readcache /
nautilus rebalancer pipeline. The plan that scopes this is at
`/Users/davidgrant/.cursor/plans/readcache_phase_2_7a953324.plan.md`
in the editor; the canonical contract this stack exercises is:

- writes for `nautilus-tenant` land on the `nautilus_ingest` Kafka
  topic instead of `mimir-ingest`;
- the `readcache-1` and `readcache-2` pods consume from
  `nautilus_ingest` (no ingester involvement);
- reads for `nautilus-tenant` are routed to the readcache pod
  currently owning the queried partition;
- the same end-to-end path works *without* the rebalancer running
  long-running rebalances; static partition ownership is configured
  via `-readcache.owned-partitions=`.

## Topology

| service              | role                                          |
| -------------------- | --------------------------------------------- |
| `distributor-1`      | accepts writes, routes per tenant             |
| `ingester-1`         | vanilla ingester for the production tenant    |
| `readcache-1`        | owns partitions `0..3`                        |
| `readcache-2`        | owns partitions `4..7`                        |
| `nautilus-rebalancer`| rebalances readcache ownership, persists logs |
| `block-builder-*`    | converts Kafka segments to TSDB blocks        |
| `query-frontend`     | PromQL entrypoint                             |
| `querier`            | scatter/gather over ingesters + store-gateway |
| `store-gateway`      | reads compacted blocks from MinIO             |
| `compactor`          | compacts blocks                               |
| `kafka_{1,2,3}`      | KRaft Kafka cluster                           |
| `minio`              | S3-compatible blocks store                    |
| `grafana`            | UI on http://localhost:3000                   |
| `tempo`              | OTel traces                                   |
| `redpanda_console`   | Kafka UI on http://localhost:8090             |

The rebalancer mounts a named volume at `/data/nautilus-rebalancer`
so its assignment logs survive container restarts; without that the
stack would re-bootstrap every routing decision on each Mimir update.

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
PASS: default-tenant query returned the expected sample
PASS: all verify steps succeeded
```

The smoke test pushes a single sample to each tenant and reads it
back through the query-frontend, asserting:

- `nautilus-tenant` (per `config/runtime.yaml`,
  `nautilus_ingest_routing=nautilus-only` +
  `readcache_read_routing=nautilus-only`) flows entirely through
  readcache; the ingester never sees the data.
- `default-tenant` (both routing knobs `disabled`) flows through the
  production ingester path.

## Per-tenant runtime config

`config/runtime.yaml` configures the two routing knobs in lockstep.
Flipping only one yields a half-broken tenant whose writes land on
one topic while reads ask the other; the limits validator rejects
this misalignment, so editing one alone will reject the runtime
config on reload.

## Caveats / known follow-ups

- The static `-readcache.owned-partitions` flag means the
  rebalancer's actual slicer round is exercised only after Phase 2D
  wires the readcache ring lifecycler. Until then, the rebalancer's
  logs persist what it _would_ assign, but ownership is fixed.
- `verify.sh` uses Python's `prometheus_client` + `python-snappy` to
  build the remote-write payload. If your environment lacks them,
  `pip install prometheus_client python-snappy` first.
- Tempo trace assertions are intentionally omitted; the dev stack is
  hermetic enough without depending on Tempo's reliability for the
  smoke test.
