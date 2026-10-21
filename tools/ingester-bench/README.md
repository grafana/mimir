# ingester-bench

`ingester-bench` runs two ingesters, A and B, side by side on the same synthetic data and the same reads. It tells you
whether B is cheaper or faster than A, and whether it still answers the same. A and B can be any two Mimir builds or
images: two releases, a release and your working tree, or the same build with different settings.

## How a run goes

1. **Backfill.** The bench generates production-shaped tenants and series from a seed and writes `-backfill` worth of
   history to an in-process Kafka, as distributors would.
2. **Replay.** Both ingesters start, consume the partition from the start and replay the backfill, as after a restart.
   The time each takes to become ready is its replay time.
3. **Warmup.** The bench keeps producing every second and sends both ingesters the same reads at the profile's rates:
   `QueryStream`, `LabelNames`, `LabelValues`, `LabelNamesAndValues`, `LabelValuesCardinality`,
   `MetricsForLabelMatchers`, `ActiveSeries`, `QueryExemplars`, `UserStats`. The default 21m is past the active series
   idle timeout, so the active series are as they'd be after hours of ingestion.
4. **Window.** For `-duration`, the bench measures both ingesters' CPU, GC, allocations, heap, RSS, ingest lag and read
   latency, and takes CPU, heap and allocation profiles.
5. **Retries.** Answers that differ are retried twice, at least 90s apart, with read-after-write consistency. Only a
   difference that persists the same way counts as a mismatch, since late samples and active series updates settle in
   the meantime.

The data is synthetic: tenants `tenant-NNN`, metrics `metric_N`, labels `lN`. Nothing comes from real tenants.

## Run it locally

```sh
go run ./tools/ingester-bench -series=300000 -backfill=1h -warmup=5m -duration=10m -out=/tmp/bench
open /tmp/bench/report.html
```

Without a configuration file, both ingesters run a Mimir binary built from the working tree, with the same flags. Give
them different binaries or flags with `-config` (see below).

Both ingesters share your machine's CPUs, so CPU numbers are noisy locally; comparing answers works as well as anywhere.
300k series with a 1h backfill fit in a laptop's memory.

## Run it in Kubernetes

```sh
IMAGE_REPO=registry.example.com/me/ingester-bench KUBE_CONTEXT=my-cluster NAMESPACE=my-namespace \
  RESULTS=~/ingester-bench-results CONFIG=my-comparison.yaml \
  tools/ingester-bench/run-in-kubernetes.sh -backfill=13h
```

The script:

1. Merges the configuration files and fails right away if they're invalid.
2. Builds an image of the bench and of Mimir from the working tree, and pushes it to `IMAGE_REPO`.
3. Runs [`job.yaml`](job.yaml): one pod with the driver (generator, Kafka and reads) and an ingester container for each
   of A and B, each in its configured image, or the bench's image when it has none.
4. Waits for the report, adds it to `RESULTS` and deletes the job.

Other settings:

- `NODE_SELECTOR` (`key=value`) places the pod.
- `STORAGE_CLASS` sets the class of the volumes for Kafka's log and the ingesters' data. The default is the cluster's.
- `PROFILE` is a profile file, put in a ConfigMap for the run.
- Arguments after the script are bench flags, such as `-backfill=13h` or `-series=500000`.

The pod is sized as a cell's ingesters:

- The driver has 8 dedicated CPUs and 32Gi of memory.
- Each ingester requests 4 CPUs and 24Gi, has a 32Gi memory limit and runs with `GOMAXPROCS=9`.
- [`kubernetes.yaml`](kubernetes.yaml) sets the ingesters' read path utilization limits to match.

For CPU numbers you can trust, run it on nodes whose kubelet has the static CPU manager policy.

A run takes the backfill, the replay, the warmup and the window: about 1h20m with a 13h backfill and the defaults.

## Configuration

`-config` takes YAML files, comma separated. Each applies over [`defaults.yaml`](defaults.yaml) and the files before it.

```yaml
common: # Both ingesters.
  args:
    blocks-storage.tsdb.head-compaction-interval: 5m
a: # Ingester A, over common.
  name: release # In the report.
  image: grafana/mimir:3.0.0 # With run-in-kubernetes.sh.
  binary: /path/to/mimir # When the bench starts the ingester itself.
  args:
    ingester.error-sample-rate: null
b: # Ingester B, likewise.
  name: tree
  args:
    some-new.flag: "true"
```

- `args` are Mimir flags, without their dash, and their values. A value replaces the flag's, from the defaults or a
  lower layer. `null` removes the flag, for example one an older release doesn't have.
- Without a `binary`, a local run builds Mimir from the working tree. In Kubernetes, the binary is `/bin/mimir` of the
  ingester's image.
- An `image` only applies with `run-in-kubernetes.sh`. A local run needs a `binary` instead.
- Unknown fields are errors, so typos don't go unnoticed.

`defaults.yaml` holds the flags of a multi-tenant cell's ingesters: postings caches, index lookup planning, timely and
early head compactions, owned series, circuit breakers, cost attribution. The bench itself sets the flags it needs:
ports, ring, Kafka, data directories, retention, out-of-order window and the tenants' runtime configuration.
[`example.yaml`](example.yaml) compares a release's image with the working tree's build.

`-print-config` prints the merged configuration as JSON, to check what a run would use.

## The report

Each run's directory (`-out`) has:

- `report.html`, with:
  - B/A summary tiles;
  - CPU, memory, ingest and read timelines over the phases;
  - resources and read latency (p50, p99) in the window;
  - per-API counts of reads sent, compared, mismatched, matched after retry and failed;
  - the slowest reads, mismatch examples and the run's settings.
- `report.json`, the same data.
- `cpu-{a,b}.pb.gz`, `heap-{a,b}.pb.gz` and `allocs-{a,b}.pb.gz`, for `go tool pprof`.
- `slow.log`, the reads slower than 5s.

`-results=<dir>` keeps every run in a directory with an `index.html` of B against A over runs. Runs of the same setup
(series, tenants, backfill, profile, query scale, ingester names) compare with each other. `-add-result=report.json`
adds a report from elsewhere, and `-render=report.json` writes its `report.html` again.

## Other flags

| Flag                   | Default       | What it does                                                                                                                                                      |
| ---------------------- | ------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `-series`              | 300000        | Series of the ingester, over all tenants.                                                                                                                         |
| `-tenants`             | the profile's | Tenants of the ingester.                                                                                                                                          |
| `-seed`                | 1             | Seed of the synthetic data and reads.                                                                                                                             |
| `-backfill`            | 4h            | History the ingesters replay. A retention's worth, `13h`, adds the cold blocks only the oldest reads read.                                                        |
| `-warmup`              | 21m           | Live feed and reads before the window.                                                                                                                            |
| `-duration`            | 20m           | Measurement window.                                                                                                                                               |
| `-retention`           | 13h           | TSDB retention, and how far back reads go.                                                                                                                        |
| `-out-of-order-window` | 2h            | Out-of-order window of the tenants.                                                                                                                               |
| `-backdated-window`    | 0             | Out-of-order window, such as `720h`, of the profile's share of tenants that write some live samples up to that late. Their reads go back as far. 0 for none.      |
| `-query-scale`         | 1             | Factor of the profile's read rates.                                                                                                                               |
| `-read-scales`         |               | Factors of some reads' rates, such as `LabelNamesAndValues=50,QueryStream=0`, so a short run gets enough of the rare reads.                                       |
| `-compare-fraction`    | 1             | Share of reads whose answers are compared. All reads are sent and timed. Comparing at production rates takes the bench more CPU than reading takes the ingesters. |
| `-concurrency`         | 256           | Reads in flight at most. Reads beyond it are dropped and counted.                                                                                                 |
| `-profile`             | built-in      | JSON profile of the series and reads. Its fields replace the built-in profile's.                                                                                  |
| `-keep`                | false         | Keep the ingesters' data, to look at after a mismatch.                                                                                                            |
| `-kafka-dir`           | memory        | Directory of the in-process Kafka's log.                                                                                                                          |
| `-cpu-profile-seconds` | 60            | Length of the CPU profiles.                                                                                                                                       |

## Profiles

The built-in profile ([`profile.go`](profile.go)) describes the series and the reads:

- **Series:**
  - tenants and how their sizes spread;
  - metrics, label names and values;
  - classic and native histograms;
  - scrape intervals, churn, late samples and exemplars;
  - with `-backdated-window`, the tenants that write samples days late, and how many.
- **Reads:**
  - rates per million series, by RPC;
  - query classes, each with its own matchers, ranges and series counts: alerting and recording rules, dashboard
    panels, explore, long-range and sharded queries, dashboard variables, the series browser and cardinality;
  - the share of the backdated tenants' reads over all of their window.

A `-profile` file replaces any of these fields.
