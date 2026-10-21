# Metrics response cache experiment

Test-only prototype; no Mimir route or configuration changes.

The fixture uses synthetic tenant labels, one classic histogram without finite
buckets and 15 counter vectors. The large case has 8,000 tenants and 144,000
text-format samples. This approximates high-cardinality collection and encoding
costs, rather than reproducing a complete distributor registry or ingestion load.

The handler uses the repository's vendored `promhttp.HandlerFor`, with
OpenMetrics enabled, matching dskit's instrumentation handler options.

## Reproduce

On Unix systems (the CPU measurement uses `getrusage`), from the repository root:

```sh
go test ./tools/metrics-cache-bench -v -count=1
go test -race ./tools/metrics-cache-bench -run '^TestCache' -count=1
go test ./tools/metrics-cache-bench -run '^$' -bench '^BenchmarkMetrics$' -benchmem -benchtime=20x -count=5
go test ./tools/metrics-cache-bench -run '^$' -bench '^BenchmarkMetricsHTTP$' -benchmem -benchtime=20x -count=5
```

If the default build cache is unavailable, set `GOCACHE` to a writable directory.
Run benchmarks sequentially to avoid competition between benchmark processes.

`cached_20_per_minute` advances a synthetic clock three seconds per request.
After warmup, every fifth request refreshes the complete response, including
compression; the other four reuse the bytes. Each 20-operation sample therefore
includes exactly four refreshes. This models one regularly spaced request stream
and one response representation. It does not wait for real 15-second intervals.

The handler benchmark includes gathering, encoding, compression and writes to a
discard response writer. Its hit-only timings exclude transfer/copying and should
not be interpreted as endpoint throughput. The HTTP benchmark adds loopback TCP,
keepalive and client reads, with automatic decompression disabled. Its
`cpu-ns/op` counts process user plus system CPU using `getrusage`, including both
server and client. `ns/op` is elapsed time; `B/op` is allocated bytes, not retained
heap. Fixtures are prepopulated; ingestion and metric updates are not benchmarked.

## Scope and tradeoffs

- A production implementation must negotiate a bounded set of content formats,
  escaping schemes and content encodings before selecting cache entries. This
  prototype deliberately gives each fixed representation its own handler.
- Cached values can lag by 15 seconds. Short-window rates, scrape interval
  differences and alerting need evaluation before enabling it.
- Independent format entries have independent refreshes. Mixed clients can
  substantially reduce savings; the single-stream ideal is four refreshes for
  twenty scrapes per minute, or 80% fewer gathers.
- The cache buffers the full body. Tests log both wire length and retained buffer
  capacity; refreshes temporarily retain old and new responses and allocate the
  collector/encoder working set. Concurrent readers can retain old responses.
- Refreshes block concurrent misses behind one mutex. Successful snapshots are
  immutable; error responses are retried rather than cached. Cancellation,
  timeout and slow-client behavior need production integration work.
- This does not reduce bytes sent over the network. It avoids repeated gathering,
  encoding and compression. Local results do not establish whole-service savings.

Tests cover body/content-header equivalence for text, OpenMetrics and protobuf,
both identity and gzip encodings, TTL boundaries, changing values, concurrent
refresh coalescing and error retry behavior.

## Local results

Apple M3 Pro, darwin/arm64, Go 1.27.2, GOMAXPROCS 11.
Mimir checkout: `74f9a811cb98dec769672ce6c1905cbd6601e2a4`.
Five samples per case, 20 requests per sample; values below are medians.

Loopback HTTP, 8,000 tenants, uncached → 15-second cache:

- identity: elapsed 175.0 → 34.1 ms/request; process CPU 298.4 → 55.2 ms/request (81.5% reduction); allocated 46.5 → 16.0 MB/request.
- gzip: elapsed 175.1 → 36.9 ms/request; process CPU 276.9 → 57.7 ms/request (79.2% reduction); allocated 47.4 → 9.9 MB/request.

Handler-only, 8,000 tenants, elapsed ms/request, uncached → cache:

- text/identity: 152.4 → 32.4.
- text/gzip: 178.2 → 40.0.
- openmetrics/identity: 160.0 → 32.0.
- openmetrics/gzip: 186.9 → 35.0.
- protobuf/identity: 161.2 → 29.9.
- protobuf/gzip: 175.4 → 34.6.

The text response occupies 9.19 MB on the wire with a 16 MiB retained buffer;
the gzip response occupies approximately 0.74 MB with a 1 MiB buffer.
Those capacities are per cached representation and exclude the registry and
refresh working set. These are sample measurements, not heap profiles.

All functional tests and the focused race tests passed. Raw output is saved in
`handler-results.txt`, `http-results.txt`, `test-results.txt` and `race-results.txt`.
