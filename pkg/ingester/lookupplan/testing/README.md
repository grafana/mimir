# Index Lookup Planning Testing

Tests and benchmarks for ingester index lookup planning with real production queries and data.

## Overview

This package provides two types of tests:

1. **Performance benchmarks** (`BenchmarkQueryExecution`) - measures query execution performance
2. **Correctness tests** (`TestCompareIngesterConfigs`) - verifies that enabling index lookup planning produces identical results to the baseline implementation

Both tests execute real PromQL queries extracted from production logs against an ingester loaded with actual TSDB data.

## Usage

Typical workflow:

1. Extract ingester data from a running pod (`kubectl` command below)
2. Extract query logs from Loki (`logcli` command below)
3. Run benchmarks to establish a baseline (`go test` command below)
4. Run benchmarks with a different ingester configuration (modify the `benchmarks_test.go`; then run `go test` again)
5. Profile specific queries with `-query-ids` flag and pprof

### Example Workflow: Profiling Slow Queries

Complete workflow for identifying and profiling slow queries:

```bash
# 1. Extract ingester data
NAMESPACE="mimir-dev-01"
INGESTER_POD="ingester-zone-a-0"
kubectl debug -n $NAMESPACE $INGESTER_POD --image ubuntu -it --target=ingester --container=copy-container -- sh
# In a separate terminal:
kubectl cp -n $NAMESPACE --container=copy-container $INGESTER_POD:/proc/1/root/data ./ingester-data
mv ./ingester-data/tsdb ./ingester-data/data

# 2. Extract query logs
logcli query -q --forward --timezone=UTC --limit=1000000 \
  --from='2025-10-15T15:15:21.0Z' --to='2025-10-15T16:15:21.0Z' \
  --output=jsonl \
  '{namespace="'$NAMESPACE'", name="query-frontend"} |= "query stats" | logfmt | path=~".*/query(_range)?"' \
  > queries.json

# 3. Run benchmarks to identify slow queries
go test -bench=. \
  -data-dir=./ingester-data \
  -query-file=./queries.json \
  -query-sample=0.1

# 4. Identify slow query IDs from output (e.g., query 42 was slow)

# 5. Profile the slow query with CPU and memory profiling
go test -bench=. \
  -data-dir=./ingester-data \
  -query-file=./queries.json \
  -query-ids=42 \
  -cpuprofile=cpu.prof \
  -memprofile=mem.prof

# 6. Analyze profiles
go tool pprof cpu.prof
go tool pprof mem.prof
```

### Running Benchmarks

```bash
# 1. Extract ingester data
NAMESPACE="mimir-dev-01"
INGESTER_POD="ingester-zone-a-0"
kubectl debug -n $NAMESPACE $INGESTER_POD --image ubuntu -it --target=ingester --container=copy-container -- sh
# In a separate terminal:
kubectl cp -n $NAMESPACE --container=copy-container $INGESTER_POD:/proc/1/root/data ./ingester-data
mv ./ingester-data/tsdb ./ingester-data/tsdb-data

# 2. Extract query logs
logcli query -q --timezone=UTC --limit=1000000 \
  --from='2025-10-15T15:15:21.0Z' --to='2025-10-15T16:15:21.0Z' \
  --output=jsonl \
  '{namespace="'$NAMESPACE'", name="query-frontend"} |= "query stats" | logfmt | path=~".*/query(_range)?"' \
  > queries.json

# 3. Run benchmarks
go test -bench=. \
  -data-dir=./ingester-data \
  -query-file=./queries.json \
  -query-sample=0.1
```

### Running Correctness Tests

```bash
# 1. Extract ingester data (same as above)
NAMESPACE="mimir-dev-01"
INGESTER_POD="ingester-zone-a-0"
kubectl debug -n $NAMESPACE $INGESTER_POD --image ubuntu -it --target=ingester --container=copy-container -- sh
# In a separate terminal:
kubectl cp -n $NAMESPACE --container=copy-container $INGESTER_POD:/proc/1/root/data ./ingester-data
mv ./ingester-data/tsdb ./ingester-data/tsdb-data

# 2. Extract query logs (same as above)
logcli query -q --timezone=UTC --limit=1000000 \
  --from='2025-10-15T15:15:21.0Z' --to='2025-10-15T16:15:21.0Z' \
  --output=jsonl \
  '{namespace="'$NAMESPACE'", name="query-frontend"} |= "query stats" | logfmt | path=~".*/query(_range)?"' \
  > queries.json

# 3. Run correctness tests
go test -v -run=TestCompareIngesterConfigs \
  -data-dir=./ingester-data \
  -query-file=./queries.json \
  -query-sample=0.01 \
  -timeout=10m
```

The correctness test creates two ingesters (one with index lookup planning enabled, one without) and verifies they return identical results.

### Running Specific Queries

To run a single query by line number:

```bash
go test -bench=. \
  -data-dir=./ingester-data \
  -query-file=./queries.json \
  -query-ids=42
```

### Additional Flags

- `-tenant-id` - filter queries by tenant ID
- `-query-ids` - comma-separated list of query line numbers (e.g., "1,5,10", mutually exclusive with `-query-sample < 1.0`)
- `-query-sample` - fraction of queries to sample (0.0 to 1.0, default 1.0)
- `-query-sample-seed` - random seed for sampling (default 1)

## Implementation Details

- Each benchmark sub-test corresponds to a single vector selector from a query
- Queries use extreme timestamps (math.MinInt64 to math.MaxInt64) to cover all blocks
- The mock stream server accumulates series and chunk counts without storing full results (except in comparison tests)
- Query sampling splits queries into 100 segments and samples continuously from each to ensure representative coverage
