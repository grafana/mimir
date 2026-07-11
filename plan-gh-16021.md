# GH-16021 Implementation Plan

https://github.com/grafana/mimir/issues/16021

## Goal

Add an experimental querier option that partitions requests to the store-gateway when they contain too many blocks according to the partitioner's internal counting policy.

Requirements:

- Add `-querier.max-blocks-per-store-request`.
- Default to `0`, which disables partitioning and preserves existing behavior.
- Keep existing replica selection unchanged, then partition each selected client's blocks into requests.
- Minimize the number of requests while balancing counted blocks across requests.
- Count only blocks with compaction level 3 or higher toward the configured maximum.
- Allow any number of compaction level 0, 1, and 2 blocks in each request.
- Keep compaction-level awareness private to the partitioning implementation and its focused tests.
- Do not implement new span logging or metrics in this change. They will be added separately.

## Configuration

Add to `pkg/querier/querier.go`:

```go
MaxBlocksPerStoreRequest int `yaml:"max_blocks_per_store_request" category:"experimental"`
```

Register:

```text
-querier.max-blocks-per-store-request
```

Semantics:

- `0`: disabled.
- Positive values: maximum counted blocks in one request to one store client.
- Negative values: invalid configuration.

The flag help and generated configuration documentation must explain that only compaction level 3 and higher blocks count toward the limit. Do not expose that policy through query execution APIs, logs, metrics, or general-purpose request types.

Pass the value from `NewBlocksStoreQueryableFromConfig()` to `newBlocksStoreReplicationSet()`.

## Request representation

Change `BlocksStoreSet.GetClientsFor()` to return one or more requests per selected client:

```go
type blocksStoreRequests map[BlocksStoreClient][][]ulid.ULID
```

Use a private named type if it reduces repeated nested iteration. It must not contain compaction-level fields or methods. Each inner `[]ulid.ULID` is the block list for one RPC. A client may have multiple request slices.

Required invariants:

1. Every input block appears exactly once across all client request slices.
2. Every block is assigned to the same eligible, non-excluded replica that the existing selection logic would choose.
3. Every request slice is non-empty.
4. Every request satisfies the configured maximum according to the private counting policy.
5. With the option disabled, or when a client's counted block total does not exceed the limit, return exactly one request slice for that client containing its existing block list in the existing order.
6. Client lookup occurs once per selected address and the resulting client is reused for all its requests.

## Partitioning implementation

Keep all compaction-level logic in `pkg/querier/blocks_store_replicated_set.go`, or in one focused private helper file next to it.

Preserve the existing ownership resolution and replica selection path exactly:

1. Resolve each block's replication set with the existing ring options, including dynamic replication.
2. Apply existing exclusions, randomization, and preferred-zone behavior.
3. Select one replica with the existing `getNonExcludedInstance()` logic.
4. Append the block to that address's list in input order.

Do not redistribute overflow to another replica. Partitioning is a post-processing step over the block lists produced by today's address selection.

While building each address's block list, retain enough private metadata to count blocks with `CompactionLevel >= 3`. Levels 0, 1, and 2 do not count.

For each non-empty address list:

1. If the option is disabled, or the counted total does not exceed the limit, emit one request containing the existing block list unchanged.
2. Otherwise, calculate the minimum request count as `ceil(counted blocks / limit)`.
3. Balance counted blocks across that many requests so their counted totals differ by at most one and none exceeds the limit.
4. Preserve relative block order within and across the request slices; partition using contiguous ranges of the existing list.
5. Keep level 0, 1, and 2 blocks in those requests without creating additional requests for them.

For example, 23 counted blocks with a limit of 10 produce requests containing 8, 8, and 7 counted blocks, rather than 10, 10, and 3.

Resolve each selected store address through the client pool once per `GetClientsFor()` call and reuse the client for all requests targeting that address.

## Query execution

Update `queryFunc` and the series, label, and search fetch methods to accept all client requests in one call.

All request slices in one consistency attempt must execute concurrently, including multiple requests to the same client. The store-gateway concurrency gate is expected to provide downstream backpressure.

Existing fetch loops should change from:

```go
for client, blockIDs := range clients {
```

to nested iteration over clients and their request slices, launching one goroutine per request.

This applies to:

- `fetchSeriesFromStores()`
- `fetchLabelNamesFromStore()`
- `fetchLabelValuesFromStore()`
- `fetchSearchLabelNamesFromStore()`
- `fetchSearchLabelValuesFromStore()`

Preserve existing stream lifetime, cancellation, warning aggregation, result merging, and retry behavior.

## Consistency retries

Treat all requests returned by one `GetClientsFor()` call as one consistency attempt.

After `queryF` returns:

- Aggregate queried-block hints across every request.
- Traverse every client's request slices when updating `attemptedBlocks`.
- Record the selected remote address for each assigned block using the existing bookkeeping; do not add separate address-deduplication machinery.
- Retry only blocks still reported missing by the consistency tracker.
- Select replicas and repartition the missing blocks on the next attempt using the same process.
- Keep retry bounds based on the dynamic replication factor, not request count.

Keep `storesHit` semantics unchanged: count distinct remote addresses, not requests.

## Tests

### Configuration tests

Add tests for:

- Default value is `0`.
- Positive values are accepted.
- Negative values fail validation.

### Replication-set and partitioner tests

Extend `pkg/querier/blocks_store_replicated_set_test.go` with focused cases:

- Disabled option returns one request per selected client and preserves current assignment and block ordering.
- A positive limit that is not reached preserves the same assignment, ordering, and request count.
- Exactly the configured number of counted blocks remains in one request.
- One additional counted block creates the minimum two requests and balances their counted blocks.
- Larger inputs use `ceil(counted blocks / limit)` requests whose counted totals differ by at most one.
- Any number of level 0, 1, and 2 blocks does not create additional requests.
- Mixed level 0, 1, 2, and 3+ blocks observes the limit only for level 3+ blocks.
- Every input block appears exactly once and relative block order is preserved.
- A selected client can receive multiple requests.
- Existing replica selection remains unchanged when partitioning is enabled, including exclusions, preferred availability zones, random load balancing, and dynamic replication factors.
- Ring buffer reuse remains safe.
- Client lookup occurs once when repeated requests target the same address.

Do not add tests for redistributing overflow to another replica. Tests outside the focused partitioning tests should not inspect compaction levels.

### Queryable tests

Update mocks and existing expectations for the per-client request slices, then add coverage for:

- Multiple requests execute concurrently within one consistency attempt.
- Multiple requests can target the same client.
- Queried-block hints are aggregated across requests.
- Only missing blocks are retried.
- Retry exclusions apply per block.
- Distinct-store accounting is unchanged when one store receives multiple requests.
- Errors cancel sibling requests and clean up streams as before.

Cover all request families:

- Series.
- Label names.
- Label values.
- Streaming label-name search.
- Streaming label-value search.

For search tests, verify that multiple streams targeting the same client survive header setup, merge correctly, and are all closed on failure.

## Documentation and generated files

After implementing the flag:

1. Run `make reference-help doc` and include generated configuration changes.
2. Add `-querier.max-blocks-per-store-request` to the Querier section of `docs/sources/mimir/configure/about-versioning.md`.
3. Add a changelog entry once the PR number is known.

## Validation

Run at least:

```text
go test ./pkg/querier/...
make format
make reference-help doc
```

Then rerun affected tests after formatting and generation.

Use `testing.T.Context()` in new tests where a test-scoped context is needed. Do not add dependencies or unrelated refactors.

## Deferred work

Do not add span logging or metrics in this implementation. A follow-up will define how partitioning is represented in debug spans and observability without exposing the internal compaction-level policy.
