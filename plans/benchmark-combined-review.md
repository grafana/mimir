# Combined Code Review: Index Planning Benchmarks

## Summary

This document combines code review and testing/benchmarking reviews for the new index lookup planning benchmark suite in `pkg/ingester/lookupplan/benchmarks/`. The implementation includes query parsing from Loki logs, label matcher extraction, query sampling, and direct ingester query execution. The code is well-structured with good test coverage, but there are critical issues that must be addressed, particularly around benchmark correctness and Mimir coding conventions.

---

# CRITICAL ISSUES (MUST FIX)

## 3. Global Variable Usage - Violates Mimir Convention

**Location**: `query_parser.go:29`

```go
var vectorQueryCache sync.Map
```

**Problem**: The codebase explicitly prohibits global variables per `pkg/CLAUDE.md`. This global cache could lead to issues in testing and makes the code harder to reason about.

**Fix**: Remove the global variable and use dependency injection:

```go
// In query_parser.go
type QueryCache struct {
    cache sync.Map
}

func NewQueryCache() *QueryCache {
    return &QueryCache{}
}

func (qc *QueryCache) PrepareVectorQueries(filepath string, tenantID string, queryIDsStr string, sampleFraction float64, seed int64) ([]vectorSelectorQuery, error) {
    // Use qc.cache instead of vectorQueryCache
    // ...
}

// In benchmarks_test.go
var queryCache = NewQueryCache()

// Update all calls to PrepareVectorQueries to use queryCache.PrepareVectorQueries(...)
```

## 4. Typo in Documentation

**Location**: `benchmarks_test.go:23`

```go
queryFileFlag = flag.String("query-file", "", `... You can obtian it by running...`)
```

**Fix**: Change "obtian" to "obtain".

---

# SHOULD FIX (Quality Issues)

## 5. Remove Unused Timing Code

**Location**: `benchmarks_test.go:114` and `queryResult` struct

```go
func executeQueryDirect(ing *ingester.Ingester, vq vectorSelectorQuery) (*queryResult, error) {
    start := time.Now()  // ⚠️ Timing inside benchmarked code
    // ...
    result.Duration = time.Since(start)  // Not used in benchmark
    return result, nil
}
```

**Problem**: The `Duration` field in `queryResult` is computed but never used. This adds overhead to the measurement.

**Fix**: Remove the timing code from `executeQueryDirect` and remove the `Duration` field from `queryResult`:

```go
type queryResult struct {
    SeriesCount int
    ChunkCount  int
    // Remove: Duration time.Duration
}

func executeQueryDirect(ing *ingester.Ingester, vq vectorSelectorQuery) (*queryResult, error) {
    // Remove: start := time.Now()

    // ... existing code ...

    // Remove: result.Duration = time.Since(start)
    return result, nil
}
```

## 6. Missing Error Context

**Location**: `query_parser.go:55`

```go
f, err := os.Open(filepath)
if err != nil {
    return nil, fmt.Errorf("failed to open query file: %w", err)
}
```

**Problem**: Error doesn't include the filepath, making debugging harder.

**Fix**: Include the filepath in the error:

```go
f, err := os.Open(filepath)
if err != nil {
    return nil, fmt.Errorf("failed to open query file %q: %w", filepath, err)
}
```

## 7. Inefficient Slice Growth

**Location**: `query_parser.go:264`

```go
var vectorQueries []vectorSelectorQuery
for i := range queries {
    // ...
    for _, matchers := range matchersList {
        vectorQueries = append(vectorQueries, vectorSelectorQuery{...})
    }
}
```

**Problem**: The slice grows without pre-allocation, potentially causing multiple reallocations.

**Fix**: Pre-allocate with estimated capacity:

```go
// Pre-allocate with estimated capacity (assume ~2 selectors per query)
vectorQueries := make([]vectorSelectorQuery, 0, len(queries)*2)
```

## 10. Silent Error Handling

**Location**: `query_parser.go:265-269`

```go
for i := range queries {
    matchersList, err := extractLabelMatchers(queries[i].Query)
    if err != nil {
        continue  // Silently skips errors
    }
    // ...
}
```

**Problem**: Errors are silently ignored, making debugging difficult.

**Fix**: Track skipped queries:

```go
var skippedCount int
for i := range queries {
    matchersList, err := extractLabelMatchers(queries[i].Query)
    if err != nil {
        skippedCount++
        continue
    }
    // ...
}
// return number of skipped queries and then log them in BenchmarkQueryExecution
```

## 12. Document Mock Interface Embedding

**Location**: `benchmarks_test.go:30-55`

```go
type mockQueryStreamServer struct {
    client.Ingester_QueryStreamServer
    result                *queryResult
    ctx                   context.Context
    seenEndOfSeriesStream bool
}
```

**Problem**: The mock embeds the full interface but only implements `Send` and `Context`. If `QueryStream` calls other methods, this will panic.

**Fix**: Document why embedding is safe:

```go
// mockQueryStreamServer implements the minimum required methods for QueryStream.
// It embeds Ingester_QueryStreamServer to satisfy the interface but only Send and Context
// are actually called by QueryStream.
type mockQueryStreamServer struct {
    client.Ingester_QueryStreamServer
    // ...
}
```

---

# NICE TO HAVE (Enhancements)

## 13. Add Package Documentation

**Location**: Top of `benchmarks_test.go`

**Issue**: No package-level comment explaining what these benchmarks measure, how to run them, or what flags are required.

**Fix**: Add package doc:

```go
// Package benchmarks provides benchmark tests for the index lookup planner.
//
// These benchmarks execute real PromQL queries extracted from production logs
// against a running ingester with actual data.
//
// Usage:
//   go test -bench=. -data-dir=/path/to/data -query-file=/path/to/queries.json
//
// Flags:
//   -data-dir: Directory containing ingester data (WAL + blocks)
//   -query-file: JSON file with query logs from Loki
//   -tenant-id: Filter queries by tenant (optional)
//   -query-sample: Fraction of queries to sample (0.0-1.0)
package benchmarks
```

## 14. Document Magic Number

**Location**: `query_parser.go:23`

```go
const (
    numSegments = 100
)
```

**Issue**: No explanation of why 100 segments was chosen.

**Fix**: Add detailed comment:

```go
const (
    // numSegments is the number of segments to split queries into for sampling.
    // This value provides a good balance between sampling granularity and performance
    // for typical query sets (1000-100000 queries). Each segment is sampled independently
    // to ensure representative coverage across the entire query distribution.
    numSegments = 100
)
```

## 16. Document Expected Log Format

**Location**: `query_parser.go:106-118`

**Fix**: Add comment documenting the expected format:

```go
// UnmarshalJSON implements custom JSON unmarshaling for Query.
// It expects the Loki log format with query parameters in the labels field:
//   {"labels": {"param_query": "...", "param_start": "...", ...}, "timestamp": "..."}
// This format is produced by the command documented in the queryFileFlag help text.
func (q *Query) UnmarshalJSON(b []byte) error {
```

## 19. Improve Test Data Generation

**Location**: `sample_queries_test.go:16`

```go
queries[i] = vectorSelectorQuery{
    originalQuery: &Query{Query: string(rune(i))},
}
```

**Problem**: Converting integers to runes creates non-printable characters.

**Fix**: Use meaningful test data:

```go
queries[i] = vectorSelectorQuery{
    originalQuery: &Query{
        QueryID: i,
        Query: fmt.Sprintf("metric_%d", i),
    },
}
```

## 20. Add Parallel Benchmarks

**Missing**: No benchmarks use `b.RunParallel()` to test concurrent query behavior.

**Recommendation**: Add concurrent benchmarks:

```go
func BenchmarkQueryExecution_Parallel(b *testing.B) {
    // Setup...
    b.RunParallel(func(pb *testing.PB) {
        for pb.Next() {
            _, _ = executeQueryDirect(ing, vq)
        }
    })
}
```

## 21. Consolidate and Improve Test Coverage

**Location**: Multiple test files

**Current State**:
- `extract_matchers_test.go`: Good coverage of label matcher extraction, including edge cases
- `sample_queries_test.go`: Comprehensive sampling tests with distribution checks, determinism, edge cases
- `query_cache_test.go`: Tests caching behavior and tenant filtering
- `query_parser_test.go`: Tests query log loading and parsing

**Issues**:

1. **Missing tests for critical functionality**:
   - No test for `executeQueryDirect` - the core benchmark execution function
   - No test for `mockQueryStreamServer` behavior
   - No test for error paths in query execution
   - No test for `PrepareVectorQueries` with `queryIDsFlag`

2. **Test data quality** (already noted in #19):
   - `sample_queries_test.go:16` uses `string(rune(i))` creating non-printable characters

3. **Redundant/overlapping tests**:
   - `query_cache_test.go` and `query_parser_test.go` both test `PrepareVectorQueries` but from different angles
   - Consider consolidating into a single comprehensive test file

**Recommendations**:

1. **Add missing tests**:
```go
// In benchmarks_test.go
func TestExecuteQueryDirect(t *testing.T) {
    // Test successful query execution
    // Test query timeout
    // Test invalid label matchers
    // Test context cancellation
}

func TestMockQueryStreamServer(t *testing.T) {
    // Test series/chunk counting
    // Test EndOfSeriesStream flag
    // Test error handling
}

func TestPrepareVectorQueries_QueryIDs(t *testing.T) {
    // Test single query ID
    // Test multiple query IDs
    // Test invalid query IDs
    // Test mutual exclusivity with sampling
}
```

2. **Consolidate query preparation tests**:
   - Merge `query_cache_test.go` into `query_parser_test.go`
   - Organize tests by functionality (loading → filtering → sampling → caching)

4. **Fix test data** (as noted in #19):
```go
queries[i] = vectorSelectorQuery{
    originalQuery: &Query{
        QueryID: i,
        Query: fmt.Sprintf("metric_%d", i),
    },
}
```
