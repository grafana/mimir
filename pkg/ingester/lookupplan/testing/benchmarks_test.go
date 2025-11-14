// SPDX-License-Identifier: AGPL-3.0-only

// Package benchmarks provides benchmark tests for the index lookup planner.
// Usage:
//
//	go test -bench=. -data-dir=/path/to/data -query-file=/path/to/queries.json
//
// For more flags see the individual flag descriptions below.
package testing

import (
	"context"
	"flag"
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/DmitriyVTitov/size"
	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/ingester/lookupplan"
	"github.com/grafana/mimir/pkg/streamingpromql/benchmarks"
	"github.com/grafana/mimir/pkg/util/bench"
)

var (
	dataDirFlag     = flag.String("data-dir", "", "Directory containing an ingester data dir (WAL + blocks for multiple tenants).")
	queryFileFlag   = flag.String("query-file", "", `File containing queries in Loki log JSON format. You can obtain it by running a command like this with logcli: logcli query -q --timezone=UTC --limit=1000000 --from='2025-10-15T15:15:21.0Z' --to='2025-10-15T16:15:21.0Z' --output=jsonl '{namespace="mimir", name="query-frontend"} |= "query stats" | logfmt | path=~".*/query(_range)?"' > logs.json`)
	tenantIDFlag    = flag.String("tenant-id", "", "Tenant ID to filter queries by. If empty, all queries are used.")
	queryIDsFlag    = flag.String("query-ids", "", "Comma-separated list of query IDs (line numbers) to benchmark. Mutually exclusive with query-sample < 1.0.")
	querySampleFlag = flag.Float64("query-sample", 1.0, "Fraction of queries to sample (0.0 to 1.0). Queries are split into 100 segments, and a continuous sample is taken from each segment.")
	querySampleSeed = flag.Int64("query-sample-seed", 1, "Random seed for query sampling.")
	queryLoader     = bench.NewQueryLoader()
)

// mockQueryStreamServer implements the minimum required methods for QueryStream.
// It embeds Ingester_QueryStreamServer to satisfy the interface but only Send and Context
// are actually called by QueryStream.
type mockQueryStreamServer struct {
	client.Ingester_QueryStreamServer
	result                *queryResult
	ctx                   context.Context
	seenEndOfSeriesStream bool
}

func (m *mockQueryStreamServer) Send(resp *client.QueryStreamResponse) error {
	defer resp.FreeBuffer()

	if len(resp.StreamingSeries) > 0 {
		for _, s := range resp.StreamingSeries {
			m.result.SeriesCount++
			m.result.ChunksCount += int(s.ChunkCount)
		}
	}

	if resp.IsEndOfSeriesStream {
		m.seenEndOfSeriesStream = true
	}

	return nil
}

func (m *mockQueryStreamServer) Context() context.Context {
	return m.ctx
}

// BenchmarkQueryExecution benchmarks query execution against a running ingester.
// This benchmark executes real PromQL queries extracted from production logs
// against a running ingester with actual data.
// Usage:
//
//	go test -bench=BenchmarkQueryExecution -data-dir=/path/to/data -query-file=/path/to/queries.json
func BenchmarkQueryExecution(b *testing.B) {
	require.NotEmpty(b, *dataDirFlag, "-data-dir flag is required")
	require.NotEmpty(b, *queryFileFlag, "-query-file flag is required")

	// Validate mutual exclusivity of query-ids and query-sample
	if *queryIDsFlag != "" && *querySampleFlag < 1.0 {
		b.Fatal("-query-ids and -query-sample < 1.0 are mutually exclusive")
	}

	// Parse query IDs if provided
	var queryIDs []int
	var err error
	if *queryIDsFlag != "" {
		queryIDs, err = parseQueryIDs(*queryIDsFlag)
		require.NoError(b, err)
	}

	queries, stats, err := queryLoader.PrepareQueries(bench.QueryLoaderConfig{
		Filepath:       *queryFileFlag,
		TenantID:       *tenantIDFlag,
		QueryIDs:       queryIDs,
		SampleFraction: *querySampleFlag,
		Seed:           *querySampleSeed,
	})
	require.NoError(b, err)
	require.NotEmpty(b, queries, "no queries after filtering and sampling")
	b.Logf("Prepared %d queries (malformed: %d, sampled: %f%% of all queries)", len(queries), stats.MalformedLines, *querySampleFlag*100)

	// Start ingester
	ing, _, cleanupFunc, err := benchmarks.StartBenchmarkIngester(*dataDirFlag, func(config *ingester.Config) {
		config.BlocksStorageConfig.TSDB.IndexLookupPlanning.Enabled = true
	})
	require.NoError(b, err)
	b.Cleanup(cleanupFunc)

	b.Log("Starting benchmark")

	// Benchmark query execution - run each vector selector as a sub-benchmark
	for _, q := range queries {
		for selectorIdx := range q.VectorSelectors {
			queryID := fmt.Sprintf("query=%d/selector=%d", q.QueryID, selectorIdx)
			b.Run(queryID, func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					queryResult, err := queryIngester(ing, q.VectorSelectors[selectorIdx], q.User)
					if err != nil {
						b.Fatalf("Query failed: %v (query: %s)", err, q.Query)
					}
					b.ReportMetric(float64(queryResult.SeriesCount), "series")
					b.ReportMetric(float64(queryResult.ChunksCount), "chunks")
				}
			})
		}
	}
}

// BenchmarkStatisticsGeneration benchmarks block statistics generation.
// Particularly useful for tuning thresholds for larger and smaller count-min sketches.
// Usage:
//
//	go test -bench=BenchmarkStatisticsGeneration -data-dir=/path/to/data
func BenchmarkStatisticsGeneration(b *testing.B) {
	require.NotEmpty(b, *dataDirFlag, "-data-dir flag is required")

	// Open the TSDB for this tenant
	db, err := tsdb.Open(*dataDirFlag, promslog.NewNopLogger(), nil, nil, nil)
	require.NoError(b, err)
	blocks := db.Blocks()

	// Benchmark each block's statistics generation
	for _, block := range blocks {
		b.Run(fmt.Sprintf("block=%s", block.Meta().ULID.String()), func(b *testing.B) {
			b.ReportAllocs()

			statsGen := lookupplan.NewStatisticsGenerator(log.NewNopLogger())
			indexReader, err := block.Index()
			require.NoError(b, err)

			// Benchmark the Stats method
			for i := 0; i < b.N; i++ {
				stats, err := statsGen.Stats(
					block.Meta(), indexReader,
					lookupplan.DefaultLabelCardinalityForSmallerSketch,
					lookupplan.DefaultLabelCardinalityForLargerSketch,
				)
				if err != nil {
					b.Fatalf("Failed to generate statistics: %v", err)
				}
				if i == 0 {
					b.ReportMetric(float64(size.Of(stats)), "stats_size_bytes")
					b.Logf("total_series=%d", stats.TotalSeries())
				}
			}
			err = indexReader.Close()
			require.NoError(b, err)
		})
	}

	// Close the TSDB after processing all blocks
	err = db.Close()
	require.NoError(b, err)
}

// queryResult contains the result of executing a query against the ingester.
type queryResult struct {
	SeriesCount int
	ChunksCount int
}

func queryIngester(ing *ingester.Ingester, matchers []*labels.Matcher, userID string) (*queryResult, error) {
	// Convert to client format
	labelMatchers, err := client.ToLabelMatchers(matchers)
	if err != nil {
		return nil, fmt.Errorf("failed to convert label matchers: %w", err)
	}

	// Create query request
	req := &client.QueryRequest{
		// Cover all blocks by using extreme timestamps.
		// Realistically the queries might be touching different periods than the data we have.
		StartTimestampMs: math.MinInt64,
		EndTimestampMs:   math.MaxInt64,
		Matchers:         labelMatchers,
	}

	ctx := user.InjectOrgID(context.Background(), userID)

	// Create mock stream that accumulates results directly
	result := &queryResult{}
	mockStream := &mockQueryStreamServer{
		ctx:    ctx,
		result: result,
	}

	// Call QueryStream directly on the ingester
	err = ing.QueryStream(req, mockStream)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	return result, nil
}

func parseQueryIDs(queryIDsStr string) ([]int, error) {
	if queryIDsStr == "" {
		return nil, nil
	}

	parts := strings.Split(queryIDsStr, ",")
	queryIDs := make([]int, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		id, err := strconv.Atoi(part)
		if err != nil {
			return nil, fmt.Errorf("invalid query ID %q: %w", part, err)
		}
		queryIDs = append(queryIDs, id)
	}

	if len(queryIDs) == 0 {
		return nil, fmt.Errorf("no valid query IDs provided")
	}

	return queryIDs, nil
}
