// SPDX-License-Identifier: AGPL-3.0-only

package benchmarks

import (
	"context"
	"flag"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/streamingpromql/benchmarks"
)

var (
	dataDirFlag     = flag.String("data-dir", "", "Directory containing an ingester data dir (WAL + blocks for multiple tenants).")
	queryFileFlag   = flag.String("query-file", "", `File containing queries in Loki log JSON format. You can obtian it by running a command like this with logcli: logcli query -q --timezone=UTC --limit=1000000 --from='2025-10-15T15:15:21.0Z' --to='2025-10-15T16:15:21.0Z' --output=jsonl '{namespace="mimir", name="query-frontend"} |= "query stats" | logfmt | path=~".*/query(_range)?"' > logs.json`)
	tenantIDFlag    = flag.String("tenant-id", "", "Tenant ID to filter queries by. If empty, all queries are used.")
	querySampleFlag = flag.Float64("query-sample", 1.0, "Fraction of queries to sample (0.0 to 1.0). Queries are split into 100 segments, and a continuous sample is taken from each segment.")
	querySampleSeed = flag.Int64("query-sample-seed", 1, "Random seed for query sampling.")
)

// mockQueryStreamServer is a server stream that accumulates query results without sending over the network.
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
			m.result.ChunkCount += int(s.ChunkCount)
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

// BenchmarkQueryExecution benchmarks query execution against a running ingester
func BenchmarkQueryExecution(b *testing.B) {
	require.NotEmpty(b, *dataDirFlag, "-data-dir flag is required")
	require.NotEmpty(b, *queryFileFlag, "-query-file flag is required")

	// Prepare vector queries (load, extract matchers, filter by tenant, sample) - this is cached
	vectorQueries, err := PrepareVectorQueries(*queryFileFlag, *tenantIDFlag, *querySampleFlag, *querySampleSeed)
	require.NoError(b, err)
	require.NotEmpty(b, vectorQueries, "no vector queries after filtering and sampling")

	b.Logf("Prepared %d vector selectors (sample: %f%%)", len(vectorQueries), *querySampleFlag*100)

	// Start ingester
	ing, _, cleanupFunc, err := benchmarks.StartBenchmarkIngester(*dataDirFlag, func(config *ingester.Config) {
		config.BlocksStorageConfig.TSDB.IndexLookupPlanning.Enabled = true
	})
	require.NoError(b, err)
	defer cleanupFunc()

	// Warm up with a simple query
	_, _ = executeQueryDirect(ing, vectorQueries[0])

	b.Log("Starting benchmark")
	b.ReportAllocs()
	b.ResetTimer()

	// Benchmark query execution - run each query as a sub-benchmark
	for _, vq := range vectorQueries {
		queryID := fmt.Sprintf("query-%d", vq.originalQuery.QueryID)
		b.Run(queryID, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				queryResult, err := executeQueryDirect(ing, vq)
				if err != nil {
					b.Fatalf("Query failed: %v (query: %s)", err, vq.originalQuery.Query)
				}
				// Report metrics only on first iteration to avoid clutter
				if i == 0 {
					b.Logf("series=%d chunks=%d", queryResult.SeriesCount, queryResult.ChunkCount)
				}
			}
		})
	}
}

// queryResult contains the result of executing a query against the ingester.
type queryResult struct {
	SeriesCount int // Number of series returned
	ChunkCount  int // Total chunks across all series
	Duration    time.Duration
}

// executeQueryDirect executes a vector selector query directly against the ingester without going through gRPC.
func executeQueryDirect(ing *ingester.Ingester, vq vectorSelectorQuery) (*queryResult, error) {
	start := time.Now()

	// Convert to client format
	labelMatchers, err := client.ToLabelMatchers(vq.matchers)
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

	ctx := user.InjectOrgID(context.Background(), vq.originalQuery.OrgID)
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

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

	result.Duration = time.Since(start)
	return result, nil
}
