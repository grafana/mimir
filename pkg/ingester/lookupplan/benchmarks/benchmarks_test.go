// SPDX-License-Identifier: AGPL-3.0-only

package benchmarks

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/grafana/mimir/pkg/ingester"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/streamingpromql/benchmarks"
)

var (
	dataDirFlag     = flag.String("data-dir", "", "Directory containing an ingester data dir (WAL + blocks for multiple tenants).")
	queryFileFlag   = flag.String("query-file", "", `File containing queries in Loki log JSON format. You can obtian it by running a command like this with logcli: logcli query -q --timezone=UTC --limit=1000000 --from='2025-10-15T15:15:21.0Z' --to='2025-10-15T16:15:21.0Z' --output=jsonl '{namespace="mimir", name="query-frontend"} |= "query stats" | logfmt | path=~".*/query(_range)?"' > logs.json`)
	querySampleFlag = flag.Float64("query-sample", 1.0, "Fraction of queries to sample (0.0 to 1.0). Queries are split into 100 segments, and a continuous sample is taken from each segment.")
	querySampleSeed = flag.Int64("query-sample-seed", 1, "Random seed for query sampling.")
)

// BenchmarkQueryExecution benchmarks query execution against a running ingester
func BenchmarkQueryExecution(b *testing.B) {
	require.NotEmpty(b, *dataDirFlag, "-data-dir flag is required")
	require.NotEmpty(b, *queryFileFlag, "-query-file flag is required")

	// Prepare vector queries (load, extract matchers, sample) - this is cached
	vectorQueries, err := PrepareVectorQueries(*queryFileFlag, *querySampleFlag, *querySampleSeed)
	require.NoError(b, err)
	require.NotEmpty(b, vectorQueries, "no vector queries after sampling")

	b.Logf("Prepared %d vector selectors (sample: %f%%)", len(vectorQueries), *querySampleFlag*100)

	// Start ingester
	addr, cleanupFunc, err := benchmarks.StartIngesterAndLoadData(*dataDirFlag, []int{}, func(config *ingester.Config) {
		config.BlocksStorageConfig.TSDB.IndexLookupPlanning.Enabled = true
	})
	require.NoError(b, err)
	defer cleanupFunc()

	// Create a single gRPC connection to the ingester
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(b, err)
	defer conn.Close()

	// Create ingester client
	ingesterClient := client.NewIngesterClient(conn)

	// Warm up the connection with a simple query
	_, _ = executeQueryWithMatchers(ingesterClient, vectorQueries[0])

	b.Log("Starting benchmark")
	b.ReportAllocs()
	b.ResetTimer()

	// Benchmark query execution - run all vector selectors in each iteration
	for i := 0; i < b.N; i++ {
		for queryIdx, vq := range vectorQueries {
			_, err := executeQueryWithMatchers(ingesterClient, vq)
			if err != nil {
				b.Logf("Query %d failed: %v (query: %s)", queryIdx, err, vq.originalQuery.Query)
				b.Fail()
			}
		}
	}
}

// queryResult contains the result of executing a query against the ingester.
type queryResult struct {
	SeriesCount int
	SampleCount int
	Duration    time.Duration
}

// executeQueryWithMatchers executes a vector selector query against the ingester.
// Note: This uses the ingester's QueryStream API which accepts label matchers
// but not full PromQL expressions. For a complete implementation, a PromQL engine would be needed.
func executeQueryWithMatchers(ingesterClient client.IngesterClient, vq vectorSelectorQuery) (*queryResult, error) {
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
	ctx, err = user.InjectIntoGRPCRequest(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to inject user into gRPC request: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	stream, err := ingesterClient.QueryStream(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	// Read response stream
	result := &queryResult{}
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to receive query response: %w", err)
		}

		if len(resp.Chunkseries) > 0 {
			for _, series := range resp.Chunkseries {
				result.SeriesCount++
				result.SampleCount += len(series.Chunks)
			}
		}
	}

	result.Duration = time.Since(start)
	return result, nil
}
