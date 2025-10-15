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
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/streamingpromql/benchmarks"
)

var (
	dataDirFlag   = flag.String("data-dir", "", "Directory containing an ingester data dir (WAL + blocks for multiple tenants).")
	queryFileFlag = flag.String("query-file", "", `File containing queries in Loki log JSON format. You can obtian it by running a query like this against loki: {namespace="", name="query-frontend"} |= "query stats" | logfmt | path=~".*/query(_range)?"`)
)

// BenchmarkQueryExecution benchmarks query execution against a running ingester
func BenchmarkQueryExecution(b *testing.B) {
	require.NotEmpty(b, *dataDirFlag, "-data-dir flag is required")
	require.NotEmpty(b, *queryFileFlag, "-query-file flag is required")

	// Parse queries from file
	queries, err := ParseQueriesFromFile(*queryFileFlag)
	require.NoError(b, err)
	require.NotEmpty(b, queries, "no queries found in file")

	b.Logf("Loaded %d queries from %s", len(queries), *queryFileFlag)

	// Start ingester
	addr, cleanupFunc, err := benchmarks.StartIngesterAndLoadData(*dataDirFlag, []int{})
	require.NoError(b, err)
	defer cleanupFunc()

	b.ReportAllocs()
	b.ResetTimer()

	// Benchmark query execution - run all queries in each iteration
	for i := 0; i < b.N; i++ {
		for queryIdx, q := range queries {
			_, err := executeQueryWithMatchers(addr, q)
			if err != nil {
				b.Logf("Query %d failed: %v (query: %s)", queryIdx, err, q.Query)
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

// executeQueryWithMatchers executes a parsed query against the ingester.
// Note: This is a simplified version that uses the ingester's QueryStream API
// which accepts label matchers but not full PromQL expressions. For a complete
// implementation, a PromQL engine would be needed.
func executeQueryWithMatchers(ingesterAddress string, q Query) (*queryResult, error) {
	start := time.Now()

	// Create gRPC connection to ingester
	conn, err := grpc.Dial(ingesterAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to ingester: %w", err)
	}
	defer conn.Close()

	// Create ingester client
	ingesterClient := client.NewIngesterClient(conn)

	// Parse PromQL query to extract label matchers
	// For now, we'll create a simple matcher to match all series with __name__
	// A proper implementation would parse the PromQL expression
	matchers, err := extractLabelMatchers(q.Query)
	if err != nil {
		return nil, fmt.Errorf("failed to extract label matchers: %w", err)
	}

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

	ctx := user.InjectOrgID(context.Background(), q.OrgID)
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

// extractLabelMatchers extracts label matchers from a PromQL query string.
// This is a simplified implementation that creates a catch-all matcher.
// A proper implementation would use the PromQL parser.
func extractLabelMatchers(query string) ([]*labels.Matcher, error) {
	// For now, create a matcher that matches any metric with __name__ label
	// This is a placeholder - a real implementation would parse the PromQL expression
	matcher, err := labels.NewMatcher(labels.MatchRegexp, "__name__", ".+")
	if err != nil {
		return nil, err
	}
	return []*labels.Matcher{matcher}, nil
}
