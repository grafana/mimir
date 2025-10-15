// SPDX-License-Identifier: AGPL-3.0-only

package benchmarks

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/streamingpromql/benchmarks"
)

var (
	dataDirFlag     = flag.String("data-dir", "", "Directory containing an ingester data dir (WAL + blocks for multiple tenants).")
	queryFileFlag   = flag.String("query-file", "", `File containing queries in Loki log JSON format. You can obtian it by running a command like this with logcli: logcli query -q --timezone=UTC --limit=1000000 --from='2025-10-15T15:15:21.0Z' --to='2025-10-15T16:15:21.0Z' --output=jsonl '{namespace="mimir", name="query-frontend"} |= "query stats" | logfmt | path=~".*/query(_range)?"' > logs.json`)
	querySampleFlag = flag.Float64("query-sample", 1.0, "Fraction of queries to sample (0.0 to 1.0). Queries are split into 100 segments, and a continuous sample is taken from each segment.")
	querySampleSeed = flag.Int64("query-sample-seed", 1, "Random seed for query sampling.")
	numSegments     = 100 // Number of segments to split queries into
)

// vectorSelectorQuery represents a single vector selector extracted from a query
type vectorSelectorQuery struct {
	originalQuery *Query
	matchers      []*labels.Matcher
}

// BenchmarkQueryExecution benchmarks query execution against a running ingester
func BenchmarkQueryExecution(b *testing.B) {
	require.NotEmpty(b, *dataDirFlag, "-data-dir flag is required")
	require.NotEmpty(b, *queryFileFlag, "-query-file flag is required")

	// Parse queries from file
	queries, err := ParseQueriesFromFile(*queryFileFlag)
	require.NoError(b, err)
	require.NotEmpty(b, queries, "no queries found in file")

	b.Logf("Loaded %d queries from %s", len(queries), *queryFileFlag)

	// Extract label matchers from each query (each query may produce multiple vector selectors)
	var vectorQueries []vectorSelectorQuery
	for i := range queries {
		matchersList, err := extractLabelMatchers(queries[i].Query)
		if err != nil {
			continue
		}

		for _, matchers := range matchersList {
			vectorQueries = append(vectorQueries, vectorSelectorQuery{
				originalQuery: &queries[i],
				matchers:      matchers,
			})
		}
	}

	b.Logf("Extracted %d vector selectors from %d queries", len(vectorQueries), len(queries))

	vectorQueries = sampleQueries(vectorQueries, *querySampleFlag, *querySampleSeed)
	b.Logf("Sampled down to %d vector selectors (%f%%)", len(vectorQueries), *querySampleFlag*100)

	require.NotEmpty(b, vectorQueries, "no vector queries after sampling")

	// Start ingester
	addr, cleanupFunc, err := benchmarks.StartIngesterAndLoadData(*dataDirFlag, []int{})
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

// extractLabelMatchers extracts label matchers from a PromQL query string.
// It parses the PromQL expression and returns a separate set of matchers for each vector selector.
// Returns a slice of slices, where each inner slice represents one vector selector's matchers.
func extractLabelMatchers(query string) ([][]*labels.Matcher, error) {
	expr, err := parser.ParseExpr(query)
	if err != nil {
		return nil, fmt.Errorf("failed to parse PromQL query: %w", err)
	}

	// Collect matchers from each vector selector separately
	var allMatcherSets [][]*labels.Matcher

	parser.Inspect(expr, func(node parser.Node, _ []parser.Node) error {
		if n, ok := node.(*parser.VectorSelector); ok {
			var matchers []*labels.Matcher

			// Add the metric name matcher if present
			if n.Name != "" {
				matcher, err := labels.NewMatcher(labels.MatchEqual, labels.MetricName, n.Name)
				if err == nil {
					matchers = append(matchers, matcher)
				}
			}

			// Add all label matchers
			for _, lm := range n.LabelMatchers {
				if lm != nil {
					matchers = append(matchers, lm)
				}
			}

			if len(matchers) > 0 {
				allMatcherSets = append(allMatcherSets, matchers)
			}
		}
		return nil
	})

	return allMatcherSets, nil
}

// sampleQueries samples queries by splitting them into segments and taking a continuous
// sample from each segment. This ensures representative coverage across the query set.
func sampleQueries(queries []vectorSelectorQuery, sampleFraction float64, seed int64) []vectorSelectorQuery {
	if sampleFraction >= 1.0 {
		return queries
	}
	if sampleFraction <= 0.0 {
		return nil
	}

	totalQueries := len(queries)
	if totalQueries == 0 {
		return queries
	}

	rng := rand.New(rand.NewSource(seed))

	// Calculate segment size
	segmentSize := totalQueries / numSegments
	if segmentSize == 0 {
		segmentSize = 1
	}

	// Calculate sample size per segment
	sampleSize := int(math.Ceil(float64(segmentSize) * sampleFraction))
	if sampleSize < 1 {
		sampleSize = 1
	}

	var sampledQueries []vectorSelectorQuery

	// Sample from each segment
	for seg := 0; seg < numSegments; seg++ {
		segmentStart := seg * segmentSize
		segmentEnd := segmentStart + segmentSize
		if segmentEnd > totalQueries {
			segmentEnd = totalQueries
		}

		// If segment is empty, skip it
		currentSegmentSize := segmentEnd - segmentStart
		if currentSegmentSize == 0 {
			break
		}

		// Adjust sample size if segment is smaller than expected
		currentSampleSize := sampleSize
		if currentSampleSize > currentSegmentSize {
			currentSampleSize = currentSegmentSize
		}

		// Pick random starting point within the segment for continuous sample
		maxStartOffset := currentSegmentSize - currentSampleSize
		startOffset := 0
		if maxStartOffset > 0 {
			startOffset = rng.Intn(maxStartOffset + 1)
		}

		// Extract continuous sample
		sampleStart := segmentStart + startOffset
		sampleEnd := sampleStart + currentSampleSize

		sampledQueries = append(sampledQueries, queries[sampleStart:sampleEnd]...)
	}

	return sampledQueries
}
