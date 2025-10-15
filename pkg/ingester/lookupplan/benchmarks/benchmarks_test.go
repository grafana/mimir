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

var dataDirFlag = flag.String("data-dir", "", "Directory containing an ingester data dir (WAL + blocks for multiple tenants).")

// BenchmarkQueryExecution benchmarks query execution against a running ingester
func BenchmarkQueryExecution(b *testing.B) {
	require.NotEmpty(b, *dataDirFlag, "-data-dir flag is required")

	addr, cleanupFunc, err := benchmarks.StartIngesterAndLoadData(*dataDirFlag, []int{})
	require.NoError(b, err)
	defer cleanupFunc()

	b.ReportAllocs()
	b.ResetTimer()

	// Benchmark query execution
	for i := 0; i < b.N; i++ {
		_, err := executeQuery(addr, "__name__", "mimir_continuous_test_sine_wave_v2")
		require.NoError(b, err)
	}
}

// queryResult contains the result of executing a query against the ingester.
type queryResult struct {
	SeriesCount int
	SampleCount int
	Duration    time.Duration
}

// executeQuery executes a query against the ingester at the given address.
func executeQuery(ingesterAddress string, labelName, labelValue string) (*queryResult, error) {
	start := time.Now()

	// Create gRPC connection to ingester
	conn, err := grpc.Dial(ingesterAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to ingester: %w", err)
	}
	defer conn.Close()

	// Create ingester client
	ingesterClient := client.NewIngesterClient(conn)

	// Create label matcher
	matcher, err := labels.NewMatcher(labels.MatchEqual, labelName, labelValue)
	if err != nil {
		return nil, fmt.Errorf("failed to create label matcher: %w", err)
	}

	// Convert to client format
	labelMatchers, err := client.ToLabelMatchers([]*labels.Matcher{matcher})
	if err != nil {
		return nil, fmt.Errorf("failed to convert label matchers: %w", err)
	}

	// Create query request
	req := &client.QueryRequest{
		// Cover all blocks by using extreme timestamps.
		StartTimestampMs: math.MinInt64,
		EndTimestampMs:   math.MaxInt64,
		Matchers:         labelMatchers,
	}

	// Execute query using QueryStream with user context
	ctx := user.InjectOrgID(context.Background(), "anonymous")
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
