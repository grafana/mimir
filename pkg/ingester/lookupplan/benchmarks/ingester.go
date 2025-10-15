// SPDX-License-Identifier: AGPL-3.0-only

package benchmarks

import (
	"context"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/prometheus/model/labels"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/streamingpromql/benchmarks"
)

// StartIngesterAndLoadBlocks starts a local ingester with blocks automatically loaded
// from the specified rootDataDir. This is a simplified wrapper around the existing
// StartIngesterAndLoadData function that reuses the ingester's automatic block discovery.
//
// Parameters:
//   - rootDataDir: Directory containing blocks to load (passed as ingester data directory)
//   - blockDirs: Currently unused, blocks are discovered automatically from rootDataDir
//
// Returns:
//   - address: gRPC address of the started ingester
//   - cleanup: Function to call to stop the ingester and cleanup resources
//   - error: Any error that occurred during startup
func StartIngesterAndLoadBlocks(rootDataDir string, blockDirs []string) (string, func(), error) {
	// Reuse existing function with empty metric sizes - no synthetic data needed
	// The ingester will automatically discover and load existing blocks from rootDataDir
	// via openExistingTSDB() method during startup
	return benchmarks.StartIngesterAndLoadData(rootDataDir, []int{})
}

// QueryResult contains the result of executing a query against the ingester.
type QueryResult struct {
	SeriesCount int
	SampleCount int
	Duration    time.Duration
}

// ExecuteQuery executes a query against the ingester at the given address.
func ExecuteQuery(ingesterAddress string, labelName, labelValue string) (*QueryResult, error) {
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
	result := &QueryResult{}
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
