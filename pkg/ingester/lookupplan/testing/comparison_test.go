// SPDX-License-Identifier: AGPL-3.0-only

package testing

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/streamingpromql/benchmarks"
	"github.com/grafana/mimir/pkg/util/bench"
)

// detailedQueryResult contains detailed information about query results including the actual series data.
type detailedQueryResult struct {
	SeriesCount int
	ChunksCount int
	Series      []seriesWithChunks
}

// seriesWithChunks holds a series and its chunks for comparison.
type seriesWithChunks struct {
	Labels     labels.Labels
	ChunkCount int
	Chunks     []chunkInfo
}

// chunkInfo holds information about a single chunk.
type chunkInfo struct {
	MinTime  int64
	MaxTime  int64
	Encoding int32
	Data     []byte
}

// mockDetailedQueryStreamServer captures full query results for comparison.
type mockDetailedQueryStreamServer struct {
	client.Ingester_QueryStreamServer
	result                *detailedQueryResult
	ctx                   context.Context
	seenEndOfSeriesStream bool
}

func (m *mockDetailedQueryStreamServer) Send(resp *client.QueryStreamResponse) error {
	// First, process series metadata
	if len(resp.StreamingSeries) > 0 {
		for _, s := range resp.StreamingSeries {
			m.result.SeriesCount++
			m.result.ChunksCount += int(s.ChunkCount)

			// Parse labels
			lbls := mimirpb.FromLabelAdaptersToLabels(s.Labels)

			series := seriesWithChunks{
				Labels:     lbls,
				ChunkCount: int(s.ChunkCount),
				Chunks:     make([]chunkInfo, 0, s.ChunkCount),
			}

			m.result.Series = append(m.result.Series, series)
		}
	}

	// Then, process chunk data
	if len(resp.StreamingSeriesChunks) > 0 {
		for _, sc := range resp.StreamingSeriesChunks {
			// SeriesIndex refers to the series in the order they were sent
			if int(sc.SeriesIndex) >= len(m.result.Series) {
				return fmt.Errorf("invalid series index %d (have %d series)", sc.SeriesIndex, len(m.result.Series))
			}

			// Store chunk information
			for _, chunk := range sc.Chunks {
				// Make a copy of the chunk data since it may be in a reused buffer
				dataCopy := make([]byte, len(chunk.Data))
				copy(dataCopy, chunk.Data)

				chunkInfo := chunkInfo{
					MinTime:  chunk.StartTimestampMs,
					MaxTime:  chunk.EndTimestampMs,
					Encoding: chunk.Encoding,
					Data:     dataCopy,
				}
				m.result.Series[sc.SeriesIndex].Chunks = append(m.result.Series[sc.SeriesIndex].Chunks, chunkInfo)
			}
		}
	}

	if resp.IsEndOfSeriesStream {
		m.seenEndOfSeriesStream = true
	}

	return nil
}

func (m *mockDetailedQueryStreamServer) Context() context.Context {
	return m.ctx
}

// TestCompareIngesterConfigs runs queries against two ingesters with different configurations
// and asserts that they return the same results.
func TestCompareIngesterConfigs(t *testing.T) {
	if *dataDirFlag == "" && *queryFileFlag == "" {
		t.Logf("Skipping %s since -data-dir and -query-file flags are not set", t.Name())
		t.SkipNow()
	}

	// Validate mutual exclusivity of query-ids and query-sample
	if *queryIDsFlag != "" && *querySampleFlag < 1.0 {
		t.Fatal("-query-ids and -query-sample < 1.0 are mutually exclusive")
	}

	// Parse query IDs if provided
	var queryIDs []int
	var err error
	if *queryIDsFlag != "" {
		queryIDs, err = parseQueryIDs(*queryIDsFlag)
		require.NoError(t, err)
	}

	queries, stats, err := queryLoader.PrepareQueries(bench.QueryLoaderConfig{
		Filepath:       *queryFileFlag,
		TenantID:       *tenantIDFlag,
		QueryIDs:       queryIDs,
		SampleFraction: *querySampleFlag,
		Seed:           *querySampleSeed,
	})
	require.NoError(t, err)
	require.NotEmpty(t, queries, "no queries after filtering and sampling")
	t.Logf("Prepared %d queries (malformed: %d, sampled: %f%% of all queries)", len(queries), stats.MalformedLines, *querySampleFlag*100)

	// Create a copy of the data directory for the second ingester. Both ingesters can't use the same data directory.
	tempDir := t.TempDir()
	dataDirCopy := filepath.Join(tempDir, "data-copy")
	t.Logf("Copying data directory to %s", dataDirCopy)
	err = copyDir(*dataDirFlag, dataDirCopy)
	require.NoError(t, err)
	t.Log("Data directory copied")

	ingesterStartTime := time.Now()
	ing1, _, cleanup1, err := benchmarks.StartBenchmarkIngester(dataDirCopy, func(config *ingester.Config) {
		config.BlocksStorageConfig.TSDB.IndexLookupPlanning.Enabled = true
	})
	require.NoError(t, err)
	t.Cleanup(cleanup1)
	t.Logf("Started ingester 1 in %s", time.Since(ingesterStartTime))

	ingesterStartTime = time.Now()
	ing2, _, cleanup2, err := benchmarks.StartBenchmarkIngester(*dataDirFlag, func(config *ingester.Config) {
		config.BlocksStorageConfig.TSDB.IndexLookupPlanning.Enabled = false
	})
	require.NoError(t, err)
	t.Cleanup(cleanup2)
	t.Logf("Started ingester 2 in %s", time.Since(ingesterStartTime))

	// Run queries against both ingesters and compare results
	for _, q := range queries {
		for selectorIdx := range q.VectorSelectors {
			testName := fmt.Sprintf("query=%d/selector=%d", q.QueryID, selectorIdx)
			t.Run(testName, func(t *testing.T) {
				// Query first ingester
				result1, err := queryIngesterDetailed(ing1, q.VectorSelectors[selectorIdx], q.User)
				require.NoError(t, err, "Query failed on ingester 1: %s", q.Query)

				// Query second ingester
				result2, err := queryIngesterDetailed(ing2, q.VectorSelectors[selectorIdx], q.User)
				require.NoError(t, err, "Query failed on ingester 2: %s", q.Query)

				require.Equalf(t, result1.SeriesCount, result2.SeriesCount, "Series count mismatch for query: %s", q.Query)
				require.Equalf(t, result1.ChunksCount, result2.ChunksCount, "Chunks count mismatch for query: %s", q.Query)
				require.Equalf(t, len(result1.Series), len(result2.Series), "Number of series mismatch for query: %s", q.Query)

				// Compare each series - order must be the same between both ingesters
				for i := 0; i < len(result1.Series); i++ {
					s1 := result1.Series[i]
					s2 := result2.Series[i]

					require.Truef(t, labels.Equal(s1.Labels, s2.Labels), "Labels mismatch at series %d for query: %s\ningester1: %s\ningester2: %s", i, q.Query, s1.Labels.String(), s2.Labels.String())
					require.Equalf(t, s1.ChunkCount, s2.ChunkCount, "Chunk count mismatch for series %s in query: %s", s1.Labels.String(), q.Query)
					require.Equalf(t, len(s1.Chunks), len(s2.Chunks), "Number of chunks mismatch for series %s in query: %s", s1.Labels.String(), q.Query)

					// Compare chunks
					for j := 0; j < len(s1.Chunks); j++ {
						c1 := s1.Chunks[j]
						c2 := s2.Chunks[j]

						require.Equalf(t, c1.MinTime, c2.MinTime, "Chunk %d MinTime mismatch for series %s in query: %s", j, s1.Labels.String(), q.Query)
						require.Equalf(t, c1.MaxTime, c2.MaxTime, "Chunk %d MaxTime mismatch for series %s in query: %s", j, s1.Labels.String(), q.Query)
						require.Equalf(t, c1.Encoding, c2.Encoding, "Chunk %d Encoding mismatch for series %s in query: %s", j, s1.Labels.String(), q.Query)
						require.Equalf(t, c1.Data, c2.Data, "Chunk %d Data mismatch for series %s in query: %s (len1=%d, len2=%d)", j, s1.Labels.String(), q.Query, len(c1.Data), len(c2.Data))
					}
				}
			})
		}
	}
}

// queryIngesterDetailed executes a query and returns detailed results for comparison.
func queryIngesterDetailed(ing *ingester.Ingester, matchers []*labels.Matcher, userID string) (*detailedQueryResult, error) {
	// Convert to client format
	labelMatchers, err := client.ToLabelMatchers(matchers)
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

	ctx := user.InjectOrgID(context.Background(), userID)

	// Create mock stream that captures full results
	result := &detailedQueryResult{
		Series: make([]seriesWithChunks, 0),
	}
	mockStream := &mockDetailedQueryStreamServer{
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

// copyDir recursively copies a directory tree.
func copyDir(src, dst string) error {
	if err := os.CopyFS(dst, os.DirFS(src)); err != nil {
		return fmt.Errorf("failed to copy directory: %w", err)
	}
	return nil
}
