// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"math"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
)

// slowSeriesStream is a QueryStream server stream that pauses after the series labels have been sent.
// It emulates what happens in production, where the chunks of a series are read from the head some
// time after its chunk refs were collected, because the series stream is large and the consumer is slow.
type slowSeriesStream struct {
	stream
	gap time.Duration
}

func (s *slowSeriesStream) Send(response *client.QueryStreamResponse) error {
	if response.IsEndOfSeriesStream {
		time.Sleep(s.gap)
	}
	return s.stream.Send(response)
}

// TestIngester_compactBlocksDueToNonOwnedSeries_ConcurrentQueryStream reproduces the failure seen in
// production while an ingester evicts non-owned series from its head:
//
//	cannot populate chunk <ref> from block 0000000000XXXXXXXRANGEHEAD: not found
//
// The early compaction first writes the non-owned series into a local block, then waits for the
// queries that were already open to finish, and finally removes the series from the head. Queries that
// are opened after that wait has finished are not waited for. If such a query collects the chunk refs of a
// series from the head and the series is removed before its chunks are read, reading the chunk fails.
//
// The test keeps querying the ingester through the real QueryStream while the real early compaction
// runs, and requires that no query fails. The data is available all along, in the head or in the local
// block, so a correct ingester must never fail these queries.
func TestIngester_compactBlocksDueToNonOwnedSeries_ConcurrentQueryStream(t *testing.T) {
	const (
		numSeries   = 100_000
		numShards   = 1000
		numReaders  = 4
		maxPause    = 60 * time.Millisecond
		streamDelay = 30 * time.Millisecond
	)

	var (
		ctx         = context.Background()
		ctxWithUser = user.InjectOrgID(ctx, userID)
	)

	cfg := defaultIngesterTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval = time.Hour
	cfg.UpdateIngesterOwnedSeries = true
	cfg.EarlyCompactionNonOwnedSeriesEnabled = true
	cfg.EarlyCompactionNonOwnedSeriesMinGracePeriod = 0 // Evict immediately.

	limits := defaultLimitsTestConfig()
	limits.EarlyHeadCompactionOwnedSeriesThreshold = 1
	limits.MaxGlobalSeriesPerUser = 0
	limits.IngestionRate = 0

	ingesters := setupTestIngesterRing(t, []string{"zone-a", "zone-b", "zone-c"}, 1, cfg, limits)
	ingester := ingesters[0]

	// Push two samples per series, so that the head's MinTime is lower than its MaxTime.
	sampleTime, err := time.Parse(time.RFC3339, "2026-05-05T00:00:00Z")
	require.NoError(t, err)
	t1 := sampleTime.UnixMilli()

	for ts := t1; ts <= t1+1; ts++ {
		for start := 0; start < numSeries; start += 5000 {
			lbls := make([][]mimirpb.LabelAdapter, 0, 5000)
			samples := make([]mimirpb.Sample, 0, 5000)
			for i := start; i < start+5000; i++ {
				lbls = append(lbls, mimirpb.FromLabelsToLabelAdapters(labels.FromStrings(
					model.MetricNameLabel, "test_metric",
					"id", strconv.Itoa(i),
					"shard", strconv.Itoa(i%numShards),
				)))
				samples = append(samples, mimirpb.Sample{TimestampMs: ts, Value: float64(i)})
			}
			_, err := ingester.Push(ctxWithUser, mimirpb.ToWriteRequest(lbls, samples, nil, nil, mimirpb.API))
			require.NoError(t, err)
		}
	}

	db := ingester.getTSDB(userID)
	require.NotNil(t, db)
	require.Equal(t, uint64(numSeries), db.Head().NumSeries())

	// The ring changed: the ingester now only owns about 10% of the series. The rest becomes non-owned.
	db.ownedTokenRanges = ring.TokenRanges{0, math.MaxUint32 / 10}
	require.True(t, db.recomputeOwnedSeries(0, "test", log.NewNopLogger()), "recomputeOwnedSeries should succeed")
	owned := db.ownedSeriesState().ownedSeriesCount
	require.Greater(t, owned, 0)
	require.Less(t, owned, numSeries)
	t.Logf("series in head: %d, owned: %d, non-owned (to be evicted): %d", numSeries, owned, numSeries-owned)

	var (
		start    = time.Now()
		done     = make(chan struct{})
		mtx      sync.Mutex
		queries  int
		failures []string
		readers  sync.WaitGroup
	)

	// Keep querying the ingester, like the queriers do, while the compaction runs. Readers pause
	// between queries, so that there are moments where no query is open (the compaction needs them to
	// evict the series).
	for r := 0; r < numReaders; r++ {
		readers.Add(1)
		go func(seed int64) {
			defer readers.Done()
			rnd := rand.New(rand.NewSource(seed))
			for {
				select {
				case <-done:
					return
				default:
				}

				s := &slowSeriesStream{stream: stream{ctx: ctxWithUser}, gap: streamDelay}
				err := ingester.QueryStream(&client.QueryRequest{
					StartTimestampMs: math.MinInt64,
					EndTimestampMs:   math.MaxInt64,
					Matchers:         []*client.LabelMatcher{{Type: client.EQUAL, Name: "shard", Value: strconv.Itoa(rnd.Intn(numShards))}},
				}, s)

				mtx.Lock()
				queries++
				if err != nil {
					failures = append(failures, "t+"+time.Since(start).Round(time.Millisecond).String()+": "+err.Error())
				}
				mtx.Unlock()

				time.Sleep(time.Duration(rnd.Int63n(int64(maxPause))))
			}
		}(int64(r))
	}

	// Run the early compaction of non-owned series (the code path used in production).
	compactionStart := time.Since(start)
	ingester.compactBlocksDueToNonOwnedSeries(ctx, 0)
	compactionEnd := time.Since(start)
	close(done)
	readers.Wait()

	t.Logf("early compaction of non-owned series ran from t+%s to t+%s, head series after: %d",
		compactionStart.Round(time.Millisecond), compactionEnd.Round(time.Millisecond), db.Head().NumSeries())
	require.Less(t, db.Head().NumSeries(), uint64(numSeries), "the non-owned series should have been evicted")

	mtx.Lock()
	defer mtx.Unlock()
	t.Logf("queries run while the compaction was in progress: %d, failed: %d", queries, len(failures))
	for _, f := range failures {
		t.Logf("  %s", f)
	}
	require.Empty(t, failures, "queries must not fail while non-owned series are being evicted from the head")
}
