// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/user"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/grafana/mimir/pkg/cardinality/cardinalitypb"
	"github.com/grafana/mimir/pkg/distributor"
	"github.com/grafana/mimir/pkg/ingester"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/util/validation"
)

// Two real ingesters, as two replicas of classic replication, both hold one
// series and only the first holds a second one. Their real SeriesCounts walks
// send label-set hashes and the querier's merge counts the shared series
// once. The distributor is replaced by replicatedIngesters, which asks every
// ingester for hashes the way the distributor does when its replication sets
// can't be added up; that decision is covered by the distributor's tests.
func TestCardinalityEstimator_TwoRealIngestersHoldingOneSeries(t *testing.T) {
	const hour = int64(time.Hour / time.Millisecond)
	shared := labels.FromStrings(labels.MetricName, "up", "pod", "a")
	onlyFirst := labels.FromStrings(labels.MetricName, "up", "pod", "b")

	now := time.Now()
	ts := now.Add(-10 * time.Minute).UnixMilli()
	first, second := startRealIngester(t, "ingester-zone-a-0"), startRealIngester(t, "ingester-zone-b-0")
	ctx := user.InjectOrgID(context.Background(), "user-1")
	push := func(i *ingester.Ingester, lsets ...labels.Labels) {
		for _, lset := range lsets {
			req := mimirpb.ToWriteRequest([][]mimirpb.LabelAdapter{mimirpb.FromLabelsToLabelAdapters(lset)}, []mimirpb.Sample{{TimestampMs: ts, Value: 1}}, nil, nil, mimirpb.API)
			_, err := i.Push(ctx, req)
			require.NoError(t, err)
		}
	}
	push(first, shared, onlyFirst)
	push(second, shared)
	ingesters := &replicatedIngesters{ingesters: []*ingester.Ingester{first, second}}

	// The window is after query-store-after, so the ingesters alone answer it.
	finder := &blocksFinderMock{}
	finder.On("GetBlocks", mock.Anything, "user-1", mock.Anything, mock.Anything).Return(bucketindex.Blocks(nil), &bucketindex.Metadata{}, nil)
	q := &BlocksStoreQueryable{compartments: []blocksStoreCompartment{{finder: finder}}, queryStoreAfter: 12 * time.Hour}
	e := NewCardinalityEstimator(q, ingesters, ingestersWithinLimit(13*time.Hour))
	e.now = func() time.Time { return now }
	start := now.Add(-time.Hour).UnixMilli()
	end := now.UnixMilli()

	for name, tc := range map[string]struct {
		groupBy string
		want    map[string][]int64
	}{
		"by metric name": {want: map[string][]int64{"up": {2}}},
		"by pod":         {groupBy: "pod", want: map[string][]int64{"a": {1}, "b": {1}}},
	} {
		t.Run(name, func(t *testing.T) {
			res, err := e.Estimate(ctx, "user-1", &CardinalityEstimateRequest{MinT: start, MaxT: end, GroupBy: tc.groupBy})
			require.NoError(t, err)
			assert.Equal(t, SourceHead, res.Source)
			assert.True(t, res.Dedup)
			assert.Equal(t, tc.want, res.Counts, "the series both ingesters hold counts once")
			assert.Equal(t, int64(3), res.SeriesCounted, "the walks counted three series between them")

			// Each ingester's own walk holds the shared series once, so the
			// merge, not one ingester, is what counts it once.
			require.Len(t, ingesters.got, 2)
			for i, resp := range ingesters.got {
				hashes := map[uint64]int{}
				for _, g := range resp.Groups {
					assert.Empty(t, g.Counts, "an answer with hashes has no counts")
					for _, h := range g.Hashes {
						hashes[h]++
					}
				}
				assert.Equal(t, 1, hashes[labels.StableHash(shared)], "ingester %d", i)
			}
		})
	}

	// Without hashes the same answers add up, so the merge is what deduplicates.
	t.Run("the same answers as counts add up", func(t *testing.T) {
		ingesters.counts = true
		defer func() { ingesters.counts = false }()
		res, err := e.Estimate(ctx, "user-1", &CardinalityEstimateRequest{MinT: start, MaxT: end})
		require.NoError(t, err)
		assert.False(t, res.Dedup)
		assert.Equal(t, map[string][]int64{"up": {3}}, res.Counts)
	})

	// Across the cut, the blocks' hashes are unioned with the ingesters'.
	t.Run("a store-gateway holding the shared series too", func(t *testing.T) {
		b1 := ulid.MustNew(1, nil)
		blockStart := now.Add(-14 * time.Hour).UnixMilli()
		blocks := bucketindex.Blocks{{ID: b1, MinTime: blockStart, MaxTime: blockStart + 2*hour}}
		finder := &blocksFinderMock{}
		finder.On("GetBlocks", mock.Anything, "user-1", mock.Anything, mock.Anything).Return(blocks, &bucketindex.Metadata{}, nil)
		gw := gatewayHoldingSeries("1.1.1.1", labels.StableHash(shared))
		q := &BlocksStoreQueryable{compartments: []blocksStoreCompartment{{finder: finder, stores: &blocksStoreSetMock{mockedResponses: []interface{}{
			map[BlocksStoreClient][][]ulid.ULID{gw: {{b1}}},
		}}}}, queryStoreAfter: 12 * time.Hour}
		e := NewCardinalityEstimator(q, ingesters, ingestersWithinLimit(13*time.Hour))
		e.now = func() time.Time { return now }
		res, err := e.Estimate(ctx, "user-1", &CardinalityEstimateRequest{MinT: blockStart, MaxT: end})
		require.NoError(t, err)
		assert.Equal(t, SourceBlocksAndHead, res.Source)
		assert.Equal(t, map[string][]int64{"up": {2}}, res.Counts, "the shared series is in the block and both ingesters")
	})
}

// replicatedIngesters stands in for the distributor over ingesters that
// replicate each other's series: it calls each one's SeriesCounts and, as the
// distributor does when the answers can't be added up, asks for hashes.
type replicatedIngesters struct {
	ingesters []*ingester.Ingester
	// counts, when set, asks for counts instead, as if the ingesters held disjoint series.
	counts bool
	// got are the last answers.
	got []*client.SeriesCountsResponse
}

func (r *replicatedIngesters) SeriesCounts(ctx context.Context, _ []*labels.Matcher, req *client.SeriesCountsRequest) (distributor.SeriesCountsResult, error) {
	sent := *req
	sent.Hashes = !r.counts
	r.got = nil
	for _, i := range r.ingesters {
		stream := &seriesCountsServerStream{ctx: ctx}
		if err := i.SeriesCounts(&sent, stream); err != nil {
			return distributor.SeriesCountsResult{}, err
		}
		resp, err := cardinalitypb.ReceiveGroups(stream.recv, nil)
		if err != nil {
			return distributor.SeriesCountsResult{}, err
		}
		r.got = append(r.got, resp)
	}
	return distributor.SeriesCountsResult{Responses: r.got, Hashed: sent.Hashes}, nil
}

// seriesCountsServerStream keeps the messages an ingester sends, for recv to
// read back as a client stream would.
type seriesCountsServerStream struct {
	grpc.ServerStream
	ctx  context.Context
	msgs []*client.SeriesCountsResponse
}

func (s *seriesCountsServerStream) Context() context.Context { return s.ctx }

func (s *seriesCountsServerStream) Send(msg *client.SeriesCountsResponse) error {
	s.msgs = append(s.msgs, msg)
	return nil
}

func (s *seriesCountsServerStream) recv() (*client.SeriesCountsResponse, error) {
	if len(s.msgs) == 0 {
		return nil, io.EOF
	}
	msg := s.msgs[0]
	s.msgs = s.msgs[1:]
	return msg, nil
}

// startRealIngester starts an ingester on its own TSDB directory and ring,
// with the cardinality estimate enabled, and stops it when the test ends.
func startRealIngester(t *testing.T, id string) *ingester.Ingester {
	t.Helper()
	kv, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	cfg := ingester.Config{}
	flagext.DefaultValues(&cfg)
	flagext.DefaultValues(&cfg.BlocksStorageConfig)
	flagext.DefaultValues(&cfg.IngestStorageConfig)
	cfg.IngesterRing.KVStore.Mock = kv
	cfg.IngesterRing.NumTokens = 1
	cfg.IngesterRing.ListenPort = 0
	cfg.IngesterRing.InstanceAddr = "localhost"
	cfg.IngesterRing.InstanceID = id
	cfg.IngesterRing.FinalSleep = 0
	cfg.IngesterRing.MinReadyDuration = 0
	cfg.IngesterRing.HeartbeatPeriod = 100 * time.Millisecond
	cfg.BlocksStorageConfig.TSDB.Dir = t.TempDir()
	cfg.BlocksStorageConfig.Bucket.Backend = "filesystem"
	cfg.BlocksStorageConfig.Bucket.Filesystem.Directory = t.TempDir()

	limits := validation.Limits{}
	flagext.DefaultValues(&limits)
	limits.CardinalityEstimateEnabled = true
	overrides := validation.NewOverrides(limits, nil)

	r, err := ring.New(cfg.IngesterRing.ToRingConfig(), ingester.IngesterRingName, ingester.IngesterRingKey, log.NewNopLogger(), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), r))
	t.Cleanup(func() { assert.NoError(t, services.StopAndAwaitTerminated(context.Background(), r)) })

	i, err := ingester.New(cfg, overrides, r, nil, nil, nil, prometheus.NewRegistry(), log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() { assert.NoError(t, services.StopAndAwaitTerminated(context.Background(), i)) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, ring.WaitInstanceState(ctx, r, id, ring.ACTIVE), fmt.Sprintf("ingester %s joins its ring", id))
	return i
}
