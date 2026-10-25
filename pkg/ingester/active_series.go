// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/tenant"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/grafana/mimir/pkg/ingester/activeseries"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

const activeSeriesMaxSizeBytes = 1 * 1024 * 1024

// ActiveSeries implements the ActiveSeries RPC. It returns a stream of active
// series that match the given matchers.
func (i *Ingester) ActiveSeries(request *client.ActiveSeriesRequest, stream client.Ingester_ActiveSeriesServer) (err error) {
	defer func() { err = i.mapReadErrorToErrorWithStatus(err) }()

	spanlog, ctx := spanlogger.New(stream.Context(), i.logger, tracer, "Ingester.ActiveSeries")
	defer spanlog.Finish()

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return err
	}

	matchers, err := client.FromLabelMatchers(request.GetMatchers())
	if err != nil {
		return fmt.Errorf("error parsing label matchers: %w", err)
	}

	// Enforce read consistency before getting TSDB (covers the case the tenant's data has not been ingested
	// in this ingester yet, but there's some to ingest in the backlog).
	if err := i.enforceReadConsistency(ctx, userID); err != nil {
		return err
	}

	db := i.getTSDB(userID)
	if db == nil {
		level.Debug(i.logger).Log("msg", "no TSDB for user", "userID", userID)
		return nil
	}

	idx, err := db.Head().Index()
	if err != nil {
		return fmt.Errorf("error getting index: %w", err)
	}

	isNativeHistogram := request.GetType() == client.NATIVE_HISTOGRAM_SERIES
	if native, ok := idx.(activeSeriesIndex); ok && !db.trackerActive() && !hasShardMatcher(matchers) {
		// An engine that keeps the active series in its series lists them in a pass over the matchers' series, where each
		// was looked up twice, for whether it's active and for its labels.
		return sendActiveSeries(stream, isNativeHistogram, func(visit func([]mimirpb.LabelAdapter, int) error) error {
			return native.ActiveSeriesMatching(ctx, matchers, db.activeCutoff(time.Now()).UnixMilli(), isNativeHistogram, visit)
		})
	}
	postings, err := getPostings(ctx, db, idx, matchers, isNativeHistogram)
	if err != nil {
		return fmt.Errorf("error listing active series: %w", err)
	}

	buf := labels.NewScratchBuilder(10)
	resp := &client.ActiveSeriesResponse{}
	currentSize := 0
	for postings.Next() {
		seriesRef, count := postings.AtBucketCount()
		err = idx.Series(seriesRef, &buf, nil)
		if err != nil {
			// Postings may be stale. Skip if no underlying series exists.
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			return fmt.Errorf("error getting series: %w", err)
		}
		m := &mimirpb.Metric{Labels: mimirpb.FromLabelsToLabelAdapters(buf.Labels())}
		mSize := m.Size()
		if isNativeHistogram {
			mSize += 8 // 8 bytes for the bucket count.
		}
		if currentSize+mSize > activeSeriesMaxSizeBytes {
			if err := client.SendActiveSeriesResponse(stream, resp); err != nil {
				return fmt.Errorf("error sending response: %w", err)
			}
			resp = &client.ActiveSeriesResponse{}
			currentSize = 0
		}
		resp.Metric = append(resp.Metric, m)
		if isNativeHistogram {
			resp.BucketCount = append(resp.BucketCount, uint64(count))
		}
		currentSize += mSize
	}
	if err := postings.Err(); err != nil {
		return fmt.Errorf("error iterating over series: %w", err)
	}

	if len(resp.Metric) > 0 {
		if err := client.SendActiveSeriesResponse(stream, resp); err != nil {
			return fmt.Errorf("error sending response: %w", err)
		}
	}

	return nil
}

func getPostings(ctx context.Context, db *userTSDB, idx tsdb.IndexReader, matchers []*labels.Matcher, isNativeHistogram bool) (activeseries.BucketCountPostings, error) {
	if db.activeSeries == nil {
		return nil, fmt.Errorf("active series tracker is not initialized")
	}

	shard, matchers, err := sharding.RemoveShardFromMatchers(matchers)
	if err != nil {
		return nil, fmt.Errorf("error removing shard matcher: %w", err)
	}

	var postings index.Postings
	if shard != nil && matchAllSeries(matchers) {
		postings = db.Head().ShardSeries(shard.ShardIndex, shard.ShardCount)
	} else {
		if native, ok := idx.(nativeMatchersIndex); ok {
			// An index that selects by matchers itself doesn't read every series with a label for each regex or negation.
			postings, err = native.PostingsForMatchersNative(ctx, matchers...)
		} else {
			postings, err = tsdb.PostingsForMatchers(ctx, idx, matchers...)
		}
		if err != nil {
			return nil, fmt.Errorf("error getting postings: %w", err)
		}
		if shard != nil {
			postings = idx.ShardedPostings(postings, shard.ShardIndex, shard.ShardCount)
		}
	}

	activeRefs := db.activeRefs(time.Now())
	if isNativeHistogram {
		return activeseries.NewNativeHistogramPostings(activeRefs, postings), nil
	}

	return &ZeroBucketCountPostings{*activeseries.NewPostings(activeRefs, postings)}, nil
}

// activeSeriesIndex is an index of an engine that keeps the active series in its series, which lists the ones matching
// matchers that were ingested since cutoffMs, with their native histogram bucket counts.
type activeSeriesIndex interface {
	ActiveSeriesMatching(ctx context.Context, matchers []*labels.Matcher, cutoffMs int64, nativeHistograms bool, visit func(lbls []mimirpb.LabelAdapter, buckets int) error) error
}

func hasShardMatcher(matchers []*labels.Matcher) bool {
	for _, m := range matchers {
		if m.Name == sharding.ShardLabel {
			return true
		}
	}
	return false
}

// sendActiveSeries sends the series list visits as the active series API does, in messages of at most
// activeSeriesMaxSizeBytes.
func sendActiveSeries(stream client.Ingester_ActiveSeriesServer, isNativeHistogram bool, list func(visit func([]mimirpb.LabelAdapter, int) error) error) error {
	scratch := takeActiveSeriesScratch()
	defer scratch.give()
	// A response's metrics and labels are copied into the scratch's slices, which the next response reuses from the
	// start once it is sent, which encodes it.
	resp := &client.ActiveSeriesResponse{Metric: scratch.pointers[:0], BucketCount: scratch.buckets[:0]}
	adapters, metrics := scratch.adapters[:0], scratch.metrics[:0]
	keep := func() {
		scratch.pointers, scratch.buckets = held(scratch.pointers, resp.Metric), held(scratch.buckets, resp.BucketCount)
		scratch.adapters, scratch.metrics = held(scratch.adapters, adapters), held(scratch.metrics, metrics)
	}
	defer keep()
	currentSize := 0
	err := list(func(lbls []mimirpb.LabelAdapter, buckets int) error {
		mSize := (&mimirpb.Metric{Labels: lbls}).Size()
		if isNativeHistogram {
			mSize += 8 // 8 bytes for the bucket count.
		}
		if currentSize+mSize > activeSeriesMaxSizeBytes {
			if err := client.SendActiveSeriesResponse(stream, resp); err != nil {
				return fmt.Errorf("error sending response: %w", err)
			}
			keep()
			resp = &client.ActiveSeriesResponse{Metric: resp.Metric[:0], BucketCount: resp.BucketCount[:0]}
			adapters, metrics = adapters[:0], metrics[:0]
			currentSize = 0
		}
		// The labels are the engine's only until visit returns. Growing a slice leaves the metrics already in the
		// response pointing into the one it had.
		start := len(adapters)
		adapters = append(adapters, lbls...)
		metrics = append(metrics, mimirpb.Metric{Labels: adapters[start:len(adapters):len(adapters)]})
		resp.Metric = append(resp.Metric, &metrics[len(metrics)-1])
		if isNativeHistogram {
			resp.BucketCount = append(resp.BucketCount, uint64(buckets))
		}
		currentSize += mSize
		return nil
	})
	if err != nil {
		return fmt.Errorf("error listing active series: %w", err)
	}
	if len(resp.Metric) > 0 {
		if err := client.SendActiveSeriesResponse(stream, resp); err != nil {
			return fmt.Errorf("error sending response: %w", err)
		}
	}
	return nil
}

// activeSeriesScratch holds the slices an active series response is built in, kept between responses: growing them
// for each was most of what an engine's listing allocated.
type activeSeriesScratch struct {
	pointers []*mimirpb.Metric
	buckets  []uint64
	adapters []mimirpb.LabelAdapter
	metrics  []mimirpb.Metric
}

var activeSeriesScratches sync.Pool

// maxKeptActiveSeriesScratch is how many labels a kept scratch holds at most: what a response of a full size has of
// series with short labels, which one of a large listing grows to as it is reused for each response.
const maxKeptActiveSeriesScratch = activeSeriesMaxSizeBytes / 8

func takeActiveSeriesScratch() *activeSeriesScratch {
	if scratch, ok := activeSeriesScratches.Get().(*activeSeriesScratch); ok {
		return scratch
	}
	return &activeSeriesScratch{}
}

// give keeps the slices for another response, unless they are large, cleared as far as a response wrote them: they
// point into one tenant's labels.
func (s *activeSeriesScratch) give() {
	if cap(s.adapters) > maxKeptActiveSeriesScratch {
		return
	}
	clear(s.pointers)
	clear(s.adapters)
	clear(s.metrics)
	s.pointers, s.buckets, s.adapters, s.metrics = s.pointers[:0], s.buckets[:0], s.adapters[:0], s.metrics[:0]
	activeSeriesScratches.Put(s)
}

// activeLabelValueIndex is an index of an engine that keeps the active series in its series, which tells whether a series
// with the label's value was ingested since cutoffMs.
type activeLabelValueIndex interface {
	IsLabelValueActive(ctx context.Context, name, value string, cutoffMs int64) (bool, error)
}

// nativeMatchersIndex is an index that has its own selection by matchers, better than assembling one from its postings.
type nativeMatchersIndex interface {
	PostingsForMatchersNative(ctx context.Context, matchers ...*labels.Matcher) (index.Postings, error)
}

// Check if matchers will match every series. Not an exhaustive check; just some common examples seen in the wild.
func matchAllSeries(matchers []*labels.Matcher) bool {
	if len(matchers) != 1 {
		return false
	}
	if matchers[0].Type == labels.MatchRegexp && matchers[0].Value == ".*" {
		return true
	}
	// Every metric in Mimir has a __name__
	if matchers[0].Name == model.MetricNameLabel && matchers[0].Type == labels.MatchRegexp && matchers[0].Value == ".+" {
		return true
	}
	if matchers[0].Name == model.MetricNameLabel && matchers[0].Type == labels.MatchNotEqual && matchers[0].Value == "" {
		return true
	}
	return false
}

type ZeroBucketCountPostings struct {
	activeseries.Postings
}

func (z *ZeroBucketCountPostings) AtBucketCount() (storage.SeriesRef, int) {
	return z.At(), 0
}

// Type check.
var _ index.Postings = &ZeroBucketCountPostings{}
var _ activeseries.BucketCountPostings = &ZeroBucketCountPostings{}
