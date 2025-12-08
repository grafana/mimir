// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"fmt"
	"slices"

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
		postings = getShardedAllPostings(ctx, idx, shard.ShardIndex, shard.ShardCount)
	} else {
		postings, err = tsdb.PostingsForMatchers(ctx, idx, matchers...)
		if err != nil {
			return nil, fmt.Errorf("error getting postings: %w", err)
		}
		if shard != nil {
			postings = idx.ShardedPostings(postings, shard.ShardIndex, shard.ShardCount)
		}
	}

	if isNativeHistogram {
		return activeseries.NewNativeHistogramPostings(db.activeSeries, postings), nil
	}

	return &ZeroBucketCountPostings{*activeseries.NewPostings(db.activeSeries, postings)}, nil
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

// Get postings for all series in one shard. idx must implement ForEachShardHash.
func getShardedAllPostings(ctx context.Context, idx tsdb.IndexReader, shardIndex, shardCount uint64) index.Postings {
	type forEachShardHasher interface {
		ForEachShardHash(fn func(ref []storage.SeriesRef, shardHash []uint64))
	}
	sidx := idx.(forEachShardHasher)
	out := make([]storage.SeriesRef, 0, 128)
	sidx.ForEachShardHash(func(ref []storage.SeriesRef, shardHash []uint64) {
		for i := range ref {
			if shardHash[i]%shardCount == shardIndex {
				out = append(out, ref[i])
			}
		}
	})
	slices.Sort(out) // We don't really need this for usage inside this module, but it's safer to conform to Postings norms.
	return index.NewListPostings(out)
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
