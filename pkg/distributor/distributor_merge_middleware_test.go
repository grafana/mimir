// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"slices"
	"testing"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/validation"
)

// newMergeTestDistributor builds a distributor suitable for exercising
// prePushMergeMiddleware in isolation. The middleware is stateless with respect
// to the distributor, so the default configuration is enough and a single
// instance can be reused across subtests.
func newMergeTestDistributor(t *testing.T) *Distributor {
	t.Helper()

	var limits validation.Limits
	flagext.DefaultValues(&limits)
	ds, _, _, _ := prepare(t, prepConfig{
		numDistributors: 1,
		limits:          &limits,
	})
	return ds[0]
}

// runPrePushMerge runs req through prePushMergeMiddleware and returns the write
// request that reaches the next PushFunc. The middleware mutates req in place,
// so the returned value shares req's backing. It also asserts the WithCleanup
// contract shared by all prePush* middlewares: the request cleanup runs exactly
// once (mirrors TestSortAndFilterMiddleware).
func runPrePushMerge(t *testing.T, d *Distributor, req *mimirpb.WriteRequest) *mimirpb.WriteRequest {
	t.Helper()

	cleanupCount := 0
	var got *mimirpb.WriteRequest
	next := func(_ context.Context, pushReq *Request) error {
		r, err := pushReq.WriteRequest()
		require.NoError(t, err)
		got = r
		pushReq.CleanUp()
		// If the middleware's WithCleanup wrapper cleans up again, this fires.
		pushReq.AddCleanup(func() { assert.Fail(t, "cleanup called twice") })
		return nil
	}

	ctx := user.InjectOrgID(context.Background(), "user")
	pushReq := NewParsedRequest(req, req.Size())
	pushReq.AddCleanup(func() { cleanupCount++ })
	require.NoError(t, d.prePushMergeMiddleware(next)(ctx, pushReq))
	assert.Equal(t, 1, cleanupCount, "request cleanup must run exactly once")
	return got
}

func exemplarTraceID(e mimirpb.Exemplar) string {
	for _, l := range e.Labels {
		if l.Name == "trace_id" {
			return l.Value
		}
	}
	return ""
}

// timeseriesByMetricName returns the single timeseries whose __name__ matches,
// so assertions don't depend on the post-merge ordering of req.Timeseries.
func timeseriesByMetricName(t *testing.T, req *mimirpb.WriteRequest, name string) mimirpb.PreallocTimeseries {
	t.Helper()

	var found []mimirpb.PreallocTimeseries
	for _, ts := range req.Timeseries {
		for _, l := range ts.Labels {
			if l.Name == model.MetricNameLabel && l.Value == name {
				found = append(found, ts)
			}
		}
	}
	require.Lenf(t, found, 1, "expected exactly one timeseries named %q", name)
	return found[0]
}

func sampleTimestamps(ts mimirpb.PreallocTimeseries) []int64 {
	out := make([]int64, 0, len(ts.Samples))
	for _, s := range ts.Samples {
		out = append(out, s.TimestampMs)
	}
	return out
}

func makeTimeseriesWithCT(lbls []string, samples []mimirpb.Sample, createdTimestamp int64) mimirpb.PreallocTimeseries {
	ts := makeTimeseries(lbls, samples, nil, nil)
	ts.CreatedTimestamp = createdTimestamp
	return ts
}

// labelsWithNonStableHashCollision returns two DIFFERENT label sets that produce
// the same mimirpb.NonStableHash — the hash prePushMergeMiddleware keys on. They
// were found with https://github.com/pstibrany/labels_hash_collisions. Because
// NonStableHash hashes the label adapters directly (name\xffvalue\xff, matching
// the slicelabels labels.Labels.Hash), the collision holds regardless of the
// labels build tag.
func labelsWithNonStableHashCollision() ([]mimirpb.LabelAdapter, []mimirpb.LabelAdapter) {
	ls1 := labelAdapters("__name__", "metric", "lbl1", "value", "lbl2", "l6CQ5y")
	ls2 := labelAdapters("__name__", "metric", "lbl1", "value", "lbl2", "v7uDlF")
	if mimirpb.NonStableHash(ls1) != mimirpb.NonStableHash(ls2) {
		panic("This code needs to be updated: find new labels with colliding NonStableHash values.")
	}
	return ls1, ls2
}

func TestDistributor_prePushMergeMiddleware(t *testing.T) {
	d := newMergeTestDistributor(t)

	t.Run("merges samples across identical label sets", func(t *testing.T) {
		lbls := []string{model.MetricNameLabel, "series_1"}
		req := &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{
			makeTimeseries(lbls, makeSamples(10, 1), nil, nil),
			makeTimeseries(lbls, makeSamples(20, 2), nil, nil),
		}}

		got := runPrePushMerge(t, d, req)

		require.Len(t, got.Timeseries, 1)
		assert.Equal(t, mimirpb.FromLabelsToLabelAdapters(labels.FromStrings(lbls...)), got.Timeseries[0].Labels)
		// The later object's samples are appended to the first; the existing
		// within-timeseries dedup handles any timestamp collisions downstream.
		assert.Equal(t, []int64{10, 20}, sampleTimestamps(got.Timeseries[0]))
	})

	t.Run("merges histograms and exemplars and keeps exemplar labels valid after pooled reuse", func(t *testing.T) {
		lbls := []string{model.MetricNameLabel, "series_1"}
		req := &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{
			makeTimeseries(lbls, makeSamples(10, 1), makeHistograms(11, generateTestHistogram(1)), makeExemplars([]string{"trace_id", "a"}, 1, 1)),
			makeTimeseries(lbls, makeSamples(20, 2), makeHistograms(21, generateTestHistogram(1)), makeExemplars([]string{"trace_id", "b"}, 1, 1)),
		}}

		got := runPrePushMerge(t, d, req)

		require.Len(t, got.Timeseries, 1)
		assert.Len(t, got.Timeseries[0].Samples, 2)
		assert.Len(t, got.Timeseries[0].Histograms, 2)
		require.Len(t, got.Timeseries[0].Exemplars, 2)

		// Pool safety: the merged-in (duplicate) timeseries is returned to the
		// pool inside the middleware. Its exemplars were shallow-appended into
		// the surviving series, so they share the same backing label strings. If
		// the middleware didn't nil the duplicate's slices before reuse,
		// ReuseTimeseries -> ClearExemplars would zero those label strings in
		// place, corrupting the survivor's exemplars. Assert they're intact.
		gotTraceIDs := []string{
			exemplarTraceID(got.Timeseries[0].Exemplars[0]),
			exemplarTraceID(got.Timeseries[0].Exemplars[1]),
		}
		assert.ElementsMatch(t, []string{"a", "b"}, gotTraceIDs)
	})

	t.Run("merges a duplicate that carries only a native histogram", func(t *testing.T) {
		// The duplicate has no float samples, exercising the len(ts.Samples) == 0
		// branch of the merge: the histogram must still be folded in and the
		// duplicate removed.
		lbls := []string{model.MetricNameLabel, "series_1"}
		req := &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{
			makeTimeseries(lbls, makeSamples(10, 1), nil, nil),
			makeTimeseries(lbls, nil, makeHistograms(21, generateTestHistogram(1)), nil),
		}}

		got := runPrePushMerge(t, d, req)

		require.Len(t, got.Timeseries, 1)
		assert.Equal(t, []int64{10}, sampleTimestamps(got.Timeseries[0]))
		assert.Len(t, got.Timeseries[0].Histograms, 1)
	})

	t.Run("merges identical label sets sharing a created timestamp", func(t *testing.T) {
		lbls := []string{model.MetricNameLabel, "series_1"}
		req := &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{
			makeTimeseriesWithCT(lbls, makeSamples(100, 1), 42),
			makeTimeseriesWithCT(lbls, makeSamples(200, 2), 42),
		}}

		got := runPrePushMerge(t, d, req)

		require.Len(t, got.Timeseries, 1)
		assert.Equal(t, int64(42), got.Timeseries[0].CreatedTimestamp)
		assert.Equal(t, []int64{100, 200}, sampleTimestamps(got.Timeseries[0]))
	})

	t.Run("does not merge identical label sets with different created timestamps", func(t *testing.T) {
		// The ingester injects a created-timestamp zero sample per object from
		// ts.CreatedTimestamp, so the created timestamp is part of the merge
		// identity. OTLP created-timestamp handling emits one object per distinct
		// created timestamp for a label set; those objects must stay separate so
		// each still triggers its own zero-sample ingestion downstream.
		lbls := []string{model.MetricNameLabel, "series_1"}
		req := &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{
			makeTimeseriesWithCT(lbls, makeSamples(100, 1), 10),
			makeTimeseriesWithCT(lbls, makeSamples(200, 2), 20),
			makeTimeseriesWithCT(lbls, makeSamples(300, 3), 10),
		}}

		got := runPrePushMerge(t, d, req)

		// CT=10 objects merge together; the CT=20 object stays separate.
		require.Len(t, got.Timeseries, 2)
		byCT := map[int64][]int64{}
		for _, ts := range got.Timeseries {
			byCT[ts.CreatedTimestamp] = sampleTimestamps(ts)
		}
		assert.Equal(t, []int64{100, 300}, byCT[10])
		assert.Equal(t, []int64{200}, byCT[20])
	})

	t.Run("folds multiple duplicate label sets into the first occurrence", func(t *testing.T) {
		seriesA := []string{model.MetricNameLabel, "series_a"}
		seriesB := []string{model.MetricNameLabel, "series_b"}
		req := &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{
			makeTimeseries(seriesA, makeSamples(1, 1), nil, nil),
			makeTimeseries(seriesB, makeSamples(2, 2), nil, nil),
			makeTimeseries(seriesA, makeSamples(3, 3), nil, nil),
			makeTimeseries(seriesA, makeSamples(4, 4), nil, nil),
		}}

		got := runPrePushMerge(t, d, req)

		require.Len(t, got.Timeseries, 2)
		assert.Equal(t, []int64{1, 3, 4}, sampleTimestamps(timeseriesByMetricName(t, got, "series_a")))
		assert.Equal(t, []int64{2}, sampleTimestamps(timeseriesByMetricName(t, got, "series_b")))
	})

	t.Run("preserves distinct label sets", func(t *testing.T) {
		// Distinct label sets must never be merged. This pins the common case where
		// different labels also produce different hashes; the same-hash collision
		// case is covered by "merges duplicates whose label sets share a hash".
		req := &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{
			makeTimeseries([]string{model.MetricNameLabel, "series_1"}, makeSamples(1, 1), nil, nil),
			makeTimeseries([]string{model.MetricNameLabel, "series_2"}, makeSamples(2, 2), nil, nil),
			makeTimeseries([]string{model.MetricNameLabel, "series_1", "zone", "a"}, makeSamples(3, 3), nil, nil),
		}}

		got := runPrePushMerge(t, d, req)

		require.Len(t, got.Timeseries, 3)
		assert.Equal(t, []int64{1}, sampleTimestamps(got.Timeseries[0]))
		assert.Equal(t, []int64{2}, sampleTimestamps(got.Timeseries[1]))
		assert.Equal(t, []int64{3}, sampleTimestamps(got.Timeseries[2]))
	})

	t.Run("merges duplicates whose label sets share a hash", func(t *testing.T) {
		// c1 and c2 are two DIFFERENT label sets that collide on the same
		// mimirpb.NonStableHash value the middleware keys on. This exercises the
		// collision overflow path: label sets sharing a hash must each be
		// deduplicated independently and never merged into each other.
		c1, c2 := labelsWithNonStableHashCollision()
		mkTS := func(lbls []mimirpb.LabelAdapter, ts int64) mimirpb.PreallocTimeseries {
			// Each object owns its own label backing, as produced by unmarshalling,
			// so returning a removed duplicate to the pool can't corrupt a survivor.
			return mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{
				Labels:  slices.Clone(lbls),
				Samples: makeSamples(ts, 1),
			}}
		}
		req := &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{
			mkTS(c1, 10), mkTS(c2, 20), mkTS(c2, 30), mkTS(c1, 40),
		}}

		got := runPrePushMerge(t, d, req)

		require.Len(t, got.Timeseries, 2)
		for _, ts := range got.Timeseries {
			switch {
			case slices.Equal(ts.Labels, c1):
				assert.Equal(t, []int64{10, 40}, sampleTimestamps(ts))
			case slices.Equal(ts.Labels, c2):
				assert.Equal(t, []int64{20, 30}, sampleTimestamps(ts))
			default:
				t.Fatalf("unexpected labels reached the ingester: %v", ts.Labels)
			}
		}
	})

	t.Run("passes through single-series request unchanged", func(t *testing.T) {
		req := &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{
			makeTimeseries([]string{model.MetricNameLabel, "series_1"}, makeSamples(10, 1), nil, nil),
		}}

		got := runPrePushMerge(t, d, req)

		require.Len(t, got.Timeseries, 1)
		assert.Equal(t, []int64{10}, sampleTimestamps(got.Timeseries[0]))
	})

	t.Run("passes through empty request unchanged", func(t *testing.T) {
		req := &mimirpb.WriteRequest{Timeseries: nil}

		got := runPrePushMerge(t, d, req)

		assert.Empty(t, got.Timeseries)
	})
}

// TestDistributor_prePushMergeMiddleware_InvalidatesMarshalCache asserts that
// after merging into an existing timeseries, the cached marshalled bytes are
// invalidated so the merged samples/histograms/exemplars are actually written to
// the wire. The marshal cache is populated only on Unmarshal, so this test
// primes it the same way the real ingest path does before running the merge.
func TestDistributor_prePushMergeMiddleware_InvalidatesMarshalCache(t *testing.T) {
	d := newMergeTestDistributor(t)

	lbls := []string{model.MetricNameLabel, "series_1"}
	// The first series carries only a sample; the second additionally carries a
	// histogram and an exemplar, so the merge exercises every slice type.
	src := &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{
		makeTimeseries(lbls, makeSamples(10, 1), nil, nil),
		makeTimeseries(lbls, makeSamples(20, 2), makeHistograms(21, generateTestHistogram(1)), makeExemplars([]string{"trace_id", "b"}, 1, 1)),
	}}

	// Prime the per-timeseries marshal cache: marshal, then unmarshal into a
	// PreallocWriteRequest (which caches the original bytes on each timeseries).
	data, err := src.Marshal()
	require.NoError(t, err)
	parsed := mimirpb.PreallocWriteRequest{}
	require.NoError(t, parsed.Unmarshal(data))
	require.Len(t, parsed.Timeseries, 2)

	got := runPrePushMerge(t, d, &parsed.WriteRequest)
	require.Len(t, got.Timeseries, 1)

	// The in-memory view is always merged; the bug is that stale cached bytes are
	// returned by Marshal(). Assert on what actually goes on the wire: re-marshal
	// and re-unmarshal, then confirm the merged data survived.
	out, err := got.Marshal()
	require.NoError(t, err)
	verify := mimirpb.PreallocWriteRequest{}
	require.NoError(t, verify.Unmarshal(out))
	require.Len(t, verify.Timeseries, 1)
	assert.Len(t, verify.Timeseries[0].Samples, 2, "merged samples must survive re-marshalling")
	assert.Len(t, verify.Timeseries[0].Histograms, 1, "merged histogram must survive re-marshalling")
	assert.Len(t, verify.Timeseries[0].Exemplars, 1, "merged exemplar must survive re-marshalling")
}

// TestDistributor_prePushMergeMiddleware_PreservesCreatedTimestampToIngester is an
// end-to-end check through the full distributor push pipeline: a request with two
// identically-labelled objects that share a created timestamp must reach the
// ingester as a single merged series that still carries that created timestamp
// (so created-timestamp zero-sample ingestion still fires).
func TestDistributor_prePushMergeMiddleware_PreservesCreatedTimestampToIngester(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), "user")

	var limits validation.Limits
	flagext.DefaultValues(&limits)
	ds, ingesters, _, _ := prepare(t, prepConfig{
		numIngesters:    2,
		happyIngesters:  2,
		numDistributors: 1,
		limits:          &limits,
	})

	const createdTS = int64(50)
	lbls := []string{model.MetricNameLabel, "series_1"}
	req := &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{
		makeTimeseriesWithCT(lbls, makeSamples(100, 1), createdTS),
		makeTimeseriesWithCT(lbls, makeSamples(200, 2), createdTS),
	}}

	_, err := ds[0].Push(ctx, req)
	require.NoError(t, err)

	sawSeries := false
	for i := range ingesters {
		received := ingesters[i].series()
		// The duplicate must have been merged away before reaching any ingester.
		assert.LessOrEqual(t, len(received), 1, "ingester should receive at most the single merged series")
		for _, s := range received {
			sawSeries = true
			assert.Equal(t, createdTS, s.CreatedTimestamp, "merged series must reach the ingester with the created timestamp preserved")
			assert.Len(t, s.Samples, 2, "both duplicate objects' samples must be merged into one series")
		}
	}
	require.True(t, sawSeries, "expected at least one ingester to receive the merged series")
}
