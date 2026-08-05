// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/validation"
)

// newMergeTestDistributor builds a distributor suitable for exercising
// prePushMergeMiddleware in isolation, with the merge_duplicate_timeseries limit
// set to mergeDuplicates. Apart from that limit the middleware is stateless with
// respect to the distributor, so the default configuration is enough and a
// single instance can be reused across subtests.
func newMergeTestDistributor(t *testing.T, mergeDuplicates bool) *Distributor {
	t.Helper()

	var limits validation.Limits
	flagext.DefaultValues(&limits)
	limits.MergeDuplicateTimeseries = mergeDuplicates
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

	ctx := user.InjectOrgID(t.Context(), "user")
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
	d := newMergeTestDistributor(t, true)

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

// TestDistributor_prePushMergeMiddleware_Disabled asserts the middleware is a
// no-op for a tenant that has merge_duplicate_timeseries turned off, which is
// the default. Duplicated timeseries objects must reach the next PushFunc
// exactly as they arrived.
func TestDistributor_prePushMergeMiddleware_Disabled(t *testing.T) {
	var defaults validation.Limits
	flagext.DefaultValues(&defaults)
	require.False(t, defaults.MergeDuplicateTimeseries, "the experimental merge must be opt-in")

	d := newMergeTestDistributor(t, false)

	lbls := []string{model.MetricNameLabel, "series_1"}
	req := &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{
		makeTimeseries(lbls, makeSamples(10, 1), nil, nil),
		makeTimeseries(lbls, makeSamples(20, 2), nil, nil),
	}}

	got := runPrePushMerge(t, d, req)

	require.Len(t, got.Timeseries, 2)
	assert.Equal(t, []int64{10}, sampleTimestamps(got.Timeseries[0]))
	assert.Equal(t, []int64{20}, sampleTimestamps(got.Timeseries[1]))
}

// TestDistributor_prePushMergeMiddleware_InterleavedDuplicates covers the
// compaction path with duplicates that are spread out rather than adjacent, the
// shape a client controls and the worst case for removal: every second index is
// dropped, so no two removals are contiguous. It pins that survivors keep their
// relative order and that each one collects its own duplicate's samples.
func TestDistributor_prePushMergeMiddleware_InterleavedDuplicates(t *testing.T) {
	d := newMergeTestDistributor(t, true)

	const numSeries = 64
	ts := make([]mimirpb.PreallocTimeseries, 0, 2*numSeries)
	for s := 0; s < numSeries; s++ {
		lbls := []string{model.MetricNameLabel, fmt.Sprintf("series_%d", s)}
		ts = append(ts,
			makeTimeseries(lbls, makeSamples(int64(s), 1), nil, nil),
			makeTimeseries(lbls, makeSamples(int64(1000+s), 2), nil, nil),
		)
	}

	got := runPrePushMerge(t, d, &mimirpb.WriteRequest{Timeseries: ts})

	require.Len(t, got.Timeseries, numSeries)
	for s, kept := range got.Timeseries {
		assert.Equal(t, fmt.Sprintf("series_%d", s), kept.Labels[0].Value, "survivors must keep their relative order")
		assert.Equal(t, []int64{int64(s), int64(1000 + s)}, sampleTimestamps(kept))
	}
}

// collidingLabelSets returns n DIFFERENT label sets that all produce the same
// mimirpb.NonStableHash. NonStableHash joins label names and values with an
// unescaped 0xff and the write request decoder does not validate label bytes, so
// a value containing 0xff is indistinguishable from a label boundary:
// {a="1\xffb\xff2"} and {a="1", b="2"} hash identically. Splitting one fixed
// token stream at different points therefore yields as many colliding label sets
// as wanted, which is what makes bounding the collision scan necessary.
func collidingLabelSets(t *testing.T, n int) [][]mimirpb.LabelAdapter {
	t.Helper()

	tokens := make([]string, 2*(n+1))
	for i := range tokens {
		tokens[i] = fmt.Sprintf("t%d", i)
	}

	sets := make([][]mimirpb.LabelAdapter, 0, n)
	for split := 0; split < n; split++ {
		// The first split labels take one token each for name and value; whatever
		// is left is folded into the final label's value.
		ls := make([]mimirpb.LabelAdapter, 0, split+1)
		for i := 0; i < split; i++ {
			ls = append(ls, mimirpb.LabelAdapter{Name: tokens[2*i], Value: tokens[2*i+1]})
		}
		ls = append(ls, mimirpb.LabelAdapter{
			Name:  tokens[2*split],
			Value: strings.Join(tokens[2*split+1:], "\xff"),
		})
		sets = append(sets, ls)
	}

	want := mimirpb.NonStableHash(sets[0])
	for i, ls := range sets {
		require.Equal(t, want, mimirpb.NonStableHash(ls), "label set %d must collide with the first", i)
		// Each split yields a different number of labels, so the sets are all
		// distinct even though they are byte-identical once hashed.
		require.Len(t, ls, i+1)
	}
	return sets
}

// TestDistributor_prePushMergeMiddleware_BoundsCollisionScan pins the behaviour
// when a client deliberately packs one hash bucket with distinct label sets. The
// scan is capped at prePushMergeMaxCollisionCandidates label sets per bucket,
// counting the primary, so the middleware stays linear. The cap must only ever
// cost merging, never correctness: distinct label sets are never merged into each
// other and no timeseries is dropped.
func TestDistributor_prePushMergeMiddleware_BoundsCollisionScan(t *testing.T) {
	d := newMergeTestDistributor(t, true)

	// More label sets than the middleware tracks per bucket, so the last one falls
	// outside the cap.
	numSets := prePushMergeMaxCollisionCandidates + 2
	sets := collidingLabelSets(t, numSets)

	mkTS := func(lbls []mimirpb.LabelAdapter, ts int64) mimirpb.PreallocTimeseries {
		// Each object owns its own label backing, as produced by unmarshalling, so
		// returning a removed duplicate to the pool can't corrupt a survivor.
		return mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{
			Labels:  slices.Clone(lbls),
			Samples: makeSamples(ts, 1),
		}}
	}

	req := &mimirpb.WriteRequest{}
	for i, ls := range sets {
		req.Timeseries = append(req.Timeseries, mkTS(ls, int64(i)))
	}
	// Duplicate one set on each side of the boundary, so the assertions pin the
	// cap exactly rather than merely pinning that some cap exists: the last set the
	// bucket tracks must still merge, and the first one it does not track must not.
	// The duplicates carry timestamp 1000+i so a merged pair is distinguishable
	// from the originals.
	dupOf := []int{0, prePushMergeMaxCollisionCandidates - 1, prePushMergeMaxCollisionCandidates, numSets - 1}
	for _, i := range dupOf {
		req.Timeseries = append(req.Timeseries, mkTS(sets[i], int64(1000+i)))
	}

	got := runPrePushMerge(t, d, req)

	// Group the survivors by label set, keeping one entry per surviving object, so
	// that "merged into one object" and "survived as two objects" are told apart.
	objectsPerSet := make([][][]int64, numSets)
	for _, kept := range got.Timeseries {
		idx := slices.IndexFunc(sets, func(ls []mimirpb.LabelAdapter) bool { return slices.Equal(ls, kept.Labels) })
		require.GreaterOrEqual(t, idx, 0, "an unknown label set survived: %v", kept.Labels)
		objectsPerSet[idx] = append(objectsPerSet[idx], sampleTimestamps(kept))
	}

	for i, objects := range objectsPerSet {
		switch {
		case !slices.Contains(dupOf, i):
			assert.Equal(t, [][]int64{{int64(i)}}, objects, "set %d was not duplicated, so it must pass through untouched", i)
		case i < prePushMergeMaxCollisionCandidates:
			// Inside the cap: the bucket tracks this label set, so its duplicate
			// merges into it and one object survives holding both samples.
			assert.Equal(t, [][]int64{{int64(i), int64(1000 + i)}}, objects, "set %d is within the cap, so its duplicate must merge", i)
		default:
			// Outside the cap: the bucket stopped tracking label sets, so the
			// duplicate is left exactly as the client sent it. Not merging is the
			// only permitted degradation; dropping or cross-merging is not.
			assert.Equal(t, [][]int64{{int64(i)}, {int64(1000 + i)}}, objects, "set %d is beyond the cap, so its duplicate must survive unmerged", i)
		}
	}

	// Two of the four duplicates merged away, the other two survived.
	require.Len(t, got.Timeseries, numSets+2)
}

// TestDistributor_prePushMergeMiddleware_DoesNotPoolOversizedSeenMaps asserts
// that a request big enough to grow the pooled lookup map past
// prePushMergeMaxPooledSeenEntries leaves that map out of the pool. A Go map
// keeps its buckets after clear() and clear() scans them, so pooling a map that
// one huge request grew would make every later request that picked it up pay for
// it however few timeseries it carried.
//
// The map the middleware uses is observable here because the pool is swapped for
// one whose New always hands back the same map. Doing that is safe even though
// the pool is a package-level variable: this test is not parallel, and the
// testing package only releases parallel tests once every sequential one has
// finished, so nothing else touches the pool while it is substituted.
func TestDistributor_prePushMergeMiddleware_DoesNotPoolOversizedSeenMaps(t *testing.T) {
	for _, tc := range []struct {
		name       string
		numSeries  int
		wantPooled bool
	}{
		{name: "at the bound", numSeries: prePushMergeMaxPooledSeenEntries, wantPooled: true},
		{name: "past the bound", numSeries: prePushMergeMaxPooledSeenEntries + 1, wantPooled: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			seen := make(map[prePushMergeSeenKey]prePushMergeSeenEntry)
			newCalls := 0

			original := prePushMergeSeenPool
			prePushMergeSeenPool = &sync.Pool{New: func() any {
				newCalls++
				return seen
			}}
			t.Cleanup(func() { prePushMergeSeenPool = original })

			ts := make([]mimirpb.PreallocTimeseries, 0, tc.numSeries)
			for s := 0; s < tc.numSeries; s++ {
				lbls := []string{model.MetricNameLabel, fmt.Sprintf("series_%d", s)}
				ts = append(ts, makeTimeseries(lbls, makeSamples(int64(s), 1), nil, nil))
			}

			got := runPrePushMerge(t, newMergeTestDistributor(t, true), &mimirpb.WriteRequest{Timeseries: ts})

			// Every label set is distinct, so nothing merges and the map ends up with
			// one entry per timeseries.
			require.Len(t, got.Timeseries, tc.numSeries)
			// The substituted pool starts empty and the middleware takes exactly one
			// map from it, so this pins that the map asserted on below is the one the
			// middleware actually used.
			require.Equal(t, 1, newCalls, "the middleware must have taken its map from the substituted pool")

			if tc.wantPooled {
				assert.Empty(t, seen, "a map within the bound must be cleared and returned to the pool")
			} else {
				assert.Len(t, seen, tc.numSeries, "an oversized map must be dropped rather than cleared and returned to the pool")
			}
		})
	}
}

// TestDistributor_prePushMergeMiddleware_InvalidatesMarshalCache asserts that
// after merging into an existing timeseries, the cached marshalled bytes are
// invalidated so the merged samples/histograms/exemplars are actually written to
// the wire. The marshal cache is populated only on Unmarshal, so this test
// primes it the same way the real ingest path does before running the merge.
func TestDistributor_prePushMergeMiddleware_InvalidatesMarshalCache(t *testing.T) {
	d := newMergeTestDistributor(t, true)

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

// TestDistributor_prePushMergeMiddleware_CountsCrossObjectDuplicates is an
// end-to-end check through the full distributor push pipeline, pinning exactly the
// behaviour issue #15550 reported: two identically-labelled objects carrying the
// same sample timestamp must be merged so the existing validation-time dedup sees
// the duplicate, counts it in cortex_discarded_samples_total, and forwards a single
// sample. Before this middleware both objects reached the ingesters, where the
// duplicate was silently dropped without being counted.
//
// The assertions use a duplicated sample timestamp and the discarded counter rather
// than the number of merged objects on purpose. mockIngester coalesces series by
// label set on arrival and appends their samples, so an unmerged request is
// indistinguishable from a merged one by series count alone, and a test written
// that way would still pass with the middleware removed. Running the same request
// with the limit disabled pins the difference from the other side.
func TestDistributor_prePushMergeMiddleware_CountsCrossObjectDuplicates(t *testing.T) {
	ctx := user.InjectOrgID(t.Context(), "user")

	const (
		createdTS   = int64(50)
		duplicateTS = int64(100)
	)

	for _, mergeEnabled := range []bool{true, false} {
		t.Run(fmt.Sprintf("merge_duplicate_timeseries=%t", mergeEnabled), func(t *testing.T) {
			var limits validation.Limits
			flagext.DefaultValues(&limits)
			limits.MergeDuplicateTimeseries = mergeEnabled
			ds, ingesters, regs, _ := prepare(t, prepConfig{
				numIngesters:    2,
				happyIngesters:  2,
				numDistributors: 1,
				limits:          &limits,
			})
			require.Len(t, regs, 1)

			lbls := []string{model.MetricNameLabel, "series_1"}
			req := &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{
				makeTimeseriesWithCT(lbls, makeSamples(duplicateTS, 1), createdTS),
				makeTimeseriesWithCT(lbls, makeSamples(duplicateTS, 2), createdTS),
			}}

			_, err := ds[0].Push(ctx, req)
			require.NoError(t, err)

			// The whole point of the merge: the cross-object duplicate becomes visible
			// to validateSeries and is counted. Without it, nothing is discarded.
			expectedDiscarded := ``
			if mergeEnabled {
				expectedDiscarded = `
					# HELP cortex_discarded_samples_total The total number of samples that were discarded.
					# TYPE cortex_discarded_samples_total counter
					cortex_discarded_samples_total{group="",reason="sample_duplicate_timestamp",user="user"} 1
				`
			}
			assert.NoError(t, testutil.GatherAndCompare(regs[0], strings.NewReader(expectedDiscarded), "cortex_discarded_samples_total"))

			expectedSamples := 2
			if mergeEnabled {
				expectedSamples = 1
			}
			sawSeries := false
			for i := range ingesters {
				for _, s := range ingesters[i].series() {
					sawSeries = true
					assert.Equal(t, createdTS, s.CreatedTimestamp, "the created timestamp must survive the middleware")
					assert.Len(t, s.Samples, expectedSamples, "unexpected number of samples reached the ingester")
				}
			}
			require.True(t, sawSeries, "expected at least one ingester to receive the series")
		})
	}
}

// BenchmarkDistributor_prePushMergeMiddleware measures the middleware on the two
// extremes it runs on: a request with no cross-object duplicates (the common,
// hot-path case, where the middleware only tracks each series) and a request
// where every object shares one label set (worst case for merging).
func BenchmarkDistributor_prePushMergeMiddleware(b *testing.B) {
	const numSeriesPerRequest = 1000
	ctx := user.InjectOrgID(context.Background(), "user")

	var limits validation.Limits
	flagext.DefaultValues(&limits)
	limits.MergeDuplicateTimeseries = true

	cases := map[string]func(s int) []string{
		"no duplicates":  func(s int) []string { return []string{model.MetricNameLabel, fmt.Sprintf("series_%d", s)} },
		"all duplicates": func(int) []string { return []string{model.MetricNameLabel, "series"} },
	}

	for name, labelsFor := range cases {
		b.Run(name, func(b *testing.B) {
			ds, _, _, _ := prepare(b, prepConfig{numDistributors: 1, limits: &limits})
			noop := func(context.Context, *Request) error { return nil }
			fn := ds[0].prePushMergeMiddleware(noop)

			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				// The middleware consumes the request: it merges in place, returns
				// duplicates to the timeseries pool and truncates the slice. So each
				// iteration needs a freshly built request, and it cannot share
				// TimeSeries pointers with a template the way
				// cloneWriteRequestForBench does. Build it with the timer stopped,
				// which excludes both its time and its allocations from the report,
				// rather than pre-generating b.N requests up front: at this
				// microsecond-scale cost per op the benchmark calibrates to a b.N
				// large enough that holding numSeriesPerRequest timeseries per
				// iteration alive at once would exhaust memory.
				b.StopTimer()
				ts := make([]mimirpb.PreallocTimeseries, 0, numSeriesPerRequest)
				for s := 0; s < numSeriesPerRequest; s++ {
					ts = append(ts, makeTimeseries(labelsFor(s), makeSamples(int64(s), float64(s)), nil, nil))
				}
				req := &mimirpb.WriteRequest{Timeseries: ts}
				b.StartTimer()

				err := fn(ctx, newRequest(func() (*mimirpb.WriteRequest, func(), int, error) {
					return req, func() {}, 0, nil
				}))
				require.NoError(b, err)
			}
		})
	}
}
