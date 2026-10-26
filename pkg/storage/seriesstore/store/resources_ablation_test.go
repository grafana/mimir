// SPDX-License-Identifier: AGPL-3.0-only

//go:build native_resources_ablation

package store

import (
	"context"
	"fmt"
	"math"
	"sync"
	"testing"
	"unsafe"

	"github.com/prometheus/common/model"
	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// ablationResourceVersion is the version of testResourceAttributes from a time on, as #17740 keeps it: with values as
// strings.
func ablationResourceVersion(from int64, service, host string) ResourceVersion {
	return ResourceVersion{
		EffectiveFrom: from,
		Identifying:   []mimirpb.ResourceAttribute{{Key: "service.instance.id", StringValue: "1"}, {Key: "service.name", StringValue: service}},
		Descriptive:   []mimirpb.ResourceAttribute{{Key: "host.cores", StringValue: "8"}, {Key: "host.name", StringValue: host}},
	}
}

// A series' marker of having a resource takes the Series' byte of padding.
func TestAblationSeriesStaySmall(t *testing.T) {
	require.Equal(t, uintptr(152), unsafe.Sizeof(seriesEntry{}))
}

// The ablation keeps each series' resource on every path, as #17740's head does: per series, with the content-hash
// shortcut for an unchanged resource, and the reverse index kept up to date.
func TestAblationKeepsSeriesResourcesOnEveryPath(t *testing.T) {
	e, err := OpenEngine(t.TempDir(), "tenant", EngineOptions{Shards: 4, SecondaryHashFunction: secondaryHash, IndexedResourceAttributes: []string{"host.name"}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = e.Close() })
	e.SetNativeResources(true)
	lbls := promlabels.FromStrings(model.MetricNameLabel, "requests_total", "pod", "pod-0")
	other := promlabels.FromStrings(model.MetricNameLabel, "requests_total", "pod", "pod-1")
	meta := e.resources.meta
	lookup := func(key, value string) []uint64 { return meta.LookupResourceAttr(key, value) }

	app := e.AppenderV2(context.Background())
	ref, err := app.Append(0, lbls, 0, 1_000, 1, nil, nil, storage.AppendV2Options{Resource: asResourceContext(testResourceAttributes("frontend", "node-1"))})
	require.NoError(t, err)
	otherRef, err := app.Append(0, other, 0, 1_000, 1, nil, nil, storage.AppendV2Options{})
	require.NoError(t, err)
	require.NoError(t, app.Commit())
	require.Equal(t, []ResourceVersion{ablationResourceVersion(1_000, "frontend", "node-1")}, resourceVersionsOf(t, e, lbls))
	require.Equal(t, []uint64{promlabels.StableHash(lbls)}, lookup("service.name", "frontend"))

	// A record with the same resource leaves out the first series, whose latest version has its content, and is the
	// memo's hit for the second.
	hits := e.resources.stats.memoHits.Load()
	_, ingested, err := e.AppendRW2(resourceRecord(t, 2_000, testResourceAttributes("frontend", "node-1"), lbls, other), 0, 1<<62, 0, false, &rw2Sink{})
	require.NoError(t, err)
	require.Equal(t, 2, ingested)
	require.Equal(t, hits+1, e.resources.stats.memoHits.Load())
	require.Equal(t, []ResourceVersion{ablationResourceVersion(1_000, "frontend", "node-1")}, resourceVersionsOf(t, e, lbls))
	require.Equal(t, []ResourceVersion{ablationResourceVersion(2_000, "frontend", "node-1")}, resourceVersionsOf(t, e, other))

	// Another host is a version of the second series only; a decoded batch with another service is one of the first's.
	_, _, err = e.AppendRW2(resourceRecord(t, 3_000, testResourceAttributes("frontend", "node-2"), other), 0, 1<<62, 0, false, &rw2Sink{})
	require.NoError(t, err)
	backend := &mimirpb.ResourceEntry{Attributes: testResourceAttributes("backend", "node-3")}
	batch := []mimirpb.PreallocTimeseries{{TimeSeries: &mimirpb.TimeSeries{Labels: mimirpb.FromLabelsToLabelAdapters(lbls), Samples: []mimirpb.Sample{{TimestampMs: 4_000, Value: 1}}, Resource: backend}}}
	_, _, err = e.AppendFloats(batch, []int{0}, 0, 1<<62, 0, false, &rw2Sink{})
	require.NoError(t, err)
	require.Equal(t, []ResourceVersion{ablationResourceVersion(1_000, "frontend", "node-1"), ablationResourceVersion(4_000, "backend", "node-3")}, resourceVersionsOf(t, e, lbls))
	require.Equal(t, []ResourceVersion{ablationResourceVersion(2_000, "frontend", "node-1"), ablationResourceVersion(3_000, "frontend", "node-2")}, resourceVersionsOf(t, e, other))
	require.ElementsMatch(t, []uint64{promlabels.StableHash(lbls)}, lookup("host.name", "node-3"))
	require.ElementsMatch(t, []uint64{promlabels.StableHash(lbls), promlabels.StableHash(other)}, lookup("service.instance.id", "1"))
	require.Equal(t, ResourceStats{Series: 2, Runs: 2, Descriptions: 3, Versions: 4, MemoHits: hits + 1, MemoMisses: e.ResourceStats().MemoMisses}, e.ResourceStats())

	// Quiet zeros and staleness markers carry no resource, and nothing is kept with the switch off.
	app = e.AppenderV2(context.Background())
	_, err = app.Append(ref, lbls, 0, 5_000, math.Float64frombits(value.QuietZeroNaN), nil, nil, storage.AppendV2Options{Resource: asResourceContext(testResourceAttributes("quiet", "node-4"))})
	require.NoError(t, err)
	_, err = app.Append(ref, lbls, 0, 6_000, math.Float64frombits(value.StaleNaN), nil, nil, storage.AppendV2Options{Resource: asResourceContext(testResourceAttributes("stale", "node-4"))})
	require.NoError(t, err)
	require.NoError(t, app.Commit())
	e.SetNativeResources(false)
	_, _, err = e.AppendRW2(resourceRecord(t, 7_000, testResourceAttributes("off", "node-4"), lbls), 0, 1<<62, 0, false, &rw2Sink{})
	require.NoError(t, err)
	require.Len(t, resourceVersionsOf(t, e, lbls), 2)

	// A time range picks the versions in force during it.
	series, _, err := e.NativeResources(context.Background(), [][]*promlabels.Matcher{{promlabels.MustNewMatcher(promlabels.MatchEqual, "pod", "pod-0")}}, 3_500, 3_600, 0)
	require.NoError(t, err)
	require.Equal(t, []ResourceVersion{ablationResourceVersion(1_000, "frontend", "node-1")}, series[0].Versions)

	// The series' removal takes their resources and their reverse index entries, as the head's garbage collection
	// does; the store keeps the contents.
	require.NoError(t, e.CompactSelectedSeries([]storage.SeriesRef{ref, otherRef}))
	require.Equal(t, ResourceStats{Descriptions: 3, MemoHits: hits + 1, MemoMisses: e.ResourceStats().MemoMisses}, e.ResourceStats())
	require.Empty(t, lookup("service.instance.id", "1"))
	require.Empty(t, lookup("host.name", "node-3"))
}

// Ingesting, looking up and evicting series at once leave the store and its reverse index consistent.
func TestAblationResourcesUnderConcurrentEvictionsAndLookups(t *testing.T) {
	e, err := OpenEngine(t.TempDir(), "tenant", EngineOptions{Shards: 4, SecondaryHashFunction: secondaryHash})
	require.NoError(t, err)
	t.Cleanup(func() { _ = e.Close() })
	e.SetNativeResources(true)
	const series = 200
	var lbls []promlabels.Labels
	var refs []storage.SeriesRef
	app := e.AppenderV2(context.Background())
	for n := range series {
		l := promlabels.FromStrings(model.MetricNameLabel, "requests_total", "pod", fmt.Sprintf("pod-%03d", n))
		ref, err := app.Append(0, l, 0, 1_000, 1, nil, nil, storage.AppendV2Options{Resource: asResourceContext(testResourceAttributes("s0", "h0"))})
		require.NoError(t, err)
		lbls, refs = append(lbls, l), append(refs, ref)
	}
	require.NoError(t, app.Commit())

	var wg sync.WaitGroup
	done := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(done)
		for step := range 100 {
			attributes := testResourceAttributes(fmt.Sprintf("s%d", step%2), fmt.Sprintf("h%d", step%3))
			_, _, err := e.AppendRW2(resourceRecord(t, int64(2_000+step*1_000), attributes, lbls...), 0, 1<<62, 0, false, &rw2Sink{})
			assert.NoError(t, err)
		}
	}()
	background := func(work func(n int)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; ; n++ {
				select {
				case <-done:
					return
				default:
				}
				work(n)
			}
		}()
	}
	matchers := [][]*promlabels.Matcher{{promlabels.MustNewMatcher(promlabels.MatchEqual, model.MetricNameLabel, "requests_total")}}
	background(func(int) {
		found, _, err := e.NativeResources(context.Background(), matchers, math.MinInt64, math.MaxInt64, 10)
		assert.NoError(t, err)
		for _, s := range found {
			for _, v := range s.Versions {
				assert.Len(t, v.Identifying, 2)
				assert.Len(t, v.Descriptive, 2)
			}
		}
	})
	background(func(n int) {
		assert.NoError(t, e.CompactSelectedSeries(refs[n%series:n%series+1]))
	})
	wg.Wait()

	require.NoError(t, e.CompactSelectedSeries(refs))
	require.Zero(t, e.resources.meta.TotalResources())
	require.Zero(t, e.resources.meta.AttrIndexKeyCount())
}

// resourceAttributeSeries is how many series the reverse index has with the attribute.
func resourceAttributeSeries(e *Engine, key, value string) int {
	return len(e.resources.meta.LookupResourceAttr(key, value))
}
