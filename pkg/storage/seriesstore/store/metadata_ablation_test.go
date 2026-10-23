// SPDX-License-Identifier: AGPL-3.0-only

//go:build native_metadata_ablation

package store

import (
	"fmt"
	"math/rand"
	"slices"
	"testing"
	"unsafe"

	"github.com/prometheus/common/model"
	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/seriesstore/chunks"
)

// The ablation's series state is #19563's nativeSeriesMetadata.
func TestAblationStateIsNineteen563s(t *testing.T) {
	require.EqualValues(t, 48, unsafe.Sizeof(ablationSeries{}))
}

// ablationModel is a series' versions as #19563 keeps them, observed one at a time: at most the cap's, the oldest
// evicted, which can't come back.
type ablationModel struct {
	points    []ablationPoint
	truncated bool
}

func (m *ablationModel) observe(at int64, value *metadata.Metadata) {
	i, found := slices.BinarySearchFunc(m.points, at, func(p ablationPoint, at int64) int {
		switch {
		case p.effectiveFrom < at:
			return -1
		case p.effectiveFrom > at:
			return 1
		}
		return 0
	})
	if found {
		m.points[i].metadata = value
	} else {
		m.points = slices.Insert(m.points, i, ablationPoint{effectiveFrom: at, metadata: value})
	}
	merged := m.points[:1]
	for _, p := range m.points[1:] {
		if *merged[len(merged)-1].metadata != *p.metadata {
			merged = append(merged, p)
		}
	}
	m.points = merged
	if len(m.points) > ablationMaxVersions {
		m.points = slices.Clone(m.points[len(m.points)-ablationMaxVersions:])
		m.truncated = true
	}
}

func TestAblationMergesLikeNineteen563(t *testing.T) {
	values := []*metadata.Metadata{
		{Type: model.MetricTypeCounter, Help: "a"}, {Type: model.MetricTypeCounter, Help: "b"}, {Type: model.MetricTypeCounter, Help: "c"},
	}
	for seed := range int64(20) {
		r := rand.New(rand.NewSource(seed))
		var native ablationSeries
		var m ablationModel
		now := int64(100_000)
		for step := range 60 {
			at := now
			if r.Intn(3) == 0 {
				at -= int64(r.Intn(30)) * 1_000
			} else {
				now += 1_000
				at = now
			}
			// A value equal to another but not the same one, as values resolved by different transactions are.
			value := *values[r.Intn(len(values))]
			native.mergeLocked([]ablationPoint{{effectiveFrom: at, metadata: &value}})
			m.observe(at, &value)
			got := append(slices.Clone(native.older), ablationPoint{effectiveFrom: native.effectiveFrom, metadata: native.metadata})
			require.Len(t, got, len(m.points), "seed %d step %d", seed, step)
			for i := range got {
				require.Equal(t, m.points[i].effectiveFrom, got[i].effectiveFrom, "seed %d step %d", seed, step)
				require.Equal(t, *m.points[i].metadata, *got[i].metadata, "seed %d step %d", seed, step)
			}
			require.Equal(t, m.truncated, native.truncated, "seed %d step %d", seed, step)
		}
	}
}

// The cache shares a value once it missed twice, as a copy of its own.
func TestAblationCacheAdmitsAValueAtItsSecondMiss(t *testing.T) {
	c := newMetricMetadataValues()
	var strs ablationStrings
	help := string([]byte("Requests served."))
	v := metadata.Metadata{Type: model.MetricTypeCounter, Help: help}
	first := c.resolve(v, &strs)
	require.Zero(t, c.values.Load())
	second := c.resolve(v, &strs)
	require.EqualValues(t, 1, c.values.Load())
	require.NotSame(t, first, second)
	require.Same(t, second, c.resolve(v, &strs))
	require.Equal(t, v, *second)
	require.NotSame(t, unsafe.StringData(help), unsafe.StringData(second.Help), "it owns its strings")
}

// A record's series share their value, a transaction ends every 150 series, the unchanged path keeps them as they
// are, a change is a version, five versions at most, and a series that leaves memory gives them up.
func TestAblationKeepsSeriesMetadataLikeNineteen563(t *testing.T) {
	var series []promlabels.Labels
	for n := range 200 {
		series = append(series, promlabels.FromStrings(model.MetricNameLabel, "requests_total", "pod", fmt.Sprintf("pod-%03d", n)))
	}
	e := metadataEngine(t, 1_000, series...)
	requests := mimirpb.SeriesMetadata{Type: mimirpb.COUNTER, Help: "Requests."}
	metadataRoutes["complete record"](t, e, 2_000, requests, series...)
	metadataRoutes["complete record"](t, e, 3_000, requests, series...)
	all := promlabels.MustNewMatcher(promlabels.MatchEqual, model.MetricNameLabel, "requests_total")
	got := versionsOf(t, e, all)
	require.Len(t, got, len(series))
	for _, versions := range got {
		require.Equal(t, []MetricMetadataVersion{{EffectiveFrom: 2_000, Metadata: metadata.Metadata{Type: model.MetricTypeCounter, Help: "Requests."}}}, versions)
	}
	// The first record's two transactions each resolve the value once, through the cache, whose second miss admits
	// it: the second transaction's series share it, the first's their transaction's copy.
	values := map[*metadata.Metadata]int{}
	stats := e.MetricMetadataStats()
	require.EqualValues(t, len(series), stats.Series)
	require.EqualValues(t, len(series), stats.Versions)
	e.perShard(func(_ int, ten *tenant, _ *chunks.DiskMapper, _ *coldState) {
		for group := range ten.series.groups {
			for i := range ten.series.groups[group].entries {
				if native := ten.series.groups[group].entries[i].series.native; native != nil {
					values[native.metadata]++
				}
			}
		}
	})
	require.Len(t, values, 2)

	// Each change is a version, the oldest past five evicted.
	one := series[:1]
	for step := range 6 {
		metadataRoutes["complete record"](t, e, int64(4_000+step*1_000), mimirpb.SeriesMetadata{Type: mimirpb.COUNTER, Help: fmt.Sprintf("v%d", step)}, one...)
	}
	found, _, err := e.NativeMetricMetadata(t.Context(), [][]*promlabels.Matcher{{promlabels.MustNewMatcher(promlabels.MatchEqual, "pod", "pod-000")}}, 0)
	require.NoError(t, err)
	require.Len(t, found, 1)
	require.Len(t, found[0].Versions, ablationMaxVersions)
	require.True(t, found[0].Truncated)
	require.False(t, found[0].Coalesced)
	require.Equal(t, "v5", found[0].Versions[ablationMaxVersions-1].Metadata.Help)
	require.EqualValues(t, 2, e.metricMetadataStats.evictions.Load())

	// Series that leave memory give their metadata up.
	var refs []storage.SeriesRef
	app := e.AppenderV2(t.Context())
	for _, l := range series {
		ref, err := app.Append(0, l, 0, 20_000, 1, nil, nil, storage.AppendV2Options{})
		require.NoError(t, err)
		refs = append(refs, ref)
	}
	require.NoError(t, app.Commit())
	require.NoError(t, e.CompactSelectedSeries(refs))
	stats = e.MetricMetadataStats()
	require.Zero(t, stats.Series)
	require.Zero(t, stats.Versions)
	require.Empty(t, versionsOf(t, e, all))
}
