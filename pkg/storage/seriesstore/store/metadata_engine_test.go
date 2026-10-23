// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"context"
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/prometheus/common/model"
	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
)

// metadataRecord is a record of one sample at t for each of labels, all with the same metadata of their own.
func metadataRecord(t *testing.T, at int64, m mimirpb.SeriesMetadata, lbls ...promlabels.Labels) *RW2Record {
	req := &mimirpb.WriteRequest{}
	for _, l := range lbls {
		req.Timeseries = append(req.Timeseries, mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{
			Labels:         mimirpb.FromLabelsToLabelAdapters(l),
			Samples:        []mimirpb.Sample{{TimestampMs: at, Value: 1}},
			SeriesMetadata: m,
		}})
	}
	records, _, err := ingest.RecordSerializerFromVersion(2).ToRecords("ingest", 0, "tenant", req, 64<<20)
	require.NoError(t, err)
	require.Len(t, records, 1)
	record, err := ReadRW2(records[0].Value, ingest.V2CommonSymbols.GetSlice(), ingest.V2RecordSymbolOffset)
	require.NoError(t, err)
	t.Cleanup(record.Release)
	return record
}

func TestEngineKeepsSeriesMetricMetadataOnEveryPath(t *testing.T) {
	e, err := OpenEngine(t.TempDir(), "tenant", EngineOptions{Shards: 4, SecondaryHashFunction: secondaryHash})
	require.NoError(t, err)
	t.Cleanup(func() { _ = e.Close() })
	e.SetNativeMetricMetadata(true)
	requests := mimirpb.SeriesMetadata{Type: mimirpb.COUNTER, Help: "Requests.", Unit: "requests"}
	served := mimirpb.SeriesMetadata{Type: mimirpb.COUNTER, Help: "Requests served.", Unit: "requests"}
	asVersion := func(at int64, m mimirpb.SeriesMetadata) MetricMetadataVersion {
		return MetricMetadataVersion{EffectiveFrom: at, Metadata: metadata.Metadata{Type: model.MetricTypeCounter, Help: m.Help, Unit: m.Unit}}
	}
	lbls := promlabels.FromStrings(model.MetricNameLabel, "requests_total", "pod", "pod-0")

	// The appender creates the series, and keeps its metadata.
	app := e.AppenderV2(context.Background())
	ref, err := app.Append(0, lbls, 0, 1_000, 1, nil, nil, storage.AppendV2Options{Metadata: metadata.Metadata{Type: model.MetricTypeCounter, Help: requests.Help, Unit: requests.Unit}})
	require.NoError(t, err)
	require.NoError(t, app.Commit())
	id := uint64(ref)
	require.Equal(t, []MetricMetadataVersion{asVersion(1_000, requests)}, e.metricMetadataVersions(id))

	// A record with the same metadata is the unchanged path: no new version, and the memo's hit for its second series.
	other := promlabels.FromStrings(model.MetricNameLabel, "requests_total", "pod", "pod-1")
	app = e.AppenderV2(context.Background())
	otherRef, err := app.Append(0, other, 0, 1_000, 1, nil, nil, storage.AppendV2Options{})
	require.NoError(t, err)
	require.NoError(t, app.Commit())
	hits := e.metricMetadataStats.memoHits.Load()
	_, ingested, err := e.AppendRW2(metadataRecord(t, 2_000, requests, lbls, other), 0, 1<<62, 0, false, &rw2Sink{})
	require.NoError(t, err)
	require.Equal(t, 2, ingested)
	require.Equal(t, []MetricMetadataVersion{asVersion(1_000, requests)}, e.metricMetadataVersions(id))
	require.Equal(t, []MetricMetadataVersion{asVersion(2_000, requests)}, e.metricMetadataVersions(uint64(otherRef)))
	if recordMemo {
		require.Equal(t, hits+1, e.metricMetadataStats.memoHits.Load())
	}

	// A record with other metadata changes it, and a decoded batch changes it back.
	_, _, err = e.AppendRW2(metadataRecord(t, 3_000, served, lbls), 0, 1<<62, 0, false, &rw2Sink{})
	require.NoError(t, err)
	batch := []mimirpb.PreallocTimeseries{{TimeSeries: &mimirpb.TimeSeries{Labels: mimirpb.FromLabelsToLabelAdapters(lbls), Samples: []mimirpb.Sample{{TimestampMs: 4_000, Value: 1}}, SeriesMetadata: requests}}}
	_, _, err = e.AppendFloats(batch, []int{0}, 0, 1<<62, 0, false, &rw2Sink{})
	require.NoError(t, err)
	expected := []MetricMetadataVersion{asVersion(1_000, requests), asVersion(3_000, served), asVersion(4_000, requests)}
	require.Equal(t, expected, e.metricMetadataVersions(id))
	require.EqualValues(t, 2, e.metricMetadata.values.Load(), "one value each")

	// Quiet zeros and staleness markers carry no metadata, and nothing is kept with the switch off.
	app = e.AppenderV2(context.Background())
	_, err = app.Append(ref, lbls, 0, 5_000, math.Float64frombits(value.QuietZeroNaN), nil, nil, storage.AppendV2Options{Metadata: metadata.Metadata{Type: model.MetricTypeGauge}})
	require.NoError(t, err)
	_, err = app.Append(ref, lbls, 0, 6_000, math.Float64frombits(value.StaleNaN), nil, nil, storage.AppendV2Options{Metadata: metadata.Metadata{Type: model.MetricTypeGauge}})
	require.NoError(t, err)
	require.NoError(t, app.Commit())
	e.SetNativeMetricMetadata(false)
	_, _, err = e.AppendRW2(metadataRecord(t, 7_000, served, lbls), 0, 1<<62, 0, false, &rw2Sink{})
	require.NoError(t, err)
	require.Equal(t, expected, e.metricMetadataVersions(id))

	// A series that leaves the engine's memory gives up its metadata, and the values go at the next reclamation.
	require.NoError(t, e.CompactSelectedSeries([]storage.SeriesRef{ref, otherRef}))
	require.Nil(t, e.metricMetadataVersions(id))
	e.metricMetadata.reclaim()
	require.Zero(t, e.metricMetadata.values.Load())
	require.Zero(t, e.metricMetadataStats.clamped.Load())
}

// Values are never resolved without a reference while records change the series' metadata, lookups resolve it,
// series leave memory and the dictionary reclaims what they gave up.
func TestMetricMetadataUnderConcurrentEvictionsAndLookups(t *testing.T) {
	e, err := OpenEngine(t.TempDir(), "tenant", EngineOptions{Shards: 4, SecondaryHashFunction: secondaryHash})
	require.NoError(t, err)
	t.Cleanup(func() { _ = e.Close() })
	e.SetNativeMetricMetadata(true)
	const series = 200
	var lbls []promlabels.Labels
	var refs []storage.SeriesRef
	app := e.AppenderV2(context.Background())
	for n := range series {
		l := promlabels.FromStrings(model.MetricNameLabel, "requests_total", "pod", fmt.Sprintf("pod-%03d", n))
		ref, err := app.Append(0, l, 0, 1_000, 1, nil, nil, storage.AppendV2Options{Metadata: metadata.Metadata{Type: model.MetricTypeCounter, Help: "v0"}})
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
			_, _, err := e.AppendRW2(metadataRecord(t, int64(2_000+step*1_000), mimirpb.SeriesMetadata{Type: mimirpb.COUNTER, Help: fmt.Sprintf("v%d", step%3)}, lbls...), 0, 1<<62, 0, false, &rw2Sink{})
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
		found, _, err := e.NativeMetricMetadata(context.Background(), matchers, 10)
		assert.NoError(t, err)
		for _, s := range found {
			for _, v := range s.Versions {
				assert.Equal(t, model.MetricTypeCounter, v.Metadata.Type)
				assert.Contains(t, []string{"v0", "v1", "v2"}, v.Metadata.Help)
			}
		}
	})
	background(func(n int) {
		assert.NoError(t, e.CompactSelectedSeries(refs[n%series:n%series+1]))
	})
	background(func(int) { e.metricMetadata.reclaim() })
	wg.Wait()

	require.NoError(t, e.CompactSelectedSeries(refs))
	e.metricMetadata.reclaim()
	require.Zero(t, e.metricMetadata.values.Load())
	for _, page := range *e.metricMetadata.pages.Load() {
		for index := range page {
			require.GreaterOrEqual(t, page[index].refs.Load(), int64(0))
		}
	}
}
