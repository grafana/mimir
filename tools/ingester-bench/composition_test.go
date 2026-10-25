// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
)

func compositionSeries(name string, metadata mimirpb.SeriesMetadata, extra ...string) mimirpb.PreallocTimeseries {
	labels := []mimirpb.LabelAdapter{{Name: "__name__", Value: name}}
	for i := 0; i < len(extra); i += 2 {
		labels = append(labels, mimirpb.LabelAdapter{Name: extra[i], Value: extra[i+1]})
	}
	return mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{Labels: labels, Samples: []mimirpb.Sample{{TimestampMs: 1, Value: 1}}, SeriesMetadata: metadata}}
}

func TestCompositionOfTodaysAndTheNewWritersRecords(t *testing.T) {
	requests := mimirpb.SeriesMetadata{Type: mimirpb.COUNTER, Help: "Requests."}
	latency := mimirpb.SeriesMetadata{Type: mimirpb.HISTOGRAM, Help: "Latency."}

	// Today's: the family's metadata copied onto the series named like it, and a stand-in for the histogram's.
	today := compositionCounter{today: true}
	today.add(&mimirpb.WriteRequest{
		Timeseries: []mimirpb.PreallocTimeseries{
			compositionSeries("requests_total", requests, "pod", "0"), compositionSeries("requests_total", requests, "pod", "1"),
			compositionSeries("latency_seconds_bucket", mimirpb.SeriesMetadata{}, "le", "1"), compositionSeries("latency_seconds_count", mimirpb.SeriesMetadata{}),
			{TimeSeries: &mimirpb.TimeSeries{Labels: []mimirpb.LabelAdapter{{Name: "__name__", Value: "latency_seconds"}}}},
		},
		Metadata: []*mimirpb.MetricMetadata{{MetricFamilyName: "latency_seconds", Type: mimirpb.HISTOGRAM, Help: "Latency."}},
	})
	// With its two families' helps, "Latency." and "Requests.".
	require.Equal(t, Composition{Records: 1, SeriesPerRecord: 4, SeriesPerValue: 2, StandInsPerRecord: 2, Families: 2,
		HelpLength: Percentiles{"0": 8, "0.1": 8, "0.5": 9, "0.9": 9, "0.99": 9, "1": 9}}, today.composition())

	// The new writer's: each series' own, and a stand-in each for the families.
	newer := compositionCounter{}
	newer.add(&mimirpb.WriteRequest{
		Timeseries: []mimirpb.PreallocTimeseries{
			compositionSeries("requests_total", requests, "pod", "0"), compositionSeries("requests_total", requests, "pod", "1"),
			compositionSeries("latency_seconds_bucket", latency, "le", "1"), compositionSeries("latency_seconds_count", latency),
			{TimeSeries: &mimirpb.TimeSeries{Labels: []mimirpb.LabelAdapter{{Name: "__name__", Value: "requests_total"}}}},
			{TimeSeries: &mimirpb.TimeSeries{Labels: []mimirpb.LabelAdapter{{Name: "__name__", Value: "latency_seconds"}}}},
		},
	})
	require.Equal(t, Composition{Records: 1, SeriesPerRecord: 4, SeriesPerValue: 2, StandInsPerRecord: 2}, newer.composition())
}

func TestCompositionOfAKafkatoolDump(t *testing.T) {
	req := &mimirpb.WriteRequest{
		Timeseries: []mimirpb.PreallocTimeseries{compositionSeries("requests_total", mimirpb.SeriesMetadata{}, "pod", "0"), compositionSeries("errors_total", mimirpb.SeriesMetadata{}, "pod", "0")},
		Metadata:   []*mimirpb.MetricMetadata{{MetricFamilyName: "requests_total", Type: mimirpb.COUNTER, Help: "Requests."}},
	}
	records, _, err := ingest.RecordSerializerFromVersion(2).ToRecords("ingest", 0, "tenant", req, 1<<20)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "dump.json")
	f, err := os.Create(path)
	require.NoError(t, err)
	for range 3 {
		for _, record := range records {
			require.NoError(t, json.NewEncoder(f).Encode(&kgo.Record{Value: record.Value, Headers: record.Headers}))
		}
	}
	require.NoError(t, f.Close())
	composition, err := compositionOfDumps([]string{path, path})
	require.NoError(t, err)
	// Over both, and with the length of the family's help.
	require.Equal(t, Composition{Records: 6, SeriesPerRecord: 2, SeriesPerValue: 1, StandInsPerRecord: 1, Families: 1,
		HelpLength: Percentiles{"0": 9, "0.1": 9, "0.5": 9, "0.9": 9, "0.99": 9, "1": 9}}, composition)
}
