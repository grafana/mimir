// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestDumpCommand_AnalyseSeries(t *testing.T) {
	mkLabels := func(nameValue ...string) []mimirpb.LabelAdapter {
		labels := make([]mimirpb.LabelAdapter, 0, len(nameValue)/2)
		for i := 0; i < len(nameValue); i += 2 {
			labels = append(labels, mimirpb.LabelAdapter{Name: nameValue[i], Value: nameValue[i+1]})
		}
		return labels
	}
	mkSeries := func(labels []mimirpb.LabelAdapter, samples ...mimirpb.Sample) mimirpb.PreallocTimeseries {
		return mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{Labels: labels, Samples: samples}}
	}

	seriesA := mkLabels("__name__", "metric_a", "job", "test")
	seriesB := mkLabels("__name__", "metric_b", "job", "test")

	at := time.Date(2026, 5, 29, 11, 0, 0, 0, time.UTC)

	// series A (tenant-1): samples at 0, 10s, 40s -> deltas 10s and 30s.
	//   min=10s, max=30s, avg=20s, samples=3.
	// series B (tenant-1): a single sample -> no deltas.
	// tenant-2: must be ignored entirely.
	records := []dumpRecord{
		{tenantID: "tenant-1", at: at, req: &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{
			mkSeries(seriesA, mimirpb.Sample{TimestampMs: 0, Value: 1}),
			mkSeries(seriesB, mimirpb.Sample{TimestampMs: 0, Value: 1}),
		}}},
		{tenantID: "tenant-1", at: at, req: &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{
			mkSeries(seriesA, mimirpb.Sample{TimestampMs: 10_000, Value: 2}),
		}}},
		{tenantID: "tenant-1", at: at, req: &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{
			mkSeries(seriesA, mimirpb.Sample{TimestampMs: 40_000, Value: 3}),
		}}},
		{tenantID: "tenant-2", at: at, req: &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{
			mkSeries(seriesA, mimirpb.Sample{TimestampMs: 0, Value: 1}, mimirpb.Sample{TimestampMs: 99_000, Value: 1}),
		}}},
	}

	dumpFile := createTestDumpFileWithTimestamps(t, records)

	app := newTestApp()
	cmd := &DumpCommand{}
	printer := &BufferedPrinter{}
	cmd.Register(app, func() *kgo.Client { return nil }, printer)

	_, err := app.Parse([]string{"dump", "analyse-series", "--file", dumpFile, "--tenant", "tenant-1"})
	require.NoError(t, err)

	lines := printer.GetLines()
	output := strings.Join(lines, "\n")

	assert.Contains(t, output, "Unique series:  2")
	assert.Contains(t, output, "Total samples:  4")

	// tenant-2 not scanned: its 99s gap must not appear.
	assert.NotContains(t, output, "1m39s")

	// series A row: 3 samples, min 10s, max 30s, avg 20s.
	var seriesARow, seriesBRow string
	for _, l := range lines {
		if strings.Contains(l, `metric_a`) {
			seriesARow = l
		}
		if strings.Contains(l, `metric_b`) {
			seriesBRow = l
		}
	}
	require.NotEmpty(t, seriesARow)
	assert.Equal(t, []string{"3", "10s", "30s", "20s"}, strings.Fields(seriesARow)[:4])

	// series B row: single sample -> deltas rendered as "-".
	require.NotEmpty(t, seriesBRow)
	assert.Equal(t, []string{"1", "-", "-", "-"}, strings.Fields(seriesBRow)[:4])
}

func TestDumpCommand_AnalyseSeries_RequiresTenant(t *testing.T) {
	dumpFile := createTestDumpFileWithTimestamps(t, nil)

	app := newTestApp()
	cmd := &DumpCommand{}
	cmd.Register(app, func() *kgo.Client { return nil }, &BufferedPrinter{})

	_, err := app.Parse([]string{"dump", "analyse-series", "--file", dumpFile})
	require.Error(t, err)
}
