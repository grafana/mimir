// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestDumpCommand_FindDuplicates(t *testing.T) {
	mkLabels := func(nameValue ...string) []mimirpb.LabelAdapter {
		labels := make([]mimirpb.LabelAdapter, 0, len(nameValue)/2)
		for i := 0; i < len(nameValue); i += 2 {
			labels = append(labels, mimirpb.LabelAdapter{Name: nameValue[i], Value: nameValue[i+1]})
		}
		return labels
	}
	mkSeries := func(labels []mimirpb.LabelAdapter, ts int64, val float64) mimirpb.PreallocTimeseries {
		return mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{
			Labels:  labels,
			Samples: []mimirpb.Sample{{TimestampMs: ts, Value: val}},
		}}
	}

	seriesA := mkLabels("__name__", "metric_a", "job", "test")
	seriesB := mkLabels("__name__", "metric_b", "job", "test")

	t1 := time.Date(2026, 5, 29, 11, 0, 1, 0, time.UTC)
	t2 := time.Date(2026, 5, 29, 11, 0, 31, 0, time.UTC)
	t3 := time.Date(2026, 5, 29, 11, 1, 1, 0, time.UTC)

	records := []dumpRecord{
		// tenant-1, series A: first occurrence of (ts=100, val=1).
		{tenantID: "tenant-1", at: t1, req: &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{
			mkSeries(seriesA, 100, 1),
			mkSeries(seriesB, 100, 1),
		}}},
		// tenant-1, series A: exact duplicate (ts=100, val=1) -> reported.
		// tenant-1, series B: same ts but different value -> NOT a duplicate.
		{tenantID: "tenant-1", at: t2, req: &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{
			mkSeries(seriesA, 100, 1),
			mkSeries(seriesB, 100, 2),
		}}},
		// tenant-1, series A: ts advanced -> NOT a duplicate.
		{tenantID: "tenant-1", at: t3, req: &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{
			mkSeries(seriesA, 200, 1),
		}}},
		// tenant-2: its own identical resend, must be excluded when --tenant=tenant-1.
		{tenantID: "tenant-2", at: t1, req: &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{
			mkSeries(seriesA, 100, 1),
		}}},
		{tenantID: "tenant-2", at: t2, req: &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{
			mkSeries(seriesA, 100, 1),
		}}},
	}

	dumpFile := createTestDumpFileWithTimestamps(t, records)

	app := newTestApp()
	cmd := &DumpCommand{}
	printer := &BufferedPrinter{}
	cmd.Register(app, func() *kgo.Client { return nil }, printer)

	_, err := app.Parse([]string{
		"dump", "find-duplicates",
		"--file", dumpFile,
		"--tenant", "tenant-1",
	})
	require.NoError(t, err)

	output := strings.Join(printer.GetLines(), "\n")

	// Exactly one duplicate (series A, tenant-1), referencing both arrival times.
	assert.Contains(t, output, `labels: {__name__="metric_a", job="test"}, received (ts=100, val=1) at 2026-05-29T11:00:01Z (offset 0) and then at 2026-05-29T11:00:31Z (offset 1)`)
	// series B differed in value -> not reported.
	assert.NotContains(t, output, "metric_b")
	// tenant-2 filtered out.
	assert.Equal(t, 1, strings.Count(output, "labels:"))
	assert.Contains(t, output, "total duplicate occurrences: 1")
}

type dumpRecord struct {
	tenantID string
	at       time.Time
	req      *mimirpb.WriteRequest
}

// createTestDumpFileWithTimestamps writes a dump file whose records carry a
// Kafka timestamp, which find-duplicates needs (the shared createTestDumpFile
// helper only sets Offset).
func createTestDumpFileWithTimestamps(t *testing.T, records []dumpRecord) string {
	t.Helper()

	file, err := os.CreateTemp(t.TempDir(), "dump_find_duplicates_test")
	require.NoError(t, err)

	encoder := json.NewEncoder(file)
	for i, r := range records {
		data, err := r.req.Marshal()
		require.NoError(t, err)

		require.NoError(t, encoder.Encode(&kgo.Record{
			Key:       []byte(r.tenantID),
			Value:     data,
			Offset:    int64(i),
			Timestamp: r.at,
		}))
	}

	require.NoError(t, file.Close())
	return file.Name()
}
