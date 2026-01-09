// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestDumpCommand_Analyse(t *testing.T) {
	// Helper to create a timeseries with labels and N samples.
	mkSeries := func(labels []mimirpb.LabelAdapter, numSamples int) mimirpb.PreallocTimeseries {
		samples := make([]mimirpb.Sample, numSamples)
		return mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{Labels: labels, Samples: samples}}
	}

	// Helper to create labels.
	mkLabels := func(nameValue ...string) []mimirpb.LabelAdapter {
		labels := make([]mimirpb.LabelAdapter, 0, len(nameValue)/2)
		for i := 0; i < len(nameValue); i += 2 {
			labels = append(labels, mimirpb.LabelAdapter{Name: nameValue[i], Value: nameValue[i+1]})
		}
		return labels
	}

	requests := []struct {
		tenantID string
		req      *mimirpb.WriteRequest
	}{
		{
			tenantID: "tenant-1",
			req: &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{
				mkSeries(mkLabels("__name__", "metric_a", "job", "test"), 2),
				mkSeries(mkLabels("__name__", "metric_b", "job", "test"), 1),
			}},
		},
		{
			tenantID: "tenant-2",
			req: &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{
				mkSeries(mkLabels("__name__", "metric_c", "instance", "localhost:9090"), 1),
				mkSeries(mkLabels("__name__", "metric_d", "instance", "localhost:9090"), 1),
				mkSeries(mkLabels("__name__", "metric_e", "instance", "localhost:9090"), 1),
			}},
		},
		{
			tenantID: "tenant-2",
			req: &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{
				mkSeries(mkLabels("__name__", "metric_c", "instance", "localhost:9090"), 1),
				mkSeries(mkLabels("__name__", "metric_d", "instance", "localhost:9090"), 1),
				mkSeries(mkLabels("__name__", "metric_f", "instance", "localhost:9090"), 1),
			}},
		},
		{
			tenantID: "tenant-3",
			req: &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{
				mkSeries(mkLabels("__name__", "metric_g", "env", "production", "region", "us-east-1"), 1),
				mkSeries(mkLabels("__name__", "metric_h", "env", "production", "region", "us-east-1"), 1),
				mkSeries(mkLabels("__name__", "metric_i", "env", "production", "region", "us-east-1"), 1),
				mkSeries(mkLabels("__name__", "metric_j", "env", "production", "region", "us-east-1"), 1),
				mkSeries(mkLabels("__name__", "metric_k", "env", "production", "region", "us-east-1"), 1),
			}},
		},
	}

	// Create the dump file.
	dumpFile := createTestDumpFile(t, requests)

	// Run the analyse command.
	app := newTestApp()
	cmd := &DumpCommand{}
	printer := &BufferedPrinter{}
	cmd.Register(app, func() *kgo.Client { return nil }, printer)

	_, err := app.Parse([]string{
		"dump", "analyse",
		"--file", dumpFile,
	})
	require.NoError(t, err)

	// Get the output and verify.
	output := strings.Join(printer.GetLines(), "\n")
	expectedOutput := `=== DUMP ANALYSIS ===
Total requests:           4
Total unique tenants:     3
Total unique series:      11
Total samples:            14
Avg samples/series:       1.27
Avg series/req:           3.25

=== TENANT CLASS DISTRIBUTION (by unique timeseries) ===
Class                   Tenants       Series  Avg series/req
Small (<10K)             100.0%       100.0%            3.25
Medium (<100K)             0.0%         0.0%            0.00
Large (>=100K)             0.0%         0.0%            0.00

=== LABEL METRICS ===
Avg labels/series:        1
Avg label name length:    6
Avg label value length:   10
Avg metric name length:   8
Unique label names:       4
Unique label values:      4`

	assert.Equal(t, expectedOutput, output)
}

// createTestDumpFile creates a temporary dump file with the given requests.
func createTestDumpFile(t *testing.T, requests []struct {
	tenantID string
	req      *mimirpb.WriteRequest
}) string {
	t.Helper()

	file, err := os.CreateTemp(t.TempDir(), "dump_analyse_test")
	require.NoError(t, err)

	encoder := json.NewEncoder(file)
	for i, r := range requests {
		data, err := r.req.Marshal()
		require.NoError(t, err)

		err = encoder.Encode(&kgo.Record{
			Key:    []byte(r.tenantID),
			Value:  data,
			Offset: int64(i),
		})
		require.NoError(t, err)
	}

	require.NoError(t, file.Close())
	return file.Name()
}
