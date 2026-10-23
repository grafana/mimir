// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/storage/seriesstore/store"
	"github.com/grafana/mimir/pkg/util/testkafka"
)

var updateGolden = flag.Bool("update-golden", false, "Rewrite the golden files of the tests that have them.")

// The metric families' metadata in each partition's records, as both record decoders read it and folded the way the
// ingester keeps it, stays the same whatever metadata of their own the series carry.
func TestLegacyMetricMetadataByPartition(t *testing.T) {
	const numPartitions = 4
	golden := map[string]map[string]map[int32]map[string][]string{}
	for _, exemplars := range []bool{true, false} {
		name := fmt.Sprintf("exemplars=%t", exemplars)
		t.Run(name, func(t *testing.T) {
			limits := prepareDefaultLimits()
			if !exemplars {
				// Exemplar-only series lose their exemplars, and keep their labels and metadata.
				limits.MaxGlobalExemplarsPerUser = 0
			}
			kafkaCluster, _ := testkafka.CreateCluster(t, numPartitions, kafkaTopic)
			distributors, _, _, _ := prepare(t, prepConfig{
				numDistributors:         1,
				numIngesters:            numPartitions,
				happyIngesters:          numPartitions,
				replicationFactor:       1,
				ingesterIngestionType:   ingesterIngestionTypeGRPC,
				ingestStorageEnabled:    true,
				ingestStoragePartitions: numPartitions,
				ingestStorageKafka:      kafkaCluster,
				limits:                  limits,
			})
			ctx := user.InjectOrgID(context.Background(), "tenant")
			_, err := distributors[0].Push(ctx, legacyMetadataFixture(time.Now().UnixMilli()))
			require.NoError(t, err)

			byDecoder := map[string]map[int32]map[string][]string{"mimirpb": {}, "seriesstore": {}}
			fold := func(decoder string, partition int32, metadata []*mimirpb.MetricMetadata) {
				if byDecoder[decoder][partition] == nil {
					byDecoder[decoder][partition] = map[string][]string{}
				}
				families := byDecoder[decoder][partition]
				for _, m := range metadata {
					entry := fmt.Sprintf("%s|%s|%s", m.Type, m.Help, m.Unit)
					if !slices.Contains(families[m.MetricFamilyName], entry) {
						families[m.MetricFamilyName] = append(families[m.MetricFamilyName], entry)
						slices.Sort(families[m.MetricFamilyName])
					}
				}
			}
			for _, record := range readAllRecordsFromKafka(t, kafkaCluster.ListenAddrs(), numPartitions, time.Second) {
				require.Equal(t, 2, ingest.ParseRecordVersion(record))
				var request mimirpb.PreallocWriteRequest
				require.NoError(t, ingest.DeserializeRecordContent(record.Value, &request, 2))
				fold("mimirpb", record.Partition, request.Metadata)
				rw2, err := store.ReadRW2(record.Value, ingest.V2CommonSymbols.GetSlice(), ingest.V2RecordSymbolOffset)
				require.NoError(t, err)
				fold("seriesstore", record.Partition, rw2.Metadata())
			}
			golden[name] = byDecoder
		})
	}
	compareGolden(t, filepath.Join("testdata", "legacy_metric_metadata_by_partition.json"), golden)
}

// legacyMetadataFixture is a request with what decides where metric families' metadata goes: series named like their
// family, two of them in one partition, conflicting metadata of a family, an exemplar-only series, a family without
// series, and a classic histogram, whose series aren't named like their family.
func legacyMetadataFixture(now int64) *mimirpb.WriteRequest {
	series := func(name string, extra ...string) mimirpb.PreallocTimeseries {
		labels := []mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: name}}
		for i := 0; i < len(extra); i += 2 {
			labels = append(labels, mimirpb.LabelAdapter{Name: extra[i], Value: extra[i+1]})
		}
		return mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{
			Labels:  labels,
			Samples: []mimirpb.Sample{{TimestampMs: now, Value: 1}},
		}}
	}

	var timeseries []mimirpb.PreallocTimeseries
	// Eight series named like their family put two in one of the four partitions.
	for n := range 8 {
		timeseries = append(timeseries, series("requests_total", "pod", fmt.Sprintf("pod-%d", n)))
	}
	for _, le := range []string{"0.1", "1", "+Inf"} {
		timeseries = append(timeseries, series("latency_seconds_bucket", "le", le))
	}
	timeseries = append(timeseries, series("latency_seconds_sum"), series("latency_seconds_count"))
	exemplarOnly := series("exemplars_total", "pod", "pod-0")
	exemplarOnly.Samples = nil
	exemplarOnly.Exemplars = []mimirpb.Exemplar{{TimestampMs: now, Value: 1, Labels: []mimirpb.LabelAdapter{{Name: "trace_id", Value: "abc"}}}}
	timeseries = append(timeseries, exemplarOnly)

	return &mimirpb.WriteRequest{
		Timeseries: timeseries,
		Metadata: []*mimirpb.MetricMetadata{
			{MetricFamilyName: "requests_total", Type: mimirpb.COUNTER, Help: "Requests.", Unit: "requests"},
			{MetricFamilyName: "requests_total", Type: mimirpb.COUNTER, Help: "Requests served.", Unit: "requests"},
			{MetricFamilyName: "latency_seconds", Type: mimirpb.HISTOGRAM, Help: "Latency.", Unit: "seconds"},
			{MetricFamilyName: "exemplars_total", Type: mimirpb.GAUGE, Help: "With exemplars."},
			{MetricFamilyName: "no_series", Type: mimirpb.GAUGE, Help: "No series of its own."},
		},
		Source: mimirpb.API,
	}
}

// compareGolden compares actual, as indented JSON, with the golden file at path, which -update-golden rewrites.
func compareGolden(t *testing.T, path string, actual any) {
	t.Helper()
	data, err := json.MarshalIndent(actual, "", "  ")
	require.NoError(t, err)
	data = append(data, '\n')
	if *updateGolden {
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, data, 0o644))
		return
	}
	expected, err := os.ReadFile(path)
	require.NoError(t, err)
	require.JSONEq(t, string(expected), string(data))
}
