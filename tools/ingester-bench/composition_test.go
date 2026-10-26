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
	today.add("tenant", &mimirpb.WriteRequest{
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
	newer.add("", &mimirpb.WriteRequest{
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

// A series' family is the one its name and the records' metadata, buckets and quantiles tell of: a metric whose name
// only ends like a histogram's or summary's series is a family of its own.
func TestFamilyNamesFollowTheRecords(t *testing.T) {
	var f familyNames
	f.learn(&mimirpb.WriteRequest{
		Timeseries: []mimirpb.PreallocTimeseries{
			compositionSeries("untyped_bucket", mimirpb.SeriesMetadata{}, "le", "1"),
			compositionSeries("rpc", mimirpb.SeriesMetadata{}, "quantile", "0.5"),
			compositionSeries("jobs", mimirpb.SeriesMetadata{Type: mimirpb.COUNTER, Help: "Jobs."}),
		},
		Metadata: []*mimirpb.MetricMetadata{
			{MetricFamilyName: "example", Type: mimirpb.GAUGE},
			{MetricFamilyName: "example_count", Type: mimirpb.COUNTER},
			{MetricFamilyName: "latency", Type: mimirpb.HISTOGRAM},
			{MetricFamilyName: "sizes", Type: mimirpb.SUMMARY},
		},
	})
	for name, family := range map[string]string{
		// Families of their own, by their metadata, or for want of any that would make them part of another.
		"example": "example", "example_count": "example_count", "jobs_count": "jobs_count", "requests_sum": "requests_sum",
		// A histogram's and a summary's series, by their metadata.
		"latency_bucket": "latency", "latency_sum": "latency", "latency_count": "latency", "sizes_sum": "sizes", "sizes_count": "sizes",
		// A summary has no buckets.
		"sizes_bucket": "sizes_bucket",
		// Without metadata, by their buckets and quantiles.
		"untyped_bucket": "untyped", "untyped_count": "untyped", "rpc_sum": "rpc", "rpc_count": "rpc", "rpc_bucket": "rpc_bucket",
		// A family whose series carried its metadata.
		"jobs": "jobs",
	} {
		require.Equal(t, family, f.of(name), name)
	}
}

// Of today's records, a gauge and a counter named like a histogram's series are two families, so two values.
func TestCompositionKeepsMetricsNamedLikeHistogramSeriesApart(t *testing.T) {
	counter := compositionCounter{today: true}
	counter.add("tenant", &mimirpb.WriteRequest{
		Timeseries: []mimirpb.PreallocTimeseries{compositionSeries("example", mimirpb.SeriesMetadata{}), compositionSeries("example_count", mimirpb.SeriesMetadata{})},
		Metadata:   []*mimirpb.MetricMetadata{{MetricFamilyName: "example", Type: mimirpb.GAUGE}, {MetricFamilyName: "example_count", Type: mimirpb.COUNTER}},
	})
	require.Equal(t, 1.0, counter.composition().SeriesPerValue)
}

// writeDump writes the tenant's request as a kafkatool dump's records, keyed by the tenant.
func writeDump(t *testing.T, tenant string, req *mimirpb.WriteRequest) string {
	t.Helper()
	records, _, err := ingest.RecordSerializerFromVersion(2).ToRecords("ingest", 0, tenant, req, 1<<20)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "dump.json")
	f, err := os.Create(path)
	require.NoError(t, err)
	for _, r := range records {
		require.NoError(t, json.NewEncoder(f).Encode(&kgo.Record{Key: []byte(tenant), Value: r.Value, Headers: r.Headers}))
	}
	require.NoError(t, f.Close())
	return path
}

// A tenant's metadata says nothing of another's metrics: one tenant's counter named like another's histogram's count
// leaves that histogram one family.
func TestCompositionKeepsTenantsFamiliesApart(t *testing.T) {
	a := writeDump(t, "tenant-a", &mimirpb.WriteRequest{
		Timeseries: []mimirpb.PreallocTimeseries{
			compositionSeries("example_bucket", mimirpb.SeriesMetadata{}, "le", "1"),
			compositionSeries("example_count", mimirpb.SeriesMetadata{}),
		},
		Metadata: []*mimirpb.MetricMetadata{{MetricFamilyName: "example", Type: mimirpb.HISTOGRAM}},
	})
	b := writeDump(t, "tenant-b", &mimirpb.WriteRequest{
		Timeseries: []mimirpb.PreallocTimeseries{compositionSeries("example_count", mimirpb.SeriesMetadata{})},
		Metadata:   []*mimirpb.MetricMetadata{{MetricFamilyName: "example_count", Type: mimirpb.COUNTER}},
	})
	alone, err := compositionOfDumps([]string{a})
	require.NoError(t, err)
	require.Equal(t, 2.0, alone.SeriesPerValue)
	alone, err = compositionOfDumps([]string{b})
	require.NoError(t, err)
	require.Equal(t, 1.0, alone.SeriesPerValue)
	both, err := compositionOfDumps([]string{a, b})
	require.NoError(t, err)
	require.Equal(t, 1.5, both.SeriesPerValue, "three series of two families, one each tenant's")
}

// A family a tenant's metadata gives two types is counted, and taken as of no type: its series are told by its buckets
// and quantiles, or else by their own names.
func TestCompositionCountsFamiliesOfConflictingTypes(t *testing.T) {
	path := writeDump(t, "tenant", &mimirpb.WriteRequest{
		Timeseries: []mimirpb.PreallocTimeseries{compositionSeries("jobs", mimirpb.SeriesMetadata{}), compositionSeries("jobs_count", mimirpb.SeriesMetadata{})},
		Metadata:   []*mimirpb.MetricMetadata{{MetricFamilyName: "jobs", Type: mimirpb.HISTOGRAM}},
	})
	other := writeDump(t, "tenant", &mimirpb.WriteRequest{
		Timeseries: []mimirpb.PreallocTimeseries{compositionSeries("jobs", mimirpb.SeriesMetadata{})},
		Metadata:   []*mimirpb.MetricMetadata{{MetricFamilyName: "jobs", Type: mimirpb.COUNTER}},
	})
	c, err := compositionOfDumps([]string{path, other})
	require.NoError(t, err)
	require.Equal(t, 1, c.ConflictingFamilies)
	// Without buckets, jobs_count is a family of its own: the first record's two series are two values.
	require.Equal(t, 3.0/3.0, c.SeriesPerValue)
}

// Of today's records that came by OTLP, a series' resource is its job and instance, and target_info series tell the
// resources' attributes, each counted once.
func TestCompositionOfTodaysOTLPRecords(t *testing.T) {
	today := compositionCounter{today: true}
	info := compositionSeries("target_info", mimirpb.SeriesMetadata{}, "host_name", "node-1", "host_cores", "8", "instance", "a", "job", "checkout")
	for range 2 {
		today.add("tenant", &mimirpb.WriteRequest{Source: mimirpb.OTLP, Timeseries: []mimirpb.PreallocTimeseries{
			compositionSeries("requests_total", mimirpb.SeriesMetadata{}, "instance", "a", "job", "checkout"),
			compositionSeries("errors_total", mimirpb.SeriesMetadata{}, "instance", "a", "job", "checkout"),
			compositionSeries("requests_total", mimirpb.SeriesMetadata{}, "instance", "b", "job", "checkout"),
			info,
		}})
	}
	today.add("tenant", &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{compositionSeries("up", mimirpb.SeriesMetadata{}, "instance", "c", "job", "node")}})
	c := today.composition()
	require.Equal(t, 2, c.OTLPRecords)
	require.Equal(t, 2.0, c.ResourcesPerRecord)
	require.Equal(t, 1.5, c.SeriesPerRecordResource)
	require.Equal(t, 8.0/9, c.OTLPSeriesShare)
	require.Equal(t, 3.0, c.SeriesPerTargetInfo)
	require.Equal(t, Percentiles{"0": 2, "0.1": 2, "0.5": 2, "0.9": 2, "0.99": 2, "1": 2}, c.TargetInfoAttributes)
	require.Equal(t, 0.5, c.TargetInfoIntShare)
	require.Zero(t, c.TableBytesPerRecord)
}
