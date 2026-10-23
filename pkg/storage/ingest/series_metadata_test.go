// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"fmt"
	"math"
	"slices"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/seriesstore/store"
)

var (
	requestsMetadata = mimirpb.SeriesMetadata{Type: mimirpb.COUNTER, Help: "Requests.", Unit: "requests"}
	errorsMetadata   = mimirpb.SeriesMetadata{Type: mimirpb.COUNTER, Help: "Errors."}
)

// seriesMetadataRequest is a request whose series carry metadata of their own, next to their metric families': two
// series of one family, one of another, and a family without series.
func seriesMetadataRequest() *mimirpb.WriteRequest {
	series := func(name, pod string, metadata mimirpb.SeriesMetadata) mimirpb.PreallocTimeseries {
		return mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{
			Labels:         mimirpb.FromLabelsToLabelAdapters(labels.FromStrings(model.MetricNameLabel, name, "pod", pod)),
			Samples:        []mimirpb.Sample{{TimestampMs: 10, Value: 1}},
			SeriesMetadata: metadata,
		}}
	}
	return &mimirpb.WriteRequest{
		Timeseries: []mimirpb.PreallocTimeseries{
			series("requests_total", "pod-0", requestsMetadata),
			series("requests_total", "pod-1", requestsMetadata),
			series("errors_total", "pod-0", errorsMetadata),
		},
		Metadata: []*mimirpb.MetricMetadata{
			{MetricFamilyName: "requests_total", Type: mimirpb.COUNTER, Help: "Requests.", Unit: "requests"},
			{MetricFamilyName: "errors_total", Type: mimirpb.COUNTER, Help: "Errors."},
			{MetricFamilyName: "no_series", Type: mimirpb.GAUGE, Help: "No series of its own."},
		},
	}
}

// The new writer's records decode without error the way main decodes them, taking metric families' metadata from
// every series. Each family's metadata then repeats, but folds to what the stand-ins alone give.
func TestSeriesMetadataRecordsDecodeLikeMainDoes(t *testing.T) {
	records, _, err := RecordSerializerFromVersion(2).ToRecords("test", 0, "tenant", seriesMetadataRequest(), math.MaxInt)
	require.NoError(t, err)
	require.Len(t, records, 1)

	var standInsOnly mimirpb.PreallocWriteRequest
	require.NoError(t, DeserializeRecordContent(records[0].Value, &standInsOnly, 2))
	asMain := mimirpb.PreallocWriteRequest{
		UnmarshalFromRW2:                true,
		RW2SymbolOffset:                 V2RecordSymbolOffset,
		RW2CommonSymbols:                V2CommonSymbols.GetSlice(),
		SkipNormalizeMetadataMetricName: true,
		SkipDeduplicateMetadata:         true,
	}
	require.NoError(t, mimirpb.Unmarshal(records[0].Value, &asMain))

	require.Equal(t, seriesOf(standInsOnly.Timeseries), seriesOf(asMain.Timeseries))
	require.Len(t, standInsOnly.Metadata, 3)
	require.Len(t, asMain.Metadata, 6)
	require.Equal(t, foldMetadata(standInsOnly.Metadata), foldMetadata(asMain.Metadata))
}

// The old writer put a metric family's metadata on the family's series named like it instead of in a stand-in. The
// new decoders take that for the series' own metadata, so the family's metadata is lost: a known gap while a
// partition holds records from both writers.
func TestOldFormatRecordsLoseMetadataOfFamiliesWithSeriesNamedLikeThem(t *testing.T) {
	req := seriesMetadataRequest()
	for _, ts := range req.Timeseries {
		ts.SeriesMetadata = mimirpb.SeriesMetadata{}
	}
	rw2, err := mimirpb.FromWriteRequestToRW2Request(req, V2CommonSymbols, V2RecordSymbolOffset)
	require.NoError(t, err)
	// The stand-ins follow the series, in the order of the request's metadata. Put each family's metadata on the
	// family's series instead, and keep the stand-in of the family without series only, as the old writer did.
	series, standIns := rw2.TimeseriesRW2[:3], rw2.TimeseriesRW2[3:]
	require.Len(t, standIns, 3)
	series[0].Metadata, series[1].Metadata, series[2].Metadata = standIns[0].Metadata, standIns[0].Metadata, standIns[1].Metadata
	rw2.TimeseriesRW2 = []mimirpb.TimeSeriesRW2{series[0], series[1], series[2], standIns[2]}
	content, err := rw2.Marshal()
	require.NoError(t, err)

	noSeries := []*mimirpb.MetricMetadata{{MetricFamilyName: "no_series", Type: mimirpb.GAUGE, Help: "No series of its own."}}
	var decoded mimirpb.PreallocWriteRequest
	require.NoError(t, DeserializeRecordContent(content, &decoded, 2))
	require.Equal(t, noSeries, decoded.Metadata)
	require.Len(t, decoded.Timeseries, 4)
	for i, expected := range []mimirpb.SeriesMetadata{requestsMetadata, requestsMetadata, errorsMetadata, {}} {
		require.Equal(t, expected, decoded.Timeseries[i].SeriesMetadata)
	}

	record, err := store.ReadRW2(content, V2CommonSymbols.GetSlice(), V2RecordSymbolOffset)
	require.NoError(t, err)
	require.Equal(t, noSeries, record.Metadata())
}

// seriesOf describes each series by its labels, samples and own metadata.
func seriesOf(timeseries []mimirpb.PreallocTimeseries) []string {
	var out []string
	for _, ts := range timeseries {
		out = append(out, fmt.Sprintf("%s %v %+v", mimirpb.FromLabelAdaptersToString(ts.Labels), ts.Samples, ts.SeriesMetadata))
	}
	return out
}

// foldMetadata folds metric families' metadata the way the ingester keeps it: each family's distinct metadata, sorted.
func foldMetadata(metadata []*mimirpb.MetricMetadata) map[string][]string {
	folded := map[string][]string{}
	for _, m := range metadata {
		entry := fmt.Sprintf("%s|%s|%s", m.Type, m.Help, m.Unit)
		if !slices.Contains(folded[m.MetricFamilyName], entry) {
			folded[m.MetricFamilyName] = append(folded[m.MetricFamilyName], entry)
			slices.Sort(folded[m.MetricFamilyName])
		}
	}
	return folded
}
