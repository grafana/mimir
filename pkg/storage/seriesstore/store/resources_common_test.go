// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/prometheus/common/model"
	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
)

func testResourceAttributes(service, host string) []mimirpb.ResourceAttribute {
	return []mimirpb.ResourceAttribute{
		{Key: "service.name", StringValue: service},
		{Key: "host.name", StringValue: host},
		{Key: "service.instance.id", StringValue: "1"},
		{Key: "host.cores", Type: mimirpb.RESOURCE_ATTRIBUTE_INT, IntValue: 8},
	}
}

func resourceRecord(t *testing.T, at int64, attributes []mimirpb.ResourceAttribute, lbls ...promlabels.Labels) *RW2Record {
	req := &mimirpb.WriteRequest{ResourceTable: []mimirpb.ResourceEntry{{Attributes: attributes}}}
	for _, l := range lbls {
		req.Timeseries = append(req.Timeseries, mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{
			Labels:      mimirpb.FromLabelsToLabelAdapters(l),
			Samples:     []mimirpb.Sample{{TimestampMs: at, Value: 1}},
			ResourceRef: 1,
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

// resourceVersionsOf returns the resource versions of the series with lbls.
func resourceVersionsOf(t *testing.T, e *Engine, lbls promlabels.Labels) []ResourceVersion {
	var matchers []*promlabels.Matcher
	lbls.Range(func(l promlabels.Label) {
		matchers = append(matchers, promlabels.MustNewMatcher(promlabels.MatchEqual, l.Name, l.Value))
	})
	series, _, err := e.NativeResources(context.Background(), [][]*promlabels.Matcher{matchers}, math.MinInt64, math.MaxInt64, 0)
	require.NoError(t, err)
	if len(series) == 0 {
		return nil
	}
	require.Len(t, series, 1)
	return series[0].Versions
}

func asResourceContext(attributes []mimirpb.ResourceAttribute) *storage.ResourceContext {
	rc := &storage.ResourceContext{}
	for _, a := range attributes {
		rc.Attributes = append(rc.Attributes, storage.ResourceAttribute{Key: a.Key, Type: storage.ResourceAttributeType(a.Type), String: a.StringValue, Bool: a.BoolValue, Int: a.IntValue, Double: a.DoubleValue, Bytes: a.BytesValue})
	}
	return rc
}

// The capped lookups bound what a lookup of native metadata visits: they count the matching series that have
// metadata, ignoring the others, fail once more than the cap match, and give each series' newest sample, which ends
// its last version.
func TestEngine_NativeLookupsCapped(t *testing.T) {
	e, err := OpenEngine(t.TempDir(), "tenant", EngineOptions{Shards: 4, SecondaryHashFunction: secondaryHash})
	require.NoError(t, err)
	t.Cleanup(func() { _ = e.Close() })
	e.SetNativeMetricMetadata(true)
	e.SetNativeResources(true)
	withMetadata := storage.AppendV2Options{
		Metadata: metadata.Metadata{Type: model.MetricTypeCounter, Help: "Requests."},
		Resource: asResourceContext(testResourceAttributes("api", "host-0")),
	}
	app := e.Appender(context.Background())
	for i := range 3 {
		lbls := promlabels.FromStrings(model.MetricNameLabel, "requests_total", "pod", fmt.Sprint("pod-", i))
		for _, ts := range []int64{1_000, 2_000 + int64(i)} {
			_, err := app.Append(0, lbls, 0, ts, 1, nil, nil, withMetadata)
			require.NoError(t, err)
		}
	}
	_, err = app.Append(0, promlabels.FromStrings(model.MetricNameLabel, "requests_total", "pod", "without-metadata"), 0, 1_000, 1, nil, nil, storage.AppendV2Options{})
	require.NoError(t, err)
	require.NoError(t, app.Commit())
	all := [][]*promlabels.Matcher{{promlabels.MustNewMatcher(promlabels.MatchEqual, model.MetricNameLabel, "requests_total")}}

	// The limit doesn't bound what's visited: all three series with metadata are, and the first by labels returned.
	metadataSeries, limited, visited, err := e.NativeMetricMetadataCapped(context.Background(), all, 1, 3)
	require.NoError(t, err)
	require.True(t, limited)
	require.Equal(t, 3, visited)
	require.Len(t, metadataSeries, 1)
	require.Equal(t, int64(2_000), metadataSeries[0].NewestSample)
	resourceSeries, limited, visited, err := e.NativeResourcesCapped(context.Background(), all, math.MinInt64, math.MaxInt64, 1, 3, NativeResourcesOptions{})
	require.NoError(t, err)
	require.True(t, limited)
	require.Equal(t, 3, visited)
	require.Len(t, resourceSeries, 1)
	require.Equal(t, int64(2_000), resourceSeries[0].NewestSample)

	_, _, _, err = e.NativeMetricMetadataCapped(context.Background(), all, 0, 2)
	require.ErrorIs(t, err, ErrLookupTooManySeries)
	_, _, _, err = e.NativeResourcesCapped(context.Background(), all, math.MinInt64, math.MaxInt64, 0, 2, NativeResourcesOptions{})
	require.ErrorIs(t, err, ErrLookupTooManySeries)
}
