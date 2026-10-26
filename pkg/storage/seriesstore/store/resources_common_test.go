// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"context"
	"math"
	"testing"

	promlabels "github.com/prometheus/prometheus/model/labels"
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
