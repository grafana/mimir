// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func resourceKeys(entry mimirpb.ResourceEntry) []string {
	keys := make([]string, 0, len(entry.Attributes))
	for _, a := range entry.Attributes {
		keys = append(keys, a.Key)
	}
	return keys
}

func TestApplyResourceLimits(t *testing.T) {
	str := func(key, value string) mimirpb.ResourceAttribute {
		return mimirpb.ResourceAttribute{Key: key, StringValue: value}
	}
	long := strings.Repeat("x", 11)
	newRequest := func() *mimirpb.WriteRequest {
		return &mimirpb.WriteRequest{
			Timeseries: []mimirpb.PreallocTimeseries{
				{TimeSeries: &mimirpb.TimeSeries{ResourceRef: 1}},
				{TimeSeries: &mimirpb.TimeSeries{ResourceRef: 2}},
				{TimeSeries: &mimirpb.TimeSeries{ResourceRef: 2}},
				{TimeSeries: &mimirpb.TimeSeries{}},
			},
			ResourceTable: []mimirpb.ResourceEntry{
				{Attributes: []mimirpb.ResourceAttribute{
					str("zone", "a"), str("service.name", "frontend"), str("host.name", "node-1"), str("os.type", "linux"),
					str("service.instance.id", "1"), {Key: "host.cores", Type: mimirpb.RESOURCE_ATTRIBUTE_INT, IntValue: 8},
					{Key: "build.id", Type: mimirpb.RESOURCE_ATTRIBUTE_BYTES, BytesValue: []byte(long)},
					str("process.command_line", long),
				}},
				{Attributes: []mimirpb.ResourceAttribute{str("service.name", long), str("host.name", "node-2")}},
			},
		}
	}

	t.Run("no limits", func(t *testing.T) {
		req := newRequest()
		applyResourceLimits(newResourceLimitMetrics(prometheus.NewPedanticRegistry()), "user", req, 0, 0)
		require.Equal(t, newRequest(), req)
	})

	t.Run("limits", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		req := newRequest()
		applyResourceLimits(newResourceLimitMetrics(reg), "user", req, 4, 10)

		// The long descriptive values go, then descriptive attributes in key order until four are left, and the service
		// attributes stay in their places.
		require.Equal(t, []string{"zone", "service.name", "os.type", "service.instance.id"}, resourceKeys(req.ResourceTable[0]))
		// A long service attribute drops the resource from its series.
		refs := make([]uint32, 0, len(req.Timeseries))
		for _, ts := range req.Timeseries {
			refs = append(refs, ts.ResourceRef)
		}
		require.Equal(t, []uint32{1, 0, 0, 0}, refs)

		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_discarded_metadata_total The total number of metadata that were discarded.
			# TYPE cortex_discarded_metadata_total counter
			cortex_discarded_metadata_total{reason="resource_attribute_value_too_long",user="user"} 2
			cortex_discarded_metadata_total{reason="resource_service_attribute_value_too_long",user="user"} 1
			cortex_discarded_metadata_total{reason="resource_too_many_attributes",user="user"} 1
		`), "cortex_discarded_metadata_total"))
	})

	t.Run("service attributes alone over the limit", func(t *testing.T) {
		req := newRequest()
		applyResourceLimits(newResourceLimitMetrics(prometheus.NewPedanticRegistry()), "user", req, 1, 0)
		require.Equal(t, []string{"service.name", "service.instance.id"}, resourceKeys(req.ResourceTable[0]))
	})
}
