// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
)

// otlpTarget is one OTLP resource's gauge, as a scrape of a target pushes it.
type otlpTarget struct {
	instance string
	// The resource's descriptive attributes.
	descriptive map[string]string
	pods        int
}

// otlpPush is a request with each target's gauge at one timestamp.
func otlpPush(at time.Time, targets ...otlpTarget) pmetric.Metrics {
	md := pmetric.NewMetrics()
	for _, target := range targets {
		rm := md.ResourceMetrics().AppendEmpty()
		attrs := rm.Resource().Attributes()
		attrs.PutStr("service.name", "api")
		attrs.PutStr("service.instance.id", target.instance)
		for k, v := range target.descriptive {
			attrs.PutStr(k, v)
		}
		gauge := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
		gauge.SetName("requests")
		points := gauge.SetEmptyGauge().DataPoints()
		for pod := range target.pods {
			point := points.AppendEmpty()
			point.SetTimestamp(pcommon.NewTimestampFromTime(at))
			point.SetDoubleValue(float64(pod))
			point.Attributes().PutStr("pod", fmt.Sprintf("pod-%d", pod))
		}
	}
	return md
}

// The harness runs info() on the seriesstore engine, with native target_info and with the join: a target whose
// resource changes, and one whose doesn't, over two partitions, enrich alike.
func TestNativeInfo_HarnessSmoke(t *testing.T) {
	skipIfSeriesstore(t, "it sets the engine itself")
	for _, engine := range []string{mimir_tsdb.EngineSeriesstore} {
		t.Run(engine, func(t *testing.T) {
			limits := defaultLimitsTestConfig()
			limits.NativeResourcesEnabled = true
			h := newNativeInfoHarness(t, engine, 2, limits)

			start := time.Now().Truncate(time.Minute).Add(-20 * time.Minute)
			for step := range 10 {
				host := "node-1"
				if step >= 5 {
					host = "node-2"
				}
				h.pushOTLP(t, otlpPush(start.Add(time.Duration(step)*time.Minute),
					otlpTarget{instance: "a", descriptive: map[string]string{"host.name": host}, pods: 3},
					otlpTarget{instance: "b", descriptive: map[string]string{"host.name": "node-3"}, pods: 3},
				))
			}

			query := `info(requests)`
			end := start.Add(9 * time.Minute)
			join := h.rangeQuery(t, false, query, start, end, 30*time.Second)
			native := h.rangeQuery(t, true, query, start, end, 30*time.Second)
			require.NotEmpty(t, join)
			require.Equal(t, join.String(), native.String())
			require.Zero(t, h.fallbacks(t))
			require.Contains(t, native.String(), `host_name="node-2"`)
		})
	}
}
