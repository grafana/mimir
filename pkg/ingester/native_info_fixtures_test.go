// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/grafana/mimir/pkg/mimirpb"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
)

// otlpResource is an OTLP resource's identity and description.
type otlpResource struct {
	namespace, name, instance string
	descriptive               map[string]string
}

// otlpGauge is a request with one resource's gauge, of the pods, at one timestamp.
func otlpGauge(at time.Time, r otlpResource, pods ...int) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	attrs := rm.Resource().Attributes()
	if r.namespace != "" {
		attrs.PutStr("service.namespace", r.namespace)
	}
	attrs.PutStr("service.name", r.name)
	attrs.PutStr("service.instance.id", r.instance)
	for k, v := range r.descriptive {
		attrs.PutStr(k, v)
	}
	gauge := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	gauge.SetName("requests")
	points := gauge.SetEmptyGauge().DataPoints()
	for _, pod := range pods {
		point := points.AppendEmpty()
		point.SetTimestamp(pcommon.NewTimestampFromTime(at))
		point.SetDoubleValue(float64(pod))
		point.Attributes().PutStr("pod", fmt.Sprintf("pod-%d", pod))
	}
	return md
}

// matrixPoints are a range query's points, by series and timestamp, as strings of their values.
func matrixPoints(m promql.Matrix) map[string]map[int64]string {
	out := map[string]map[int64]string{}
	for _, s := range m {
		points := map[int64]string{}
		for _, p := range s.Floats {
			points[p.T] = fmt.Sprint(p.F)
		}
		out[s.Metric.String()] = points
	}
	return out
}

// differingSteps are the timestamps where the two results differ: a series that has a point in one and not the other,
// or another value.
func differingSteps(a, b promql.Matrix) []int64 {
	pa, pb := matrixPoints(a), matrixPoints(b)
	steps := map[int64]struct{}{}
	compare := func(x, y map[string]map[int64]string) {
		for series, points := range x {
			for ts, v := range points {
				if y[series][ts] != v {
					steps[ts] = struct{}{}
				}
			}
		}
	}
	compare(pa, pb)
	compare(pb, pa)
	var out []int64
	for ts := range steps {
		out = append(out, ts)
	}
	slices.Sort(out)
	return out
}

// nativeInfoFixture is pushes to the harness, the info() queries over its range, and the steps at which native
// target_info may enrich differently from the join: none for a fixture native must equal the join on.
type nativeInfoFixture struct {
	push func(t testing.TB, h *nativeInfoHarness, at func(minutes float64) time.Time)
	// The queries, each evaluated over the minutes [from, to] of the fixture by step.
	queries  []string
	from, to float64
	step     time.Duration
	// The minutes at which native and the join differ, as the fixture's documented class predicts.
	differAt []float64
}

func TestNativeInfo_Fixtures(t *testing.T) {
	skipIfSeriesstore(t, "it sets the engine itself")
	api := func(instance, host string) otlpResource {
		return otlpResource{name: "api", instance: instance, descriptive: map[string]string{"host.name": host}}
	}
	everyMinute := func(from, to int, push func(minute int)) {
		for minute := from; minute <= to; minute++ {
			push(minute)
		}
	}
	fixtures := map[string]nativeInfoFixture{
		// One identity replaces another that collapses onto the same job and instance, service.namespace x and
		// service.name api against service.name x/api: the join and native both have the second from its first push.
		"sequential collision": {
			push: func(t testing.TB, h *nativeInfoHarness, at func(float64) time.Time) {
				everyMinute(0, 9, func(m int) {
					r := otlpResource{namespace: "x", name: "api", instance: "a", descriptive: map[string]string{"host.name": "node-1"}}
					if m >= 5 {
						r = otlpResource{name: "x/api", instance: "a", descriptive: map[string]string{"host.name": "node-2"}}
					}
					h.pushOTLP(t, otlpGauge(at(float64(m)), r, 0, 1))
				})
			},
			queries: []string{`info(requests)`},
			from:    0, to: 9, step: 30 * time.Second,
		},
		// A target stops pushing for longer than the lookback delta, then goes on with the same resource: an instant
		// selector has no inner series in the gap either, so native's samples through it change nothing.
		"gap under instant selectors": {
			push: func(t testing.TB, h *nativeInfoHarness, at func(float64) time.Time) {
				for _, m := range []int{0, 1, 2, 3, 12, 13, 14} {
					h.pushOTLP(t, otlpGauge(at(float64(m)), api("a", "node-1"), 0, 1))
				}
			},
			queries: []string{`info(requests)`},
			from:    0, to: 14, step: time.Minute,
		},
		// A target's series in different partitions see a change at their own next samples: pod-0 at minute 5, pod-1
		// 15s later. Both enrich with the new host from minute 5, as the join's newest target_info sample has it.
		"a change seen at different samples": {
			push: func(t testing.TB, h *nativeInfoHarness, at func(float64) time.Time) {
				everyMinute(0, 4, func(m int) { h.pushOTLP(t, otlpGauge(at(float64(m)), api("a", "node-1"), 0, 1, 2, 3)) })
				h.pushOTLP(t, otlpGauge(at(5), api("a", "node-2"), 0, 2))
				h.pushOTLP(t, otlpGauge(at(5.25), api("a", "node-2"), 1, 3))
				everyMinute(6, 9, func(m int) { h.pushOTLP(t, otlpGauge(at(float64(m)), api("a", "node-2"), 0, 1, 2, 3)) })
			},
			queries: []string{`info(requests)`},
			from:    0, to: 9, step: 15 * time.Second,
		},
		// Every ingester restarts partway, and keeps its native resources over the restart: the lookup's coverage still
		// starts at the first push, so native info() has its own versions across the restart, without the stored
		// target_info.
		"a reopened engine partway": {
			push: func(t testing.TB, h *nativeInfoHarness, at func(float64) time.Time) {
				everyMinute(0, 4, func(m int) { h.pushOTLP(t, otlpGauge(at(float64(m)), api("a", "node-1"), 0, 1)) })
				require.Equal(t, at(0).UnixMilli(), h.nativeResourcesCoverageStart(t, "requests"))
				for partition := range h.ingesters {
					h.restart(t, partition)
				}
				require.Equal(t, at(0).UnixMilli(), h.nativeResourcesCoverageStart(t, "requests"), "the restart keeps the coverage start")
				everyMinute(5, 9, func(m int) {
					host := "node-1"
					if m >= 7 {
						host = "node-2"
					}
					h.pushOTLP(t, otlpGauge(at(float64(m)), api("a", host), 0, 1))
				})
			},
			queries: []string{`info(requests)`},
			from:    0, to: 9, step: 15 * time.Second,
		},
		// Coverage boundaries: the tenant's native resources go on partway, as an experiment tenant's do, so the lookup's
		// coverage starts with the first push after. Before it, the stored target_info is in force, and the lookback
		// bridges from its last sample to the first that the ingesters see the resource with.
		"native resources turned on partway": {
			push: func(t testing.TB, h *nativeInfoHarness, at func(float64) time.Time) {
				h.setNativeResources(false)
				everyMinute(0, 4, func(m int) { h.pushOTLP(t, otlpGauge(at(float64(m)), api("a", "node-1"), 0, 1)) })
				h.setNativeResources(true)
				everyMinute(5, 9, func(m int) {
					host := "node-1"
					if m >= 7 {
						host = "node-2"
					}
					h.pushOTLP(t, otlpGauge(at(float64(m)), api("a", host), 0, 1))
				})
				require.Equal(t, at(5).UnixMilli(), h.nativeResourcesCoverageStart(t, "requests"), "the coverage starts partway")
			},
			queries: []string{`info(requests)`},
			from:    0, to: 9, step: 15 * time.Second,
		},
		// A change between pushes, read through the lookback, an @ modifier and an offset.
		"@, offsets and lookback": {
			push: func(t testing.TB, h *nativeInfoHarness, at func(float64) time.Time) {
				everyMinute(0, 9, func(m int) {
					host := "node-1"
					if m >= 5 {
						host = "node-2"
					}
					h.pushOTLP(t, otlpGauge(at(float64(m)), api("a", host), 0, 1))
				})
			},
			queries: []string{`info(requests offset 2m)`, `info(requests @ %d)`, `info(last_over_time(requests[3m]))`},
			from:    2, to: 9, step: 20 * time.Second,
		},
		// Documented difference: a gap inside a version. rate(x[1h]) has values in the gap, which the join's
		// target_info has no sample within the lookback delta of, while native's version is in force through it.
		"gap under rate(x[1h])": {
			push: func(t testing.TB, h *nativeInfoHarness, at func(float64) time.Time) {
				for _, m := range []int{0, 1, 2, 3, 4, 20, 21, 22} {
					h.pushOTLP(t, otlpGauge(at(float64(m)), api("a", "node-1"), 0, 1))
				}
			},
			queries: []string{`info(rate(requests[1h]))`},
			from:    0, to: 22, step: time.Minute,
			// The join's last target_info sample before the gap is at minute 4, which the lookback delta of 5m, open at
			// its start, no longer sees at minute 9; its next is at minute 20.
			differAt: []float64{9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19},
		},
		// Documented difference: a Remote Write series that only shares the target's job and instance, without the
		// resource, through the target's gap. The join can't enrich it where the target_info has no recent sample.
		"an inner series that only shares the job and instance": {
			push: func(t testing.TB, h *nativeInfoHarness, at func(float64) time.Time) {
				for _, m := range []int{0, 1, 2, 20, 21} {
					h.pushOTLP(t, otlpGauge(at(float64(m)), api("a", "node-1"), 0))
				}
				everyMinute(0, 21, func(m int) {
					h.pushRemoteWrite(t, mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{
						Labels:  mimirpb.FromLabelsToLabelAdapters(labels.FromStrings("__name__", "up", "instance", "a", "job", "api")),
						Samples: []mimirpb.Sample{{TimestampMs: at(float64(m)).UnixMilli(), Value: 1}},
					}})
				})
			},
			queries: []string{`info(up)`},
			from:    0, to: 21, step: time.Minute,
			// The target's last push before the gap is at minute 2, which the lookback delta no longer sees at minute 7.
			differAt: []float64{7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19},
		},
		// Documented difference: two identities push for the same job and instance at once. The join picks the
		// target_info with the newest sample at each step, which alternates; native keeps the identity whose version
		// started later, the second's, from its start.
		"a concurrent collision": {
			push: func(t testing.TB, h *nativeInfoHarness, at func(float64) time.Time) {
				first := otlpResource{namespace: "x", name: "api", instance: "a", descriptive: map[string]string{"host.name": "node-1"}}
				second := otlpResource{name: "x/api", instance: "a", descriptive: map[string]string{"host.name": "node-2"}}
				everyMinute(0, 9, func(m int) {
					h.pushOTLP(t, otlpGauge(at(float64(m)), first, 0))
					if m >= 3 {
						// Half a minute after the first, so that the join alternates.
						h.pushOTLP(t, otlpGauge(at(float64(m)+0.5), second, 1))
					}
				})
			},
			queries: []string{`info(requests)`},
			from:    3, to: 9, step: 30 * time.Second,
			// From minute 3 on, at the steps where the first's sample is the newer one: the whole minutes.
			differAt: []float64{4, 5, 6, 7, 8, 9},
		},
	}

	for _, engine := range []string{mimir_tsdb.EngineSeriesstore} {
		for name, f := range fixtures {
			t.Run(engine+"/"+name, func(t *testing.T) {
				limits := defaultLimitsTestConfig()
				limits.NativeResourcesEnabled = true
				h := newNativeInfoHarness(t, engine, 2, limits)
				base := time.Now().Truncate(time.Minute).Add(-30 * time.Minute)
				at := func(minutes float64) time.Time { return base.Add(time.Duration(minutes * float64(time.Minute))) }
				f.push(t, h, at)

				var differAt []int64
				for _, m := range f.differAt {
					differAt = append(differAt, at(m).UnixMilli())
				}
				for _, query := range f.queries {
					if slices.Contains([]byte(query), '%') {
						query = fmt.Sprintf(query, at(7).Unix())
					}
					join := h.rangeQuery(t, false, query, at(f.from), at(f.to), f.step)
					native := h.rangeQuery(t, true, query, at(f.from), at(f.to), f.step)
					require.NotEmpty(t, join, query)
					require.Equal(t, differAt, differingSteps(join, native), "%s\njoin:\n%s\nnative:\n%s", query, join, native)
				}
				// A fixture that passes by measuring the join twice would prove nothing.
				require.Zero(t, h.fallbacks(t))
				require.Positive(t, h.synthesized(t))
			})
		}
	}
}
