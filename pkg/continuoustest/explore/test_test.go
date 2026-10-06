// SPDX-License-Identifier: AGPL-3.0-only

package explore

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/log"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/continuoustest"
	"github.com/grafana/mimir/pkg/mimirpb"
)

const testDefinition = `
native_histograms:
  sample_types: [int, float]
  kinds: [counter, gauge]
  schemas: [-4, 0, 8]
paces:
  test:
    slot_duration: 1m
    scenario_lifetime: 2m
    write_interval: 10s
    series_per_scenario: 2
    max_active_series: 4
`

func TestTest_Run(t *testing.T) {
	client := newFakeClient()
	reg := prometheus.NewPedanticRegistry()
	test := newTestTest(t, client, false, reg)

	start := time.Unix(1_800_000_000, 0).UTC().Truncate(time.Minute)
	for now := start; now.Before(start.Add(10 * time.Minute)); now = now.Add(30 * time.Second) {
		require.NoError(t, test.Run(context.Background(), now), "run at %s", now)
	}

	// Scenarios that started after the first run are written from their first step to the end of
	// their lifetime: 12 steps of 2 series each.
	for slot := start.Unix()/60 + 1; slot < start.Unix()/60+8; slot++ {
		for series := range 2 {
			samples := client.seriesSamples(slot, series)
			require.Len(t, samples, 12, "slot %d series %d", slot, series)
		}
	}
	// The failure counter of every started combination exists, at zero.
	require.Equal(t, testutil.CollectAndCount(test.metrics.queryResultChecksTotal), testutil.CollectAndCount(test.metrics.queryResultChecksFailedTotal))
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range families {
		if mf.GetName() == "mimir_continuous_test_explore_query_result_checks_failed_total" {
			for _, m := range mf.GetMetric() {
				require.Zero(t, m.GetCounter().GetValue())
			}
		}
	}
	require.Positive(t, testutil.ToFloat64(test.metrics.writesTotal))
	require.Equal(t, float64(12), testutil.ToFloat64(test.metrics.cycleLength))
}

func TestTest_DetectsWrongResults(t *testing.T) {
	client := newFakeClient()
	test := newTestTest(t, client, false, prometheus.NewPedanticRegistry())
	now := time.Unix(1_800_000_000, 0).UTC()
	require.NoError(t, test.Run(context.Background(), now))

	client.corrupt = func(h *prompb.Histogram) { h.Sum++ }
	err := test.Run(context.Background(), now.Add(10*time.Second))
	require.ErrorContains(t, err, "query result check of steps")

	var failed float64
	for _, s := range []string{"int", "float"} {
		for _, k := range []string{"counter", "gauge"} {
			for _, schema := range []string{"-4", "0", "8"} {
				failed += testutil.ToFloat64(test.metrics.queryResultChecksFailedTotal.WithLabelValues(s, k, schema))
			}
		}
	}
	require.Positive(t, failed)
}

func TestTest_DetectsCorruptedEarlierStep(t *testing.T) {
	client := newFakeClient()
	test := newTestTest(t, client, false, prometheus.NewPedanticRegistry())
	start := time.Unix(1_800_000_000, 0).UTC().Truncate(time.Minute)
	for now := start; now.Before(start.Add(2 * time.Minute)); now = now.Add(30 * time.Second) {
		require.NoError(t, test.Run(context.Background(), now))
	}

	// Corrupt the first sample of a scenario that has since written and verified later steps.
	samples := client.seriesSamples(start.Unix()/60+1, 0)
	require.Greater(t, len(samples), 1)
	first := int64(math.MaxInt64)
	for ts := range samples {
		first = min(first, ts)
	}
	h := samples[first]
	h.Sum++
	samples[first] = h

	err := test.Run(context.Background(), start.Add(2*time.Minute))
	require.ErrorContains(t, err, "step 0: expected histogram")
}

func TestTest_SkipsAheadAfterAPause(t *testing.T) {
	client := newFakeClient()
	test := newTestTest(t, client, false, prometheus.NewPedanticRegistry())
	run := func(now time.Time) error {
		client.oldestStored = now.Add(-10 * time.Minute)
		return test.Run(context.Background(), now)
	}

	start := time.Unix(1_800_000_000, 0).UTC().Truncate(time.Minute)
	require.NoError(t, run(start))
	require.NoError(t, run(start.Add(30*time.Second)))

	// Writing the steps missed during the pause would succeed, but they wouldn't be stored, so
	// their checks would fail.
	resumed := start.Add(3 * time.Hour)
	for now := resumed; now.Before(resumed.Add(3 * time.Minute)); now = now.Add(30 * time.Second) {
		require.NoError(t, run(now), "run at %s", now)
	}
	require.Positive(t, testutil.ToFloat64(test.metrics.queriesTotal))
}

func TestTest_ResumesAfterRestart(t *testing.T) {
	client := newFakeClient()
	now := time.Unix(1_800_000_000, 0).UTC()
	first := newTestTest(t, client, false, prometheus.NewPedanticRegistry())
	require.NoError(t, first.Run(context.Background(), now))

	// The restarted test rewrites the latest step, which the fake client rejects unless it has the
	// same value.
	restarted := newTestTest(t, client, false, prometheus.NewPedanticRegistry())
	require.NoError(t, restarted.Run(context.Background(), now))
	require.NoError(t, restarted.Run(context.Background(), now.Add(30*time.Second)))
}

func TestTest_WriteRejected(t *testing.T) {
	client := newFakeClient()
	client.writeStatus = 400
	test := newTestTest(t, client, false, prometheus.NewPedanticRegistry())
	now := time.Unix(1_800_000_000, 0).UTC()

	require.ErrorContains(t, test.Run(context.Background(), now), "rejected with status code 400")
	require.Positive(t, testutil.ToFloat64(test.metrics.writesFailedTotal.WithLabelValues("400")))
}

func TestTest_SmokeTest(t *testing.T) {
	client := newFakeClient()
	test := newTestTest(t, client, true, prometheus.NewPedanticRegistry())
	require.NoError(t, test.Run(context.Background(), time.Unix(1_800_000_000, 0).UTC()))
	require.Len(t, client.series, 12*2, "every scenario of the cycle writes its series")
}

func TestTest_InitErrors(t *testing.T) {
	path := writeDefinition(t)

	cfg := Config{DefinitionFile: path, Pace: "test", Seed: 1, ScenarioSource: ScenarioSourceSchedule}
	managerCfg := continuoustest.ManagerConfig{RunInterval: 30 * time.Second}

	otlp := newFakeClient()
	otlp.protocol = "otlp-http"
	test := NewTest(cfg, otlp, managerCfg, log.NewNopLogger(), nil)
	require.ErrorContains(t, test.Init(context.Background(), time.Now()), "otlp-http")

	missingPace := cfg
	missingPace.Pace = "missing"
	test = NewTest(missingPace, newFakeClient(), managerCfg, log.NewNopLogger(), nil)
	require.ErrorContains(t, test.Init(context.Background(), time.Now()), `pace "missing" is not defined`)

	unknownSource := cfg
	unknownSource.ScenarioSource = "dice"
	test = NewTest(unknownSource, newFakeClient(), managerCfg, log.NewNopLogger(), nil)
	require.ErrorContains(t, test.Init(context.Background(), time.Now()), `unknown scenario source "dice"`)

	test = NewTest(cfg, newFakeClient(), continuoustest.ManagerConfig{RunInterval: 5 * time.Minute}, log.NewNopLogger(), nil)
	require.ErrorContains(t, test.Init(context.Background(), time.Now()), "shorter than the run interval")
}

func TestTest_AntithesisSource_Run(t *testing.T) {
	client := newFakeClient()
	test := newTestTestWithSource(t, client, false, prometheus.NewPedanticRegistry(), ScenarioSourceAntithesis)

	start := time.Unix(1_800_000_000, 0).UTC().Truncate(time.Minute)
	for now := start; now.Before(start.Add(10 * time.Minute)); now = now.Add(30 * time.Second) {
		require.NoError(t, test.Run(context.Background(), now), "run at %s", now)
	}

	// Scenarios are drawn independently, so across 10 slots there are almost surely several
	// different combinations and every scenario has its own seed.
	combinations, seeds := map[string]bool{}, map[string]bool{}
	for _, s := range client.series {
		combinations[s.labels.Get(labelSampleType)+s.labels.Get(labelKind)+s.labels.Get(labelSchema)] = true
		seeds[s.labels.Get(labelScenario)+"/"+s.labels.Get(labelScenarioSeed)] = true
	}
	require.Greater(t, len(combinations), 1)
	require.Len(t, seeds, 11, "one seed per slot: the 2 active at the first run, then 1 per minute")
}

func TestTest_AntithesisSource_Restart(t *testing.T) {
	client := newFakeClient()
	now := time.Unix(1_800_000_000, 0).UTC()
	first := newTestTestWithSource(t, client, false, prometheus.NewPedanticRegistry(), ScenarioSourceAntithesis)
	require.NoError(t, first.Run(context.Background(), now))

	// The restarted test draws new scenarios for the slots that are still active. Their series are
	// distinct from the abandoned ones, even when written at the same timestamps.
	restarted := newTestTestWithSource(t, client, false, prometheus.NewPedanticRegistry(), ScenarioSourceAntithesis)
	require.NoError(t, restarted.Run(context.Background(), now))
	require.NoError(t, restarted.Run(context.Background(), now.Add(30*time.Second)))

	seedsPerSlot := map[string]map[string]bool{}
	for _, s := range client.series {
		slot := s.labels.Get(labelScenario)
		if seedsPerSlot[slot] == nil {
			seedsPerSlot[slot] = map[string]bool{}
		}
		seedsPerSlot[slot][s.labels.Get(labelScenarioSeed)] = true
	}
	for slot, seeds := range seedsPerSlot {
		require.Len(t, seeds, 2, "slot %s should have a scenario from each process", slot)
	}
}

func TestTest_AntithesisSource_SmokeTest(t *testing.T) {
	client := newFakeClient()
	test := newTestTestWithSource(t, client, true, prometheus.NewPedanticRegistry(), ScenarioSourceAntithesis)
	require.NoError(t, test.Run(context.Background(), time.Unix(1_800_000_000, 0).UTC()))
	require.Len(t, client.series, 12*2)
}

func newTestTest(t *testing.T, client *fakeClient, smokeTest bool, reg prometheus.Registerer) *Test {
	return newTestTestWithSource(t, client, smokeTest, reg, ScenarioSourceSchedule)
}

func newTestTestWithSource(t *testing.T, client *fakeClient, smokeTest bool, reg prometheus.Registerer, source string) *Test {
	cfg := Config{DefinitionFile: writeDefinition(t), Pace: "test", Seed: 1, ScenarioSource: source}
	test := NewTest(cfg, client, continuoustest.ManagerConfig{SmokeTest: smokeTest, RunInterval: 30 * time.Second}, log.NewNopLogger(), reg)
	require.NoError(t, test.Init(context.Background(), time.Now()))
	return test
}

func writeDefinition(t *testing.T) string {
	path := filepath.Join(t.TempDir(), "definition.yaml")
	require.NoError(t, os.WriteFile(path, []byte(testDefinition), 0o600))
	return path
}

// fakeClient stores written histograms in memory and answers instant and range queries of the
// form last_over_time(<selector>[1s]).
type fakeClient struct {
	protocol    continuoustest.WriteProtocol
	writeStatus int
	series      map[string]*fakeSeries
	// corrupt, if set, modifies every histogram returned by queries.
	corrupt func(*prompb.Histogram)
	// Samples older than oldestStored are accepted but not stored, like Mimir with ingest storage
	// does with samples that are too old.
	oldestStored time.Time
}

type fakeSeries struct {
	labels  labels.Labels
	samples map[int64]prompb.Histogram
}

func newFakeClient() *fakeClient {
	return &fakeClient{protocol: "prometheus", writeStatus: 200, series: map[string]*fakeSeries{}}
}

func (c *fakeClient) WriteSeries(_ context.Context, series []prompb.TimeSeries, _ []prompb.MetricMetadata) (int, error) {
	if c.writeStatus != 200 {
		return c.writeStatus, errors.New("write rejected")
	}
	for _, ts := range series {
		b := labels.NewScratchBuilder(len(ts.Labels))
		for _, l := range ts.Labels {
			b.Add(l.Name, l.Value)
		}
		lbls := b.Labels()
		s, ok := c.series[lbls.String()]
		if !ok {
			s = &fakeSeries{labels: lbls, samples: map[int64]prompb.Histogram{}}
			c.series[lbls.String()] = s
		}
		for _, h := range ts.Histograms {
			if h.Timestamp < c.oldestStored.UnixMilli() {
				continue
			}
			if existing, ok := s.samples[h.Timestamp]; ok && existing.String() != h.String() {
				return 400, fmt.Errorf("duplicate sample for timestamp %d with a different value", h.Timestamp)
			}
			s.samples[h.Timestamp] = h
		}
	}
	return 200, nil
}

func (c *fakeClient) Query(_ context.Context, query string, ts time.Time, _ ...continuoustest.RequestOption) (model.Vector, error) {
	expr, err := parser.NewParser(parser.Options{}).ParseExpr(query)
	if err != nil {
		return nil, err
	}
	call, ok := expr.(*parser.Call)
	if !ok || call.Func.Name != "last_over_time" {
		return nil, fmt.Errorf("unsupported query %q", query)
	}
	selector := call.Args[0].(*parser.MatrixSelector).VectorSelector.(*parser.VectorSelector)

	var out model.Vector
	for _, s := range c.series {
		if !matchesAll(selector.LabelMatchers, s.labels) {
			continue
		}
		h, ok := s.samples[ts.UnixMilli()]
		if !ok {
			continue
		}
		if c.corrupt != nil {
			c.corrupt(&h)
		}
		var fh = h.ToFloatHistogram()
		if !h.IsFloatHistogram() {
			fh = h.ToIntHistogram().ToFloat(nil)
		}
		metric := model.Metric{}
		s.labels.Range(func(l labels.Label) { metric[model.LabelName(l.Name)] = model.LabelValue(l.Value) })
		out = append(out, &model.Sample{Metric: metric, Timestamp: model.TimeFromUnixNano(ts.UnixNano()), Histogram: mimirpb.FromFloatHistogramToPromHistogram(fh)})
	}
	return out, nil
}

func matchesAll(matchers []*labels.Matcher, lbls labels.Labels) bool {
	for _, m := range matchers {
		if !m.Matches(lbls.Get(m.Name)) {
			return false
		}
	}
	return true
}

func (c *fakeClient) seriesSamples(slot int64, series int) map[int64]prompb.Histogram {
	for _, s := range c.series {
		if s.labels.Get(labelScenario) == fmt.Sprint(slot) && s.labels.Get(labelSeriesID) == fmt.Sprint(series) {
			return s.samples
		}
	}
	return nil
}

func (c *fakeClient) QueryRange(ctx context.Context, query string, start, end time.Time, step time.Duration, _ ...continuoustest.RequestOption) (model.Matrix, error) {
	streams := map[string]*model.SampleStream{}
	for ts := start; !ts.After(end); ts = ts.Add(step) {
		vector, err := c.Query(ctx, query, ts)
		if err != nil {
			return nil, err
		}
		for _, sample := range vector {
			stream, ok := streams[sample.Metric.String()]
			if !ok {
				stream = &model.SampleStream{Metric: sample.Metric}
				streams[sample.Metric.String()] = stream
			}
			stream.Histograms = append(stream.Histograms, model.SampleHistogramPair{Timestamp: sample.Timestamp, Histogram: sample.Histogram})
		}
	}
	var out model.Matrix
	for _, stream := range streams {
		out = append(out, stream)
	}
	return out, nil
}

func (c *fakeClient) Metadata(context.Context, string) (v1.Metadata, error) {
	return v1.Metadata{}, errors.New("not implemented")
}

func (c *fakeClient) Protocol() continuoustest.WriteProtocol {
	return c.protocol
}
