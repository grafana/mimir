// SPDX-License-Identifier: AGPL-3.0-only

package explore

import (
	"context"
	"errors"
	"flag"
	"fmt"
	mathrand "math/rand"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"

	antithesisrandom "github.com/antithesishq/antithesis-sdk-go/random"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/multierror"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"

	"github.com/grafana/mimir/pkg/continuoustest"
	"github.com/grafana/mimir/pkg/mimirpb"
)

const (
	testName = "explore"

	ScenarioSourceSchedule   = "schedule"
	ScenarioSourceAntithesis = "antithesis"
)

type Config struct {
	Enabled        bool
	DefinitionFile string
	Pace           string
	Seed           uint64
	ScenarioSource string
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.Enabled, "tests.explore.enabled", false, "Set to true to run the explore test, which gradually writes and reads back scenarios drawn from a definition of the behavior space to explore. Its series are independent from the series of other tests.")
	f.StringVar(&cfg.DefinitionFile, "tests.explore.definition-file", "", "Path to a YAML file defining the behavior space and the paces at which to explore it. If empty, the built-in definition is used.")
	f.StringVar(&cfg.Pace, "tests.explore.pace", "production", "Name of the pace, from the definition, at which to explore the behavior space.")
	f.Uint64Var(&cfg.Seed, "tests.explore.seed", 1, "Seed of the exploration schedule. With the same seed and definition, the same scenario always runs in the same time slot. Set to 0 to pick a random seed at startup.")
	f.StringVar(&cfg.ScenarioSource, "tests.explore.scenario-source", ScenarioSourceSchedule, fmt.Sprintf("How the scenario of each slot is chosen. With %q, scenarios follow the schedule determined by the seed and the definition, which covers every value and every pair of values in each cycle. With %q, the dimension values and seed of each scenario are drawn from the Antithesis SDK when the scenario starts, so that Antithesis can guide the exploration. Outside Antithesis, the draws come from crypto/rand.", ScenarioSourceSchedule, ScenarioSourceAntithesis))
}

// Test gradually explores a behavior space: in every time slot a new scenario starts, writes its
// series for its lifetime, and reads them back after every write.
type Test struct {
	cfg         Config
	client      continuoustest.MimirClient
	smokeTest   bool
	runInterval time.Duration
	logger      log.Logger
	metrics     *metrics

	pace     Pace
	seed     uint64
	dims     []dimension
	explorer *explorer
	// decisions, if set, chooses scenarios instead of the explorer.
	decisions *mathrand.Rand

	commonLabels []prompb.Label
	states       map[int64]*scenarioState
	lastRun      time.Time
	// run counts what the current run did, for its summary log line.
	run runStats
}

type runStats struct {
	writes, writesFailed, checks, checksFailed int
}

type scenarioState struct {
	scenario Scenario
	// nextStep is the next step to write.
	nextStep int64
	// firstWrittenStep and lastWrittenStep are the first and last steps written successfully, or
	// -1 if none was. Every step in between was written too.
	firstWrittenStep, lastWrittenStep int64
}

func NewTest(cfg Config, client continuoustest.MimirClient, managerCfg continuoustest.ManagerConfig, logger log.Logger, reg prometheus.Registerer) *Test {
	return &Test{
		cfg:         cfg,
		client:      client,
		smokeTest:   managerCfg.SmokeTest,
		runInterval: managerCfg.RunInterval,
		logger:      log.With(logger, "test", testName),
		metrics:     newMetrics(reg),
		states:      map[int64]*scenarioState{},
	}
}

// Name implements continuoustest.Test.
func (t *Test) Name() string {
	return testName
}

// Init implements continuoustest.Test.
func (t *Test) Init(_ context.Context, _ time.Time) error {
	if t.client.Protocol() == "otlp-http" {
		return errors.New("the explore test doesn't support the otlp-http write protocol yet")
	}

	def, err := LoadDefinition(t.cfg.DefinitionFile)
	if err != nil {
		return err
	}
	pace, ok := def.Paces[t.cfg.Pace]
	if !ok {
		return fmt.Errorf("pace %q is not defined", t.cfg.Pace)
	}
	switch t.cfg.ScenarioSource {
	case ScenarioSourceSchedule:
	case ScenarioSourceAntithesis:
		t.decisions = mathrand.New(antithesisrandom.Source())
	default:
		return fmt.Errorf("unknown scenario source %q, supported values are %q and %q", t.cfg.ScenarioSource, ScenarioSourceSchedule, ScenarioSourceAntithesis)
	}
	if !t.smokeTest && pace.ScenarioLifetime < t.runInterval {
		return fmt.Errorf("pace %q has a scenario_lifetime of %s, which is shorter than the run interval %s, so some scenarios would never be written", t.cfg.Pace, pace.ScenarioLifetime, t.runInterval)
	}

	t.seed = t.cfg.Seed
	if t.seed == 0 {
		t.seed = rand.Uint64()
	}
	t.pace = pace
	t.dims = def.NativeHistograms.dimensions()
	t.explorer = newExplorer(t.seed, t.dims)
	t.commonLabels = []prompb.Label{
		{Name: labelPace, Value: t.cfg.Pace},
		{Name: labelProtocol, Value: string(t.client.Protocol())},
		{Name: labelSeed, Value: strconv.FormatUint(t.seed, 10)},
	}
	t.metrics.cycleLength.Set(float64(t.explorer.cycleLength()))

	level.Info(t.logger).Log("msg", "Initialized explore test", "scenario_source", t.cfg.ScenarioSource, "seed", t.seed, "pace", t.cfg.Pace, "cycle_length", t.explorer.cycleLength(), "cycle_duration", time.Duration(t.explorer.cycleLength())*pace.SlotDuration)
	return nil
}

// Run implements continuoustest.Test.
func (t *Test) Run(ctx context.Context, now time.Time) error {
	t.run = runStats{}
	if t.smokeTest {
		err := t.runSmokeTest(ctx, now)
		t.logRunSummary(int(t.explorer.cycleLength()))
		return err
	}

	errs := multierror.New()
	oldest := t.oldestWritableTime(now)
	if !t.lastRun.IsZero() && t.lastRun.Before(oldest) {
		level.Warn(t.logger).Log("msg", "The previous run was too long ago, skipping the steps that are too old to write", "previous_run", t.lastRun, "oldest_step_time", oldest)
	}
	active := t.activeSlots(now, oldest)
	for _, slot := range active {
		state, ok := t.states[slot]
		if !ok {
			s, err := t.newScenario(slot)
			if err != nil {
				errs.Add(err)
				continue
			}
			state = t.newScenarioState(s, now)
			t.states[slot] = state
			t.metrics.scenarioStarted(s)
			level.Info(t.logger).Log("msg", "Starting scenario", "scenario", s, "first_step", state.nextStep)
		}
		s := state.scenario
		t.skipStepsBefore(state, oldest)
		errs.Add(t.writeScenario(ctx, s, state, t.latestStep(s, now)))
		if state.lastWrittenStep >= 0 {
			errs.Add(t.verifyScenario(ctx, s, state.firstWrittenStep, state.lastWrittenStep))
		}
	}

	for slot := range t.states {
		if len(active) == 0 || slot < active[0] {
			delete(t.states, slot)
		}
	}
	if len(active) > 0 && t.decisions == nil {
		t.metrics.cyclePosition.Set(float64(active[len(active)-1] % t.explorer.cycleLength()))
	}
	t.lastRun = now
	t.logRunSummary(len(active))
	return errs.Err()
}

func (t *Test) logRunSummary(scenarios int) {
	lvl := level.Info
	if t.run.writesFailed > 0 || t.run.checksFailed > 0 {
		lvl = level.Warn
	}
	lvl(t.logger).Log("msg", "Explore run finished", "scenarios", scenarios, "writes", t.run.writes, "writes_failed", t.run.writesFailed, "checks", t.run.checks, "checks_failed", t.run.checksFailed)
}

// runSmokeTest writes and reads back one sample of every scenario in the current cycle.
func (t *Test) runSmokeTest(ctx context.Context, now time.Time) error {
	errs := multierror.New()
	length := t.explorer.cycleLength()
	first := now.UnixNano() / int64(t.pace.SlotDuration) / length * length
	for slot := first; slot < first+length; slot++ {
		s, err := t.newScenario(slot)
		if err != nil {
			errs.Add(err)
			continue
		}
		s.Start = now.Truncate(t.pace.WriteInterval)
		t.metrics.scenarioStarted(s)

		state := &scenarioState{scenario: s, firstWrittenStep: -1, lastWrittenStep: -1}
		if err := t.writeScenario(ctx, s, state, 0); err != nil {
			errs.Add(err)
			continue
		}
		if state.lastWrittenStep < 0 {
			errs.Add(fmt.Errorf("scenario %s: write rejected", s))
			continue
		}
		errs.Add(t.verifyScenario(ctx, s, 0, 0))
	}
	return errs.Err()
}

// oldestWritableTime returns the time of the oldest step that is still written at the given time.
// Mimir rejects samples that are too old, and with ingest storage it does so after the write
// request succeeded, so after a pause the test skips ahead instead of catching up.
func (t *Test) oldestWritableTime(now time.Time) time.Time {
	return now.Add(-2 * t.runInterval)
}

// activeSlots returns the slots of the scenarios that have steps due since the previous run, oldest
// first. This includes scenarios whose lifetime ended since the previous run, so their last steps
// are written, unless they are older than oldest.
func (t *Test) activeSlots(now, oldest time.Time) []int64 {
	since := now
	if !t.lastRun.IsZero() && t.lastRun.Before(now) {
		since = t.lastRun
		if since.Before(oldest) {
			since = oldest
		}
	}
	last := now.UnixNano() / int64(t.pace.SlotDuration)
	first := (since.UnixNano()-int64(t.pace.ScenarioLifetime))/int64(t.pace.SlotDuration) + 1
	slots := make([]int64, 0, last-first+1)
	for slot := first; slot <= last; slot++ {
		slots = append(slots, slot)
	}
	return slots
}

// newScenario chooses the scenario of the given slot. With the Antithesis scenario source, the
// choices are drawn when the scenario starts, so they can't be recomputed later: the scenario must
// be kept for its lifetime, and its series are identified by its seed.
func (t *Test) newScenario(slot int64) (Scenario, error) {
	start := time.Unix(0, slot*int64(t.pace.SlotDuration)).UTC()
	if t.decisions != nil {
		values := make([]string, len(t.dims))
		for i, d := range t.dims {
			values[i] = antithesisrandom.RandomChoice(d.values)
		}
		return newScenario(slot, start, t.decisions.Uint64(), t.dims, values)
	}
	return newScenario(slot, start, mix(t.seed, uint64(slot)), t.dims, t.explorer.combination(slot))
}

// latestStep returns the latest step of the scenario that is due at the given time.
func (t *Test) latestStep(s Scenario, now time.Time) int64 {
	step := int64(now.Sub(s.Start) / t.pace.WriteInterval)
	maxStep := int64(t.pace.ScenarioLifetime/t.pace.WriteInterval) - 1
	return min(step, maxStep)
}

func (t *Test) newScenarioState(s Scenario, now time.Time) *scenarioState {
	// A scenario that started after the previous run is written from its first step. Otherwise,
	// for example after a restart, writing resumes from the latest step: rewriting a step that was
	// already written is harmless because it has the same value.
	first := t.latestStep(s, now)
	if !t.lastRun.IsZero() && !s.Start.Before(t.lastRun) {
		first = 0
	}
	return &scenarioState{scenario: s, nextStep: first, firstWrittenStep: -1, lastWrittenStep: -1}
}

// skipStepsBefore skips the steps of the scenario before the given time that aren't written yet.
func (t *Test) skipStepsBefore(state *scenarioState, oldest time.Time) {
	first := int64(0)
	if d := oldest.Sub(state.scenario.Start); d > 0 {
		first = int64((d + t.pace.WriteInterval - 1) / t.pace.WriteInterval)
	}
	if state.nextStep >= first {
		return
	}
	state.nextStep = first
	// The skipped steps are never written, so only the steps after them can be verified.
	state.firstWrittenStep, state.lastWrittenStep = -1, -1
}

func (t *Test) stepTimestamp(s Scenario, step int64) time.Time {
	return s.Start.Add(time.Duration(step) * t.pace.WriteInterval)
}

// writeScenario writes the steps of the scenario from its next step up to and including lastStep.
func (t *Test) writeScenario(ctx context.Context, s Scenario, state *scenarioState, lastStep int64) error {
	metadata := []prompb.MetricMetadata{s.Metadata()}
	for ; state.nextStep <= lastStep; state.nextStep++ {
		step := state.nextStep
		ts := t.stepTimestamp(s, step)
		series := make([]prompb.TimeSeries, 0, t.pace.SeriesPerScenario)
		for i := range t.pace.SeriesPerScenario {
			var sample prompb.Histogram
			if h, fh := s.Histogram(i, step); h != nil {
				sample = prompb.FromIntHistogram(ts.UnixMilli(), h)
			} else {
				sample = prompb.FromFloatHistogram(ts.UnixMilli(), fh)
			}
			series = append(series, prompb.TimeSeries{
				Labels:     s.SeriesLabels(i, t.commonLabels),
				Histograms: []prompb.Histogram{sample},
			})
		}

		t.metrics.writesTotal.Inc()
		t.run.writes++
		statusCode, err := t.client.WriteSeries(ctx, series, metadata)
		if statusCode/100 != 2 || err != nil {
			t.run.writesFailed++
		}
		switch {
		case statusCode/100 == 2 && err == nil:
			if state.firstWrittenStep < 0 {
				state.firstWrittenStep = step
			}
			state.lastWrittenStep = step
		case statusCode/100 == 4:
			// Every generated sample is valid, so a rejection is a failure. Retrying won't succeed, so
			// the step is skipped.
			t.metrics.writesFailedTotal.WithLabelValues(strconv.Itoa(statusCode)).Inc()
			level.Warn(t.logger).Log("msg", "Write rejected", "scenario", s, "step", step, "status_code", statusCode, "err", err)
			return fmt.Errorf("scenario %s: write of step %d rejected with status code %d: %w", s, step, statusCode, err)
		default:
			// The step is retried on the next run.
			t.metrics.writesFailedTotal.WithLabelValues(strconv.Itoa(statusCode)).Inc()
			level.Warn(t.logger).Log("msg", "Write failed", "scenario", s, "step", step, "status_code", statusCode, "err", err)
			return fmt.Errorf("scenario %s: write of step %d failed with status code %d: %w", s, step, statusCode, err)
		}
	}
	return nil
}

// verifyScenario reads back every series of the scenario at every step from from to to, and
// compares them with the samples that were written. Results cache is disabled, so that every step
// is read from storage.
func (t *Test) verifyScenario(ctx context.Context, s Scenario, from, to int64) error {
	start, end := t.stepTimestamp(s, from), t.stepTimestamp(s, to)
	query := t.scenarioQuery(s)
	logger := log.With(t.logger, "scenario", s, "from_step", from, "to_step", to, "query", query, "start", start.UnixMilli(), "end", end.UnixMilli())
	dimLabels := dimensionLabelValues(s)

	t.metrics.queriesTotal.Inc()
	matrix, err := t.client.QueryRange(ctx, query, start, end, t.pace.WriteInterval, continuoustest.WithResultsCacheEnabled(false))
	if err != nil {
		t.metrics.queriesFailedTotal.Inc()
		level.Warn(logger).Log("msg", "Query failed", "err", err)
		return fmt.Errorf("scenario %s: query failed: %w", s, err)
	}

	t.metrics.queryResultChecksTotal.WithLabelValues(dimLabels...).Inc()
	t.run.checks++
	if err := t.checkResult(s, from, to, matrix); err != nil {
		t.metrics.queryResultChecksFailedTotal.WithLabelValues(dimLabels...).Inc()
		t.run.checksFailed++
		level.Warn(logger).Log("msg", "Query result check failed", "err", err)
		return fmt.Errorf("scenario %s: query result check of steps %d to %d failed: %w", s, from, to, err)
	}
	level.Info(logger).Log("msg", "Query result check succeeded", "series", len(matrix), "steps", to-from+1)
	return nil
}

// scenarioQuery returns a query selecting the samples of every series of the scenario. The [1s]
// range keeps the lookback period from returning samples of earlier steps.
func (t *Test) scenarioQuery(s Scenario) string {
	matchers := []string{
		fmt.Sprintf("__name__=%q", s.MetricName()),
		fmt.Sprintf("%s=%q", labelScenario, strconv.FormatInt(s.Slot, 10)),
		fmt.Sprintf("%s=%q", labelScenarioSeed, strconv.FormatUint(s.Seed, 10)),
	}
	for _, l := range t.commonLabels {
		matchers = append(matchers, fmt.Sprintf("%s=%q", l.Name, l.Value))
	}
	return fmt.Sprintf("last_over_time({%s}[1s])", strings.Join(matchers, ", "))
}

func (t *Test) checkResult(s Scenario, from, to int64, matrix model.Matrix) error {
	if len(matrix) != t.pace.SeriesPerScenario {
		return fmt.Errorf("expected %d series but got %d", t.pace.SeriesPerScenario, len(matrix))
	}
	seen := make(map[int]bool, len(matrix))
	for _, stream := range matrix {
		series, err := strconv.Atoi(string(stream.Metric[labelSeriesID]))
		if err != nil || series < 0 || series >= t.pace.SeriesPerScenario || seen[series] {
			return fmt.Errorf("unexpected series %s", stream.Metric)
		}
		seen[series] = true

		if len(stream.Values) > 0 {
			return fmt.Errorf("series %s: expected histograms but got %d floats", stream.Metric, len(stream.Values))
		}
		expected := s.ExpectedHistograms(series, from, to)
		if len(stream.Histograms) != len(expected) {
			return fmt.Errorf("series %s: expected %d samples but got %d", stream.Metric, len(expected), len(stream.Histograms))
		}
		for i, pair := range stream.Histograms {
			step := from + int64(i)
			if ts := t.stepTimestamp(s, step).UnixMilli(); int64(pair.Timestamp) != ts {
				return fmt.Errorf("series %s: expected step %d at %d but got a sample at %d", stream.Metric, step, ts, pair.Timestamp)
			}
			want := mimirpb.FromFloatHistogramToPromHistogram(expected[i])
			if !histogramsEqual(pair.Histogram, want) {
				return fmt.Errorf("series %s: step %d: expected histogram %s but got %s", stream.Metric, step, want, pair.Histogram)
			}
		}
	}
	return nil
}

// histogramsEqual compares histograms exactly: the generated values are exactly representable,
// and the query API encodes floats without loss.
func histogramsEqual(actual, expected *model.SampleHistogram) bool {
	if actual.Count != expected.Count || actual.Sum != expected.Sum || len(actual.Buckets) != len(expected.Buckets) {
		return false
	}
	for i, b := range actual.Buckets {
		if *b != *expected.Buckets[i] {
			return false
		}
	}
	return true
}

func dimensionLabelValues(s Scenario) []string {
	values := make([]string, 0, len(s.Dimensions))
	for _, l := range s.Dimensions {
		values = append(values, l.Value)
	}
	return values
}

type metrics struct {
	writesTotal                  prometheus.Counter
	writesFailedTotal            *prometheus.CounterVec
	queriesTotal                 prometheus.Counter
	queriesFailedTotal           prometheus.Counter
	queryResultChecksTotal       *prometheus.CounterVec
	queryResultChecksFailedTotal *prometheus.CounterVec
	scenariosStartedTotal        *prometheus.CounterVec
	cycleLength                  prometheus.Gauge
	cyclePosition                prometheus.Gauge
}

func newMetrics(reg prometheus.Registerer) *metrics {
	dimLabels := []string{labelSampleType, labelKind, labelSchema}
	return &metrics{
		writesTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "mimir_continuous_test_explore_writes_total",
			Help: "Total number of write requests attempted by the explore test.",
		}),
		writesFailedTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "mimir_continuous_test_explore_writes_failed_total",
			Help: "Total number of failed write requests of the explore test.",
		}, []string{"status_code"}),
		queriesTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "mimir_continuous_test_explore_queries_total",
			Help: "Total number of queries attempted by the explore test.",
		}),
		queriesFailedTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "mimir_continuous_test_explore_queries_failed_total",
			Help: "Total number of failed queries of the explore test.",
		}),
		queryResultChecksTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "mimir_continuous_test_explore_query_result_checks_total",
			Help: "Total number of query results checked by the explore test, by scenario dimension values.",
		}, dimLabels),
		queryResultChecksFailedTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "mimir_continuous_test_explore_query_result_checks_failed_total",
			Help: "Total number of query results of the explore test that failed the check, by scenario dimension values.",
		}, dimLabels),
		scenariosStartedTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "mimir_continuous_test_explore_scenarios_started_total",
			Help: "Total number of scenarios started by the explore test, by dimension and value.",
		}, []string{"dimension", "value"}),
		cycleLength: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "mimir_continuous_test_explore_cycle_length_slots",
			Help: "Number of slots in a cycle of the explore test. A cycle covers every dimension value, and every pair of values, at least once.",
		}),
		cyclePosition: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "mimir_continuous_test_explore_cycle_position_slots",
			Help: "Position of the most recently started scenario within its cycle.",
		}),
	}
}

func (m *metrics) scenarioStarted(s Scenario) {
	for _, l := range s.Dimensions {
		m.scenariosStartedTotal.WithLabelValues(l.Name, l.Value).Inc()
	}
	// Create the failure counter at zero, so that no failures is distinguishable from no checks.
	m.queryResultChecksFailedTotal.WithLabelValues(dimensionLabelValues(s)...)
}
