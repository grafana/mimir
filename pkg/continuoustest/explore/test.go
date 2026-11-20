// SPDX-License-Identifier: AGPL-3.0-only

package explore

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"time"

	antithesisrandom "github.com/antithesishq/antithesis-sdk-go/random"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/multierror"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"

	"github.com/grafana/mimir/pkg/continuoustest"
	"github.com/grafana/mimir/pkg/mimirpb"
)

const (
	testName = "explore"

	RandomSourceSeed       = "seed"
	RandomSourceAntithesis = "antithesis"

	// defaultMetadataTimeout is how long written metadata can take to be readable. Unlike series,
	// metadata reads don't wait for strong read consistency.
	defaultMetadataTimeout = 5 * time.Second
	metadataRetryInterval  = 500 * time.Millisecond
)

type Config struct {
	Enabled              bool
	DefinitionFile       string
	WriteInterval        time.Duration
	Seed                 uint64
	RandomSource         string
	NameValidationScheme model.ValidationScheme
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.Enabled, "tests.explore.enabled", false, "Set to true to run the explore test, which repeatedly writes samples of series drawn from a definition of the behavior space to explore, and reads them back. Its series are independent from the series of other tests.")
	f.StringVar(&cfg.DefinitionFile, "tests.explore.definition-file", "", "Path to a YAML file defining the behavior space to explore. If empty, the built-in definition is used.")
	f.DurationVar(&cfg.WriteInterval, "tests.explore.write-interval", time.Minute, "How often the explore test writes samples and reads them back. Must be a whole number of seconds. Series are labeled with the write interval, so tests with different write intervals write different series.")
	f.Uint64Var(&cfg.Seed, "tests.explore.seed", 1, "Seed of the explore test. With the same seed and definition, series have the same bucket layouts and values, and each write interval writes the same series with the same protocol. Set to 0 to pick a random seed at startup.")
	f.StringVar(&cfg.RandomSource, "tests.explore.random-source", RandomSourceSeed, fmt.Sprintf("Source of the random choices of each write: its protocol, how many series it writes, and which. With %q, the choices are determined by the seed and the time of the write. With %q, they are drawn from the Antithesis SDK, so that Antithesis can guide the exploration. Outside Antithesis, the draws come from crypto/rand.", RandomSourceSeed, RandomSourceAntithesis))
	cfg.NameValidationScheme = model.LegacyValidation
	f.Var(&cfg.NameValidationScheme, "tests.explore.name-validation-scheme", fmt.Sprintf("Name validation scheme of the tenant, as configured with -validation.name-validation-scheme. With %s, series with UTF-8 names are only written with OTLP, whose endpoint escapes the names. Supported values: %s, %s.", model.LegacyValidation, model.LegacyValidation, model.UTF8Validation))
}

// Client reads for the explore test, and provides a client for each protocol it writes with.
type Client interface {
	continuoustest.MimirClient
	WithWriteProtocol(continuoustest.WriteProtocol) (continuoustest.MimirClient, error)
	MetricMetadata(ctx context.Context, metricName string) ([]v1.Metadata, error)
}

// Test explores a behavior space. Every write interval, it picks a protocol and a few series,
// writes a sample of each series with a single request, and reads the samples back.
type Test struct {
	cfg             Config
	client          Client
	writers         map[Protocol]continuoustest.MimirClient
	smokeTest       bool
	runInterval     time.Duration
	metadataTimeout time.Duration
	logger          log.Logger
	metrics         *metrics

	seed         uint64
	explorer     *explorer
	commonLabels []prompb.Label

	// started is whether the test ran before, and lastStep the last step it wrote or skipped. A
	// step is the number of write intervals since the Unix epoch.
	started  bool
	lastStep int64
	// run counts what the current run did, for its summary log line.
	run runStats
}

type runStats struct {
	writes, writesFailed, checks, checksFailed int
}

// write is a single write request: a sample of each of its series at the same time, with one
// protocol.
type write struct {
	ts       time.Time
	protocol Protocol
	series   []Series
}

func (w write) String() string {
	return fmt.Sprintf("write at %s with protocol %s", w.ts.Format(time.RFC3339), w.protocol)
}

func (w write) seriesString() string {
	parts := make([]string, 0, len(w.series))
	for _, s := range w.series {
		parts = append(parts, s.String())
	}
	return strings.Join(parts, "; ")
}

func NewTest(cfg Config, client Client, managerCfg continuoustest.ManagerConfig, logger log.Logger, reg prometheus.Registerer) *Test {
	return &Test{
		cfg:             cfg,
		client:          client,
		smokeTest:       managerCfg.SmokeTest,
		runInterval:     managerCfg.RunInterval,
		metadataTimeout: defaultMetadataTimeout,
		logger:          log.With(logger, "test", testName),
		metrics:         newMetrics(reg),
	}
}

// Name implements continuoustest.Test.
func (t *Test) Name() string {
	return testName
}

// Init implements continuoustest.Test.
func (t *Test) Init(_ context.Context, _ time.Time) error {
	def, err := LoadDefinition(t.cfg.DefinitionFile)
	if err != nil {
		return err
	}
	if i := t.cfg.WriteInterval; i < time.Second || i%time.Second != 0 {
		return fmt.Errorf("write interval %s is not a positive number of seconds", i)
	}
	if s := t.cfg.RandomSource; s != RandomSourceSeed && s != RandomSourceAntithesis {
		return fmt.Errorf("unknown random source %q, supported values are %q and %q", s, RandomSourceSeed, RandomSourceAntithesis)
	}
	if s := t.cfg.NameValidationScheme; s != model.LegacyValidation && s != model.UTF8Validation {
		return fmt.Errorf("unsupported name validation scheme %s", s)
	}

	t.seed = t.cfg.Seed
	if t.seed == 0 {
		t.seed = rand.Uint64()
	}
	t.explorer, err = newExplorer(t.seed, def, exclusions(t.cfg.NameValidationScheme))
	if err != nil {
		return err
	}
	t.writers = map[Protocol]continuoustest.MimirClient{}
	for _, p := range def.Protocols {
		w, err := t.client.WithWriteProtocol(p.writeProtocol())
		if err != nil {
			return fmt.Errorf("creating the client of protocol %s: %w", p, err)
		}
		t.writers[p] = w
	}
	t.commonLabels = []prompb.Label{
		{Name: labelWriteInterval, Value: model.Duration(t.cfg.WriteInterval).String()},
		{Name: labelSeed, Value: strconv.FormatUint(t.seed, 10)},
	}

	level.Info(t.logger).Log("msg", "Initialized explore test", "random_source", t.cfg.RandomSource, "seed", t.seed, "write_interval", t.cfg.WriteInterval, "name_validation_scheme", t.cfg.NameValidationScheme, "series", t.explorer.numSeries())
	return nil
}

// Run implements continuoustest.Test.
func (t *Test) Run(ctx context.Context, now time.Time) error {
	t.run = runStats{}
	var err error
	if t.smokeTest {
		err = t.runSmokeTest(ctx, now)
	} else {
		err = t.runWrites(ctx, now)
	}
	lvl := level.Info
	if t.run.writesFailed > 0 || t.run.checksFailed > 0 {
		lvl = level.Warn
	}
	lvl(t.logger).Log("msg", "Explore run finished", "writes", t.run.writes, "writes_failed", t.run.writesFailed, "checks", t.run.checks, "checks_failed", t.run.checksFailed)
	return err
}

// runWrites does the write of every write interval since the previous run.
func (t *Test) runWrites(ctx context.Context, now time.Time) error {
	latest := now.UnixNano() / int64(t.cfg.WriteInterval)
	if !t.started {
		// A previous process may have done the write of the latest step, with other choices if they
		// were drawn from Antithesis. Mimir rejects a sample with the same timestamp as an existing
		// one unless they are identical, so writes start at the next step.
		t.started, t.lastStep = true, latest
		level.Info(t.logger).Log("msg", "Writes start at the next write interval", "first_write", t.stepTime(latest+1))
		return nil
	}

	first := t.lastStep + 1
	if oldest := t.firstWritableStep(now); first < oldest {
		level.Warn(t.logger).Log("msg", "The previous run was too long ago, skipping the writes that are too old", "skipped_writes", oldest-first, "first_write", t.stepTime(oldest))
		first = oldest
	}
	errs := multierror.New()
	for step := first; step <= latest; step++ {
		errs.Add(t.writeAndVerify(ctx, t.newWrite(step)))
	}
	t.lastStep = max(t.lastStep, latest)
	return errs.Err()
}

// runSmokeTest writes and reads back a sample of every series with every protocol that can write
// it, in writes of each size in turn. The writes are a second apart, so that the query of each
// write only selects its own samples.
func (t *Test) runSmokeTest(ctx context.Context, now time.Time) error {
	var writes []write
	for _, p := range t.explorer.protocols {
		series := t.explorer.allSeries(p)
		for i := 0; len(series) > 0; i++ {
			n := min(t.explorer.writeSizes[i%len(t.explorer.writeSizes)], len(series))
			writes = append(writes, write{protocol: p, series: series[:n]})
			series = series[n:]
		}
	}
	errs := multierror.New()
	last := now.Truncate(time.Second)
	for i, w := range writes {
		w.ts = last.Add(time.Duration(i+1-len(writes)) * time.Second)
		errs.Add(t.writeAndVerify(ctx, w))
	}
	return errs.Err()
}

// firstWritableStep returns the first step that is still written at the given time. Mimir rejects
// samples that are too old, and with ingest storage it does so after the write request succeeded,
// so after a pause the test skips ahead instead of catching up.
func (t *Test) firstWritableStep(now time.Time) int64 {
	interval := int64(t.cfg.WriteInterval)
	return (now.Add(-2*t.runInterval).UnixNano() + interval - 1) / interval
}

func (t *Test) stepTime(step int64) time.Time {
	return time.Unix(0, step*int64(t.cfg.WriteInterval)).UTC()
}

// newWrite chooses the protocol and the series of the write of the given step.
func (t *Test) newWrite(step int64) write {
	choose := newRand(t.seed, uint64(step)).IntN
	if t.cfg.RandomSource == RandomSourceAntithesis {
		choose = antithesisChoose
	}
	protocol, series := t.explorer.pick(choose)
	return write{ts: t.stepTime(step), protocol: protocol, series: series}
}

// antithesisChoose returns an integer in [0, n) drawn from the Antithesis SDK.
func antithesisChoose(n int) int {
	choices := make([]int, n)
	for i := range choices {
		choices[i] = i
	}
	return antithesisrandom.RandomChoice(choices)
}

func (t *Test) writeAndVerify(ctx context.Context, w write) error {
	if err := t.writeSamples(ctx, w); err != nil {
		return err
	}
	return t.verify(ctx, w)
}

func (t *Test) writeSamples(ctx context.Context, w write) error {
	timeseries := make([]prompb.TimeSeries, 0, len(w.series))
	var metadata []prompb.MetricMetadata
	for _, s := range w.series {
		timeseries = append(timeseries, s.TimeSeries(w.ts, t.commonLabels))
		m := s.Metadata(w.protocol)
		if !slices.ContainsFunc(metadata, func(o prompb.MetricMetadata) bool { return o.MetricFamilyName == m.MetricFamilyName }) {
			metadata = append(metadata, m)
		}
		t.metrics.samplesTotal.WithLabelValues(labelProtocol, string(w.protocol)).Inc()
		for _, l := range s.Dimensions {
			t.metrics.samplesTotal.WithLabelValues(l.Name, l.Value).Inc()
		}
	}

	protocol := string(w.protocol)
	t.metrics.writesTotal.WithLabelValues(protocol, strconv.Itoa(len(w.series))).Inc()
	t.run.writes++
	statusCode, err := t.writers[w.protocol].WriteSeries(ctx, timeseries, metadata)
	if statusCode/100 == 2 && err == nil {
		return nil
	}
	t.run.writesFailed++
	t.metrics.writesFailedTotal.WithLabelValues(protocol, strconv.Itoa(statusCode)).Inc()
	logger := log.With(t.logger, "timestamp", w.ts.UnixMilli(), "protocol", w.protocol, "series", w.seriesString(), "status_code", statusCode, "err", err)
	if statusCode/100 == 4 {
		// Every generated sample is valid, so a rejection is a failure.
		level.Warn(logger).Log("msg", "Write rejected")
		return fmt.Errorf("%s: rejected with status code %d: %w", w, statusCode, err)
	}
	level.Warn(logger).Log("msg", "Write failed")
	return fmt.Errorf("%s: failed with status code %d: %w", w, statusCode, err)
}

// verify reads back the samples of the write and the metadata of their metrics. It also detects
// series that the query returns but the write didn't write, like series with extra labels.
func (t *Test) verify(ctx context.Context, w write) error {
	query := t.writeQuery(w)
	logger := log.With(t.logger, "timestamp", w.ts.UnixMilli(), "protocol", w.protocol, "query", query)

	t.metrics.queriesTotal.Inc()
	vector, err := t.client.Query(ctx, query, w.ts, continuoustest.WithResultsCacheEnabled(false))
	if err != nil {
		t.metrics.queriesFailedTotal.Inc()
		level.Warn(logger).Log("msg", "Query failed", "err", err)
		return fmt.Errorf("%s: query failed: %w", w, err)
	}
	unmatched := make(map[string]*model.Sample, len(vector))
	for _, sample := range vector {
		unmatched[sample.Metric.String()] = sample
	}

	checkErrs := make([]error, len(w.series))
	for i, s := range w.series {
		expected := t.expectedMetric(w.protocol, s).String()
		sample, ok := unmatched[expected]
		delete(unmatched, expected)
		switch {
		case !ok:
			checkErrs[i] = fmt.Errorf("expected a sample of series %s but got none", expected)
		case s.Family == FamilyFloat && sample.Histogram != nil:
			checkErrs[i] = fmt.Errorf("expected float %v but got histogram %s", s.Float(w.ts), sample.Histogram)
		case s.Family == FamilyFloat:
			if want := s.Float(w.ts); float64(sample.Value) != want {
				checkErrs[i] = fmt.Errorf("expected float %v but got %s", want, sample.Value)
			}
		case sample.Histogram == nil:
			checkErrs[i] = fmt.Errorf("expected a histogram but got float %s", sample.Value)
		default:
			want := mimirpb.FromFloatHistogramToPromHistogram(s.ExpectedHistogram(w.ts))
			if !histogramsEqual(sample.Histogram, want) {
				checkErrs[i] = fmt.Errorf("expected histogram %s but got %s", want, sample.Histogram)
			}
		}
	}
	var unexpected error
	for _, sample := range unmatched {
		unexpected = fmt.Errorf("got %d unexpected series, such as %s", len(unmatched), sample.Metric)
		break
	}

	metadata := map[string][]v1.Metadata{}
	for i, s := range w.series {
		want, ok := expectedMetadata(s, w.protocol)
		if !ok {
			continue
		}
		name := t.storedMetricName(w.protocol, s)
		got, ok := metadata[name]
		if !ok {
			if got, err = t.readMetadata(ctx, name, want); err != nil {
				level.Warn(logger).Log("msg", "Metadata query failed", "err", err)
				return fmt.Errorf("%s: metadata query failed: %w", w, err)
			}
			metadata[name] = got
		}
		if checkErrs[i] == nil && !slices.Contains(got, want) {
			checkErrs[i] = fmt.Errorf("expected metadata %+v but got %+v", want, got)
		}
	}

	errs := multierror.New()
	for i, s := range w.series {
		t.metrics.queryResultChecksTotal.Inc()
		t.run.checks++
		if err := errors.Join(checkErrs[i], unexpected); err != nil {
			t.metrics.queryResultChecksFailedTotal.Inc()
			t.run.checksFailed++
			level.Warn(logger).Log("msg", "Query result check failed", "series", s, "err", err)
			errs.Add(fmt.Errorf("%s: series %s: %w", w, s, err))
		}
	}
	if errs.Err() == nil {
		level.Info(logger).Log("msg", "Write verified", "series", w.seriesString())
	}
	return errs.Err()
}

// readMetadata reads the metadata of the given metric until it includes want, or until the metadata
// timeout.
func (t *Test) readMetadata(ctx context.Context, metric string, want v1.Metadata) ([]v1.Metadata, error) {
	deadline := time.Now().Add(t.metadataTimeout)
	for {
		t.metrics.queriesTotal.Inc()
		got, err := t.client.MetricMetadata(ctx, metric)
		if err != nil {
			t.metrics.queriesFailedTotal.Inc()
			return nil, err
		}
		if slices.Contains(got, want) || !time.Now().Before(deadline) {
			return got, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(metadataRetryInterval):
		}
	}
}

// expectedMetadata returns the metadata written with the series by the given protocol as Mimir
// stores it, or false if Mimir may not store it. Remote write 2.0, which ingest storage also uses
// for its records, can't tell metadata of an unknown type without help or unit from no metadata.
func expectedMetadata(s Series, protocol Protocol) (v1.Metadata, bool) {
	m := s.Metadata(protocol)
	if m.Type == prompb.MetricMetadata_UNKNOWN && m.Help == "" && m.Unit == "" {
		return v1.Metadata{}, false
	}
	return v1.Metadata{Type: v1.MetricType(strings.ToLower(m.Type.String())), Help: m.Help, Unit: s.PrometheusUnit()}, true
}

// escapesNames returns whether names written with the given protocol are stored escaped. Unless
// the tenant accepts UTF-8 names, the OTLP endpoint escapes them.
func (t *Test) escapesNames(protocol Protocol) bool {
	return protocol == ProtocolOTLP && t.cfg.NameValidationScheme != model.UTF8Validation
}

// storedMetricName returns the name that the metric of the series is stored with, after a write
// with the given protocol.
func (t *Test) storedMetricName(protocol Protocol, s Series) string {
	if t.escapesNames(protocol) {
		return s.metricName(CharsetLegacy)
	}
	return s.MetricName()
}

// expectedMetric returns the labels that the series is expected to be read back with, after a
// write with the given protocol.
func (t *Test) expectedMetric(protocol Protocol, s Series) model.Metric {
	m := model.Metric{}
	for _, l := range s.Labels(t.commonLabels) {
		name, value := l.Name, l.Value
		switch {
		case name == model.MetricNameLabel:
			value = t.storedMetricName(protocol, s)
		case name == utf8LabelName && t.escapesNames(protocol):
			name = escapedUTF8LabelName
		}
		m[model.LabelName(name)] = model.LabelValue(value)
	}
	return m
}

// writeQuery returns a query selecting the samples of the series of the write, at the time of the
// write. Each series is selected by all of its labels: ingesters can answer a selector with the
// series that matched it up to a TTL earlier, so a selector that matched the series of earlier
// writes could miss a new series. The series of a write differ in their dimension labels, so or,
// which ignores metric names, keeps them all. The [1s] range keeps the lookback period from
// returning the samples of earlier writes, which are at least a second older.
func (t *Test) writeQuery(w write) string {
	selectors := make([]string, 0, len(w.series))
	for _, s := range w.series {
		metric := t.expectedMetric(w.protocol, s)
		matchers := make([]string, 0, len(metric))
		for _, name := range slices.Sorted(maps.Keys(metric)) {
			matchers = append(matchers, labels.MustNewMatcher(labels.MatchEqual, string(name), string(metric[name])).String())
		}
		selectors = append(selectors, fmt.Sprintf("last_over_time({%s}[1s])", strings.Join(matchers, ", ")))
	}
	return strings.Join(selectors, " or ")
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

type metrics struct {
	writesTotal                  *prometheus.CounterVec
	writesFailedTotal            *prometheus.CounterVec
	samplesTotal                 *prometheus.CounterVec
	queriesTotal                 prometheus.Counter
	queriesFailedTotal           prometheus.Counter
	queryResultChecksTotal       prometheus.Counter
	queryResultChecksFailedTotal prometheus.Counter
}

func newMetrics(reg prometheus.Registerer) *metrics {
	return &metrics{
		writesTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "mimir_continuous_test_explore_writes_total",
			Help: "Total number of write requests attempted by the explore test, by protocol and number of series written.",
		}, []string{labelProtocol, "write_size"}),
		writesFailedTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "mimir_continuous_test_explore_writes_failed_total",
			Help: "Total number of failed write requests of the explore test, by protocol.",
		}, []string{labelProtocol, "status_code"}),
		samplesTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "mimir_continuous_test_explore_samples_total",
			Help: "Total number of samples that the explore test attempted to write, by dimension and value. The protocol of the write counts as a dimension.",
		}, []string{"dimension", "value"}),
		queriesTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "mimir_continuous_test_explore_queries_total",
			Help: "Total number of queries attempted by the explore test.",
		}),
		queriesFailedTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "mimir_continuous_test_explore_queries_failed_total",
			Help: "Total number of failed queries of the explore test.",
		}),
		queryResultChecksTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "mimir_continuous_test_explore_query_result_checks_total",
			Help: "Total number of samples read back and checked by the explore test.",
		}),
		queryResultChecksFailedTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "mimir_continuous_test_explore_query_result_checks_failed_total",
			Help: "Total number of samples read back by the explore test that failed the check. The log line of each failure has the series and the error.",
		}),
	}
}
