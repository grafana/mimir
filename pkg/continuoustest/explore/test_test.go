// SPDX-License-Identifier: AGPL-3.0-only

package explore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/model"
	"github.com/prometheus/otlptranslator"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/continuoustest"
	"github.com/grafana/mimir/pkg/mimirpb"
)

// testDefinition has 64 combinations of series dimension values. With the legacy name validation
// scheme, v1 and v2 each write the 28 series with legacy names: every sample type and kind, with
// 3 exponential schemas and 2 layouts, or with custom buckets and the gaps layout. OTLP writes the
// 12 int counters with exponential buckets of both charsets. That makes 34 distinct series. With
// the UTF-8 scheme, v1 and v2 each write 56 series, which are all the series.
const testDefinition = `
protocols: [v1, v2, otlp]
write_sizes: [1, 2, 5, 10]
charsets: [legacy, utf8]
native_histograms:
  sample_types: [int, float]
  kinds: [counter, gauge]
  schemas: [-4, 0, 8, -53]
  layouts: [gaps, negative]
paces:
  test:
    write_interval: 10s
`

// testStart is a multiple of the write interval.
var testStart = time.Unix(1_800_000_000, 0).UTC()

func TestTest_Run(t *testing.T) {
	client := newFakeClient()
	test := newTestTest(t, client, false, prometheus.NewPedanticRegistry())

	require.NoError(t, test.Run(context.Background(), testStart))
	require.Empty(t, client.writes, "the first run only determines when writes start")
	runFor(t, test, testStart.Add(30*time.Second), 10*time.Minute)

	// Every write interval after the first run has its write, up to the last run 10 minutes after
	// the first one.
	require.Len(t, client.writes, 60)
	samples := 0
	for i, w := range client.writes {
		require.Equal(t, testStart.Add(time.Duration(i+1)*10*time.Second).UnixMilli(), w.ts, "write %d", i)
		samples += len(w.series)
	}
	require.Equal(t, float64(60), sumCounters(test.metrics.writesTotal))
	require.Equal(t, float64(samples), sumCounters(test.metrics.queryResultChecksTotal))
	require.Zero(t, sumCounters(test.metrics.queryResultChecksFailedTotal))
	require.Zero(t, sumCounters(test.metrics.writesFailedTotal))
	// The failure counter of every checked combination exists, at zero.
	require.Equal(t, testutil.CollectAndCount(test.metrics.queryResultChecksTotal), testutil.CollectAndCount(test.metrics.queryResultChecksFailedTotal))
}

func TestTest_MetricLabelsAreTheDimensions(t *testing.T) {
	def, err := LoadDefinition("")
	require.NoError(t, err)
	var names []string
	for _, d := range def.dimensions() {
		names = append(names, d.name)
	}
	require.Equal(t, names, dimensionNames())
}

func TestTest_WritesPickProtocolsSizesAndSeries(t *testing.T) {
	client := newFakeClient()
	test := newTestTest(t, client, false, prometheus.NewPedanticRegistry())
	runFor(t, test, testStart, time.Hour)

	expectedProtocols := map[continuoustest.WriteProtocol]string{"prometheus": "v1", "prometheus2": "v2", "otlp-http": "otlp"}
	protocols, sizes := map[string]bool{}, map[int]bool{}
	for _, w := range client.writes {
		protocol := expectedProtocols[w.protocol]
		protocols[protocol] = true
		sizes[len(w.series)] = true
		for _, s := range w.series {
			if protocol == "otlp" {
				require.Equal(t, "int", s.Get(labelSampleType), "series %s", s)
				require.Equal(t, "counter", s.Get(labelKind), "series %s", s)
				require.NotEqual(t, "-53", s.Get(labelSchema), "series %s", s)
			} else {
				require.Equal(t, "legacy", s.Get(labelCharset), "series %s", s)
			}
		}
		families := map[string]bool{}
		for _, m := range w.metadata {
			require.False(t, families[m.MetricFamilyName], "every metric of the write has its metadata once")
			families[m.MetricFamilyName] = true
		}
		require.NotEmpty(t, families)
	}
	require.Equal(t, map[string]bool{"v1": true, "v2": true, "otlp": true}, protocols)
	require.Equal(t, map[int]bool{1: true, 2: true, 5: true, 10: true}, sizes)
	for _, p := range []string{"v1", "v2", "otlp"} {
		require.Positive(t, testutil.ToFloat64(test.metrics.samplesTotal.WithLabelValues(labelProtocol, p)), "protocol %s", p)
	}
}

func TestTest_SeriesAreLongLived(t *testing.T) {
	client := newFakeClient()
	test := newTestTest(t, client, false, prometheus.NewPedanticRegistry())
	runFor(t, test, testStart, time.Hour)

	require.Len(t, client.series, 34, "every series has been picked")
	mixedProtocols := 0
	for _, s := range client.series {
		require.Greater(t, len(s.samples), 1, "series %s should have been picked more than once", s.labels)
		protocols := map[continuoustest.WriteProtocol]bool{}
		for _, sample := range s.samples {
			protocols[sample.protocol] = true
		}
		if len(protocols) > 1 {
			mixedProtocols++
		}
	}
	require.Positive(t, mixedProtocols, "some series should have samples written with different protocols")
}

func TestTest_NewSeriesWithCachedMatchingSeries(t *testing.T) {
	client := newFakeClient()
	client.cacheMatchingSeries = true
	test := newTestTest(t, client, false, prometheus.NewPedanticRegistry())
	runFor(t, test, testStart, time.Hour)

	require.Len(t, client.series, 34, "every series has been created, most of them after earlier queries")
	require.Zero(t, sumCounters(test.metrics.queryResultChecksFailedTotal))
}

func TestTest_Deterministic(t *testing.T) {
	writes := func(seed uint64) []fakeWrite {
		client := newFakeClient()
		test := newTestTestWithConfig(t, client, false, prometheus.NewPedanticRegistry(), func(cfg *Config) { cfg.Seed = seed })
		runFor(t, test, testStart, 5*time.Minute)
		return client.writes
	}
	require.Equal(t, writes(1), writes(1))
	require.NotEqual(t, writes(1), writes(2))
}

func TestTest_UTF8NamesWithTheLegacyScheme(t *testing.T) {
	client := newFakeClient()
	test := newTestTest(t, client, true, prometheus.NewPedanticRegistry())
	require.NoError(t, test.Run(context.Background(), testStart))

	utf8Series := 0
	for _, s := range client.series {
		if s.labels.Get(labelCharset) != "utf8" {
			continue
		}
		utf8Series++
		require.Equal(t, counterMetricName, s.labels.Get(model.MetricNameLabel), "OTLP escapes the metric name")
		require.Equal(t, utf8LabelValue, s.labels.Get(escapedUTF8LabelName), "OTLP escapes the label name")
		for _, sample := range s.samples {
			require.Equal(t, continuoustest.WriteProtocol("otlp-http"), sample.protocol, "only OTLP accepts UTF-8 names")
		}
	}
	require.Equal(t, 6, utf8Series)
}

func TestTest_UTF8NamesWithTheUTF8Scheme(t *testing.T) {
	client := newFakeClient()
	client.nameValidationScheme = model.UTF8Validation
	test := newTestTestWithConfig(t, client, true, prometheus.NewPedanticRegistry(), func(cfg *Config) {
		cfg.NameValidationScheme = model.UTF8Validation
	})
	require.NoError(t, test.Run(context.Background(), testStart))
	require.Len(t, client.series, 56)

	protocols := map[continuoustest.WriteProtocol]bool{}
	for _, s := range client.series {
		if s.labels.Get(labelCharset) != "utf8" {
			continue
		}
		require.Contains(t, []string{utf8CounterMetricName, utf8GaugeMetricName}, s.labels.Get(model.MetricNameLabel))
		require.Equal(t, utf8LabelValue, s.labels.Get(utf8LabelName))
		for _, sample := range s.samples {
			protocols[sample.protocol] = true
		}
	}
	require.Len(t, protocols, 3, "every protocol writes UTF-8 names")
}

func TestTest_DetectsWrongResults(t *testing.T) {
	client := newFakeClient()
	test := newTestTest(t, client, false, prometheus.NewPedanticRegistry())
	require.NoError(t, test.Run(context.Background(), testStart))

	client.corrupt = func(h *prompb.Histogram) { h.Sum++ }
	err := test.Run(context.Background(), testStart.Add(30*time.Second))
	require.ErrorContains(t, err, "expected histogram")
	require.Positive(t, sumCounters(test.metrics.queryResultChecksFailedTotal))
}

func TestTest_DetectsWrongLabels(t *testing.T) {
	client := newFakeClient()
	client.corruptMetric = func(m model.Metric) { m["extra"] = "label" }
	test := newTestTest(t, client, true, prometheus.NewPedanticRegistry())
	err := test.Run(context.Background(), testStart)
	require.ErrorContains(t, err, "but got none")
	require.ErrorContains(t, err, `extra="label"`)
}

func TestTest_DetectsUnexpectedSeries(t *testing.T) {
	client := newFakeClient()
	client.extraSeries = true
	test := newTestTest(t, client, true, prometheus.NewPedanticRegistry())
	err := test.Run(context.Background(), testStart)
	require.ErrorContains(t, err, "got 1 unexpected series")
	require.NotContains(t, err.Error(), "but got none")
}

func TestTest_DetectsMissingMetadata(t *testing.T) {
	client := newFakeClient()
	client.dropMetadata = true
	test := newTestTest(t, client, true, prometheus.NewPedanticRegistry())
	test.metadataTimeout = 0
	err := test.Run(context.Background(), testStart)
	require.ErrorContains(t, err, "expected metadata")
	require.Positive(t, sumCounters(test.metrics.queryResultChecksFailedTotal))
}

func TestTest_WaitsForMetadata(t *testing.T) {
	client := newFakeClient()
	client.hiddenMetadataReads = 1
	test := newTestTest(t, client, true, prometheus.NewPedanticRegistry())
	require.NoError(t, test.Run(context.Background(), testStart))
	require.Zero(t, client.hiddenMetadataReads)
}

func TestTest_SkipsAheadAfterAPause(t *testing.T) {
	client := newFakeClient()
	test := newTestTest(t, client, false, prometheus.NewPedanticRegistry())
	run := func(now time.Time) error {
		client.oldestStored = now.Add(-10 * time.Minute)
		return test.Run(context.Background(), now)
	}
	require.NoError(t, run(testStart))
	require.NoError(t, run(testStart.Add(30*time.Second)))

	// Doing the writes missed during the pause would succeed, but their samples wouldn't be stored,
	// so their checks would fail.
	resumed := testStart.Add(3 * time.Hour)
	before := len(client.writes)
	require.NoError(t, run(resumed))
	require.Len(t, client.writes, before+7, "the writes of the last two run intervals, including the current one")
	require.Equal(t, resumed.Add(-time.Minute).UnixMilli(), client.writes[before].ts)
}

func TestTest_ResumesAfterRestart(t *testing.T) {
	for _, source := range []string{RandomSourceSeed, RandomSourceAntithesis} {
		t.Run(source, func(t *testing.T) {
			client := newFakeClient()
			withSource := func(cfg *Config) { cfg.RandomSource = source }
			first := newTestTestWithConfig(t, client, false, prometheus.NewPedanticRegistry(), withSource)
			require.NoError(t, first.Run(context.Background(), testStart))
			require.NoError(t, first.Run(context.Background(), testStart.Add(30*time.Second)))

			// The restarted test doesn't redo the write of the latest step, which could pick a series
			// already written at that time with another protocol.
			restarted := newTestTestWithConfig(t, client, false, prometheus.NewPedanticRegistry(), withSource)
			before := len(client.writes)
			require.NoError(t, restarted.Run(context.Background(), testStart.Add(30*time.Second)))
			require.Len(t, client.writes, before)
			require.NoError(t, restarted.Run(context.Background(), testStart.Add(time.Minute)))
			require.Len(t, client.writes, before+3)

			timestamps := map[int64]bool{}
			for _, w := range client.writes {
				require.False(t, timestamps[w.ts], "two writes at %d", w.ts)
				timestamps[w.ts] = true
			}
		})
	}
}

func TestTest_WriteRejected(t *testing.T) {
	client := newFakeClient()
	client.writeStatus = 400
	test := newTestTest(t, client, false, prometheus.NewPedanticRegistry())
	require.NoError(t, test.Run(context.Background(), testStart))

	require.ErrorContains(t, test.Run(context.Background(), testStart.Add(30*time.Second)), "rejected with status code 400")
	require.Equal(t, float64(3), sumCounters(test.metrics.writesFailedTotal))
	require.Zero(t, testutil.ToFloat64(test.metrics.queriesTotal), "rejected writes aren't read back")
}

func TestTest_WriteFailed(t *testing.T) {
	client := newFakeClient()
	client.writeStatus = 500
	test := newTestTest(t, client, false, prometheus.NewPedanticRegistry())
	require.NoError(t, test.Run(context.Background(), testStart))
	require.ErrorContains(t, test.Run(context.Background(), testStart.Add(30*time.Second)), "failed with status code 500")

	// Failed writes aren't retried: the next run carries on with the next writes.
	client.writeStatus = 200
	require.NoError(t, test.Run(context.Background(), testStart.Add(time.Minute)))
	require.Len(t, client.writes, 3)
	require.Equal(t, testStart.Add(40*time.Second).UnixMilli(), client.writes[0].ts)
}

func TestTest_SmokeTest(t *testing.T) {
	for _, source := range []string{RandomSourceSeed, RandomSourceAntithesis} {
		t.Run(source, func(t *testing.T) {
			client := newFakeClient()
			test := newTestTestWithConfig(t, client, true, prometheus.NewPedanticRegistry(), func(cfg *Config) { cfg.RandomSource = source })
			now := testStart.Add(1500 * time.Millisecond)
			require.NoError(t, test.Run(context.Background(), now))

			// Writes of 1, 2, 5, 10, 1, 2, 5 and 2 series write the 28 series of v1 and of v2, and
			// writes of 1, 2, 5 and 4 series write the 12 series of OTLP.
			require.Len(t, client.writes, 20)
			samples := map[continuoustest.WriteProtocol]int{}
			for i, w := range client.writes {
				require.Equal(t, now.Truncate(time.Second).Add(time.Duration(i-19)*time.Second).UnixMilli(), w.ts, "write %d", i)
				samples[w.protocol] += len(w.series)
			}
			require.Equal(t, map[continuoustest.WriteProtocol]int{"prometheus": 28, "prometheus2": 28, "otlp-http": 12}, samples)
			require.Len(t, client.series, 34)
		})
	}
}

func TestTest_InitErrors(t *testing.T) {
	path := writeDefinition(t)

	cfg := Config{DefinitionFile: path, Pace: "test", Seed: 1, RandomSource: RandomSourceSeed, NameValidationScheme: model.LegacyValidation}
	managerCfg := continuoustest.ManagerConfig{RunInterval: 30 * time.Second}

	noOTLP := newFakeClient()
	noOTLP.unsupportedProtocol = "otlp-http"
	test := NewTest(cfg, noOTLP, managerCfg, log.NewNopLogger(), nil)
	require.ErrorContains(t, test.Init(context.Background(), time.Now()), "creating the client of protocol otlp")

	missingPace := cfg
	missingPace.Pace = "missing"
	test = NewTest(missingPace, newFakeClient(), managerCfg, log.NewNopLogger(), nil)
	require.ErrorContains(t, test.Init(context.Background(), time.Now()), `pace "missing" is not defined`)

	unknownSource := cfg
	unknownSource.RandomSource = "dice"
	test = NewTest(unknownSource, newFakeClient(), managerCfg, log.NewNopLogger(), nil)
	require.ErrorContains(t, test.Init(context.Background(), time.Now()), `unknown random source "dice"`)

	unsetScheme := cfg
	unsetScheme.NameValidationScheme = model.UnsetValidation
	test = NewTest(unsetScheme, newFakeClient(), managerCfg, log.NewNopLogger(), nil)
	require.ErrorContains(t, test.Init(context.Background(), time.Now()), "unsupported name validation scheme unset")
}

func TestTest_AntithesisSource_Run(t *testing.T) {
	client := newFakeClient()
	test := newTestTestWithConfig(t, client, false, prometheus.NewPedanticRegistry(), func(cfg *Config) {
		cfg.RandomSource = RandomSourceAntithesis
	})
	runFor(t, test, testStart, 10*time.Minute)

	require.Len(t, client.writes, 57)
	protocols := map[continuoustest.WriteProtocol]bool{}
	for _, w := range client.writes {
		protocols[w.protocol] = true
		for _, s := range w.series {
			if w.protocol == "otlp-http" {
				require.Equal(t, "int", s.Get(labelSampleType), "series %s", s)
				require.Equal(t, "counter", s.Get(labelKind), "series %s", s)
			} else {
				require.Equal(t, "legacy", s.Get(labelCharset), "series %s", s)
			}
		}
	}
	require.Greater(t, len(protocols), 1)
}

// runFor runs the test every 30 seconds for the given duration, starting at start.
func runFor(t *testing.T, test *Test, start time.Time, d time.Duration) {
	for now := start; now.Before(start.Add(d)); now = now.Add(30 * time.Second) {
		require.NoError(t, test.Run(context.Background(), now), "run at %s", now)
	}
}

func newTestTest(t *testing.T, client *fakeClient, smokeTest bool, reg prometheus.Registerer) *Test {
	return newTestTestWithConfig(t, client, smokeTest, reg, func(*Config) {})
}

func newTestTestWithConfig(t *testing.T, client *fakeClient, smokeTest bool, reg prometheus.Registerer, modify func(*Config)) *Test {
	cfg := Config{DefinitionFile: writeDefinition(t), Pace: "test", Seed: 1, RandomSource: RandomSourceSeed, NameValidationScheme: model.LegacyValidation}
	modify(&cfg)
	test := NewTest(cfg, client, continuoustest.ManagerConfig{SmokeTest: smokeTest, RunInterval: 30 * time.Second}, log.NewNopLogger(), reg)
	require.NoError(t, test.Init(context.Background(), time.Now()))
	return test
}

func writeDefinition(t *testing.T) string {
	path := filepath.Join(t.TempDir(), "definition.yaml")
	require.NoError(t, os.WriteFile(path, []byte(testDefinition), 0o600))
	return path
}

// fakeClient stores written histograms and metadata in memory, and answers metadata queries and
// instant queries of the form last_over_time(<selector>[1s]), or unions of them with or.
type fakeClient struct {
	protocol            continuoustest.WriteProtocol
	unsupportedProtocol continuoustest.WriteProtocol
	// nameValidationScheme is the name validation scheme of the tenant. With legacy, remote write
	// requests with UTF-8 names are rejected, and the names of OTLP requests are escaped.
	nameValidationScheme model.ValidationScheme
	writeStatus          int
	series               map[string]*fakeSeries
	metadata             map[string][]v1.Metadata
	// writes holds every accepted write, in order.
	writes []fakeWrite
	// corrupt, if set, modifies every histogram returned by queries.
	corrupt func(*prompb.Histogram)
	// corruptMetric, if set, modifies the labels of every series returned by queries.
	corruptMetric func(model.Metric)
	// extraSeries is whether queries return a series that wasn't written, with the first sample.
	extraSeries bool
	// cacheMatchingSeries is whether the series matching a selector are cached the first time the
	// selector is queried, and never refreshed. Mimir's postings for matchers cache does the same
	// for the duration of its TTL.
	cacheMatchingSeries bool
	matchingSeries      map[string][]*fakeSeries
	// dropMetadata is whether written metadata is discarded.
	dropMetadata bool
	// hiddenMetadataReads is the number of metadata queries that return no metadata, like metadata
	// queries that run before the written metadata is readable.
	hiddenMetadataReads int
	// Samples older than oldestStored are accepted but not stored, like Mimir with ingest storage
	// does with samples that are too old.
	oldestStored time.Time
}

type fakeSeries struct {
	labels  labels.Labels
	samples map[int64]fakeSample
}

type fakeSample struct {
	histogram prompb.Histogram
	protocol  continuoustest.WriteProtocol
}

type fakeWrite struct {
	protocol continuoustest.WriteProtocol
	ts       int64
	series   []labels.Labels
	metadata []prompb.MetricMetadata
}

func newFakeClient() *fakeClient {
	return &fakeClient{
		protocol:             "prometheus",
		nameValidationScheme: model.LegacyValidation,
		writeStatus:          200,
		series:               map[string]*fakeSeries{},
		metadata:             map[string][]v1.Metadata{},
		matchingSeries:       map[string][]*fakeSeries{},
	}
}

func (c *fakeClient) WithWriteProtocol(protocol continuoustest.WriteProtocol) (continuoustest.MimirClient, error) {
	if protocol == c.unsupportedProtocol {
		return nil, fmt.Errorf("unsupported protocol %q", protocol)
	}
	return &fakeProtocolClient{fakeClient: c, protocol: protocol}, nil
}

// fakeProtocolClient writes to its fakeClient with a given protocol.
type fakeProtocolClient struct {
	*fakeClient
	protocol continuoustest.WriteProtocol
}

func (c *fakeProtocolClient) WriteSeries(_ context.Context, series []prompb.TimeSeries, metadata []prompb.MetricMetadata) (int, error) {
	return c.write(series, metadata, c.protocol)
}

func (c *fakeProtocolClient) Protocol() continuoustest.WriteProtocol {
	return c.protocol
}

func (c *fakeClient) WriteSeries(_ context.Context, series []prompb.TimeSeries, metadata []prompb.MetricMetadata) (int, error) {
	return c.write(series, metadata, c.protocol)
}

func (c *fakeClient) write(series []prompb.TimeSeries, metadata []prompb.MetricMetadata, protocol continuoustest.WriteProtocol) (int, error) {
	if c.writeStatus != 200 {
		return c.writeStatus, errors.New("write rejected")
	}
	w := fakeWrite{protocol: protocol, metadata: metadata}
	escape := protocol == "otlp-http" && c.nameValidationScheme != model.UTF8Validation
	for _, ts := range series {
		b := labels.NewScratchBuilder(len(ts.Labels))
		for _, l := range ts.Labels {
			name, value := l.Name, l.Value
			switch {
			case escape && name == model.MetricNameLabel:
				value = escapeMetricName(value)
			case escape:
				name = escapeLabelName(name)
			case name == model.MetricNameLabel && !c.nameValidationScheme.IsValidMetricName(value):
				return 400, fmt.Errorf("invalid metric name %q", value)
			case !c.nameValidationScheme.IsValidLabelName(name):
				return 400, fmt.Errorf("invalid label name %q", name)
			}
			b.Add(name, value)
		}
		b.Sort()
		lbls := b.Labels()
		w.series = append(w.series, lbls)
		s, ok := c.series[lbls.String()]
		if !ok {
			s = &fakeSeries{labels: lbls, samples: map[int64]fakeSample{}}
			c.series[lbls.String()] = s
		}
		for _, h := range ts.Histograms {
			w.ts = h.Timestamp
			if h.Timestamp < c.oldestStored.UnixMilli() {
				continue
			}
			// Like Mimir, a sample with the timestamp of an existing one is rejected unless they are
			// identical. Different protocols can store the same histogram differently, for example
			// with more empty buckets.
			sample := fakeSample{histogram: h, protocol: protocol}
			if existing, ok := s.samples[h.Timestamp]; ok && (existing.histogram.String() != h.String() || existing.protocol != protocol) {
				return 400, fmt.Errorf("duplicate sample for timestamp %d", h.Timestamp)
			}
			s.samples[h.Timestamp] = sample
		}
	}
	for _, m := range metadata {
		if c.dropMetadata {
			break
		}
		name := m.MetricFamilyName
		if escape {
			name = escapeMetricName(name)
		}
		md := v1.Metadata{Type: v1.MetricType(strings.ToLower(m.Type.String())), Help: m.Help, Unit: m.Unit}
		if !slices.Contains(c.metadata[name], md) {
			c.metadata[name] = append(c.metadata[name], md)
		}
	}
	c.writes = append(c.writes, w)
	return 200, nil
}

func escapeMetricName(name string) string {
	namer := otlptranslator.MetricNamer{}
	escaped, err := namer.Build(otlptranslator.Metric{Name: name, Type: otlptranslator.MetricTypeExponentialHistogram})
	if err != nil {
		panic(err)
	}
	return escaped
}

func escapeLabelName(name string) string {
	namer := otlptranslator.LabelNamer{UnderscoreLabelSanitization: true, PreserveMultipleUnderscores: true}
	escaped, err := namer.Build(name)
	if err != nil {
		panic(err)
	}
	return escaped
}

func (c *fakeClient) Query(_ context.Context, query string, ts time.Time, _ ...continuoustest.RequestOption) (model.Vector, error) {
	expr, err := parser.NewParser(parser.Options{}).ParseExpr(query)
	if err != nil {
		return nil, err
	}
	out, err := c.eval(expr, ts)
	if err != nil {
		return nil, fmt.Errorf("query %q: %w", query, err)
	}
	if c.extraSeries && len(out) > 0 {
		extra := *out[0]
		extra.Metric = extra.Metric.Clone()
		extra.Metric["extra"] = "series"
		out = append(out, &extra)
	}
	return out, nil
}

func (c *fakeClient) eval(expr parser.Expr, ts time.Time) (model.Vector, error) {
	if b, ok := expr.(*parser.BinaryExpr); ok && b.Op == parser.LOR {
		lhs, err := c.eval(b.LHS, ts)
		if err != nil {
			return nil, err
		}
		rhs, err := c.eval(b.RHS, ts)
		if err != nil {
			return nil, err
		}
		// Like in PromQL, or ignores metric names when it matches series.
		lhsSeries := map[model.Fingerprint]bool{}
		for _, s := range lhs {
			lhsSeries[withoutMetricName(s.Metric).Fingerprint()] = true
		}
		for _, s := range rhs {
			if !lhsSeries[withoutMetricName(s.Metric).Fingerprint()] {
				lhs = append(lhs, s)
			}
		}
		return lhs, nil
	}
	call, ok := expr.(*parser.Call)
	if !ok || call.Func.Name != "last_over_time" {
		return nil, errors.New("unsupported expression")
	}
	selector := call.Args[0].(*parser.MatrixSelector).VectorSelector.(*parser.VectorSelector)

	var out model.Vector
	for _, s := range c.seriesMatching(selector) {
		sample, ok := s.samples[ts.UnixMilli()]
		if !ok {
			continue
		}
		h := sample.histogram
		if c.corrupt != nil {
			c.corrupt(&h)
		}
		var fh = h.ToFloatHistogram()
		if !h.IsFloatHistogram() {
			fh = h.ToIntHistogram().ToFloat(nil)
		}
		metric := model.Metric{}
		s.labels.Range(func(l labels.Label) { metric[model.LabelName(l.Name)] = model.LabelValue(l.Value) })
		if c.corruptMetric != nil {
			c.corruptMetric(metric)
		}
		out = append(out, &model.Sample{Metric: metric, Timestamp: model.TimeFromUnixNano(ts.UnixNano()), Histogram: mimirpb.FromFloatHistogramToPromHistogram(fh)})
	}
	return out, nil
}

func (c *fakeClient) seriesMatching(selector *parser.VectorSelector) []*fakeSeries {
	key := selector.String()
	if cached, ok := c.matchingSeries[key]; ok {
		return cached
	}
	var matching []*fakeSeries
	for _, s := range c.series {
		if matchesAll(selector.LabelMatchers, s.labels) {
			matching = append(matching, s)
		}
	}
	if c.cacheMatchingSeries {
		c.matchingSeries[key] = matching
	}
	return matching
}

func withoutMetricName(m model.Metric) model.Metric {
	m = m.Clone()
	delete(m, model.MetricNameLabel)
	return m
}

func matchesAll(matchers []*labels.Matcher, lbls labels.Labels) bool {
	for _, m := range matchers {
		if !m.Matches(lbls.Get(m.Name)) {
			return false
		}
	}
	return true
}

func (c *fakeClient) QueryRange(context.Context, string, time.Time, time.Time, time.Duration, ...continuoustest.RequestOption) (model.Matrix, error) {
	return nil, errors.New("not implemented")
}

func (c *fakeClient) Metadata(context.Context, string) (v1.Metadata, error) {
	return v1.Metadata{}, errors.New("not implemented")
}

func (c *fakeClient) MetricMetadata(_ context.Context, metric string) ([]v1.Metadata, error) {
	if c.hiddenMetadataReads > 0 {
		c.hiddenMetadataReads--
		return nil, nil
	}
	return c.metadata[metric], nil
}

func (c *fakeClient) Protocol() continuoustest.WriteProtocol {
	return c.protocol
}

func sumCounters(c prometheus.Collector) float64 {
	ch := make(chan prometheus.Metric)
	go func() {
		c.Collect(ch)
		close(ch)
	}()
	var sum float64
	for m := range ch {
		var pb dto.Metric
		if err := m.Write(&pb); err == nil {
			sum += pb.GetCounter().GetValue()
		}
	}
	return sum
}
