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

// testDefinition has 10 float and 64 native histogram combinations of series dimension values.
// With the legacy name validation scheme, v1 and v2 each write the 33 series with legacy names:
// the 5 float types, and the 28 native histograms of every sample type and kind, with 3
// exponential schemas and 2 layouts, or with custom buckets and the gaps layout. OTLP writes the
// float counters and gauges, and the 12 native histogram int counters with exponential buckets,
// of both charsets, so 16 series. That makes 41 distinct series. With the UTF-8 scheme, v1 and v2
// each write 66 series, which are all the series. Every series has a unit, which OTLP writes as an
// OTLP unit, and a help text with special characters.
const testDefinition = `
protocols: [v1, v2, otlp]
write_sizes: [1, 2, 5, 10]
charsets: [legacy, utf8]
metadata:
  units: [seconds]
  helps: [utf8]
floats:
  types: [counter, gauge, unknown, info, stateset]
native_histograms:
  sample_types: [int, float]
  kinds: [counter, gauge]
  schemas: [-4, 0, 8, -53]
  layouts: [gaps, negative]
`

// testWriteInterval is the write interval of the tests, and testStart is a multiple of it.
const testWriteInterval = 10 * time.Second

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
		for _, s := range w.series {
			require.Equal(t, "10s", s.Get(labelWriteInterval), "write %d", i)
			require.Equal(t, "1", s.Get(labelSeed), "write %d", i)
		}
		samples += len(w.series)
	}
	require.Equal(t, float64(60), sumCounters(test.metrics.writesTotal))
	require.Equal(t, float64(samples), testutil.ToFloat64(test.metrics.queryResultChecksTotal))
	require.Zero(t, testutil.ToFloat64(test.metrics.queryResultChecksFailedTotal))
	require.Zero(t, sumCounters(test.metrics.writesFailedTotal))
}

func TestTest_WriteInterval(t *testing.T) {
	client := newFakeClient()
	test := newTestTestWithConfig(t, client, false, prometheus.NewPedanticRegistry(), func(cfg *Config) { cfg.WriteInterval = time.Minute })
	require.NoError(t, test.Run(context.Background(), testStart))
	runFor(t, test, testStart.Add(30*time.Second), 10*time.Minute)

	require.Len(t, client.writes, 10)
	for i, w := range client.writes {
		require.Equal(t, testStart.Add(time.Duration(i+1)*time.Minute).UnixMilli(), w.ts, "write %d", i)
		for _, s := range w.series {
			require.Equal(t, "1m", s.Get(labelWriteInterval), "write %d", i)
		}
	}
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
				requireOTLPCanWrite(t, s)
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
	for _, f := range []Family{FamilyFloat, FamilyNativeHistogram} {
		require.Positive(t, testutil.ToFloat64(test.metrics.samplesTotal.WithLabelValues(labelFamily, string(f))), "family %s", f)
	}
}

// requireOTLPCanWrite checks that OTLP can write the series: OTLP writes float counters and
// gauges, and native histogram int counters with exponential buckets.
func requireOTLPCanWrite(t *testing.T, s labels.Labels) {
	if s.Get(labelFamily) == string(FamilyFloat) {
		require.Contains(t, []string{"counter", "gauge"}, s.Get(labelType), "series %s", s)
		return
	}
	require.Equal(t, "int", s.Get(labelSampleType), "series %s", s)
	require.Equal(t, "counter", s.Get(labelKind), "series %s", s)
	require.NotEqual(t, "-53", s.Get(labelSchema), "series %s", s)
}

func TestTest_SeriesAreLongLived(t *testing.T) {
	client := newFakeClient()
	test := newTestTest(t, client, false, prometheus.NewPedanticRegistry())
	runFor(t, test, testStart, time.Hour)

	require.Len(t, client.series, 41, "every series has been picked")
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

	require.Len(t, client.series, 41, "every series has been created, most of them after earlier queries")
	require.Zero(t, testutil.ToFloat64(test.metrics.queryResultChecksFailedTotal))
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
		name := s.labels.Get(model.MetricNameLabel)
		require.True(t, strings.HasPrefix(name, metricNamePrefix), "OTLP escapes the metric name %s", name)
		require.Equal(t, utf8LabelValue, s.labels.Get(escapedUTF8LabelName), "OTLP escapes the label name")
		for _, sample := range s.samples {
			require.Equal(t, continuoustest.WriteProtocol("otlp-http"), sample.protocol, "only OTLP accepts UTF-8 names")
		}
	}
	require.Equal(t, 8, utf8Series, "the float counter and gauge, and the 6 native histogram int counters with exponential buckets")
}

func TestTest_UTF8NamesWithTheUTF8Scheme(t *testing.T) {
	client := newFakeClient()
	client.nameValidationScheme = model.UTF8Validation
	test := newTestTestWithConfig(t, client, true, prometheus.NewPedanticRegistry(), func(cfg *Config) {
		cfg.NameValidationScheme = model.UTF8Validation
	})
	require.NoError(t, test.Run(context.Background(), testStart))
	require.Len(t, client.series, 66)

	protocols := map[continuoustest.WriteProtocol]bool{}
	for _, s := range client.series {
		if s.labels.Get(labelCharset) != "utf8" {
			continue
		}
		require.True(t, strings.HasPrefix(s.labels.Get(model.MetricNameLabel), utf8MetricNamePrefix), "series %s", s.labels)
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
	require.Positive(t, testutil.ToFloat64(test.metrics.queryResultChecksFailedTotal))
}

func TestTest_DetectsWrongFloats(t *testing.T) {
	client := newFakeClient()
	client.corruptFloat = func(v float64) float64 { return v + 0.25 }
	test := newTestTest(t, client, true, prometheus.NewPedanticRegistry())
	err := test.Run(context.Background(), testStart)
	require.ErrorContains(t, err, "expected float")
	require.NotContains(t, err.Error(), "expected histogram")
	require.Positive(t, testutil.ToFloat64(test.metrics.queryResultChecksFailedTotal))
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
	require.Positive(t, testutil.ToFloat64(test.metrics.queryResultChecksFailedTotal))
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

			// Writes of 1, 2, 5, 10, 1, 2, 5 and 7 series write the 33 series of v1 and of v2, and
			// writes of 1, 2, 5 and 8 series write the 16 series of OTLP.
			require.Len(t, client.writes, 20)
			samples := map[continuoustest.WriteProtocol]int{}
			for i, w := range client.writes {
				require.Equal(t, now.Truncate(time.Second).Add(time.Duration(i-19)*time.Second).UnixMilli(), w.ts, "write %d", i)
				samples[w.protocol] += len(w.series)
			}
			require.Equal(t, map[continuoustest.WriteProtocol]int{"prometheus": 33, "prometheus2": 33, "otlp-http": 16}, samples)
			require.Len(t, client.series, 41)
		})
	}
}

// Every combination of a unit and a help text is written with every protocol, and read back as
// Mimir stores it.
func TestTest_SmokeTestWritesEveryMetadata(t *testing.T) {
	const definition = `
protocols: [v1, v2, otlp]
write_sizes: [10]
charsets: [legacy, utf8]
metadata: {units: [none, seconds, bytes], helps: [none, ascii, utf8]}
floats: {types: [counter, gauge]}
native_histograms: {sample_types: [int], kinds: [counter], schemas: [0], layouts: [gaps]}
`
	client := newFakeClient()
	test := newTestTestWithConfig(t, client, true, prometheus.NewPedanticRegistry(), func(cfg *Config) {
		cfg.DefinitionFile = writeDefinition(t, definition)
	})
	require.NoError(t, test.Run(context.Background(), testStart))

	// v1 and v2 write the 27 series with legacy names, and OTLP the 54 series of both charsets.
	require.Equal(t, float64(27+27+54), testutil.ToFloat64(test.metrics.queryResultChecksTotal))
	require.Zero(t, testutil.ToFloat64(test.metrics.queryResultChecksFailedTotal))
	units, helps := map[string]bool{}, map[string]bool{}
	for _, metadata := range client.metadata {
		for _, m := range metadata {
			units[m.Unit] = true
			helps[m.Help] = true
		}
	}
	require.Equal(t, map[string]bool{"": true, "seconds": true, "bytes": true}, units)
	require.Len(t, helps, 1+3*3*2, "an empty help, and for each of the 3 metric types and 3 protocols, an ASCII and a UTF-8 help")
}

// Mimir may not store metadata of an unknown type without help or unit, so it isn't expected back.
func TestTest_ExpectsNoEmptyMetadata(t *testing.T) {
	const definition = `
protocols: [v1, v2]
write_sizes: [1]
charsets: [legacy]
metadata: {units: [none], helps: [none]}
floats: {types: [unknown]}
`
	client := newFakeClient()
	test := newTestTestWithConfig(t, client, true, prometheus.NewPedanticRegistry(), func(cfg *Config) {
		cfg.DefinitionFile = writeDefinition(t, definition)
	})
	require.NoError(t, test.Run(context.Background(), testStart))
	require.Empty(t, client.metadata)
	require.Equal(t, float64(2), testutil.ToFloat64(test.metrics.queryResultChecksTotal))
	require.Zero(t, testutil.ToFloat64(test.metrics.queryResultChecksFailedTotal))
}

func TestTest_InitErrors(t *testing.T) {
	path := writeDefinition(t, testDefinition)

	cfg := Config{DefinitionFile: path, WriteInterval: testWriteInterval, Seed: 1, RandomSource: RandomSourceSeed, NameValidationScheme: model.LegacyValidation}
	managerCfg := continuoustest.ManagerConfig{RunInterval: 30 * time.Second}

	noOTLP := newFakeClient()
	noOTLP.unsupportedProtocol = "otlp-http"
	test := NewTest(cfg, noOTLP, managerCfg, log.NewNopLogger(), nil)
	require.ErrorContains(t, test.Init(context.Background(), time.Now()), "creating the client of protocol otlp")

	for _, interval := range []time.Duration{0, 500 * time.Millisecond, 1500 * time.Millisecond} {
		invalidInterval := cfg
		invalidInterval.WriteInterval = interval
		test = NewTest(invalidInterval, newFakeClient(), managerCfg, log.NewNopLogger(), nil)
		require.ErrorContains(t, test.Init(context.Background(), time.Now()), fmt.Sprintf("write interval %s is not a positive number of seconds", interval))
	}

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
				requireOTLPCanWrite(t, s)
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
	cfg := Config{DefinitionFile: writeDefinition(t, testDefinition), WriteInterval: testWriteInterval, Seed: 1, RandomSource: RandomSourceSeed, NameValidationScheme: model.LegacyValidation}
	modify(&cfg)
	test := NewTest(cfg, client, continuoustest.ManagerConfig{SmokeTest: smokeTest, RunInterval: 30 * time.Second}, log.NewNopLogger(), reg)
	require.NoError(t, test.Init(context.Background(), time.Now()))
	return test
}

func writeDefinition(t *testing.T, definition string) string {
	path := filepath.Join(t.TempDir(), "definition.yaml")
	require.NoError(t, os.WriteFile(path, []byte(definition), 0o600))
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
	// corruptFloat, if set, modifies every float value returned by queries.
	corruptFloat func(float64) float64
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
	// histogram is nil for float samples.
	histogram *prompb.Histogram
	value     float64
	protocol  continuoustest.WriteProtocol
}

func (s fakeSample) String() string {
	if s.histogram != nil {
		return fmt.Sprintf("%s %s", s.protocol, s.histogram)
	}
	return fmt.Sprintf("%s %v", s.protocol, s.value)
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
		samples := map[int64]fakeSample{}
		for _, sample := range ts.Samples {
			samples[sample.Timestamp] = fakeSample{value: sample.Value, protocol: protocol}
		}
		for _, h := range ts.Histograms {
			samples[h.Timestamp] = fakeSample{histogram: &h, protocol: protocol}
		}
		for timestamp, sample := range samples {
			w.ts = timestamp
			if timestamp < c.oldestStored.UnixMilli() {
				continue
			}
			// Like Mimir, a sample with the timestamp of an existing one is rejected unless they are
			// identical. Different protocols can store the same histogram differently, for example
			// with more empty buckets.
			if existing, ok := s.samples[timestamp]; ok && existing.String() != sample.String() {
				return 400, fmt.Errorf("duplicate sample for timestamp %d", timestamp)
			}
			s.samples[timestamp] = sample
		}
	}
	for _, m := range metadata {
		if c.dropMetadata {
			break
		}
		if m.Type == prompb.MetricMetadata_UNKNOWN && m.Help == "" && m.Unit == "" {
			// Like Mimir with ingest storage, which can't tell this metadata from no metadata.
			continue
		}
		name := m.MetricFamilyName
		if escape {
			name = escapeMetricName(name)
		}
		md := v1.Metadata{Type: v1.MetricType(strings.ToLower(m.Type.String())), Help: m.Help, Unit: m.Unit}
		if protocol == "otlp-http" {
			// Like Mimir, the OTLP endpoint translates OTLP units, such as s to seconds.
			unitNamer := otlptranslator.UnitNamer{}
			md.Unit = unitNamer.Build(m.Unit)
		}
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
		metric := model.Metric{}
		s.labels.Range(func(l labels.Label) { metric[model.LabelName(l.Name)] = model.LabelValue(l.Value) })
		if c.corruptMetric != nil {
			c.corruptMetric(metric)
		}
		out = append(out, c.querySample(metric, ts, sample))
	}
	return out, nil
}

func (c *fakeClient) querySample(metric model.Metric, ts time.Time, sample fakeSample) *model.Sample {
	out := &model.Sample{Metric: metric, Timestamp: model.TimeFromUnixNano(ts.UnixNano())}
	if sample.histogram == nil {
		out.Value = model.SampleValue(sample.value)
		if c.corruptFloat != nil {
			out.Value = model.SampleValue(c.corruptFloat(sample.value))
		}
		return out
	}
	h := *sample.histogram
	if c.corrupt != nil {
		c.corrupt(&h)
	}
	var fh = h.ToFloatHistogram()
	if !h.IsFloatHistogram() {
		fh = h.ToIntHistogram().ToFloat(nil)
	}
	out.Histogram = mimirpb.FromFloatHistogramToPromHistogram(fh)
	return out
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
