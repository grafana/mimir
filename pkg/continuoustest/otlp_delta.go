// SPDX-License-Identifier: AGPL-3.0-only

package continuoustest

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

const (
	otlpDeltaCounter   = "mimir_continuous_test_otlp_delta_counter_total"
	otlpDeltaHistogram = "mimir_continuous_test_otlp_delta_histogram"
	otlpDeltaStep      = 20 * time.Second
)

type OTLPDeltaTestConfig struct {
	Enabled          bool
	WriteEndpoint    string
	ReadinessTimeout time.Duration
	HistoricalAge    time.Duration
	MaxQueryAge      time.Duration
}

func (cfg *OTLPDeltaTestConfig) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.Enabled, "tests.otlp-delta-test.enabled", false, "Enable native OTLP delta counter and exponential histogram write/read checks. Requires a tenant configured for native delta ingestion.")
	f.StringVar(&cfg.WriteEndpoint, "tests.otlp-delta-test.write-endpoint", "", "Complete OTLP/HTTP metrics URL, including /v1/metrics for Alloy or /otlp/v1/metrics for Mimir. Uses the test client's tenant and authentication settings.")
	f.DurationVar(&cfg.ReadinessTimeout, "tests.otlp-delta-test.readiness-timeout", 30*time.Second, "Maximum time to wait for newly written delta samples to be queryable.")
	f.DurationVar(&cfg.HistoricalAge, "tests.otlp-delta-test.historical-age", 2*time.Hour, "Minimum sample age for historical checks. Checks begin after this process has accumulated enough history; age alone does not guarantee a store-gateway read.")
	f.DurationVar(&cfg.MaxQueryAge, "tests.otlp-delta-test.max-query-age", 24*time.Hour, "Maximum age of delta test batches retained in memory for historical queries. History is reset on process restart.")
}

func (cfg OTLPDeltaTestConfig) Validate() error {
	if !cfg.Enabled {
		return nil
	}

	u, err := url.Parse(cfg.WriteEndpoint)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.Fragment != "" || u.User != nil {
		return errors.New("tests.otlp-delta-test.write-endpoint must be an absolute HTTP(S) URL without credentials or a fragment")
	}

	if cfg.ReadinessTimeout <= 0 {
		return errors.New("tests.otlp-delta-test.readiness-timeout must be positive")
	}

	if cfg.HistoricalAge <= 0 || cfg.MaxQueryAge <= cfg.HistoricalAge {
		return errors.New("tests.otlp-delta-test.max-query-age must be greater than historical-age, and historical-age must be positive")
	}

	return nil
}

type OTLPDeltaClient interface {
	WriteOTLPMetrics(context.Context, string, pmetric.Metrics) (int, error)
	QueryRange(context.Context, string, time.Time, time.Time, time.Duration, ...RequestOption) (model.Matrix, error)
}

type OTLPDeltaTest struct {
	cfg         OTLPDeltaTestConfig
	client      OTLPDeltaClient
	logger      log.Logger
	metrics     *TestMetrics
	runID       string
	history     []time.Time
	lastWritten time.Time
}

func NewOTLPDeltaTest(cfg OTLPDeltaTestConfig, client OTLPDeltaClient, logger log.Logger, reg prometheus.Registerer) *OTLPDeltaTest {
	return &OTLPDeltaTest{
		cfg:     cfg,
		client:  client,
		logger:  log.With(logger, "test", "otlp-delta"),
		metrics: NewTestMetrics("otlp-delta", reg),
	}
}

func (t *OTLPDeltaTest) Name() string {
	return "otlp-delta"
}

func (t *OTLPDeltaTest) Init(_ context.Context, _ time.Time) error {
	if err := t.cfg.Validate(); err != nil {
		return err
	}

	if !t.cfg.Enabled {
		return nil
	}
	t.runID = uuid.NewString()
	t.metrics.InitializeCountersToZero("otlp_delta")
	t.metrics.InitializeCountersToZero("recent")
	t.metrics.InitializeCountersToZero("historical")
	return nil
}

func (t *OTLPDeltaTest) Run(ctx context.Context, now time.Time) error {
	if !t.cfg.Enabled {
		return nil
	}

	// Keep batches disjoint when the runner invokes the test more often than once per minute.
	end := now.UTC().Truncate(otlpDeltaStep).Add(-otlpDeltaStep)
	if !t.lastWritten.IsZero() && end.Sub(t.lastWritten) < 3*otlpDeltaStep {
		return nil
	}

	start := time.Now()
	status, err := t.client.WriteOTLPMetrics(ctx, t.cfg.WriteEndpoint, otlpDeltaMetrics(t.runID, end))
	t.metrics.writesTotal.WithLabelValues("otlp_delta").Inc()
	t.metrics.writesLatency.WithLabelValues("otlp_delta").Observe(time.Since(start).Seconds())
	if err != nil {
		t.metrics.writesFailedTotal.WithLabelValues(strconv.Itoa(status), "otlp_delta").Inc()
		level.Error(t.logger).Log("msg", "Failed to write OTLP delta metrics", "err", err)
		return err
	}

	t.lastWritten = end
	t.history = append(t.history, end)
	for len(t.history) > 0 && t.history[0].Before(now.Add(-t.cfg.MaxQueryAge)) {
		t.history = t.history[1:]
	}

	err = t.check(ctx, end, "recent", true)

	// Recheck the oldest retained batch once it reaches the configured historical age.
	for _, historicalEnd := range t.history {
		if !historicalEnd.After(now.Add(-t.cfg.HistoricalAge)) {
			err = errors.Join(err, t.check(ctx, historicalEnd, "historical", false))
			break
		}
	}

	return err
}

func (t *OTLPDeltaTest) check(ctx context.Context, end time.Time, kind string, retry bool) error {
	t.metrics.queryResultChecksTotal.WithLabelValues(kind).Inc()

	var err error

	// Newly written samples may take time to become visible through the read path.
	if retry {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, t.cfg.ReadinessTimeout)
		defer cancel()
	}

	for {
		err = t.checkBatch(ctx, end, kind)
		if err == nil {
			return nil
		}

		if !retry || ctx.Err() != nil {
			break
		}

		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}

	t.metrics.queryResultChecksFailedTotal.WithLabelValues(kind).Inc()
	level.Error(t.logger).Log("msg", "OTLP delta query check failed", "kind", kind, "end", end, "err", err)

	return fmt.Errorf("%s OTLP delta check: %w", kind, err)
}

func (t *OTLPDeltaTest) checkBatch(ctx context.Context, end time.Time, kind string) error {
	start := end.Add(-2 * otlpDeltaStep)

	for _, name := range []string{otlpDeltaCounter, otlpDeltaHistogram} {
		selector := fmt.Sprintf("%s{test_run_id=%q}", name, t.runID)

		for _, timestamps := range []bool{false, true} {
			query := selector

			// Check source timestamps so PromQL lookback cannot hide a missing sample.
			if timestamps {
				query = "timestamp(" + selector + ")"
			}

			begin := time.Now()
			result, err := t.client.QueryRange(ctx, query, start, end, otlpDeltaStep, WithResultsCacheEnabled(false))
			t.metrics.queriesTotal.WithLabelValues(kind).Inc()
			t.metrics.queriesLatency.WithLabelValues(kind, "false").Observe(time.Since(begin).Seconds())

			if err != nil {
				t.metrics.queriesFailedTotal.WithLabelValues(kind).Inc()
				return err
			}

			if err := checkOTLPDeltaResult(result, start, name == otlpDeltaHistogram && !timestamps, timestamps); err != nil {
				return fmt.Errorf("%s: %w", query, err)
			}
		}
	}

	return nil
}

// The value changes at every sample so cumulative conversion cannot pass the raw-value checks.
func otlpDeltaValue(ts time.Time) int64 {
	return (ts.Unix()/int64(otlpDeltaStep/time.Second))%5 + 1
}

func otlpDeltaMetrics(runID string, end time.Time) pmetric.Metrics {
	md := pmetric.NewMetrics()
	metrics := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics()

	counter := metrics.AppendEmpty()
	counter.SetName(otlpDeltaCounter)
	sum := counter.SetEmptySum()
	sum.SetIsMonotonic(true)
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)

	histogram := metrics.AppendEmpty()
	histogram.SetName(otlpDeltaHistogram)
	hist := histogram.SetEmptyExponentialHistogram()
	hist.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)

	for i := 0; i < 3; i++ {
		ts := end.Add(time.Duration(i-2) * otlpDeltaStep)
		n := otlpDeltaValue(ts)

		point := sum.DataPoints().AppendEmpty()
		point.Attributes().PutStr("test_run_id", runID)
		point.SetStartTimestamp(pcommon.NewTimestampFromTime(ts.Add(-otlpDeltaStep)))
		point.SetTimestamp(pcommon.NewTimestampFromTime(ts))
		point.SetIntValue(n)

		// Each interval contains one zero observation and n observations with value 2.
		hp := hist.DataPoints().AppendEmpty()
		hp.Attributes().PutStr("test_run_id", runID)
		hp.SetStartTimestamp(point.StartTimestamp())
		hp.SetTimestamp(point.Timestamp())
		hp.SetScale(0)
		hp.SetZeroThreshold(0.001)
		hp.SetZeroCount(1)
		hp.SetCount(uint64(n + 1))
		hp.SetSum(float64(2 * n))
		hp.Positive().SetOffset(0)
		hp.Positive().BucketCounts().FromRaw([]uint64{uint64(n)})
	}

	return md
}

func checkOTLPDeltaResult(result model.Matrix, start time.Time, histogram, timestamps bool) error {
	if len(result) != 1 {
		return fmt.Errorf("expected one series, got %d", len(result))
	}

	series := result[0]
	if histogram {
		if len(series.Histograms) != 3 || len(series.Values) != 0 {
			return fmt.Errorf("expected three histogram samples, got %d histograms and %d floats", len(series.Histograms), len(series.Values))
		}
	} else if len(series.Values) != 3 || len(series.Histograms) != 0 {
		return fmt.Errorf("expected three float samples, got %d floats and %d histograms", len(series.Values), len(series.Histograms))
	}

	for i := 0; i < 3; i++ {
		ts := start.Add(time.Duration(i) * otlpDeltaStep)
		n := otlpDeltaValue(ts)
		wantTS := model.TimeFromUnixNano(ts.UnixNano())

		if histogram {
			sample := series.Histograms[i]

			if sample.Timestamp != wantTS || sample.Histogram == nil ||
				float64(sample.Histogram.Count) != float64(n+1) ||
				float64(sample.Histogram.Sum) != float64(2*n) {
				return fmt.Errorf("unexpected histogram at %s: got %v, expected count=%d sum=%d", ts, sample, n+1, 2*n)
			}
		} else {
			want := float64(n)

			// Check source timestamps so PromQL lookback cannot hide a missing sample.
			if timestamps {
				want = float64(ts.Unix())
			}

			sample := series.Values[i]
			if sample.Timestamp != wantTS || float64(sample.Value) != want {
				return fmt.Errorf("unexpected sample at %s: got %v, expected %g", ts, sample, want)
			}
		}
	}

	return nil
}
