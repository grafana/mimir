package continuoustest

import (
	"context"
	"flag"
	"fmt"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/multierror"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"time"

	"github.com/go-kit/log"
)

type RecordingRuleReadSeriesTestConfig struct {
	MaxQueryAge time.Duration
}

func (cfg *RecordingRuleReadSeriesTestConfig) RegisterFlags(f *flag.FlagSet) {
	f.DurationVar(&cfg.MaxQueryAge, "tests.recording-rule-read-series-test.max-query-age", 7*24*time.Hour, "Limit how far into the past metrics can be queried.")

}

type RecordingRuleReadSeriesTest struct {
	name    string
	cfg     RecordingRuleReadSeriesTestConfig
	client  MimirClient
	logger  log.Logger
	metrics *TestMetrics

	recordedFloatMetric MetricHistory
}

const recordingRuleMetricName = "continuous_test:time"

func NewRecordingRuleReadSeriesTest(cfg RecordingRuleReadSeriesTestConfig, client MimirClient, logger log.Logger, reg prometheus.Registerer) *RecordingRuleReadSeriesTest {
	const name = "recording-rule-read-series"

	return &RecordingRuleReadSeriesTest{
		name:    name,
		cfg:     cfg,
		client:  client,
		logger:  log.With(logger, "test", name),
		metrics: NewTestMetrics(name, reg),
	}
}

// Name implements Test.
func (t *RecordingRuleReadSeriesTest) Name() string {
	return t.name
}

// Init implements Test.
func (t *RecordingRuleReadSeriesTest) Init(_ context.Context, _ time.Time) error {
	t.metrics.InitializeCountersToZero(floatTypeLabel)
	return nil
}

// Run implements Test.
func (t *RecordingRuleReadSeriesTest) Run(ctx context.Context, now time.Time) error {
	// Collect all errors on this test run
	errs := new(multierror.MultiError)

	t.RunInner(ctx, now, errs)

	return errs.Err()
}

func (t *RecordingRuleReadSeriesTest) RunInner(
	ctx context.Context,
	now time.Time,
	errs *multierror.MultiError,
	// records *MetricHistory,
) {

	//queryRanges, queryInstants, err := t.getQueryTimeRanges(now, records)
	//if err != nil {
	//	errs.Add(err)
	//}

	queryMetric := queryRecordingRule(recordingRuleMetricName, "5m")
	matrix, err := t.client.QueryInstantRangeVector(ctx, queryMetric, now, WithResultsCacheEnabled(false))
	if err != nil {
		level.Warn(t.logger).Log("msg", "Failed to execute instant query", "err", err)
		errs.Add(errors.Wrap(err, "failed to execute instant query"))
		return
	}

	if len(matrix) != 1 {
		errs.Add(fmt.Errorf("expected 1 series in the result but got %d", len(matrix)))
		return
	}

	samples := matrix[0].Values
	if len(samples) == 0 {
		errs.Add(errors.New("expected at least one sample in the result"))
		return
	}

	latestSample := samples[len(samples)-1]
	ts := time.UnixMilli(int64(latestSample.Timestamp)).UTC()

	updatedNow := time.Now().UTC()
	fmt.Println("updatedNow", updatedNow)
	fmt.Println("latest sample", ts)
	fmt.Println("difference", updatedNow.Sub(ts))
	if now.Sub(ts) > 1*time.Minute {
		errs.Add(fmt.Errorf("latest sample is too old: %s", ts))
		return
	}
}
