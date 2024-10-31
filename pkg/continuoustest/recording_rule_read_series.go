package continuoustest

import (
	"flag"
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
