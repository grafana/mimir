// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
	util_test "github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/validation"
)

// swappableTenantLimits lets a test change a tenant's limits while the ingester reads them.
type swappableTenantLimits struct {
	mu     sync.Mutex
	limits map[string]*validation.Limits
}

func (s *swappableTenantLimits) ByUserID(userID string) *validation.Limits {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.limits[userID]
}

func (s *swappableTenantLimits) AllByUserID() map[string]*validation.Limits {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.limits
}

func (s *swappableTenantLimits) set(userID string, l *validation.Limits) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.limits[userID] = l
}

func TestIngester_DelayedSeriesReplayOnPromotion(t *testing.T) {
	const userID = "user-1"

	tenantLimitsFor := func(retiredAt time.Time) *validation.Limits {
		l := defaultLimitsTestConfig()
		l.OutOfOrderTimeWindow = model.Duration(time.Hour)
		l.DelayedSeries = validation.DelayedSeriesConfig{{Match: `{__name__="delayed_metric"}`, RetiredAt: retiredAt}}
		require.NoError(t, l.DelayedSeries.Validate())
		return &l
	}
	tenantLimits := &swappableTenantLimits{limits: map[string]*validation.Limits{userID: tenantLimitsFor(time.Time{})}}
	overrides := validation.NewOverrides(defaultLimitsTestConfig(), tenantLimits)

	reg := prometheus.NewPedanticRegistry()
	cfg := defaultIngesterTestConfig(t)
	cfg.DelayedSeriesReplayWindow = time.Hour
	ing, kafkaCluster, _ := createTestIngesterWithIngestStorage(t, &cfg, overrides, nil, reg, util_test.NewTestingLogger(t))
	require.NoError(t, services.StartAndAwaitRunning(t.Context(), ing))
	t.Cleanup(func() {
		// t.Context() is already canceled when cleanups run.
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), ing))
	})
	require.NotNil(t, ing.delayedSeriesReplayer)

	producer, err := kgo.NewClient(kgo.SeedBrokers(kafkaCluster.ListenAddrs()...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	require.NoError(t, err)
	t.Cleanup(producer.Close)

	now := time.Now()
	sampleTimes := []time.Time{now.Add(-10 * time.Minute), now.Add(-5 * time.Minute)}
	for _, ts := range sampleTimes {
		req := &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{
			{TimeSeries: &mimirpb.TimeSeries{
				Labels:  []mimirpb.LabelAdapter{{Name: "__name__", Value: "delayed_metric"}},
				Samples: []mimirpb.Sample{{TimestampMs: ts.UnixMilli(), Value: 1}},
			}},
			{TimeSeries: &mimirpb.TimeSeries{
				Labels:  []mimirpb.LabelAdapter{{Name: "__name__", Value: "other_metric"}},
				Samples: []mimirpb.Sample{{TimestampMs: ts.UnixMilli(), Value: 1}},
			}},
		}}
		value, err := req.Marshal()
		require.NoError(t, err)
		require.NoError(t, producer.ProduceSync(t.Context(), &kgo.Record{
			Topic:     cfg.IngestStorageConfig.KafkaConfig.Topic,
			Partition: ing.ingestPartitionID,
			Key:       []byte(userID),
			Value:     value,
			Headers:   []kgo.RecordHeader{ingest.RecordVersionHeader(1)},
		}).FirstErr())
	}

	// While the rule applies, only other_metric reaches the head.
	test.Poll(t, 5*time.Second, 2, func() interface{} {
		return int(testutil.ToFloat64(ing.delayedSeries.samples.WithLabelValues(userID)))
	})
	require.Equal(t, 0, countSamples(t, ing, userID, "delayed_metric"))
	require.Equal(t, 2, countSamples(t, ing, userID, "other_metric"))

	// Promote: retire the rule. The ingester's replay worker runs the replay it triggers.
	tenantLimits.set(userID, tenantLimitsFor(now))
	ing.delayedSeriesReplayer.discover([]string{userID}, now)
	succeeded := func() interface{} {
		return int(testutil.ToFloat64(ing.delayedSeriesReplayer.replays.WithLabelValues("succeeded")))
	}
	test.Poll(t, 10*time.Second, 1, succeeded)

	require.Equal(t, 2, countSamples(t, ing, userID, "delayed_metric"), "the replay appends the samples skipped while the rule applied")
	require.Equal(t, 2, countSamples(t, ing, userID, "other_metric"), "the replay doesn't duplicate series that were never delayed")
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_ingester_delayed_series_replay_samples_total Total number of samples of promoted delayed series appended by replays.
		# TYPE cortex_ingester_delayed_series_replay_samples_total counter
		cortex_ingester_delayed_series_replay_samples_total 2
	`), "cortex_ingester_delayed_series_replay_samples_total"))

	// A rule is replayed once per retirement.
	ing.delayedSeriesReplayer.discover([]string{userID}, now)
	require.Empty(t, ing.delayedSeriesReplayer.jobs)
	require.Equal(t, 1, succeeded())
}

func countSamples(t *testing.T, ing *Ingester, userID, metric string) int {
	db := ing.getTSDB(userID)
	if db == nil {
		return 0
	}
	q, err := db.Querier(0, time.Now().Add(time.Hour).UnixMilli())
	require.NoError(t, err)
	defer func() { require.NoError(t, q.Close()) }()

	set := q.Select(t.Context(), true, nil, labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, metric))
	count := 0
	for set.Next() {
		it := set.At().Iterator(nil)
		for it.Next() != 0 {
			count++
		}
	}
	require.NoError(t, set.Err())
	return count
}
