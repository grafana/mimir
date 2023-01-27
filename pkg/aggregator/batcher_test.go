package aggregator

import (
	"context"
	"flag"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"
)

func defaultConfig() Config {
	f := flag.NewFlagSet("", flag.PanicOnError)
	cfg := Config{}
	cfg.RegisterFlags(f) // Set default config.
	return cfg
}

func TestStartStopBatcher(t *testing.T) {
	batcher := NewBatcher(defaultConfig(), nil, nil, log.NewNopLogger())

	require.NoError(t, batcher.starting(context.Background()))
	time.Sleep(100 * time.Millisecond)
	require.NoError(t, batcher.stopping(nil))
}

func TestBatchingAggregates(t *testing.T) {
	user1 := "user1"
	user2 := "user2"
	reg := prometheus.NewPedanticRegistry()

	type collectedPushType struct {
		ts   []mimirpb.PreallocTimeseries
		user string
	}
	var collectedPushes []collectedPushType
	push := func(ctx context.Context, req *mimirpb.WriteRequest) (*mimirpb.WriteResponse, error) {
		var collectedPush collectedPushType
		for _, ts := range req.Timeseries {
			collectedPush.ts = append(collectedPush.ts, mimirpb.PreallocTimeseries{})
			collectedPush.ts[len(collectedPush.ts)-1] = mimirpb.DeepCopyTimeseries(collectedPush.ts[len(collectedPush.ts)-1], ts, false)
		}
		var err error
		collectedPush.user, err = user.ExtractOrgID(ctx)
		require.NoError(t, err)

		collectedPushes = append(collectedPushes, collectedPush)
		return nil, nil
	}

	cfg := defaultConfig()
	cfg.FlushInterval = time.Hour // Effectively disable ticker for the sake of this test.
	batcher := NewBatcher(cfg, push, reg, log.NewNopLogger())
	require.NoError(t, batcher.starting(context.Background()))

	batcher.AddSample(user1, "aggregated_metrics{user=\""+user1+"\"}", mimirpb.Sample{Value: 1, TimestampMs: 1000})
	batcher.AddSample(user2, "aggregated_metrics{user=\""+user2+"\"}", mimirpb.Sample{Value: 1001, TimestampMs: 1123})
	batcher.AddSample(user1, "aggregated_metrics{user=\""+user1+"\"}", mimirpb.Sample{Value: 2, TimestampMs: 2000})
	batcher.AddSample(user2, "aggregated_metrics{user=\""+user2+"\"}", mimirpb.Sample{Value: 1002, TimestampMs: 2123})
	batcher.AddSample(user1, "aggregated_metrics{user=\""+user1+"\"}", mimirpb.Sample{Value: 3, TimestampMs: 3000})
	batcher.AddSample(user2, "aggregated_metrics{user=\""+user2+"\"}", mimirpb.Sample{Value: 1003, TimestampMs: 3123})
	batcher.AddSample(user1, "aggregated_metrics{user=\""+user1+"\"}", mimirpb.Sample{Value: 4, TimestampMs: 4000})
	batcher.AddSample(user2, "aggregated_metrics{user=\""+user2+"\"}", mimirpb.Sample{Value: 1004, TimestampMs: 4123})
	batcher.AddSample(user1, "aggregated_metrics{user=\""+user1+"\"}", mimirpb.Sample{Value: 5, TimestampMs: 5000})
	batcher.AddSample(user2, "aggregated_metrics{user=\""+user2+"\"}", mimirpb.Sample{Value: 1005, TimestampMs: 5123})

	// Wait for the batcher to finish ingesting all the submitted samples.
	require.Eventually(t, func() bool {
		err := testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_aggregation_batcher_ingested_aggregation_results_total The total number of aggregation results that have been ingested by this aggregation result batcher.
			# TYPE cortex_aggregation_batcher_ingested_aggregation_results_total counter
			cortex_aggregation_batcher_ingested_aggregation_results_total{user="`+user1+`"} 5
			cortex_aggregation_batcher_ingested_aggregation_results_total{user="`+user2+`"} 5
	`), "cortex_aggregation_batcher_ingested_aggregation_results_total")
		return err == nil
	}, time.Second, 10*time.Millisecond)

	batcher.flush()

	// We ingested samples of 2 different users into the batcher, it should create one batch per user.
	require.Len(t, collectedPushes, 2)
	require.Len(t, collectedPushes[0].ts, 5)
	require.Len(t, collectedPushes[1].ts, 5)

	// Check that a request has been pushed for each of the two users
	pushedUsers := map[string]int{} // user -> index in collectedPushes
	for pushIdx := range collectedPushes {
		pushedUsers[collectedPushes[pushIdx].user] = pushIdx
	}
	require.Contains(t, pushedUsers, user1)
	require.Contains(t, pushedUsers, user2)

	// Check that all the correct labels were received.
	var userIdx int
	for _, user := range []string{user1, user2} {
		userIdx = pushedUsers[user]
		for i := 0; i < 5; i++ {
			require.Equal(t, collectedPushes[userIdx].ts[i].TimeSeries.Labels, []mimirpb.LabelAdapter{{
				Name:  model.MetricNameLabel,
				Value: "aggregated_metrics",
			}, {
				Name:  "user",
				Value: user,
			}})
		}
	}

	// Check that user1 received the correct values.
	userIdx = pushedUsers[user1]
	for valueIdx, expectedValue := range []float64{1, 2, 3, 4, 5} {
		require.Equal(t, expectedValue, collectedPushes[userIdx].ts[valueIdx].Samples[0].Value)
	}

	// Check that user1 received the correct timestamps.
	for tsIdx, expectedTs := range []int64{1000, 2000, 3000, 4000, 5000} {
		require.Equal(t, expectedTs, collectedPushes[userIdx].ts[tsIdx].Samples[0].TimestampMs)
	}

	// Check that user2 received the correct values.
	userIdx = pushedUsers[user2]
	for valueIdx, expectedValue := range []float64{1001, 1002, 1003, 1004, 1005} {
		require.Equal(t, expectedValue, collectedPushes[userIdx].ts[valueIdx].Samples[0].Value)
	}

	// Check that user2 received the correct timestamps.
	for tsIdx, expectedTs := range []int64{1123, 2123, 3123, 4123, 5123} {
		require.Equal(t, expectedTs, collectedPushes[userIdx].ts[tsIdx].Samples[0].TimestampMs)
	}
}
