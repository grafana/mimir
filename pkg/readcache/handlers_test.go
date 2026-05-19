// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"context"
	"flag"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/util/testkafka"
	"github.com/grafana/mimir/pkg/util/validation"
)

// TestReadcache_SamplesIngestedTotal_PerPartition produces sample
// batches with distinct counts to two partitions and asserts that
// cortex_readcache_samples_ingested_total attributes them to the
// right partition label. This is the source-of-truth metric for
// "samples/sec ingested per partition" — use rate() over it in
// dashboards.
func TestReadcache_SamplesIngestedTotal_PerPartition(t *testing.T) {
	const (
		tenantID = "user-1"
		topic    = "test-topic"
	)

	_, addr := testkafka.CreateCluster(t, 4, topic)

	cfg := newTestConfigNoKafka(t)
	cfg.KafkaTopic = topic
	cfg.OwnedPartitions = "0,1"

	var kafkaCfg ingest.KafkaConfig
	kafkaCfg.RegisterFlags(flag.NewFlagSet("", flag.PanicOnError))
	kafkaCfg.Address = flagext.StringSliceCSV{addr}
	kafkaCfg.Topic = topic
	cfg.Kafka = kafkaCfg

	limits := validation.NewOverrides(validation.Limits{}, nil)
	reg := prometheus.NewPedanticRegistry()

	rc, err := New(cfg, limits, nil, log.NewNopLogger(), reg)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	require.NoError(t, services.StartAndAwaitRunning(ctx, rc))
	defer func() { _ = services.StopAndAwaitTerminated(ctx, rc) }()

	writer := makeKafkaWriter(t, kafkaCfg, reg)
	defer func() { _ = services.StopAndAwaitTerminated(ctx, writer) }()

	wreqCtx := user.InjectOrgID(ctx, tenantID)
	produce := func(partition int32, series string, samples int) {
		t.Helper()
		ts := []mimirpb.PreallocTimeseries{
			{TimeSeries: &mimirpb.TimeSeries{
				Labels: mimirpb.FromLabelsToLabelAdapters(
					labels.FromStrings("__name__", series, "src", "test")),
				Samples: make([]mimirpb.Sample, samples),
			}},
		}
		for i := 0; i < samples; i++ {
			ts[0].Samples[i] = mimirpb.Sample{TimestampMs: int64(i + 1), Value: float64(i)}
		}
		require.NoError(t, writer.WriteSync(wreqCtx, topic, partition, tenantID, &mimirpb.WriteRequest{Timeseries: ts}))
	}

	// Send distinct sample counts to each partition so a mis-
	// attribution would surface immediately rather than blending.
	produce(0, "p0_series", 3)
	produce(1, "p1_series", 7)

	require.Eventually(t, func() bool {
		return testutil.ToFloat64(rc.samplesIngestedTotal.WithLabelValues("0")) >= 3 &&
			testutil.ToFloat64(rc.samplesIngestedTotal.WithLabelValues("1")) >= 7
	}, 10*time.Second, 100*time.Millisecond, "samples should be counted per partition")

	assert.Equal(t, float64(3), testutil.ToFloat64(rc.samplesIngestedTotal.WithLabelValues("0")),
		"partition 0 should be attributed exactly 3 samples")
	assert.Equal(t, float64(7), testutil.ToFloat64(rc.samplesIngestedTotal.WithLabelValues("1")),
		"partition 1 should be attributed exactly 7 samples")
	assert.Equal(t, float64(0), testutil.ToFloat64(rc.samplesIngestedTotal.WithLabelValues("2")),
		"partition 2 was not owned and must have zero samples")
}
