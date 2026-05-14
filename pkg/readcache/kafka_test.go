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
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/util/testkafka"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestReadcache_KafkaConsumption(t *testing.T) {
	const (
		tenantID = "user-1"
		topic    = "test-topic"
	)

	cluster, addr := testkafka.CreateCluster(t, 4, topic)
	_ = cluster

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

	rc, err := New(cfg, limits, log.NewNopLogger(), reg)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	require.NoError(t, services.StartAndAwaitRunning(ctx, rc))
	defer func() { _ = services.StopAndAwaitTerminated(ctx, rc) }()

	// Produce a WriteRequest to partition 0 via the kafka writer.
	writer := makeKafkaWriter(t, kafkaCfg, reg)
	defer func() { _ = services.StopAndAwaitTerminated(ctx, writer) }()

	// Build a write request for tenantID with a single series.
	req := &mimirpb.WriteRequest{
		Timeseries: []mimirpb.PreallocTimeseries{
			{
				TimeSeries: &mimirpb.TimeSeries{
					Labels: mimirpb.FromLabelsToLabelAdapters(
						labels.FromStrings("__name__", "http_requests_total", "job", "test")),
					Samples: []mimirpb.Sample{{TimestampMs: 1, Value: 42.0}},
				},
			},
		},
	}

	wreqCtx := user.InjectOrgID(ctx, tenantID)
	require.NoError(t, writer.WriteSync(wreqCtx, topic, 0, tenantID, req))

	// Wait for the head to receive the series.
	require.Eventually(t, func() bool {
		db, err := rc.getOrOpenTSDB(tenantID, 0)
		if err != nil || db == nil {
			return false
		}
		return db.Head().NumSeries() > 0
	}, 10*time.Second, 100*time.Millisecond, "partition 0 head should contain pushed series")

	// Partition 1 was owned but received no data — its TSDB should
	// still be openable lazily but contain no series.
	db, err := rc.getOrOpenTSDB(tenantID, 1)
	require.NoError(t, err)
	if db != nil {
		assert.Equal(t, uint64(0), db.Head().NumSeries())
	}

	// Partition 2 is not owned: getOrOpenTSDB returns nil.
	db, err = rc.getOrOpenTSDB(tenantID, 2)
	require.NoError(t, err)
	assert.Nil(t, db)
}

// makeKafkaWriter is a small helper that constructs an ingest writer
// for tests. It hides the boilerplate of NewWriter (which expects a
// flag-derived config and a registerer).
func makeKafkaWriter(t *testing.T, cfg ingest.KafkaConfig, reg prometheus.Registerer) *ingest.Writer {
	t.Helper()
	w := ingest.NewWriter(cfg, log.NewNopLogger(), reg)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), w))
	return w
}

// newTestConfigNoKafka is like newTestConfig(t, false, 0) but inlined
// to make the dependency between the test and the surrounding fixture
// explicit.
func newTestConfigNoKafka(t *testing.T) Config {
	t.Helper()
	return newTestConfig(t, false, 0)
}
