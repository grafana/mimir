// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"context"
	"flag"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/dskit/flagext"

	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/util/testkafka"
	"github.com/grafana/mimir/pkg/util/validation"
)

// newTestConfig returns a fully-defaulted Config rooted at t.TempDir.
// Use this in tests where you don't care about the specific values of
// any flag besides DataDir.
//
// If withKafka is true a fake Kafka cluster is created with
// numPartitions partitions and the kafka config is wired to it.
func newTestConfig(t *testing.T, withKafka bool, numPartitions int32) Config {
	t.Helper()

	const topic = "test-topic"

	var cfg Config
	cfg.RegisterFlags(flag.NewFlagSet("", flag.PanicOnError), log.NewNopLogger())
	cfg.DataDir = t.TempDir()
	cfg.InstanceID = "test"
	cfg.KafkaTopic = topic

	var blocksCfg tsdb.BlocksStorageConfig
	blocksCfg.RegisterFlags(flag.NewFlagSet("", flag.PanicOnError))
	blocksCfg.TSDB.Dir = t.TempDir()
	cfg.BlocksStorage = blocksCfg

	if withKafka {
		_, addr := testkafka.CreateCluster(t, numPartitions, topic)
		var kafkaCfg ingest.KafkaConfig
		kafkaCfg.RegisterFlags(flag.NewFlagSet("", flag.PanicOnError))
		kafkaCfg.Address = flagext.StringSliceCSV{addr}
		kafkaCfg.Topic = topic
		cfg.Kafka = kafkaCfg
	}

	return cfg
}

func TestConfig_Validate(t *testing.T) {
	t.Run("defaults are valid", func(t *testing.T) {
		cfg := newTestConfig(t, false, 0)
		require.NoError(t, cfg.Validate())
	})

	t.Run("missing instance id is rejected", func(t *testing.T) {
		cfg := newTestConfig(t, false, 0)
		cfg.InstanceID = ""
		assert.Error(t, cfg.Validate())
	})

	t.Run("missing data-dir is rejected", func(t *testing.T) {
		cfg := newTestConfig(t, false, 0)
		cfg.DataDir = ""
		assert.Error(t, cfg.Validate())
	})

	t.Run("missing kafka-topic is rejected", func(t *testing.T) {
		cfg := newTestConfig(t, false, 0)
		cfg.KafkaTopic = ""
		assert.Error(t, cfg.Validate())
	})
}

func TestConfig_ParseOwnedPartitions(t *testing.T) {
	tcs := map[string]struct {
		in   string
		want []int32
		err  bool
	}{
		"empty":             {in: "", want: nil},
		"single":            {in: "5", want: []int32{5}},
		"multiple":          {in: "0,1,2", want: []int32{0, 1, 2}},
		"deduplicates":      {in: "1,1,2,1", want: []int32{1, 2}},
		"with whitespace":   {in: " 1 , 2 , 3 ", want: []int32{1, 2, 3}},
		"trailing comma ok": {in: "1,2,3,", want: []int32{1, 2, 3}},
		"negative rejected": {in: "-1", err: true},
		"non-numeric":       {in: "abc", err: true},
	}
	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			cfg := Config{OwnedPartitions: tc.in}
			got, err := cfg.ParseOwnedPartitions()
			if tc.err {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestReadcache_Lifecycle(t *testing.T) {
	cfg := newTestConfig(t, true, 64)
	cfg.OwnedPartitions = "0,7,42"

	limits := validation.NewOverrides(validation.Limits{}, nil)

	r, err := New(cfg, limits, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(t, services.StartAndAwaitRunning(ctx, r))

	owned := r.OwnedPartitions()
	assert.ElementsMatch(t, []int32{0, 7, 42}, owned)

	require.NoError(t, services.StopAndAwaitTerminated(ctx, r))
}

func TestReadcache_GetOrOpenTSDB(t *testing.T) {
	cfg := newTestConfig(t, true, 2)
	cfg.OwnedPartitions = "0,1"

	limits := validation.NewOverrides(validation.Limits{}, nil)

	r, err := New(cfg, limits, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(t, services.StartAndAwaitRunning(ctx, r))
	defer func() { _ = services.StopAndAwaitTerminated(ctx, r) }()

	// Unknown partition returns nil.
	db, err := r.getOrOpenTSDB("user-1", 999)
	require.NoError(t, err)
	assert.Nil(t, db)

	// Owned partition opens a fresh TSDB.
	db, err = r.getOrOpenTSDB("user-1", 0)
	require.NoError(t, err)
	require.NotNil(t, db)

	// Second call returns the same instance.
	db2, err := r.getOrOpenTSDB("user-1", 0)
	require.NoError(t, err)
	assert.Same(t, db, db2)

	// Different partitions are different TSDBs.
	dbOther, err := r.getOrOpenTSDB("user-1", 1)
	require.NoError(t, err)
	require.NotNil(t, dbOther)
	assert.NotSame(t, db, dbOther)
}
