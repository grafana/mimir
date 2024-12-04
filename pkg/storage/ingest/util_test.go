// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestIngesterPartitionID(t *testing.T) {
	t.Run("with zones", func(t *testing.T) {
		actual, err := IngesterPartitionID("ingester-zone-a-0")
		require.NoError(t, err)
		assert.EqualValues(t, 0, actual)

		actual, err = IngesterPartitionID("ingester-zone-b-0")
		require.NoError(t, err)
		assert.EqualValues(t, 0, actual)

		actual, err = IngesterPartitionID("ingester-zone-a-1")
		require.NoError(t, err)
		assert.EqualValues(t, 1, actual)

		actual, err = IngesterPartitionID("ingester-zone-b-1")
		require.NoError(t, err)
		assert.EqualValues(t, 1, actual)

		actual, err = IngesterPartitionID("mimir-write-zone-c-2")
		require.NoError(t, err)
		assert.EqualValues(t, 2, actual)
	})

	t.Run("without zones", func(t *testing.T) {
		actual, err := IngesterPartitionID("ingester-0")
		require.NoError(t, err)
		assert.EqualValues(t, 0, actual)

		actual, err = IngesterPartitionID("ingester-1")
		require.NoError(t, err)
		assert.EqualValues(t, 1, actual)

		actual, err = IngesterPartitionID("mimir-write-2")
		require.NoError(t, err)
		assert.EqualValues(t, 2, actual)
	})

	t.Run("should return error if the ingester ID has a non supported format", func(t *testing.T) {
		_, err := IngesterPartitionID("unknown")
		require.Error(t, err)

		_, err = IngesterPartitionID("ingester-zone-a-")
		require.Error(t, err)

		_, err = IngesterPartitionID("ingester-zone-a")
		require.Error(t, err)
	})
}

func TestResultPromise(t *testing.T) {
	t.Run("wait() should block until a result has been notified", func(t *testing.T) {
		var (
			wg  = sync.WaitGroup{}
			rw  = newResultPromise[int]()
			ctx = context.Background()
		)

		// Spawn few goroutines waiting for the result.
		for i := 0; i < 3; i++ {
			runAsync(&wg, func() {
				actual, err := rw.wait(ctx)
				require.NoError(t, err)
				require.Equal(t, 12345, actual)
			})
		}

		// Notify the result.
		rw.notify(12345, nil)

		// Wait until all goroutines have done.
		wg.Wait()
	})

	t.Run("wait() should block until an error has been notified", func(t *testing.T) {
		var (
			wg        = sync.WaitGroup{}
			rw        = newResultPromise[int]()
			ctx       = context.Background()
			resultErr = errors.New("test error")
		)

		// Spawn few goroutines waiting for the result.
		for i := 0; i < 3; i++ {
			runAsync(&wg, func() {
				actual, err := rw.wait(ctx)
				require.Equal(t, resultErr, err)
				require.Equal(t, 0, actual)
			})
		}

		// Notify the result.
		rw.notify(0, resultErr)

		// Wait until all goroutines have done.
		wg.Wait()
	})

	t.Run("wait() should return when the input context timeout expires", func(t *testing.T) {
		var (
			rw  = newResultPromise[int]()
			ctx = context.Background()
		)

		ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()

		actual, err := rw.wait(ctxWithTimeout)
		require.Equal(t, context.DeadlineExceeded, err)
		require.Equal(t, 0, actual)
	})
}

func TestCreateTopic(t *testing.T) {
	createKafkaCluster := func(t *testing.T) (string, *kfake.Cluster) {
		cluster, err := kfake.NewCluster(kfake.NumBrokers(1))
		require.NoError(t, err)
		t.Cleanup(cluster.Close)

		addrs := cluster.ListenAddrs()
		require.Len(t, addrs, 1)

		return addrs[0], cluster
	}

	t.Run("should create the partitions", func(t *testing.T) {
		var (
			addr, cluster = createKafkaCluster(t)
			cfg           = KafkaConfig{
				Topic:                            "topic-name",
				Address:                          addr,
				AutoCreateTopicDefaultPartitions: 100,
			}
		)

		cluster.ControlKey(kmsg.AlterConfigs.Int16(), func(request kmsg.Request) (kmsg.Response, error, bool) {
			r := request.(*kmsg.CreateTopicsRequest)

			assert.Len(t, r.Topics, 1)
			res := r.Topics[0]
			assert.Equal(t, cfg.Topic, res.Topic)
			assert.Equal(t, 100, res.NumPartitions)
			assert.Equal(t, -1, res.ReplicationFactor)
			assert.Empty(t, res.Configs)
			assert.Empty(t, res.ReplicaAssignment)

			return &kmsg.CreateTopicsResponse{
				Version: r.Version,
				Topics: []kmsg.CreateTopicsResponseTopic{
					{
						Topic:             cfg.Topic,
						NumPartitions:     res.NumPartitions,
						ReplicationFactor: 3,
					},
				},
			}, nil, true
		})

		logger := log.NewNopLogger()

		require.NoError(t, createTopic(cfg, logger))
	})

	t.Run("should return an error if the request fails", func(t *testing.T) {
		var (
			addr, cluster = createKafkaCluster(t)
			cfg           = KafkaConfig{
				Address:                          addr,
				AutoCreateTopicDefaultPartitions: 100,
			}
		)

		cluster.ControlKey(kmsg.CreateTopics.Int16(), func(_ kmsg.Request) (kmsg.Response, error, bool) {
			return &kmsg.CreateTopicsResponse{}, errors.New("failed request"), true
		})

		logger := log.NewNopLogger()

		require.NoError(t, createTopic(cfg, logger))
	})

	t.Run("should return an error if the request succeed but the response contains an error", func(t *testing.T) {
		var (
			addr, cluster = createKafkaCluster(t)
			cfg           = KafkaConfig{
				Topic:                            "topic-name",
				Address:                          addr,
				AutoCreateTopicDefaultPartitions: 100,
			}
		)

		cluster.ControlKey(kmsg.AlterConfigs.Int16(), func(kReq kmsg.Request) (kmsg.Response, error, bool) {
			req := kReq.(*kmsg.CreateTopicsRequest)
			assert.Len(t, req.Topics, 1)
			res := req.Topics[0]
			assert.Equal(t, cfg.Topic, res.Topic)
			assert.Equal(t, 100, res.NumPartitions)
			assert.Equal(t, -1, res.ReplicationFactor)
			assert.Empty(t, res.Configs)
			assert.Empty(t, res.ReplicaAssignment)

			return &kmsg.CreateTopicsResponse{
				Version: req.Version,
				Topics: []kmsg.CreateTopicsResponseTopic{
					{
						Topic:             cfg.Topic,
						NumPartitions:     res.NumPartitions,
						ReplicationFactor: 3,
					},
				},
			}, nil, true
		})

		logger := log.NewNopLogger()

		require.NoError(t, createTopic(cfg, logger))
	})

	t.Run("should not return error when topic already exists", func(t *testing.T) {
		var (
			addr, _ = createKafkaCluster(t)
			cfg     = KafkaConfig{
				Topic:                            "topic-name",
				Address:                          addr,
				AutoCreateTopicDefaultPartitions: 100,
			}
			logger = log.NewNopLogger()
		)

		// First call should create the topic
		assert.NoError(t, createTopic(cfg, logger))

		// Second call should succeed because topic already exists
		assert.NoError(t, createTopic(cfg, logger))
	})
}
