// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/concurrency"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
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

func TestSetDefaultNumberOfPartitionsForAutocreatedTopics(t *testing.T) {
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
				Address:                          addr,
				AutoCreateTopicDefaultPartitions: 100,
			}
		)

		cluster.ControlKey(kmsg.AlterConfigs.Int16(), func(request kmsg.Request) (kmsg.Response, error, bool) {
			r := request.(*kmsg.AlterConfigsRequest)

			require.Len(t, r.Resources, 1)
			res := r.Resources[0]
			require.Equal(t, kmsg.ConfigResourceTypeBroker, res.ResourceType)
			require.Len(t, res.Configs, 1)
			cfg := res.Configs[0]
			require.Equal(t, "num.partitions", cfg.Name)
			require.NotNil(t, *cfg.Value)
			require.Equal(t, "100", *cfg.Value)

			return &kmsg.AlterConfigsResponse{
				Version: r.Version,
			}, nil, true
		})

		logs := concurrency.SyncBuffer{}
		logger := log.NewLogfmtLogger(&logs)

		setDefaultNumberOfPartitionsForAutocreatedTopics(cfg, logger)
		require.NotContains(t, logs.String(), "err")
	})

	t.Run("should log an error if the request fails", func(t *testing.T) {
		var (
			addr, cluster = createKafkaCluster(t)
			cfg           = KafkaConfig{
				Address:                          addr,
				AutoCreateTopicDefaultPartitions: 100,
			}
		)

		cluster.ControlKey(kmsg.AlterConfigs.Int16(), func(request kmsg.Request) (kmsg.Response, error, bool) {
			return &kmsg.AlterConfigsResponse{}, errors.New("failed request"), true
		})

		logs := concurrency.SyncBuffer{}
		logger := log.NewLogfmtLogger(&logs)

		setDefaultNumberOfPartitionsForAutocreatedTopics(cfg, logger)
		require.Contains(t, logs.String(), "err")
	})

	t.Run("should log an error if the request succeed but the response contains an error", func(t *testing.T) {
		var (
			addr, cluster = createKafkaCluster(t)
			cfg           = KafkaConfig{
				Address:                          addr,
				AutoCreateTopicDefaultPartitions: 100,
			}
		)

		cluster.ControlKey(kmsg.AlterConfigs.Int16(), func(kReq kmsg.Request) (kmsg.Response, error, bool) {
			req := kReq.(*kmsg.AlterConfigsRequest)
			require.Len(t, req.Resources, 1)

			return &kmsg.AlterConfigsResponse{
				Version: req.Version,
				Resources: []kmsg.AlterConfigsResponseResource{
					{
						ResourceType: req.Resources[0].ResourceType,
						ResourceName: req.Resources[0].ResourceName,
						ErrorCode:    kerr.InvalidRequest.Code,
						ErrorMessage: pointerOf(kerr.InvalidRequest.Message),
					},
				},
			}, nil, true
		})

		logs := concurrency.SyncBuffer{}
		logger := log.NewLogfmtLogger(&logs)

		setDefaultNumberOfPartitionsForAutocreatedTopics(cfg, logger)
		require.Contains(t, logs.String(), "err")
	})

}
