// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/testkafka"
)

func TestNewKafkaReaderClient(t *testing.T) {
	t.Run("should support SASL plain authentication", func(t *testing.T) {
		const (
			topicName     = "test"
			numPartitions = 1
			username      = "mimir"
			password      = "supersecret"
		)

		_, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicName, testkafka.WithSASLPlain(username, password))

		t.Run("should fail if the provided auth is wrong", func(t *testing.T) {
			t.Parallel()

			cfg := createTestKafkaConfig(clusterAddr, topicName)
			cfg.SASLUsername = username
			cfg.SASLPassword = "wrong"

			client, err := NewKafkaReaderClient(cfg, nil, log.NewNopLogger())
			require.NoError(t, err)
			t.Cleanup(client.Close)

			require.Error(t, client.Ping(context.Background()))
		})

		t.Run("should succeed if the provided auth is good", func(t *testing.T) {
			t.Parallel()

			cfg := createTestKafkaConfig(clusterAddr, topicName)
			cfg.SASLUsername = username
			cfg.SASLPassword = password

			client, err := NewKafkaReaderClient(cfg, nil, log.NewNopLogger())
			require.NoError(t, err)
			t.Cleanup(client.Close)

			require.NoError(t, client.Ping(context.Background()))
		})
	})
}
