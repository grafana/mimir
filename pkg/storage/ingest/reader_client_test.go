// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"bytes"
	"context"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/testkafka"
)

func TestNewKafkaReaderClient_ClientRackWithConcurrentFetching(t *testing.T) {
	const (
		topicName     = "test"
		numPartitions = 1
	)

	_, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicName)

	tests := map[string]struct {
		clientRack          string
		fetchConcurrencyMax int
		expectWarning       bool
	}{
		"no rack configured": {
			clientRack:          "",
			fetchConcurrencyMax: 12,
			expectWarning:       false,
		},
		"rack configured and concurrent fetching enabled": {
			clientRack:          "zone-a",
			fetchConcurrencyMax: 12,
			expectWarning:       true,
		},
		"rack configured and concurrent fetching disabled": {
			clientRack:          "zone-a",
			fetchConcurrencyMax: 0,
			expectWarning:       false,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			logs := bytes.NewBuffer(nil)
			logger := log.NewLogfmtLogger(logs)

			cfg := createTestKafkaConfig(clusterAddr, topicName)
			cfg.ClientRack = testData.clientRack
			cfg.FetchConcurrencyMax = testData.fetchConcurrencyMax

			client, err := NewKafkaReaderClient(cfg, nil, logger)
			require.NoError(t, err)
			t.Cleanup(client.Close)

			if testData.expectWarning {
				assert.Contains(t, logs.String(), "the configured Kafka client rack has no effect because concurrent fetching is enabled")
			} else {
				assert.NotContains(t, logs.String(), "has no effect because concurrent fetching is enabled")
			}
		})
	}
}

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
			cfg.SASL.Username = username
			require.NoError(t, cfg.SASL.Password.Set("wrong"))

			client, err := NewKafkaReaderClient(cfg, nil, log.NewNopLogger())
			require.NoError(t, err)
			t.Cleanup(client.Close)

			require.Error(t, client.Ping(context.Background()))
		})

		t.Run("should succeed if the provided auth is good", func(t *testing.T) {
			t.Parallel()

			cfg := createTestKafkaConfig(clusterAddr, topicName)
			cfg.SASL.Username = username
			require.NoError(t, cfg.SASL.Password.Set(password))

			client, err := NewKafkaReaderClient(cfg, nil, log.NewNopLogger())
			require.NoError(t, err)
			t.Cleanup(client.Close)

			require.NoError(t, client.Ping(context.Background()))
		})
	})

	t.Run("should support SASL SCRAM-SHA-256 authentication", func(t *testing.T) {
		const (
			topicName     = "test"
			numPartitions = 1
			username      = "mimir"
			password      = "supersecret"
		)

		_, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicName, testkafka.WithSASLScramSHA256(username, password))

		t.Run("should fail if the provided auth is wrong", func(t *testing.T) {
			t.Parallel()

			cfg := createTestKafkaConfig(clusterAddr, topicName)
			cfg.SASL.Username = username
			cfg.SASL.Mechanism = SASLMechanismScramSHA256
			require.NoError(t, cfg.SASL.Password.Set("wrong"))

			client, err := NewKafkaReaderClient(cfg, nil, log.NewNopLogger())
			require.NoError(t, err)
			t.Cleanup(client.Close)

			require.Error(t, client.Ping(context.Background()))
		})

		t.Run("should succeed if the provided auth is good", func(t *testing.T) {
			t.Parallel()

			cfg := createTestKafkaConfig(clusterAddr, topicName)
			cfg.SASL.Username = username
			cfg.SASL.Mechanism = SASLMechanismScramSHA256
			require.NoError(t, cfg.SASL.Password.Set(password))

			client, err := NewKafkaReaderClient(cfg, nil, log.NewNopLogger())
			require.NoError(t, err)
			t.Cleanup(client.Close)

			require.NoError(t, client.Ping(context.Background()))
		})
	})

	t.Run("should support SASL SCRAM-SHA-512 authentication", func(t *testing.T) {
		const (
			topicName     = "test"
			numPartitions = 1
			username      = "mimir"
			password      = "supersecret"
		)

		_, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicName, testkafka.WithSASLScramSHA512(username, password))

		t.Run("should fail if the provided auth is wrong", func(t *testing.T) {
			t.Parallel()

			cfg := createTestKafkaConfig(clusterAddr, topicName)
			cfg.SASL.Username = username
			cfg.SASL.Mechanism = SASLMechanismScramSHA512
			require.NoError(t, cfg.SASL.Password.Set("wrong"))

			client, err := NewKafkaReaderClient(cfg, nil, log.NewNopLogger())
			require.NoError(t, err)
			t.Cleanup(client.Close)

			require.Error(t, client.Ping(context.Background()))
		})

		t.Run("should succeed if the provided auth is good", func(t *testing.T) {
			t.Parallel()

			cfg := createTestKafkaConfig(clusterAddr, topicName)
			cfg.SASL.Username = username
			cfg.SASL.Mechanism = SASLMechanismScramSHA512
			require.NoError(t, cfg.SASL.Password.Set(password))

			client, err := NewKafkaReaderClient(cfg, nil, log.NewNopLogger())
			require.NoError(t, err)
			t.Cleanup(client.Close)

			require.NoError(t, client.Ping(context.Background()))
		})
	})
}
