// SPDX-License-Identifier: AGPL-3.0-only
//go:build requires_docker

package integration

import (
	"testing"
	"time"

	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/e2emimir"
)

func TestIngestStorageKafkaAuth(t *testing.T) {
	tests := map[string]e2edb.KafkaConfig{
		"no auth": {
			AuthMode: e2edb.KafkaAuthNone,
		},
		"SASL plaintext": {
			AuthMode: e2edb.KafkaAuthSASLPlain,
		},
	}

	for testName, kafkaConfig := range tests {
		t.Run(testName, func(t *testing.T) {
			s, err := e2e.NewScenario(networkName)
			require.NoError(t, err)
			defer s.Close()

			// Start dependencies.
			consul := e2edb.NewConsul()
			kafka := kafkaConfig.New()
			require.NoError(t, s.StartAndWaitReady(consul, kafka))

			flags := mergeFlags(
				IngestStorageFlags(kafkaConfig.AuthMode),
				map[string]string{
					// Use filesystem backend for blocks storage so that no
					// object storage (S3/Minio) is required.
					// "-blocks-storage.backend":            "filesystem",
					// "-blocks-storage.filesystem.dir":     filepath.Join(e2e.ContainerSharedDir, "blocks"),
					"-blocks-storage.tsdb.ship-interval": "0",

					// Only query ingesters for recent data so we don't need
					// any store-gateway instances running.
					"-querier.query-store-after": "12h",
				},
			)

			distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
			ingester := e2emimir.NewIngester("ingester-0", consul.NetworkHTTPEndpoint(), flags)
			querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), flags)
			require.NoError(t, s.StartAndWaitReady(distributor, ingester, querier))

			client, err := e2emimir.NewClient(distributor.HTTPEndpoint(), querier.HTTPEndpoint(), "", "", userID)
			require.NoError(t, err)

			// Push a simple float series.
			now := time.Now()
			series, expectedVector, _ := generateFloatSeries("test_series", now)
			require.EventuallyWithT(t, func(c *assert.CollectT) {
				res, err := client.Push(series)
				require.NoError(c, err)
				require.Equal(c, 200, res.StatusCode)
			}, 10*time.Second, time.Second/2)

			// Verify the ingester has received the series.
			require.NoError(t, ingester.WaitSumMetrics(e2e.Greater(0), "cortex_ingester_memory_series"))

			// Query the series back and verify the result matches what was pushed.
			result, err := client.Query("test_series", now)
			require.NoError(t, err)
			require.Equal(t, model.ValVector, result.Type())
			assert.Equal(t, expectedVector, result.(model.Vector))
		})
	}
}
