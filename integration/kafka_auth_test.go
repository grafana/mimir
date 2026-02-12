// SPDX-License-Identifier: AGPL-3.0-only
//go:build requires_docker

package integration

import (
	"fmt"
	"os"
	"path/filepath"
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
	tests := map[string]struct {
		setup func(*testing.T, *e2e.Scenario) (e2edb.KafkaConfig, map[string]string)
	}{
		"no auth": {
			setup: func(t *testing.T, s *e2e.Scenario) (e2edb.KafkaConfig, map[string]string) {
				return e2edb.KafkaConfig{
					AuthMode: e2edb.KafkaAuthNone,
				}, nil
			},
		},
		"SASL plaintext": {
			setup: func(t *testing.T, s *e2e.Scenario) (e2edb.KafkaConfig, map[string]string) {
				return e2edb.KafkaConfig{
					AuthMode: e2edb.KafkaAuthSASLPlain,
				}, nil
			},
		},
		"SASL SCRAM-SHA-256": {
			setup: func(t *testing.T, s *e2e.Scenario) (e2edb.KafkaConfig, map[string]string) {
				return e2edb.KafkaConfig{
					AuthMode: e2edb.KafkaAuthSASLScramSHA256,
				}, nil
			},
		},
		"SASL SCRAM-SHA-512": {
			setup: func(t *testing.T, s *e2e.Scenario) (e2edb.KafkaConfig, map[string]string) {
				return e2edb.KafkaConfig{
					AuthMode: e2edb.KafkaAuthSASLScramSHA512,
				}, nil
			},
		},
		"SASL OAUTHBEARER token": {
			setup: func(t *testing.T, s *e2e.Scenario) (e2edb.KafkaConfig, map[string]string) {
				dex := e2edb.NewDex()
				require.NoError(t, s.StartAndWaitReady(dex))

				token, err := dex.FetchToken()
				require.NoError(t, err)

				return e2edb.KafkaConfig{
					AuthMode:    e2edb.KafkaAuthSASLOAuthToken,
					DexEndpoint: dex.NetworkHTTPEndpoint(),
				}, map[string]string{"-ingest-storage.kafka.sasl-oauthbearer-token": token}
			},
		},
		"SASL OAUTHBEARER file": {
			setup: func(t *testing.T, s *e2e.Scenario) (e2edb.KafkaConfig, map[string]string) {
				dex := e2edb.NewDex()
				require.NoError(t, s.StartAndWaitReady(dex))

				token, err := dex.FetchToken()
				require.NoError(t, err)

				err = os.WriteFile(filepath.Join(s.SharedDir(), "oauth-token.json"), []byte(fmt.Sprintf(`{"token":"%s"}`, token)), 0644)
				require.NoError(t, err)

				return e2edb.KafkaConfig{
					AuthMode:    e2edb.KafkaAuthSASLOAuthTokenFile,
					DexEndpoint: dex.NetworkHTTPEndpoint(),
				}, map[string]string{"-ingest-storage.kafka.sasl-oauthbearer-file-path": "/shared/oauth-token.json"}
			},
		},
	}

	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			s, err := e2e.NewScenario(networkName)
			require.NoError(t, err)
			defer s.Close()

			kafkaConfig, flags := tc.setup(t, s)
			consul := e2edb.NewConsul()
			kafka := kafkaConfig.New()
			require.NoError(t, s.StartAndWaitReady(consul, kafka))

			flags = mergeFlags(
				IngestStorageFlags(kafkaConfig.AuthMode),
				map[string]string{
					"-blocks-storage.tsdb.ship-interval": "0",
					"-querier.query-store-after":         "12h",
				},
				flags,
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
