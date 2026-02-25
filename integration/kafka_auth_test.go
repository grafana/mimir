// SPDX-License-Identifier: AGPL-3.0-only
//go:build requires_docker

package integration

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
		setup func(*testing.T, *e2e.Scenario) (_ e2edb.KafkaConfig, commonFlags map[string]string, serviceFlags map[string]map[string]string)
	}{
		"no auth": {
			setup: func(t *testing.T, s *e2e.Scenario) (e2edb.KafkaConfig, map[string]string, map[string]map[string]string) {
				return e2edb.KafkaConfig{
					AuthMode: e2edb.KafkaAuthNone,
				}, nil, nil
			},
		},
		"SASL plaintext": {
			setup: func(t *testing.T, s *e2e.Scenario) (e2edb.KafkaConfig, map[string]string, map[string]map[string]string) {
				return e2edb.KafkaConfig{
					AuthMode: e2edb.KafkaAuthSASLPlain,
				}, nil, nil
			},
		},
		"SASL SCRAM-SHA-256": {
			setup: func(t *testing.T, s *e2e.Scenario) (e2edb.KafkaConfig, map[string]string, map[string]map[string]string) {
				return e2edb.KafkaConfig{
					AuthMode: e2edb.KafkaAuthSASLScramSHA256,
				}, nil, nil
			},
		},
		"SASL SCRAM-SHA-512": {
			setup: func(t *testing.T, s *e2e.Scenario) (e2edb.KafkaConfig, map[string]string, map[string]map[string]string) {
				return e2edb.KafkaConfig{
					AuthMode: e2edb.KafkaAuthSASLScramSHA512,
				}, nil, nil
			},
		},
		"SASL OAUTHBEARER token": {
			setup: func(t *testing.T, s *e2e.Scenario) (e2edb.KafkaConfig, map[string]string, map[string]map[string]string) {
				dex := e2edb.NewDex()
				require.NoError(t, s.StartAndWaitReady(dex))

				token, err := dex.FetchToken()
				require.NoError(t, err)

				return e2edb.KafkaConfig{
					AuthMode:    e2edb.KafkaAuthSASLOAuthToken,
					DexEndpoint: dex.NetworkHTTPEndpoint(),
				}, map[string]string{"-ingest-storage.kafka.sasl-oauthbearer-token": token}, nil
			},
		},
		"SASL OAUTHBEARER file": {
			setup: func(t *testing.T, s *e2e.Scenario) (e2edb.KafkaConfig, map[string]string, map[string]map[string]string) {
				dex := e2edb.NewDex()
				require.NoError(t, s.StartAndWaitReady(dex))

				token, err := dex.FetchToken()
				require.NoError(t, err)

				err = os.WriteFile(filepath.Join(s.SharedDir(), "oauth-token.json"), []byte(fmt.Sprintf(`{"token":"%s"}`, token)), 0644)
				require.NoError(t, err)

				return e2edb.KafkaConfig{
					AuthMode:    e2edb.KafkaAuthSASLOAuthTokenFile,
					DexEndpoint: dex.NetworkHTTPEndpoint(),
				}, map[string]string{"-ingest-storage.kafka.sasl-oauthbearer-file-path": "/shared/oauth-token.json"}, nil
			},
		},
		"SASL OAUTHBEARER reauth pipe": {
			setup: func(t *testing.T, s *e2e.Scenario) (e2edb.KafkaConfig, map[string]string, map[string]map[string]string) {
				dex := e2edb.NewDex()
				require.NoError(t, s.StartAndWaitReady(dex))

				serviceFlags := map[string]map[string]string{}
				for _, service := range []string{"distributor", "ingester"} {
					// Sidecar that reads from reauth-pipe, refreshes the token,
					// and writes to tokens.jsonl.
					// This simulates an external token management service that would be used
					// in production to handle OAuth token lifecycle management.
					tokenRefreshScript := fmt.Sprintf(`
						set -euo pipefail
						apk add --no-cache curl jq >/dev/null
						while true; do
							head -n1 /shared/%[1]s-reauth-pipe > /dev/null
							echo "Got reauth request; refreshing token"
							RESPONSE=$(curl -s -X POST \
								--connect-timeout 5 \
								--max-time 10 \
								-d "grant_type=password" \
								-d "username=%[2]s" \
								-d "password=%[3]s" \
								-d "client_id=%[4]s" \
								-d "client_secret=%[5]s" \
								-d "scope=openid" \
								"http://%[6]s/dex/token" 2>&1)
							TOKEN=$(echo "$RESPONSE" | jq -r .access_token 2>/dev/null)
							if [ "$TOKEN" != "null" ] && [ -n "$TOKEN" ]; then
								# Update the current token
								echo "Refreshed token; responding"
								echo '{"token": "'"$TOKEN"'"}' > /shared/%[1]s-tokens.jsonl
								echo "Responded"
							else
								echo "ERROR: Failed to fetch valid token from response: $RESPONSE"
							fi
						done
					`,
						service,
						e2edb.DexUserEmail, e2edb.DexUserPassword, e2edb.DexClientID, e2edb.DexClientSecret,
						strings.TrimPrefix(dex.NetworkHTTPEndpoint(), "http://"))

					// Create named pipes for the OAuth token.
					_, err := e2emimir.RunInContainerShell(s, fmt.Sprintf("mkfifo /shared/%[1]s-reauth-pipe /shared/%[1]s-tokens.jsonl", service))
					require.NoError(t, err)

					// Start the refresher sidecar.
					tokenRefresher := e2e.NewConcreteService(
						service+"-token-refresher",
						e2emimir.ShellImage,
						e2e.NewCommand("sh", "-c", tokenRefreshScript),
						e2e.NewCmdReadinessProbe(e2e.NewCommand("true")), // Always ready
						0, // dummy port
					)
					require.NoError(t, s.Start(tokenRefresher))

					t.Cleanup(func() {
						_ = tokenRefresher.Kill()
						_, _ = e2emimir.RunInContainerShell(s, fmt.Sprintf("rm -f /shared/%[1]s-reauth-pipe /shared/%[1]s-tokens.jsonl", service))
					})

					serviceFlags[service] = map[string]string{
						"-ingest-storage.kafka.sasl-oauthbearer-file-path":                fmt.Sprintf("/shared/%s-tokens.jsonl", service),
						"-ingest-storage.kafka.sasl-oauthbearer-reauth-request-file-path": fmt.Sprintf("/shared/%s-reauth-pipe", service),
					}
				}

				return e2edb.KafkaConfig{
					AuthMode:    e2edb.KafkaAuthSASLOAuthTokenFile,
					DexEndpoint: dex.NetworkHTTPEndpoint(),
				}, nil, serviceFlags
			},
		},
	}

	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			s, err := e2e.NewScenario(networkName)
			require.NoError(t, err)
			defer s.Close()

			kafkaConfig, flags, serviceFlags := tc.setup(t, s)
			consul := e2edb.NewConsul()
			kafka := kafkaConfig.New()
			require.NoError(t, s.StartAndWaitReady(consul, kafka))

			flags = mergeFlags(
				map[string]string{
					"-blocks-storage.tsdb.ship-interval": "0",
					"-querier.query-store-after":         "12h",
				},
				flags,
			)

			distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), mergeFlags(flags, IngestStorageFlags(kafkaConfig.AuthMode), serviceFlags["distributor"]))
			ingester := e2emimir.NewIngester("ingester-0", consul.NetworkHTTPEndpoint(), mergeFlags(flags, IngestStorageFlags(kafkaConfig.AuthMode), serviceFlags["ingester"]))
			querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), mergeFlags(flags, serviceFlags["querier"]))
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
