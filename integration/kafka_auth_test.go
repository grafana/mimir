// SPDX-License-Identifier: AGPL-3.0-only

package integration

import (
	"crypto/x509"
	"crypto/x509/pkix"
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
		setup     func(*testing.T, *e2e.Scenario) (*e2edb.KafkaConfig, map[string]string)
		postStart func(*testing.T, *e2e.Scenario, *e2edb.KafkaService) map[string]string
	}{
		"no auth": {
			setup: func(t *testing.T, s *e2e.Scenario) (*e2edb.KafkaConfig, map[string]string) {
				return &e2edb.KafkaConfig{
					AuthMode: e2edb.KafkaAuthNone,
				}, nil
			},
		},
		"SASL plaintext": {
			setup: func(t *testing.T, s *e2e.Scenario) (*e2edb.KafkaConfig, map[string]string) {
				return &e2edb.KafkaConfig{
					AuthMode: e2edb.KafkaAuthSASLPlain,
				}, nil
			},
		},
		"SASL SCRAM-SHA-256": {
			setup: func(t *testing.T, s *e2e.Scenario) (*e2edb.KafkaConfig, map[string]string) {
				return &e2edb.KafkaConfig{
					AuthMode: e2edb.KafkaAuthSASLScramSHA256,
				}, nil
			},
		},
		"SASL SCRAM-SHA-512": {
			setup: func(t *testing.T, s *e2e.Scenario) (*e2edb.KafkaConfig, map[string]string) {
				return &e2edb.KafkaConfig{
					AuthMode: e2edb.KafkaAuthSASLScramSHA512,
				}, nil
			},
		},
		"SASL OAUTHBEARER token": {
			setup: func(t *testing.T, s *e2e.Scenario) (*e2edb.KafkaConfig, map[string]string) {
				dex := e2edb.NewDex()
				require.NoError(t, s.StartAndWaitReady(dex))

				token, err := dex.FetchToken()
				require.NoError(t, err)

				return &e2edb.KafkaConfig{
					AuthMode:    e2edb.KafkaAuthSASLOAuthToken,
					DexEndpoint: dex.NetworkHTTPEndpoint(),
				}, map[string]string{"-ingest-storage.kafka.sasl-oauthbearer-token": token}
			},
		},
		"SASL OAUTHBEARER file": {
			setup: func(t *testing.T, s *e2e.Scenario) (*e2edb.KafkaConfig, map[string]string) {
				dex := e2edb.NewDex()
				require.NoError(t, s.StartAndWaitReady(dex))

				token, err := dex.FetchToken()
				require.NoError(t, err)

				err = os.WriteFile(filepath.Join(s.SharedDir(), "oauth-token.json"), []byte(fmt.Sprintf(`{"token":"%s"}`, token)), 0644)
				require.NoError(t, err)

				return &e2edb.KafkaConfig{
					AuthMode:    e2edb.KafkaAuthSASLOAuthTokenFile,
					DexEndpoint: dex.NetworkHTTPEndpoint(),
				}, map[string]string{"-ingest-storage.kafka.sasl-oauthbearer-file-path": "/shared/oauth-token.json"}
			},
		},
		"mTLS": {
			setup: func(t *testing.T, s *e2e.Scenario) (*e2edb.KafkaConfig, map[string]string) {
				return &e2edb.KafkaConfig{AuthMode: e2edb.KafkaAuthMTLS}, nil
			},
			postStart: func(t *testing.T, s *e2e.Scenario, kafka *e2edb.KafkaService) map[string]string {
				clientCertPath := filepath.Join(s.SharedDir(), "kafka-client.crt")
				clientKeyPath := filepath.Join(s.SharedDir(), "kafka-client.key")

				// Write client certificate using Kafka's configured CA.
				require.NoError(t, kafka.WriteCertificate(
					&x509.Certificate{
						Subject:     pkix.Name{CommonName: "mimir"},
						ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
					},
					clientCertPath,
					clientKeyPath,
				))

				return map[string]string{
					"-ingest-storage.kafka.tls-cert-path": filepath.Join(e2e.ContainerSharedDir, "kafka-client.crt"),
					"-ingest-storage.kafka.tls-key-path":  filepath.Join(e2e.ContainerSharedDir, "kafka-client.key"),
				}
			},
		},
		"SASL OAUTHBEARER HTTP socket": {
			setup: func(t *testing.T, s *e2e.Scenario) (*e2edb.KafkaConfig, map[string]string) {
				dex := e2edb.NewDex()
				require.NoError(t, s.StartAndWaitReady(dex))

				// We need to run the server for the domain socket in a
				// container because sharing sockets with the host doesn't
				// always work. Let's use a simple script.
				handlerScript := `#!/bin/sh

# Fetch a fresh token from Dex over the Docker network.
REQ='grant_type=password&scope=openid'
REQ="${REQ}&username=` + e2edb.DexUserEmail + `"
REQ="${REQ}&password=` + e2edb.DexUserPassword + `"
REQ="${REQ}&client_id=` + e2edb.DexClientID + `"
REQ="${REQ}&client_secret=` + e2edb.DexClientSecret + `"
RESP=$(wget -q -O- --post-data "$REQ" http://dex:5556/dex/token)

# Extract the access_token field and wrap it in the expected JSON schema.
TOKEN=$(printf '%s' "$RESP" | sed -n 's/.*"access_token":[[:space:]]*"\([^"]*\)".*/\1/p')
BODY="{\"token\":\"$TOKEN\"}"

printf 'HTTP/1.1 200 OK\r\n'
printf 'Content-Type: application/json\r\n'
printf 'Content-Length: %d\r\n' "${#BODY}"
printf '\r\n'
printf '%s' "$BODY"
`

				err := os.WriteFile(filepath.Join(s.SharedDir(), "oauth-handler.sh"), []byte(handlerScript), 0755)
				require.NoError(t, err)

				socatSvc := e2e.NewConcreteService(
					"oauth-proxy",
					e2emimir.ShellImage,
					e2e.NewCommandWithoutEntrypoint("sh", "-c",
						`apk add --no-cache socat >/dev/null 2>&1 &&`+
							`rm -f /shared/oauth.sock &&`+
							`exec socat UNIX-LISTEN:/shared/oauth.sock,fork,mode=777 SYSTEM:/shared/oauth-handler.sh`),
					e2e.NewCmdReadinessProbe(e2e.NewCommandWithoutEntrypoint("test", "-S", "/shared/oauth.sock")),
				)
				require.NoError(t, s.StartAndWaitReady(socatSvc))

				return &e2edb.KafkaConfig{
					AuthMode:    e2edb.KafkaAuthSASLOAuthTokenFile,
					DexEndpoint: dex.NetworkHTTPEndpoint(),
				}, map[string]string{"-ingest-storage.kafka.sasl-oauthbearer-http-socket-path": "/shared/oauth.sock"}
			},
		},
		"MSK_IAM": {
			setup: func(t *testing.T, s *e2e.Scenario) (*e2edb.KafkaConfig, map[string]string) {
				if os.Getenv("MIMIR_TEST_MSK_IAM_ADDRESS") == "" {
					t.Skip("skipping MSK_IAM test: set MIMIR_TEST_MSK_IAM_* env vars to run this test")
				}

				// Return zero KafkaConfig to signal that no local Kafka cluster should be started.
				return nil, map[string]string{
					"-ingest-storage.kafka.address":                    os.Getenv("MIMIR_TEST_MSK_IAM_ADDRESS"),
					"-ingest-storage.kafka.sasl-mechanism":             "AWS_MSK_IAM",
					"-ingest-storage.kafka.sasl-msk-iam-access-key":    os.Getenv("MIMIR_TEST_MSK_IAM_ACCESS_KEY"),
					"-ingest-storage.kafka.sasl-msk-iam-secret-key":    os.Getenv("MIMIR_TEST_MSK_IAM_SECRET_KEY"),
					"-ingest-storage.kafka.sasl-msk-iam-session-token": os.Getenv("MIMIR_TEST_MSK_IAM_SESSION_TOKEN"),
					"-ingest-storage.kafka.tls-enabled":                "true",
				}
			},
		},
	}

	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			s, err := e2e.NewScenario(networkName)
			require.NoError(t, err)
			defer s.Close()

			consul := e2edb.NewConsul()

			kafkaConfig, flags := tc.setup(t, s)
			authMode := e2edb.KafkaAuthNone
			if kafkaConfig != nil {
				authMode = kafkaConfig.AuthMode

				kafka := kafkaConfig.New()
				require.NoError(t, s.StartAndWaitReady(consul, kafka))

				if tc.postStart != nil {
					extraFlags := tc.postStart(t, s, kafka)
					flags = mergeFlags(flags, extraFlags)
				}
			} else {
				// External Kafka (e.g. MSK). No local Kafka cluster to start.
				require.NoError(t, s.StartAndWaitReady(consul))
			}
			flags = mergeFlags(
				IngestStorageFlags(authMode),
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
			require.EventuallyWithT(t, func(c *assert.CollectT) {
				result, err := client.Query("test_series", now)
				require.NoError(c, err)
				require.Equal(c, model.ValVector, result.Type())
				assert.Equal(c, expectedVector, result.(model.Vector))
			}, 10*time.Second, time.Second/2)
		})
	}
}
