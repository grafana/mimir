// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/integration/configs.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.
//go:build requires_docker

package integration

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"text/template"

	e2e "github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"

	"github.com/grafana/mimir/pkg/querier/api"
)

const (
	userID             = "e2e-user"
	defaultNetworkName = "e2e-mimir-test"
	mimirBucketName    = "mimir"
	blocksBucketName   = "mimir-blocks"
	alertsBucketName   = "mimir-alerts"
	mimirConfigFile    = "config.yaml"
	clientCertFile     = "certs/client.crt"
	clientKeyFile      = "certs/client.key"
	caCertFile         = "certs/root.crt"
	serverCertFile     = "certs/server.crt"
	serverKeyFile      = "certs/server.key"
)

// GetNetworkName returns the docker network name to run tests within.
func GetNetworkName() string {
	// If the E2E_NETWORK_NAME is set, use that for the network name.
	// Otherwise, return the default network name.
	if os.Getenv("E2E_NETWORK_NAME") != "" {
		return os.Getenv("E2E_NETWORK_NAME")
	}

	return defaultNetworkName
}

var (
	networkName = GetNetworkName()

	mimirAlertmanagerUserConfigYaml = `route:
  receiver: "example_receiver"
  group_by: ["example_groupby"]
receivers:
  - name: "example_receiver"
`

	mimirAlertmanagerUserClassicConfigYaml = `route:
  receiver: test
  group_by: [foo]
  routes:
    - matchers:
      - foo=bar
      - bar=baz
receivers:
  - name: test
`

	mimirAlertmanagerUserUTF8ConfigYaml = `route:
  receiver: test
  group_by: [bar🙂]
  routes:
    - matchers:
      - foo=bar
      - bar🙂=baz
receivers:
  - name: test
`

	mimirRulerUserConfigYaml = `groups:
- name: rule
  interval: 100s
  rules:
  - record: test_rule
    alert: ""
    expr: up
    for: 0s
    labels: {}
    annotations: {}
`
)

var (
	AlertmanagerFlags = func() map[string]string {
		return map[string]string{
			"-alertmanager.configs.poll-interval": "1s",
			"-alertmanager.web.external-url":      "http://localhost/alertmanager",
		}
	}

	AlertmanagerShardingFlags = func(consulAddress string, replicationFactor int) map[string]string {
		return map[string]string{
			"-alertmanager.sharding-ring.store":              "consul",
			"-alertmanager.sharding-ring.consul.hostname":    consulAddress,
			"-alertmanager.sharding-ring.replication-factor": strconv.Itoa(replicationFactor),
		}
	}

	AlertmanagerPersisterFlags = func(interval string) map[string]string {
		return map[string]string{
			"-alertmanager.persist-interval": interval,
		}
	}

	AlertmanagerLocalFlags = func() map[string]string {
		return map[string]string{
			"-alertmanager-storage.backend":    "local",
			"-alertmanager-storage.local.path": filepath.Join(e2e.ContainerSharedDir, "alertmanager_configs"),
		}
	}

	AlertmanagerS3Flags = func() map[string]string {
		return map[string]string{
			"-alertmanager-storage.backend":              "s3",
			"-alertmanager-storage.s3.access-key-id":     e2edb.MinioAccessKey,
			"-alertmanager-storage.s3.secret-access-key": e2edb.MinioSecretKey,
			"-alertmanager-storage.s3.bucket-name":       alertsBucketName,
			"-alertmanager-storage.s3.endpoint":          fmt.Sprintf("%s-minio-9000:9000", networkName),
			"-alertmanager-storage.s3.insecure":          "true",
		}
	}

	AlertmanagerGrafanaCompatibilityFlags = func() map[string]string {
		return map[string]string{
			"-alertmanager.grafana-alertmanager-compatibility-enabled": "true",
		}
	}

	RulerFlags = func() map[string]string {
		return map[string]string{
			"-ruler.poll-interval":             "2s",
			"-ruler.evaluation-delay-duration": "0",
		}
	}

	RulerShardingFlags = func(consulAddress string) map[string]string {
		return map[string]string{
			"-ruler.ring.store":           "consul",
			"-ruler.ring.consul.hostname": consulAddress,
		}
	}

	BlocksStorageS3Flags = func() map[string]string {
		return map[string]string{
			"-blocks-storage.backend":              "s3",
			"-blocks-storage.s3.access-key-id":     e2edb.MinioAccessKey,
			"-blocks-storage.s3.secret-access-key": e2edb.MinioSecretKey,
			"-blocks-storage.s3.endpoint":          fmt.Sprintf("%s-minio-9000:9000", networkName),
			"-blocks-storage.s3.insecure":          "true",
			"-blocks-storage.s3.bucket-name":       blocksBucketName,
		}
	}

	BlocksStorageFlags = func() map[string]string {
		return map[string]string{
			"-blocks-storage.tsdb.block-ranges-period":          "1m",
			"-blocks-storage.bucket-store.ignore-blocks-within": "0",
			"-blocks-storage.bucket-store.sync-interval":        "5s",
			"-blocks-storage.tsdb.retention-period":             "5m",
			"-blocks-storage.tsdb.ship-interval":                "1m",
			"-blocks-storage.tsdb.head-compaction-interval":     "1s",
			"-querier.query-store-after":                        "0",
			"-compactor.cleanup-interval":                       "2s",
		}
	}

	CommonStorageBackendFlags = func() map[string]string {
		return map[string]string{
			"-common.storage.backend":              "s3",
			"-common.storage.s3.access-key-id":     e2edb.MinioAccessKey,
			"-common.storage.s3.secret-access-key": e2edb.MinioSecretKey,
			"-common.storage.s3.endpoint":          fmt.Sprintf("%s-minio-9000:9000", networkName),
			"-common.storage.s3.insecure":          "true",
			"-common.storage.s3.bucket-name":       mimirBucketName,
			// Also prefix blocks storage to allow config sanity checks pass.
			"-blocks-storage.storage-prefix": "blocks",
		}
	}

	DefaultSingleBinaryFlags = func() map[string]string {
		return map[string]string{
			"-auth.multitenancy-enabled": "true",
			// Query-frontend worker.
			"-querier.frontend-client.backoff-min-period": "100ms",
			"-querier.frontend-client.backoff-max-period": "100ms",
			"-querier.frontend-client.backoff-retries":    "1",
			"-querier.max-concurrent":                     "4",
			// Distributor.
			"-distributor.ring.store": "memberlist",
			// Ingester.
			"-ingester.ring.replication-factor": "1",
			"-ingester.ring.num-tokens":         "512",
		}
	}

	BlocksStorageConfig = buildConfigFromTemplate(`
blocks_storage:
  backend:             s3

  tsdb:
    block_ranges_period: ["1m"]
    retention_period:    5m
    ship_interval:       1m

  bucket_store:
    sync_interval: 5s

  s3:
    bucket_name:       mimir-blocks
    access_key_id:     {{.MinioAccessKey}}
    secret_access_key: {{.MinioSecretKey}}
    endpoint:          {{.MinioEndpoint}}
    insecure:          true
`, struct {
		MinioAccessKey string
		MinioSecretKey string
		MinioEndpoint  string
	}{
		MinioAccessKey: e2edb.MinioAccessKey,
		MinioSecretKey: e2edb.MinioSecretKey,
		MinioEndpoint:  fmt.Sprintf("%s-minio-9000:9000", networkName),
	})

	IngestStorageFlags = func() map[string]string {
		return map[string]string{
			"-ingest-storage.enabled":       "true",
			"-ingest-storage.kafka.topic":   "ingest",
			"-ingest-storage.kafka.address": fmt.Sprintf("%s-kafka:9092", networkName),

			// To simplify integration tests, we use strong read consistency by default.
			// Integration tests that want to test the eventual consistency can override it.
			"-ingest-storage.read-consistency": api.ReadConsistencyStrong,

			// Frequently poll last produced offset in order to have a low end-to-end latency
			// and faster integration tests.
			"-ingest-storage.kafka.last-produced-offset-poll-interval": "50ms",
			"-ingest-storage.kafka.last-produced-offset-retry-timeout": "1s",

			// Do not wait before switching an INACTIVE partition to ACTIVE.
			"-ingester.partition-ring.min-partition-owners-count":    "0",
			"-ingester.partition-ring.min-partition-owners-duration": "0s",
		}
	}
)

func buildConfigFromTemplate(tmpl string, data interface{}) string {
	t, err := template.New("config").Parse(tmpl)
	if err != nil {
		panic(err)
	}

	w := &strings.Builder{}
	if err = t.Execute(w, data); err != nil {
		panic(err)
	}

	return w.String()
}
