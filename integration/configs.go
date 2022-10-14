// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/integration/configs.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.
//go:build requires_docker
// +build requires_docker

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
)

const (
	userID              = "e2e-user"
	defaultNetworkName  = "e2e-mimir-test"
	mimirBucketName     = "mimir"
	blocksBucketName    = "mimir-blocks"
	alertsBucketName    = "mimir-alerts"
	rulestoreBucketName = "mimir-ruler"
	mimirConfigFile     = "config.yaml"
	clientCertFile      = "certs/client.crt"
	clientKeyFile       = "certs/client.key"
	caCertFile          = "certs/root.crt"
	serverCertFile      = "certs/server.crt"
	serverKeyFile       = "certs/server.key"
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

	mimirRulerEvalStaleNanConfigYaml = `groups:
- name: rule
  interval: 1s
  rules:
  - record: stale_nan_eval
    expr: a_sometimes_stale_nan_series * 2
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

	RulerFlags = func() map[string]string {
		return map[string]string{
			"-ruler.poll-interval": "2s",
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
			"-blocks-storage.bucket-store.bucket-index.enabled": "false",
			"-blocks-storage.bucket-store.ignore-blocks-within": "0",
			"-blocks-storage.bucket-store.sync-interval":        "5s",
			"-blocks-storage.tsdb.retention-period":             "5m",
			"-blocks-storage.tsdb.ship-interval":                "1m",
			"-blocks-storage.tsdb.head-compaction-interval":     "1s",
			"-querier.query-store-after":                        "0",
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
			"-querier.max-concurrent":                     "1",
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
    bucket_index:
      enabled: false 

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
