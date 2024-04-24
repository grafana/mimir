// SPDX-License-Identifier: AGPL-3.0-only
//go:build requires_docker

package integration

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/grafana/e2e"
	e2ecache "github.com/grafana/e2e/cache"
	e2edb "github.com/grafana/e2e/db"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/e2emimir"
)

func TestMimirShouldStartInSingleBinaryModeWithAllMemcachedConfigured(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, blocksBucketName)
	memcached := e2ecache.NewMemcached()
	require.NoError(t, s.StartAndWaitReady(consul, minio, memcached))

	flags := mergeFlags(BlocksStorageFlags(), BlocksStorageS3Flags(), map[string]string{
		// Memcached.
		"-query-frontend.cache-results":                                   "true",
		"-query-frontend.results-cache.backend":                           "memcached",
		"-query-frontend.results-cache.memcached.addresses":               "dns+" + memcached.NetworkEndpoint(e2ecache.MemcachedPort),
		"-blocks-storage.bucket-store.metadata-cache.backend":             "memcached",
		"-blocks-storage.bucket-store.metadata-cache.memcached.addresses": "dns+" + memcached.NetworkEndpoint(e2ecache.MemcachedPort),
		"-blocks-storage.bucket-store.index-cache.backend":                "memcached",
		"-blocks-storage.bucket-store.index-cache.memcached.addresses":    "dns+" + memcached.NetworkEndpoint(e2ecache.MemcachedPort),
		"-blocks-storage.bucket-store.chunks-cache.backend":               "memcached",
		"-blocks-storage.bucket-store.chunks-cache.memcached.addresses":   "dns+" + memcached.NetworkEndpoint(e2ecache.MemcachedPort),
		// Ingester.
		"-ingester.ring.store":                     "consul",
		"-ingester.ring.consul.hostname":           consul.NetworkHTTPEndpoint(),
		"-ingester.partition-ring.store":           "consul",
		"-ingester.partition-ring.consul.hostname": consul.NetworkHTTPEndpoint(),
		// Distributor.
		"-ingester.ring.replication-factor": "2",
		"-distributor.ring.store":           "consul",
		"-distributor.ring.consul.hostname": consul.NetworkHTTPEndpoint(),
		// Store-gateway.
		"-store-gateway.sharding-ring.store":              "consul",
		"-store-gateway.sharding-ring.consul.hostname":    consul.NetworkHTTPEndpoint(),
		"-store-gateway.sharding-ring.replication-factor": "1",
		// Compactor.
		"-compactor.ring.store":           "consul",
		"-compactor.ring.consul.hostname": consul.NetworkHTTPEndpoint(),
	})

	// Ensure Mimir successfully starts.
	mimir := e2emimir.NewSingleBinary("mimir-1", e2e.MergeFlags(DefaultSingleBinaryFlags(), flags))
	require.NoError(t, s.StartAndWaitReady(mimir))

	// Ensure proper memcached metrics are present.
	require.NoError(t, mimir.WaitSumMetricsWithOptions(e2e.GreaterOrEqual(1), []string{"thanos_cache_client_info"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "frontend-cache"),
		labels.MustNewMatcher(labels.MatchEqual, "backend", "memcached"),
	)))
	require.NoError(t, mimir.WaitSumMetricsWithOptions(e2e.GreaterOrEqual(1), []string{"thanos_cache_client_info"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "chunks-cache"),
		labels.MustNewMatcher(labels.MatchEqual, "backend", "memcached"),
	)))
	require.NoError(t, mimir.WaitSumMetricsWithOptions(e2e.GreaterOrEqual(1), []string{"thanos_cache_client_info"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "metadata-cache"),
		labels.MustNewMatcher(labels.MatchEqual, "backend", "memcached"),
	)))
}

// TestMimirCanParseIntZeroAsZeroDuration checks that integer 0 can be used as zero duration in the yaml configuration.
// When parsing config using gopkg.in/yaml.v3 this means that it should include this change: https://github.com/go-yaml/yaml/pull/876
// It is written as an acceptance test to ensure that software that vendors this (i.e., GEM) will also run this test.
func TestMimirCanParseIntZeroAsZeroDuration(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	const singleProcessConfigFile = "docs/configurations/single-process-config-blocks.yaml"

	// Use an example single process config file.
	config, err := os.ReadFile(filepath.Join(getMimirProjectDir(), singleProcessConfigFile))
	require.NoError(t, err, "unable to read config file")

	// Ensure that there's `server:` to replace in the config, otherwise we're testing nothing.
	require.Containsf(t, string(config), "server:", "Config file %s doesn't contain a 'server:' section anymore.")
	// Add a 'graceful_shutdown_timeout: 0' after 'server:'.
	config = []byte(strings.ReplaceAll(string(config), "server:", "server:\n  graceful_shutdown_timeout: 0"))
	// Write the config and use it.
	require.NoError(t, writeFileToSharedDir(s, mimirConfigFile, config))

	// Use filesystem as storage backend to run this test faster without minio.
	flags := map[string]string{
		"-common.storage.backend":        "filesystem",
		"-common.storage.filesystem.dir": "./bucket",
		"-blocks-storage.storage-prefix": "blocks",
	}

	mimir := e2emimir.NewSingleBinary("mimir-1", flags, e2emimir.WithPorts(9009, 9095), e2emimir.WithConfigFile(mimirConfigFile))
	require.NoError(t, s.StartAndWaitReady(mimir))
}
