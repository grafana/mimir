// SPDX-License-Identifier: AGPL-3.0-only
//go:build requires_docker
// +build requires_docker

package integration

import (
	"testing"

	"github.com/grafana/e2e"
	e2ecache "github.com/grafana/e2e/cache"
	e2edb "github.com/grafana/e2e/db"
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

	flags := mergeFlags(BlocksStorageFlags(), map[string]string{
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
		"-ingester.ring.store":           "consul",
		"-ingester.ring.consul.hostname": consul.NetworkHTTPEndpoint(),
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
		"-compactor.cleanup-interval":     "2s", // Update bucket index often.
	})

	// Ensure Mimir successfully starts.
	mimir := e2emimir.NewSingleBinary("mimir-1", e2e.MergeFlags(DefaultSingleBinaryFlags(), flags))
	require.NoError(t, s.StartAndWaitReady(mimir))
}
