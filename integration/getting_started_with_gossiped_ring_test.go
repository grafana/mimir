// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/integration/getting_started_with_gossiped_ring_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.
// +build requires_docker

package integration

import (
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/e2e"
	e2edb "github.com/grafana/mimir/integration/e2e/db"
	"github.com/grafana/mimir/integration/e2emimir"
)

func TestGettingStartedWithGossipedRing(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	minio := e2edb.NewMinio(9000, bucketName)
	require.NoError(t, s.StartAndWaitReady(minio))

	// Start Mimir components.
	require.NoError(t, copyFileToSharedDir(s, "docs/configuration/single-process-config-blocks-gossip-1.yaml", "config1.yaml"))
	require.NoError(t, copyFileToSharedDir(s, "docs/configuration/single-process-config-blocks-gossip-2.yaml", "config2.yaml"))

	// We don't care for storage part too much here. Both Mimir instances will write new blocks to /tmp, but that's fine.
	flags := map[string]string{
		// decrease timeouts to make test faster. should still be fine with two instances only
		"-ingester.join-after":                                     "0s", // join quickly
		"-ingester.observe-period":                                 "5s", // to avoid conflicts in tokens
		"-blocks-storage.bucket-store.sync-interval":               "1s", // sync continuously
		"-blocks-storage.backend":                                  "s3",
		"-blocks-storage.s3.bucket-name":                           bucketName,
		"-blocks-storage.s3.access-key-id":                         e2edb.MinioAccessKey,
		"-blocks-storage.s3.secret-access-key":                     e2edb.MinioSecretKey,
		"-blocks-storage.s3.endpoint":                              fmt.Sprintf("%s-minio-9000:9000", networkName),
		"-blocks-storage.s3.insecure":                              "true",
		"-store-gateway.sharding-ring.wait-stability-min-duration": "0", // start quickly
		"-store-gateway.sharding-ring.wait-stability-max-duration": "0", // start quickly
	}

	// This mimir will fail to join the cluster configured in yaml file. That's fine.
	mimir1 := e2emimir.NewSingleBinaryWithConfigFile("mimir-1", "config1.yaml", e2e.MergeFlags(flags, map[string]string{
		"-ingester.lifecycler.addr": networkName + "-mimir-1", // Ingester's hostname in docker setup
	}), "", 9109, 9195)

	mimir2 := e2emimir.NewSingleBinaryWithConfigFile("mimir-2", "config2.yaml", e2e.MergeFlags(flags, map[string]string{
		"-ingester.lifecycler.addr": networkName + "-mimir-2", // Ingester's hostname in docker setup
		"-memberlist.join":          networkName + "-mimir-1:7946",
	}), "", 9209, 9295)

	require.NoError(t, s.StartAndWaitReady(mimir1))
	require.NoError(t, s.StartAndWaitReady(mimir2))

	// Both Mimir servers should see each other.
	require.NoError(t, mimir1.WaitSumMetrics(e2e.Equals(2), "memberlist_client_cluster_members_count"))
	require.NoError(t, mimir2.WaitSumMetrics(e2e.Equals(2), "memberlist_client_cluster_members_count"))

	// Both Mimir servers should have 512 tokens for ingesters ring and 512 tokens for store-gateways ring.
	for _, ringName := range []string{"ingester", "store-gateway", "ruler"} {
		ringMatcher := labels.MustNewMatcher(labels.MatchEqual, "name", ringName)

		require.NoError(t, mimir1.WaitSumMetricsWithOptions(e2e.Equals(2*512), []string{"cortex_ring_tokens_total"}, e2e.WithLabelMatchers(ringMatcher)))
		require.NoError(t, mimir2.WaitSumMetricsWithOptions(e2e.Equals(2*512), []string{"cortex_ring_tokens_total"}, e2e.WithLabelMatchers(ringMatcher)))
	}

	// We need two "ring members" visible from both Mimir instances for ingesters
	require.NoError(t, mimir1.WaitSumMetricsWithOptions(e2e.Equals(2), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	require.NoError(t, mimir2.WaitSumMetricsWithOptions(e2e.Equals(2), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	// We need two "ring members" visible from both Mimir instances for rulers
	require.NoError(t, mimir1.WaitSumMetricsWithOptions(e2e.Equals(2), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ruler"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	require.NoError(t, mimir2.WaitSumMetricsWithOptions(e2e.Equals(2), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ruler"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	c1, err := e2emimir.NewClient(mimir1.HTTPEndpoint(), mimir1.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	c2, err := e2emimir.NewClient(mimir2.HTTPEndpoint(), mimir2.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	// Push some series to Mimir2
	now := time.Now()
	series, expectedVector := generateSeries("series_1", now)

	res, err := c2.Push(series)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	// Query the series via Mimir 1
	result, err := c1.Query("series_1", now)
	require.NoError(t, err)
	require.Equal(t, model.ValVector, result.Type())
	assert.Equal(t, expectedVector, result.(model.Vector))

	// Before flushing the blocks we expect no store-gateway has loaded any block.
	require.NoError(t, mimir1.WaitSumMetrics(e2e.Equals(0), "cortex_bucket_store_blocks_loaded"))
	require.NoError(t, mimir2.WaitSumMetrics(e2e.Equals(0), "cortex_bucket_store_blocks_loaded"))

	// Flush blocks from ingesters to the store.
	for _, instance := range []*e2emimir.MimirService{mimir1, mimir2} {
		res, err = e2e.GetRequest("http://" + instance.HTTPEndpoint() + "/flush")
		require.NoError(t, err)
		require.Equal(t, 204, res.StatusCode)
	}

	// Given store-gateway blocks sharding is enabled with the default replication factor of 3,
	// and ingestion replication factor is 1, we do expect the series has been ingested by 1
	// single ingester and so we have 1 block shipped from ingesters and loaded by both store-gateways.
	require.NoError(t, mimir1.WaitSumMetrics(e2e.Equals(1), "cortex_bucket_store_blocks_loaded"))
	require.NoError(t, mimir2.WaitSumMetrics(e2e.Equals(1), "cortex_bucket_store_blocks_loaded"))
}
