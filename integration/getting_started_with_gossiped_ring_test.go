// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/integration/getting_started_with_gossiped_ring_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.
//go:build requires_docker

package integration

import (
	"fmt"
	"testing"
	"time"

	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/e2emimir"
)

func TestGettingStartedWithGossipedRing(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	minio := e2edb.NewMinio(9000, blocksBucketName)
	require.NoError(t, s.StartAndWaitReady(minio))

	// Start Mimir components.
	require.NoError(t, copyFileToSharedDir(s, "docs/configurations/single-process-config-blocks-gossip-1.yaml", "config1.yaml"))
	require.NoError(t, copyFileToSharedDir(s, "docs/configurations/single-process-config-blocks-gossip-2.yaml", "config2.yaml"))

	// We don't care for storage part too much here. Both Mimir instances will write new blocks to /tmp, but that's fine.
	flags := map[string]string{
		// decrease timeouts to make test faster. should still be fine with two instances only
		"-ingester.ring.observe-period":                     "5s", // to avoid conflicts in tokens
		"-blocks-storage.bucket-store.sync-interval":        "1s", // sync continuously
		"-compactor.cleanup-interval":                       "1s", // update bucket index continuously
		"-blocks-storage.bucket-store.ignore-blocks-within": "0",
		"-blocks-storage.backend":                           "s3",
		"-blocks-storage.s3.bucket-name":                    blocksBucketName,
		"-blocks-storage.s3.access-key-id":                  e2edb.MinioAccessKey,
		"-blocks-storage.s3.secret-access-key":              e2edb.MinioSecretKey,
		"-blocks-storage.s3.endpoint":                       fmt.Sprintf("%s-minio-9000:9000", networkName),
		"-blocks-storage.s3.insecure":                       "true",
	}

	// This mimir will fail to join the cluster configured in yaml file. That's fine.
	mimir1 := e2emimir.NewSingleBinary("mimir-1", e2e.MergeFlags(flags, map[string]string{
		"-ingester.ring.instance-addr": networkName + "-mimir-1", // Ingester's hostname in docker setup
	}), e2emimir.WithPorts(9109, 9095), e2emimir.WithConfigFile("config1.yaml"))

	mimir2 := e2emimir.NewSingleBinary("mimir-2", e2e.MergeFlags(flags, map[string]string{
		"-ingester.ring.instance-addr": networkName + "-mimir-2", // Ingester's hostname in docker setup
		"-memberlist.join":             networkName + "-mimir-1:7946",
	}), e2emimir.WithPorts(9209, 9095), e2emimir.WithConfigFile("config2.yaml"))

	require.NoError(t, s.StartAndWaitReady(mimir1))
	require.NoError(t, s.StartAndWaitReady(mimir2))

	// Both Mimir servers should see each other.
	require.NoError(t, mimir1.WaitSumMetrics(e2e.Equals(2), "memberlist_client_cluster_members_count"))
	require.NoError(t, mimir2.WaitSumMetrics(e2e.Equals(2), "memberlist_client_cluster_members_count"))

	for _, ringName := range []string{"ingester", "store-gateway", "ruler", "compactor", "distributor"} {
		ringMatcher := labels.MustNewMatcher(labels.MatchEqual, "name", ringName)

		expectedTokens := 2 * 512 // Ingesters, store-gateways and compactors use 512 tokens by default.
		if ringName == "ruler" {
			expectedTokens = 2 * 128 // rulers use 128 tokens by default
		}
		if ringName == "distributor" {
			expectedTokens = 2 * 1 // distributors use one token only
		}

		require.NoError(t, mimir1.WaitSumMetricsWithOptions(e2e.Equals(float64(expectedTokens)), []string{"cortex_ring_tokens_total"}, e2e.WithLabelMatchers(ringMatcher)), ringName)
		require.NoError(t, mimir2.WaitSumMetricsWithOptions(e2e.Equals(float64(expectedTokens)), []string{"cortex_ring_tokens_total"}, e2e.WithLabelMatchers(ringMatcher)), ringName)
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

	runTestGettingStartedWithGossipedRing(t, mimir1, mimir2, "series_1", generateFloatSeries, 0)
	runTestGettingStartedWithGossipedRing(t, mimir1, mimir2, "hseries_1", generateHistogramSeries, 1)
}

func runTestGettingStartedWithGossipedRing(t *testing.T, mimir1 *e2emimir.MimirService, mimir2 *e2emimir.MimirService, seriesName string, genSeries generateSeriesFunc, blocksLoadedOffset float64) {
	c1, err := e2emimir.NewClient(mimir1.HTTPEndpoint(), mimir1.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	c2, err := e2emimir.NewClient(mimir2.HTTPEndpoint(), mimir2.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	// Push some series to Mimir 2
	now := time.Now()
	series, expectedVector, _ := genSeries(seriesName, now)

	res, err := c2.Push(series)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	// Query the series via Mimir 1
	result, err := c1.Query(seriesName, now)
	require.NoError(t, err)
	require.Equal(t, model.ValVector, result.Type())
	assert.Equal(t, expectedVector, result.(model.Vector))

	// Before flushing the blocks we expect no store-gateway has loaded any block.
	require.NoError(t, mimir1.WaitSumMetrics(e2e.Equals(0+blocksLoadedOffset), "cortex_bucket_store_blocks_loaded"))
	require.NoError(t, mimir2.WaitSumMetrics(e2e.Equals(0+blocksLoadedOffset), "cortex_bucket_store_blocks_loaded"))

	// Flush blocks from ingesters to the store.
	for _, instance := range []*e2emimir.MimirService{mimir1, mimir2} {
		res, err := e2e.DoGet("http://" + instance.HTTPEndpoint() + "/ingester/flush")
		require.NoError(t, err)
		require.Equal(t, 204, res.StatusCode)
	}

	// Given store-gateway blocks sharding is enabled with the default replication factor of 3,
	// and ingestion replication factor is 1, we do expect the series has been ingested by 1
	// single ingester and so we have 1 block shipped from ingesters and loaded by both store-gateways.
	require.NoError(t, mimir1.WaitSumMetrics(e2e.Equals(1+blocksLoadedOffset), "cortex_bucket_store_blocks_loaded"))
	require.NoError(t, mimir2.WaitSumMetrics(e2e.Equals(1+blocksLoadedOffset), "cortex_bucket_store_blocks_loaded"))

	// Make sure that no DNS failures occurred.
	// No actual DNS lookups are necessarily performed, so we can't really assert on that.
	mlMatcher := labels.MustNewMatcher(labels.MatchEqual, "component", "memberlist")
	require.NoError(t, mimir1.WaitSumMetricsWithOptions(e2e.Equals(0), []string{"cortex_dns_failures_total"}, e2e.WithLabelMatchers(mlMatcher)))
	require.NoError(t, mimir2.WaitSumMetricsWithOptions(e2e.Equals(0), []string{"cortex_dns_failures_total"}, e2e.WithLabelMatchers(mlMatcher)))
}
