// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/integration/backward_compatibility_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.
// +build requires_docker

package integration

import (
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/e2e"
	e2edb "github.com/grafana/mimir/integration/e2e/db"
	"github.com/grafana/mimir/integration/e2emimir"
)

var (
	// If you change the image tag, remember to update it in the preloading done
	// by GitHub Actions too (see .github/workflows/test-build-deploy.yml).
	//nolint:unused
	previousVersionImages = map[string]func(map[string]string) map[string]string{
		"quay.io/cortexproject/cortex:v1.8.0":  preCortex110Flags,
		"quay.io/cortexproject/cortex:v1.9.0":  preCortex110Flags,
		"quay.io/cortexproject/cortex:v1.10.0": nil,
	}
)

func preCortex110Flags(flags map[string]string) map[string]string {
	return e2e.MergeFlagsWithoutRemovingEmpty(flags, map[string]string{
		// Store-gateway "wait ring stability" has been introduced in 1.10.0
		"-store-gateway.sharding-ring.wait-stability-min-duration": "",
		"-store-gateway.sharding-ring.wait-stability-max-duration": "",
	})
}

func TestBackwardCompatibility(t *testing.T) {
	for previousImage, flagsFn := range previousVersionImages {
		t.Run(fmt.Sprintf("Backward compatibility upgrading from %s", previousImage), func(t *testing.T) {
			flags := BlocksStorageFlags()
			if flagsFn != nil {
				flags = flagsFn(flags)
			}

			runBackwardCompatibilityTest(t, previousImage, flags)
		})
	}
}

func TestNewDistributorsCanPushToOldIngestersWithReplication(t *testing.T) {
	for previousImage, flagsFn := range previousVersionImages {
		t.Run(fmt.Sprintf("Backward compatibility upgrading from %s", previousImage), func(t *testing.T) {
			flags := BlocksStorageFlags()
			if flagsFn != nil {
				flags = flagsFn(flags)
			}

			runNewDistributorsCanPushToOldIngestersWithReplication(t, previousImage, flags)
		})
	}
}

func runBackwardCompatibilityTest(t *testing.T, previousImage string, flagsForOldImage map[string]string) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	flagTSDBPath := map[string]string{
		"-blocks-storage.tsdb.dir": e2e.ContainerSharedDir + "/tsdb-shared",
	}

	flagsNew := mergeFlags(
		BlocksStorageFlags(),
		flagTSDBPath,
	)

	flagsForOldImage = mergeFlags(
		flagsForOldImage,
		flagTSDBPath,
	)

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, flagsNew["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	// Start other Mimir components (ingester running on previous version).
	ingester := e2emimir.NewIngester("ingester-old", consul.NetworkHTTPEndpoint(), flagsForOldImage, previousImage)
	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), BlocksStorageFlags(), "")
	assert.NoError(t, s.StartAndWaitReady(distributor, ingester))

	// Wait until the distributor has updated the ring.
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

	// Push some series to Mimir.
	now := time.Now()
	series, expectedVector := generateSeries("series_1", now)

	c, err := e2emimir.NewClient(distributor.HTTPEndpoint(), "", "", "", "user-1")
	require.NoError(t, err)

	res, err := c.Push(series)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	// Stop ingester on old version
	require.NoError(t, s.Stop(ingester))

	ingester = e2emimir.NewIngester("ingester-new", consul.NetworkHTTPEndpoint(), flagsNew, "")
	require.NoError(t, s.StartAndWaitReady(ingester))

	// Wait until the distributor has updated the ring.
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

	checkQueries(t,
		consul,
		expectedVector,
		previousImage,
		flagsForOldImage,
		BlocksStorageFlags(),
		now,
		s,
		1,
	)
}

// Check for issues like https://github.com/cortexproject/cortex/issues/2356
func runNewDistributorsCanPushToOldIngestersWithReplication(t *testing.T, previousImage string, flagsForPreviousImage map[string]string) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	flagsForNewImage := mergeFlags(BlocksStorageFlags(), map[string]string{
		"-distributor.replication-factor": "3",
	})

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, flagsForNewImage["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	// Start other Mimir components (ingester running on previous version).
	ingester1 := e2emimir.NewIngester("ingester-1", consul.NetworkHTTPEndpoint(), flagsForPreviousImage, previousImage)
	ingester2 := e2emimir.NewIngester("ingester-2", consul.NetworkHTTPEndpoint(), flagsForPreviousImage, previousImage)
	ingester3 := e2emimir.NewIngester("ingester-3", consul.NetworkHTTPEndpoint(), flagsForPreviousImage, previousImage)
	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flagsForNewImage, "")
	require.NoError(t, s.StartAndWaitReady(distributor, ingester1, ingester2, ingester3))

	// Wait until the distributor has updated the ring.
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(1536), "cortex_ring_tokens_total"))

	// Push some series to Mimir.
	now := time.Now()
	series, expectedVector := generateSeries("series_1", now)

	c, err := e2emimir.NewClient(distributor.HTTPEndpoint(), "", "", "", "user-1")
	require.NoError(t, err)

	res, err := c.Push(series)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	checkQueries(t, consul,
		expectedVector,
		previousImage,
		flagsForPreviousImage,
		flagsForNewImage,
		now,
		s,
		3,
	)
}

func checkQueries(
	t *testing.T,
	consul *e2e.HTTPService,
	expectedVector model.Vector,
	previousImage string,
	flagsForOldImage, flagsForNewImage map[string]string,
	now time.Time,
	s *e2e.Scenario,
	numIngesters int,
) {
	cases := map[string]struct {
		queryFrontendImage string
		queryFrontendFlags map[string]string
		querierImage       string
		querierFlags       map[string]string
	}{
		"old query-frontend, new querier": {
			queryFrontendImage: previousImage,
			queryFrontendFlags: flagsForOldImage,
			querierImage:       "",
			querierFlags:       flagsForNewImage,
		},
		"new query-frontend, old querier": {
			queryFrontendImage: "",
			queryFrontendFlags: flagsForNewImage,
			querierImage:       previousImage,
			querierFlags:       flagsForOldImage,
		},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			// Start query-frontend.
			queryFrontend := e2emimir.NewQueryFrontend("query-frontend", c.queryFrontendFlags, c.queryFrontendImage)
			require.NoError(t, s.Start(queryFrontend))
			defer func() {
				require.NoError(t, s.Stop(queryFrontend))
			}()

			// Start querier.
			querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), e2e.MergeFlagsWithoutRemovingEmpty(c.querierFlags, map[string]string{
				"-querier.frontend-address": queryFrontend.NetworkGRPCEndpoint(),
			}), c.querierImage)

			require.NoError(t, s.Start(querier))
			defer func() {
				require.NoError(t, s.Stop(querier))
			}()

			// Wait until querier and query-frontend are ready, and the querier has updated the ring.
			require.NoError(t, s.WaitReady(querier, queryFrontend))
			require.NoError(t, querier.WaitSumMetrics(e2e.Equals(float64(numIngesters*512)), "cortex_ring_tokens_total"))

			// Query the series.
			for _, endpoint := range []string{queryFrontend.HTTPEndpoint(), querier.HTTPEndpoint()} {
				c, err := e2emimir.NewClient("", endpoint, "", "", "user-1")
				require.NoError(t, err)

				result, err := c.Query("series_1", now)
				require.NoError(t, err)
				require.Equal(t, model.ValVector, result.Type())
				assert.Equal(t, expectedVector, result.(model.Vector))
			}
		})
	}
}
