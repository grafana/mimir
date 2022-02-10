// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/integration/backward_compatibility_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.
//go:build requires_docker
// +build requires_docker

package integration

import (
	"fmt"
	"os"
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

// previousVersionImages returns a list of previous image version to test backwards
// compatibility against. If MIMIR_PREVIOUS_IMAGES is set to a comma separted list of image versions,
// then those will be used instead of the default versions. Note that the overriding of flags
// is not currently possible when overriding the previous image versions via the environment variable.
func previousVersionImages() map[string]e2emimir.FlagMapper {
	if os.Getenv("MIMIR_PREVIOUS_IMAGES") != "" {
		overrideImageVersions := os.Getenv("MIMIR_PREVIOUS_IMAGES")
		previousVersionImages := map[string]e2emimir.FlagMapper{}

		// Overriding of flags is not currently supported when overriding the list of images,
		// so set all override functions to nil
		for _, image := range strings.Split(overrideImageVersions, ",") {
			previousVersionImages[image] = func(flags map[string]string) map[string]string {
				flags["-store.engine"] = "blocks"
				flags["-server.http-listen-port"] = "8080"
				flags["-store-gateway.sharding-enabled"] = "true"
				return flags
			}
		}

		return previousVersionImages
	}

	return DefaultPreviousVersionImages
}

func TestBackwardCompatibility(t *testing.T) {
	for previousImage, oldFlagsMapper := range previousVersionImages() {
		t.Run(fmt.Sprintf("Backward compatibility upgrading from %s", previousImage), func(t *testing.T) {
			runBackwardCompatibilityTest(t, previousImage, oldFlagsMapper)
		})
	}
}

func TestNewDistributorsCanPushToOldIngestersWithReplication(t *testing.T) {
	for previousImage, oldFlagsMapper := range previousVersionImages() {
		t.Run(fmt.Sprintf("Backward compatibility upgrading from %s", previousImage), func(t *testing.T) {
			runNewDistributorsCanPushToOldIngestersWithReplication(t, previousImage, oldFlagsMapper)
		})
	}
}

func runBackwardCompatibilityTest(t *testing.T, previousImage string, oldFlagsMapper e2emimir.FlagMapper) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	flagTSDBPath := map[string]string{
		"-blocks-storage.tsdb.dir": e2e.ContainerSharedDir + "/tsdb-shared",
	}

	flags := mergeFlags(
		BlocksStorageFlags(),
		flagTSDBPath,
	)

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	// Start other Mimir components (ingester running on previous version).
	ingester := e2emimir.NewIngester("ingester-old", consul.NetworkHTTPEndpoint(), flags, previousImage, e2emimir.WithFlagMapper(oldFlagsMapper))
	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), BlocksStorageFlags(), "")
	assert.NoError(t, s.StartAndWaitReady(distributor, ingester))

	// Wait until the distributor has updated the ring.
	// The distributor should have 512 tokens for the ingester ring and 1 for the distributor ring
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512+1), "cortex_ring_tokens_total"))

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

	ingester = e2emimir.NewIngester("ingester-new", consul.NetworkHTTPEndpoint(), flags, "")
	require.NoError(t, s.StartAndWaitReady(ingester))

	// Wait until the distributor has updated the ring.
	// The distributor should have 512 tokens for the ingester ring and 1 for the distributor ring
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512+1), "cortex_ring_tokens_total"))

	checkQueries(t,
		consul,
		expectedVector,
		previousImage,
		flags,
		oldFlagsMapper,
		now,
		s,
		1,
	)
}

// Check for issues like https://github.com/cortexproject/cortex/issues/2356
func runNewDistributorsCanPushToOldIngestersWithReplication(t *testing.T, previousImage string, oldFlagsMapper e2emimir.FlagMapper) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	flags := mergeFlags(BlocksStorageFlags(), map[string]string{
		"-distributor.replication-factor": "3",
	})

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	// Start other Mimir components (ingester running on previous version).
	ingester1 := e2emimir.NewIngester("ingester-1", consul.NetworkHTTPEndpoint(), flags, previousImage, e2emimir.WithFlagMapper(oldFlagsMapper))
	ingester2 := e2emimir.NewIngester("ingester-2", consul.NetworkHTTPEndpoint(), flags, previousImage, e2emimir.WithFlagMapper(oldFlagsMapper))
	ingester3 := e2emimir.NewIngester("ingester-3", consul.NetworkHTTPEndpoint(), flags, previousImage, e2emimir.WithFlagMapper(oldFlagsMapper))
	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags, "")
	require.NoError(t, s.StartAndWaitReady(distributor, ingester1, ingester2, ingester3))

	// Wait until the distributor has updated the ring.
	// The distributor should have 512 tokens for each ingester and 1 for the distributor ring
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(3*512+1), "cortex_ring_tokens_total"))

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
		flags,
		oldFlagsMapper,
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
	flags map[string]string,
	oldFlagsMapper e2emimir.FlagMapper,
	now time.Time,
	s *e2e.Scenario,
	numIngesters int,
) {
	cases := map[string]struct {
		queryFrontendImage      string
		queryFrontendFlags      map[string]string
		queryFrontendFlagMapper e2emimir.FlagMapper
		querierImage            string
		querierFlags            map[string]string
		querierFlagMapper       e2emimir.FlagMapper
	}{
		"old query-frontend, new querier": {
			queryFrontendImage:      previousImage,
			queryFrontendFlags:      flags,
			queryFrontendFlagMapper: oldFlagsMapper,
			querierImage:            "",
			querierFlags:            flags,
			querierFlagMapper:       e2emimir.NoopFlagMapper,
		},
		"new query-frontend, old querier": {
			queryFrontendImage:      "",
			queryFrontendFlags:      flags,
			queryFrontendFlagMapper: e2emimir.NoopFlagMapper,
			querierImage:            previousImage,
			querierFlags:            flags,
			querierFlagMapper:       oldFlagsMapper,
		},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			// Start query-frontend.
			queryFrontend := e2emimir.NewQueryFrontend("query-frontend", c.queryFrontendFlags, c.queryFrontendImage, e2emimir.WithFlagMapper(c.queryFrontendFlagMapper))
			require.NoError(t, s.Start(queryFrontend))
			defer func() {
				require.NoError(t, s.Stop(queryFrontend))
			}()

			// Start querier.
			querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), e2e.MergeFlagsWithoutRemovingEmpty(c.querierFlags, map[string]string{
				"-querier.frontend-address": queryFrontend.NetworkGRPCEndpoint(),
			}), c.querierImage, e2emimir.WithFlagMapper(c.querierFlagMapper))

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
