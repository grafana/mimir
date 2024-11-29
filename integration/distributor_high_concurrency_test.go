// SPDX-License-Identifier: AGPL-3.0-only
//go:build requires_docker

package integration

import (
	"fmt"
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/e2emimir"
)

func TestDistributorHighConcurrency(t *testing.T) {
	for _, caching := range []bool{false, true} {
		for _, poolWriteReqs := range []bool{false, true} {
			t.Run(fmt.Sprintf("caching_unmarshal_data=%t, pooling_write_requests=%t", caching, poolWriteReqs), func(t *testing.T) {
				testDistributorHighConcurrency(t, caching, poolWriteReqs)
			})
		}
	}
}

func testDistributorHighConcurrency(t *testing.T, cachingUnmarshalDataEnabled bool, poolWriteRequestBuffer bool) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	t.Cleanup(s.Close)

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, blocksBucketName)
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	baseFlags := map[string]string{
		"-distributor.ingestion-tenant-shard-size": "0",
		"-ingester.ring.heartbeat-period":          "1s",
		"-ingester.out-of-order-time-window":       "0",
		"-blocks-storage.tsdb.block-ranges-period": "2h", // This is changed by BlocksStorageFlags to 1m, but we don't want to run any compaction in our test.

		"-timeseries-unmarshal-caching-optimization-enabled": strconv.FormatBool(cachingUnmarshalDataEnabled),
		"-distributor.write-requests-buffer-pooling-enabled": strconv.FormatBool(poolWriteRequestBuffer),
	}

	flags := mergeFlags(
		BlocksStorageFlags(),
		BlocksStorageS3Flags(),
		baseFlags,
	)

	// Start Mimir components.
	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
	ingester := e2emimir.NewIngester("ingester", consul.NetworkHTTPEndpoint(), flags)
	querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.StartAndWaitReady(distributor, ingester, querier))

	// Wait until distributor has updated the ring.
	require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	// Wait until querier has updated the ring.
	require.NoError(t, querier.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	client, err := e2emimir.NewClient(distributor.HTTPEndpoint(), querier.HTTPEndpoint(), "", "", userID)
	require.NoError(t, err)

	// Start N writers, each writing 1 series.
	// All samples are within 20mins time range.
	// Each writer then queries for its own series.

	const (
		writers   = 25
		samples   = 1000
		timeRange = 20 * time.Minute
	)

	writeEnd := time.Now().UTC().Truncate(timeRange)
	writeStart := writeEnd.Add(-timeRange)
	step := timeRange / samples

	wg := sync.WaitGroup{}
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()

			serName := fmt.Sprintf("series_%d", i)

			r := rand.New(rand.NewSource(time.Now().UnixMilli()))

			// Build list of pushed samples, so that we can compare them via a query after writing.
			exp := model.Matrix{{Metric: model.Metric{"__name__": model.LabelValue(serName)}}}

			added := 0
			for ts := writeStart; ts.Before(writeEnd); ts = ts.Add(step) {
				sam := r.Float64()

				ser := []prompb.TimeSeries{
					{
						Labels:  []prompb.Label{{Name: "__name__", Value: serName}},
						Samples: []prompb.Sample{{Timestamp: ts.UnixMilli(), Value: sam}},
					},
				}

				exp[0].Values = append(exp[0].Values, model.SamplePair{Timestamp: model.Time(ts.UnixMilli()), Value: model.SampleValue(sam)})

				res, err := client.Push(ser)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, res.StatusCode, res.Status)

				added++
			}

			require.Equal(t, samples, added)

			// query all samples back
			query := fmt.Sprintf("%s[%s]", serName, model.Duration(timeRange+time.Millisecond)) // Add millisecond to ensure we get the first point (ranges are left-open).
			result, err := client.Query(query, writeEnd)
			require.NoError(t, err)
			require.Equal(t, exp, result)
		}()
	}

	wg.Wait()

	client.CloseIdleConnections()
}
