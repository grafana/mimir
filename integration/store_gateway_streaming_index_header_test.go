// SPDX-License-Identifier: AGPL-3.0-only
//go:build requires_docker
// +build requires_docker

package integration

import (
	"fmt"
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

func TestStoreGatewayIndexHeaderReaders(t *testing.T) {
	cases := map[string]bool{
		"mmap reader":      false,
		"streaming reader": true,
	}

	for name, streamingReaderEnabled := range cases {
		t.Run(name, func(t *testing.T) {
			s, err := e2e.NewScenario(networkName)
			require.NoError(t, err)
			defer s.Close()

			minio := e2edb.NewMinio(9000, mimirBucketName)
			require.NoError(t, s.StartAndWaitReady(minio))

			flags := mergeFlags(
				BlocksStorageFlags(),
				CommonStorageBackendFlags(),
				map[string]string{
					"-blocks-storage.bucket-store.index-header.stream-reader-enabled": fmt.Sprintf("%v", streamingReaderEnabled),

					"-ingester.ring.replication-factor": "1",

					// Frequently compact and ship blocks to storage so we can query them through the store gateway.
					"-blocks-storage.tsdb.block-ranges-period":          "2s",
					"-blocks-storage.tsdb.ship-interval":                "1s",
					"-blocks-storage.tsdb.retention-period":             "3s",
					"-blocks-storage.tsdb.head-compaction-idle-timeout": "1s",
				},
			)

			mimirInstance := e2emimir.NewSingleBinary("mimir-single-binary-1", flags)
			require.NoError(t, s.StartAndWaitReady(mimirInstance))

			c, err := e2emimir.NewClient(mimirInstance.HTTPEndpoint(), mimirInstance.HTTPEndpoint(), mimirInstance.HTTPEndpoint(), mimirInstance.HTTPEndpoint(), "user-1")
			require.NoError(t, err)

			// Wait for the ingester to join the ring and become active - this prevents "empty ring" errors later when we try to query data.
			require.NoError(t, mimirInstance.WaitSumMetricsWithOptions(
				e2e.Equals(1),
				[]string{"cortex_ring_members"},
				e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"), labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE")),
			))

			// Push some data to the cluster.
			now := time.Now()
			series, expectedVector, expectedMatrix := generateSeries("test_series_1", now, prompb.Label{Name: "foo", Value: "bar"})

			res, err := c.Push(series)
			require.NoError(t, err)
			require.Equal(t, 200, res.StatusCode)

			// Wait until the TSDB head is shipped to storage, removed from the ingester, and loaded by the
			// store-gateway to ensure we're querying the store-gateway (and thus exercising the index-header).
			require.NoError(t, mimirInstance.WaitSumMetrics(e2e.GreaterOrEqual(1), "cortex_ingester_shipper_uploads_total"))
			require.NoError(t, mimirInstance.WaitSumMetrics(e2e.Equals(0), "cortex_ingester_memory_series"))
			require.NoError(t, mimirInstance.WaitSumMetrics(e2e.GreaterOrEqual(1), "cortex_bucket_store_blocks_loaded"))

			// Verify we can read the data we just pushed, both with an instant query and a range query.
			queryResult, err := c.Query("test_series_1", now)
			require.NoError(t, err)
			require.Equal(t, model.ValVector, queryResult.Type())
			require.Equal(t, expectedVector, queryResult.(model.Vector))

			rangeResult, err := c.QueryRange("test_series_1", now.Add(-5*time.Minute), now, 15*time.Second)
			require.NoError(t, err)
			require.Equal(t, model.ValMatrix, rangeResult.Type())
			require.Equal(t, expectedMatrix, rangeResult.(model.Matrix))

			// Verify we can retrieve the labels we just pushed.
			labelValues, err := c.LabelValues("foo", prometheusMinTime, prometheusMaxTime, nil)
			require.NoError(t, err)
			require.Equal(t, model.LabelValues{"bar"}, labelValues)

			labelNames, err := c.LabelNames(prometheusMinTime, prometheusMaxTime)
			require.NoError(t, err)
			require.Equal(t, []string{"__name__", "foo"}, labelNames)
		})
	}
}
