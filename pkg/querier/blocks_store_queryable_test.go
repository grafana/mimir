// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/blocks_store_queryable_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querier

import (
	"context"
	crand "crypto/rand"
	"io"
	"math"
	"math/rand"
	"net"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/types"
	gogoStatus "github.com/gogo/status"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/grpcclient"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/user"
	"github.com/grafana/regexp"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/storegateway"
	"github.com/grafana/mimir/pkg/storegateway/hintspb"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/globalerror"
	"github.com/grafana/mimir/pkg/util/limiter"
	util_log "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/test"
)

func TestBlocksStoreQuerier_Select(t *testing.T) {
	const (
		metricName = "test_metric"
		minT       = int64(10)
		maxT       = int64(20)
	)

	var (
		block1           = ulid.MustNew(1, nil)
		block2           = ulid.MustNew(2, nil)
		block3           = ulid.MustNew(3, nil)
		block4           = ulid.MustNew(4, nil)
		metricNameLabel  = labels.FromStrings(model.MetricNameLabel, metricName)
		series1Label     = labels.FromStrings(model.MetricNameLabel, metricName, "series", "1")
		series2Label     = labels.FromStrings(model.MetricNameLabel, metricName, "series", "2")
		noOpQueryLimiter = limiter.NewQueryLimiter(0, 0, 0, 0, nil)
	)

	type valueResult struct {
		t  int64
		v  float64
		fh *histogram.FloatHistogram
	}

	type seriesResult struct {
		lbls   labels.Labels
		values []valueResult
	}

	tests := map[string]struct {
		finderResult      bucketindex.Blocks
		finderErr         error
		storeSetResponses []interface{}
		limits            BlocksStoreLimits
		queryLimiter      *limiter.QueryLimiter
		expectedSeries    []seriesResult
		expectedErr       error
		expectedMetrics   string
		queryShardID      string
	}{
		"no block in the storage matching the query time range": {
			finderResult: nil,
			limits:       &blocksStoreLimitsMock{},
			queryLimiter: noOpQueryLimiter,
			expectedErr:  nil,
		},
		"error while finding blocks matching the query time range": {
			finderErr:    errors.New("unable to find blocks"),
			limits:       &blocksStoreLimitsMock{},
			queryLimiter: noOpQueryLimiter,
			expectedErr:  errors.New("unable to find blocks"),
		},
		"error while getting clients to query the store-gateway": {
			finderResult: bucketindex.Blocks{
				{ID: block1},
				{ID: block2},
			},
			storeSetResponses: []interface{}{
				errors.New("no client found"),
			},
			limits:       &blocksStoreLimitsMock{},
			queryLimiter: noOpQueryLimiter,
			expectedErr:  errors.New("no client found"),
		},
		"a single store-gateway instance holds the required blocks (single returned series)": {
			finderResult: bucketindex.Blocks{
				{ID: block1},
				{ID: block2},
			},
			storeSetResponses: []interface{}{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesResponses: newSeriesResponseBuilder().
						addValue(metricNameLabel, minT, 1).
						addValue(metricNameLabel, minT+1, 2).
						addBlocks(block1, block2).
						addFetchedIndexBytes(50).
						build(),
					}: {block1, block2},
				},
			},
			limits:       &blocksStoreLimitsMock{},
			queryLimiter: noOpQueryLimiter,
			expectedSeries: []seriesResult{
				{
					lbls: metricNameLabel,
					values: []valueResult{
						{t: minT, v: 1},
						{t: minT + 1, v: 2},
					},
				},
			},
			expectedMetrics: `
				# HELP cortex_querier_blocks_found_total Number of blocks found based on query time range.
				# TYPE cortex_querier_blocks_found_total counter
				cortex_querier_blocks_found_total 2

				# HELP cortex_querier_blocks_queried_total Number of blocks queried to satisfy query. Compared to blocks found, some blocks may have been filtered out thanks to query and compactor sharding.
				# TYPE cortex_querier_blocks_queried_total counter
				cortex_querier_blocks_queried_total 2

				# HELP cortex_querier_blocks_with_compactor_shard_but_incompatible_query_shard_total Blocks that couldn't be checked for query and compactor sharding optimization due to incompatible shard counts.
				# TYPE cortex_querier_blocks_with_compactor_shard_but_incompatible_query_shard_total counter
				cortex_querier_blocks_with_compactor_shard_but_incompatible_query_shard_total 0

				# HELP cortex_querier_query_storegateway_chunks_total Number of chunks received from store gateways at query time.
				# TYPE cortex_querier_query_storegateway_chunks_total counter
				cortex_querier_query_storegateway_chunks_total 2

				# HELP cortex_querier_storegateway_instances_hit_per_query Number of store-gateway instances hit for a single query.
				# TYPE cortex_querier_storegateway_instances_hit_per_query histogram
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="0"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="1"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="2"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="4"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="8"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="16"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="32"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="64"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="128"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="256"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="512"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="1024"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="2048"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="+Inf"} 1
				cortex_querier_storegateway_instances_hit_per_query_sum 1
				cortex_querier_storegateway_instances_hit_per_query_count 1

				# HELP cortex_querier_storegateway_refetches_per_query Number of re-fetches attempted while querying store-gateway instances due to missing blocks.
				# TYPE cortex_querier_storegateway_refetches_per_query histogram
				cortex_querier_storegateway_refetches_per_query_bucket{le="0"} 1
				cortex_querier_storegateway_refetches_per_query_bucket{le="1"} 1
				cortex_querier_storegateway_refetches_per_query_bucket{le="2"} 1
				cortex_querier_storegateway_refetches_per_query_bucket{le="+Inf"} 1
				cortex_querier_storegateway_refetches_per_query_sum 0
				cortex_querier_storegateway_refetches_per_query_count 1

				# HELP cortex_querier_blocks_consistency_checks_failed_total Total number of queries that failed consistency checks. A failed consistency check means that some of at least one block which had to be queried wasn't present in any of the store-gateways.
				# TYPE cortex_querier_blocks_consistency_checks_failed_total counter
				cortex_querier_blocks_consistency_checks_failed_total 0

				# HELP cortex_querier_blocks_consistency_checks_total Total number of queries that needed to run with consistency checks. A consistency check is required when querying blocks from store-gateways to make sure that all blocks are queried.
				# TYPE cortex_querier_blocks_consistency_checks_total counter
				cortex_querier_blocks_consistency_checks_total 1
			`,
		},
		"a single store-gateway instance holds the required blocks (single returned series) - multiple chunks per series for stats": {
			finderResult: bucketindex.Blocks{
				{ID: block1},
				{ID: block2},
			},
			storeSetResponses: []interface{}{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesResponses: newSeriesResponseBuilder().
						addChunks(metricNameLabel,
							createAggrChunkWithSamples(promql.FPoint{T: minT, F: 1}),
							createAggrChunkWithSamples(promql.FPoint{T: minT + 1, F: 2}),
						).
						addBlocks(block1, block2).
						addFetchedIndexBytes(50).
						build(),
					}: {block1, block2},
				},
			},
			limits:       &blocksStoreLimitsMock{},
			queryLimiter: noOpQueryLimiter,
			expectedSeries: []seriesResult{
				{
					lbls: metricNameLabel,
					values: []valueResult{
						{t: minT, v: 1},
						{t: minT + 1, v: 2},
					},
				},
			},
			expectedMetrics: `
				# HELP cortex_querier_blocks_found_total Number of blocks found based on query time range.
				# TYPE cortex_querier_blocks_found_total counter
				cortex_querier_blocks_found_total 2

				# HELP cortex_querier_blocks_queried_total Number of blocks queried to satisfy query. Compared to blocks found, some blocks may have been filtered out thanks to query and compactor sharding.
				# TYPE cortex_querier_blocks_queried_total counter
				cortex_querier_blocks_queried_total 2

				# HELP cortex_querier_blocks_with_compactor_shard_but_incompatible_query_shard_total Blocks that couldn't be checked for query and compactor sharding optimization due to incompatible shard counts.
				# TYPE cortex_querier_blocks_with_compactor_shard_but_incompatible_query_shard_total counter
				cortex_querier_blocks_with_compactor_shard_but_incompatible_query_shard_total 0

				# HELP cortex_querier_query_storegateway_chunks_total Number of chunks received from store gateways at query time.
				# TYPE cortex_querier_query_storegateway_chunks_total counter
				cortex_querier_query_storegateway_chunks_total 2

				# HELP cortex_querier_storegateway_instances_hit_per_query Number of store-gateway instances hit for a single query.
				# TYPE cortex_querier_storegateway_instances_hit_per_query histogram
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="0"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="1"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="2"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="4"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="8"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="16"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="32"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="64"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="128"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="256"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="512"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="1024"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="2048"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="+Inf"} 1
				cortex_querier_storegateway_instances_hit_per_query_sum 1
				cortex_querier_storegateway_instances_hit_per_query_count 1

				# HELP cortex_querier_storegateway_refetches_per_query Number of re-fetches attempted while querying store-gateway instances due to missing blocks.
				# TYPE cortex_querier_storegateway_refetches_per_query histogram
				cortex_querier_storegateway_refetches_per_query_bucket{le="0"} 1
				cortex_querier_storegateway_refetches_per_query_bucket{le="1"} 1
				cortex_querier_storegateway_refetches_per_query_bucket{le="2"} 1
				cortex_querier_storegateway_refetches_per_query_bucket{le="+Inf"} 1
				cortex_querier_storegateway_refetches_per_query_sum 0
				cortex_querier_storegateway_refetches_per_query_count 1

				# HELP cortex_querier_blocks_consistency_checks_failed_total Total number of queries that failed consistency checks. A failed consistency check means that some of at least one block which had to be queried wasn't present in any of the store-gateways.
				# TYPE cortex_querier_blocks_consistency_checks_failed_total counter
				cortex_querier_blocks_consistency_checks_failed_total 0

				# HELP cortex_querier_blocks_consistency_checks_total Total number of queries that needed to run with consistency checks. A consistency check is required when querying blocks from store-gateways to make sure that all blocks are queried.
				# TYPE cortex_querier_blocks_consistency_checks_total counter
				cortex_querier_blocks_consistency_checks_total 1
			`,
		},
		"a single store-gateway instance holds the required blocks (multiple returned series)": {
			finderResult: bucketindex.Blocks{
				{ID: block1},
				{ID: block2},
			},
			storeSetResponses: []interface{}{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesResponses: newSeriesResponseBuilder().
						addValue(series1Label, minT, 1).
						addValue(series1Label, minT+1, 2).
						addValue(series2Label, minT, 3).
						addBlocks(block1, block2).
						build(),
					}: {block1, block2},
				},
			},
			limits:       &blocksStoreLimitsMock{},
			queryLimiter: noOpQueryLimiter,
			expectedSeries: []seriesResult{
				{
					lbls: series1Label,
					values: []valueResult{
						{t: minT, v: 1},
						{t: minT + 1, v: 2},
					},
				}, {
					lbls: series2Label,
					values: []valueResult{
						{t: minT, v: 3},
					},
				},
			},
			expectedMetrics: `
				# HELP cortex_querier_blocks_found_total Number of blocks found based on query time range.
				# TYPE cortex_querier_blocks_found_total counter
				cortex_querier_blocks_found_total 2

				# HELP cortex_querier_blocks_queried_total Number of blocks queried to satisfy query. Compared to blocks found, some blocks may have been filtered out thanks to query and compactor sharding.
				# TYPE cortex_querier_blocks_queried_total counter
				cortex_querier_blocks_queried_total 2

				# HELP cortex_querier_blocks_with_compactor_shard_but_incompatible_query_shard_total Blocks that couldn't be checked for query and compactor sharding optimization due to incompatible shard counts.
				# TYPE cortex_querier_blocks_with_compactor_shard_but_incompatible_query_shard_total counter
				cortex_querier_blocks_with_compactor_shard_but_incompatible_query_shard_total 0

				# HELP cortex_querier_query_storegateway_chunks_total Number of chunks received from store gateways at query time.
				# TYPE cortex_querier_query_storegateway_chunks_total counter
				cortex_querier_query_storegateway_chunks_total 3

				# HELP cortex_querier_storegateway_instances_hit_per_query Number of store-gateway instances hit for a single query.
				# TYPE cortex_querier_storegateway_instances_hit_per_query histogram
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="0"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="1"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="2"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="4"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="8"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="16"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="32"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="64"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="128"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="256"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="512"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="1024"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="2048"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="+Inf"} 1
				cortex_querier_storegateway_instances_hit_per_query_sum 1
				cortex_querier_storegateway_instances_hit_per_query_count 1

				# HELP cortex_querier_storegateway_refetches_per_query Number of re-fetches attempted while querying store-gateway instances due to missing blocks.
				# TYPE cortex_querier_storegateway_refetches_per_query histogram
				cortex_querier_storegateway_refetches_per_query_bucket{le="0"} 1
				cortex_querier_storegateway_refetches_per_query_bucket{le="1"} 1
				cortex_querier_storegateway_refetches_per_query_bucket{le="2"} 1
				cortex_querier_storegateway_refetches_per_query_bucket{le="+Inf"} 1
				cortex_querier_storegateway_refetches_per_query_sum 0
				cortex_querier_storegateway_refetches_per_query_count 1

				# HELP cortex_querier_blocks_consistency_checks_failed_total Total number of queries that failed consistency checks. A failed consistency check means that some of at least one block which had to be queried wasn't present in any of the store-gateways.
				# TYPE cortex_querier_blocks_consistency_checks_failed_total counter
				cortex_querier_blocks_consistency_checks_failed_total 0

				# HELP cortex_querier_blocks_consistency_checks_total Total number of queries that needed to run with consistency checks. A consistency check is required when querying blocks from store-gateways to make sure that all blocks are queried.
				# TYPE cortex_querier_blocks_consistency_checks_total counter
				cortex_querier_blocks_consistency_checks_total 1
			`,
		},
		"multiple store-gateway instances holds the required blocks without overlapping series (single returned series)": {
			finderResult: bucketindex.Blocks{
				{ID: block1},
				{ID: block2},
			},
			storeSetResponses: []interface{}{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesResponses: newSeriesResponseBuilder().
						addValue(metricNameLabel, minT, 1).
						addBlocks(block1).
						build(),
					}: {block1},
					&storeGatewayClientMock{remoteAddr: "2.2.2.2", mockedSeriesResponses: newSeriesResponseBuilder().
						addValue(metricNameLabel, minT+1, 2).
						addBlocks(block2).
						build(),
					}: {block2},
				},
			},
			limits:       &blocksStoreLimitsMock{},
			queryLimiter: noOpQueryLimiter,
			expectedSeries: []seriesResult{
				{
					lbls: metricNameLabel,
					values: []valueResult{
						{t: minT, v: 1},
						{t: minT + 1, v: 2},
					},
				},
			},
			expectedMetrics: `
				# HELP cortex_querier_blocks_found_total Number of blocks found based on query time range.
				# TYPE cortex_querier_blocks_found_total counter
				cortex_querier_blocks_found_total 2

				# HELP cortex_querier_blocks_queried_total Number of blocks queried to satisfy query. Compared to blocks found, some blocks may have been filtered out thanks to query and compactor sharding.
				# TYPE cortex_querier_blocks_queried_total counter
				cortex_querier_blocks_queried_total 2

				# HELP cortex_querier_blocks_with_compactor_shard_but_incompatible_query_shard_total Blocks that couldn't be checked for query and compactor sharding optimization due to incompatible shard counts.
				# TYPE cortex_querier_blocks_with_compactor_shard_but_incompatible_query_shard_total counter
				cortex_querier_blocks_with_compactor_shard_but_incompatible_query_shard_total 0

				# HELP cortex_querier_query_storegateway_chunks_total Number of chunks received from store gateways at query time.
				# TYPE cortex_querier_query_storegateway_chunks_total counter
				cortex_querier_query_storegateway_chunks_total 2

				# HELP cortex_querier_storegateway_instances_hit_per_query Number of store-gateway instances hit for a single query.
				# TYPE cortex_querier_storegateway_instances_hit_per_query histogram
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="0"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="1"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="2"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="4"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="8"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="16"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="32"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="64"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="128"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="256"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="512"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="1024"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="2048"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="+Inf"} 1
				cortex_querier_storegateway_instances_hit_per_query_sum 2
				cortex_querier_storegateway_instances_hit_per_query_count 1

				# HELP cortex_querier_storegateway_refetches_per_query Number of re-fetches attempted while querying store-gateway instances due to missing blocks.
				# TYPE cortex_querier_storegateway_refetches_per_query histogram
				cortex_querier_storegateway_refetches_per_query_bucket{le="0"} 1
				cortex_querier_storegateway_refetches_per_query_bucket{le="1"} 1
				cortex_querier_storegateway_refetches_per_query_bucket{le="2"} 1
				cortex_querier_storegateway_refetches_per_query_bucket{le="+Inf"} 1
				cortex_querier_storegateway_refetches_per_query_sum 0
				cortex_querier_storegateway_refetches_per_query_count 1

				# HELP cortex_querier_blocks_consistency_checks_failed_total Total number of queries that failed consistency checks. A failed consistency check means that some of at least one block which had to be queried wasn't present in any of the store-gateways.
				# TYPE cortex_querier_blocks_consistency_checks_failed_total counter
				cortex_querier_blocks_consistency_checks_failed_total 0

				# HELP cortex_querier_blocks_consistency_checks_total Total number of queries that needed to run with consistency checks. A consistency check is required when querying blocks from store-gateways to make sure that all blocks are queried.
				# TYPE cortex_querier_blocks_consistency_checks_total counter
				cortex_querier_blocks_consistency_checks_total 1
			`,
		},
		"multiple store-gateway instances holds the required blocks with overlapping series (single returned series)": {
			finderResult: bucketindex.Blocks{
				{ID: block1},
				{ID: block2},
			},
			storeSetResponses: []interface{}{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesResponses: newSeriesResponseBuilder().
						addValue(metricNameLabel, minT+1, 2).
						addBlocks(block1).
						build(),
					}: {block1},
					&storeGatewayClientMock{remoteAddr: "2.2.2.2", mockedSeriesResponses: newSeriesResponseBuilder().
						addValue(metricNameLabel, minT, 1).
						addValue(metricNameLabel, minT+1, 2).
						addBlocks(block2).
						build(),
					}: {block2},
				},
			},
			limits:       &blocksStoreLimitsMock{},
			queryLimiter: noOpQueryLimiter,
			expectedSeries: []seriesResult{
				{
					lbls: metricNameLabel,
					values: []valueResult{
						{t: minT, v: 1},
						{t: minT + 1, v: 2},
					},
				},
			},
			expectedMetrics: `
				# HELP cortex_querier_blocks_found_total Number of blocks found based on query time range.
				# TYPE cortex_querier_blocks_found_total counter
				cortex_querier_blocks_found_total 2

				# HELP cortex_querier_blocks_queried_total Number of blocks queried to satisfy query. Compared to blocks found, some blocks may have been filtered out thanks to query and compactor sharding.
				# TYPE cortex_querier_blocks_queried_total counter
				cortex_querier_blocks_queried_total 2

				# HELP cortex_querier_blocks_with_compactor_shard_but_incompatible_query_shard_total Blocks that couldn't be checked for query and compactor sharding optimization due to incompatible shard counts.
				# TYPE cortex_querier_blocks_with_compactor_shard_but_incompatible_query_shard_total counter
				cortex_querier_blocks_with_compactor_shard_but_incompatible_query_shard_total 0

				# HELP cortex_querier_query_storegateway_chunks_total Number of chunks received from store gateways at query time.
				# TYPE cortex_querier_query_storegateway_chunks_total counter
				cortex_querier_query_storegateway_chunks_total 3

				# HELP cortex_querier_storegateway_instances_hit_per_query Number of store-gateway instances hit for a single query.
				# TYPE cortex_querier_storegateway_instances_hit_per_query histogram
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="0"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="1"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="2"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="4"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="8"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="16"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="32"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="64"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="128"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="256"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="512"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="1024"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="2048"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="+Inf"} 1
				cortex_querier_storegateway_instances_hit_per_query_sum 2
				cortex_querier_storegateway_instances_hit_per_query_count 1

				# HELP cortex_querier_storegateway_refetches_per_query Number of re-fetches attempted while querying store-gateway instances due to missing blocks.
				# TYPE cortex_querier_storegateway_refetches_per_query histogram
				cortex_querier_storegateway_refetches_per_query_bucket{le="0"} 1
				cortex_querier_storegateway_refetches_per_query_bucket{le="1"} 1
				cortex_querier_storegateway_refetches_per_query_bucket{le="2"} 1
				cortex_querier_storegateway_refetches_per_query_bucket{le="+Inf"} 1
				cortex_querier_storegateway_refetches_per_query_sum 0
				cortex_querier_storegateway_refetches_per_query_count 1

				# HELP cortex_querier_blocks_consistency_checks_failed_total Total number of queries that failed consistency checks. A failed consistency check means that some of at least one block which had to be queried wasn't present in any of the store-gateways.
				# TYPE cortex_querier_blocks_consistency_checks_failed_total counter
				cortex_querier_blocks_consistency_checks_failed_total 0

				# HELP cortex_querier_blocks_consistency_checks_total Total number of queries that needed to run with consistency checks. A consistency check is required when querying blocks from store-gateways to make sure that all blocks are queried.
				# TYPE cortex_querier_blocks_consistency_checks_total counter
				cortex_querier_blocks_consistency_checks_total 1
			`,
		},
		"multiple store-gateway instances holds the required blocks with overlapping series (multiple returned series)": {
			finderResult: bucketindex.Blocks{
				{ID: block1},
				{ID: block2},
			},
			storeSetResponses: []interface{}{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesResponses: newSeriesResponseBuilder().
						addValue(series1Label, minT+1, 2).
						addValue(series2Label, minT, 1).
						addBlocks(block1).
						build(),
					}: {block1},
					&storeGatewayClientMock{remoteAddr: "2.2.2.2", mockedSeriesResponses: newSeriesResponseBuilder().
						addValue(series1Label, minT, 1).
						addValue(series1Label, minT+1, 2).
						addBlocks(block2).
						build(),
					}: {block2},
					&storeGatewayClientMock{remoteAddr: "3.3.3.3", mockedSeriesResponses: newSeriesResponseBuilder().
						addValue(series2Label, minT, 1).
						addValue(series2Label, minT+1, 3).
						addBlocks(block3).
						build(),
					}: {block3},
				},
			},
			limits:       &blocksStoreLimitsMock{},
			queryLimiter: noOpQueryLimiter,
			expectedSeries: []seriesResult{
				{
					lbls: series1Label,
					values: []valueResult{
						{t: minT, v: 1},
						{t: minT + 1, v: 2},
					},
				}, {
					lbls: series2Label,
					values: []valueResult{
						{t: minT, v: 1},
						{t: minT + 1, v: 3},
					},
				},
			},
			expectedMetrics: `
				# HELP cortex_querier_storegateway_instances_hit_per_query Number of store-gateway instances hit for a single query.
				# TYPE cortex_querier_storegateway_instances_hit_per_query histogram
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="0"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="1"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="2"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="4"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="8"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="16"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="32"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="64"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="128"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="256"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="512"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="1024"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="2048"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="+Inf"} 1
				cortex_querier_storegateway_instances_hit_per_query_sum 3
				cortex_querier_storegateway_instances_hit_per_query_count 1

				# HELP cortex_querier_storegateway_refetches_per_query Number of re-fetches attempted while querying store-gateway instances due to missing blocks.
				# TYPE cortex_querier_storegateway_refetches_per_query histogram
				cortex_querier_storegateway_refetches_per_query_bucket{le="0"} 1
				cortex_querier_storegateway_refetches_per_query_bucket{le="1"} 1
				cortex_querier_storegateway_refetches_per_query_bucket{le="2"} 1
				cortex_querier_storegateway_refetches_per_query_bucket{le="+Inf"} 1
				cortex_querier_storegateway_refetches_per_query_sum 0
				cortex_querier_storegateway_refetches_per_query_count 1

				# HELP cortex_querier_blocks_found_total Number of blocks found based on query time range.
				# TYPE cortex_querier_blocks_found_total counter
				cortex_querier_blocks_found_total 2
				# HELP cortex_querier_blocks_queried_total Number of blocks queried to satisfy query. Compared to blocks found, some blocks may have been filtered out thanks to query and compactor sharding.
				# TYPE cortex_querier_blocks_queried_total counter
				cortex_querier_blocks_queried_total 2
				# HELP cortex_querier_blocks_with_compactor_shard_but_incompatible_query_shard_total Blocks that couldn't be checked for query and compactor sharding optimization due to incompatible shard counts.
				# TYPE cortex_querier_blocks_with_compactor_shard_but_incompatible_query_shard_total counter
				cortex_querier_blocks_with_compactor_shard_but_incompatible_query_shard_total 0
				# HELP cortex_querier_query_storegateway_chunks_total Number of chunks received from store gateways at query time.
				# TYPE cortex_querier_query_storegateway_chunks_total counter
				cortex_querier_query_storegateway_chunks_total 6

				# HELP cortex_querier_blocks_consistency_checks_failed_total Total number of queries that failed consistency checks. A failed consistency check means that some of at least one block which had to be queried wasn't present in any of the store-gateways.
				# TYPE cortex_querier_blocks_consistency_checks_failed_total counter
				cortex_querier_blocks_consistency_checks_failed_total 0

				# HELP cortex_querier_blocks_consistency_checks_total Total number of queries that needed to run with consistency checks. A consistency check is required when querying blocks from store-gateways to make sure that all blocks are queried.
				# TYPE cortex_querier_blocks_consistency_checks_total counter
				cortex_querier_blocks_consistency_checks_total 1
			`,
		},
		"a single store-gateway instance returns no response implies querying another instance for the same blocks (consistency check passed)": {
			finderResult: bucketindex.Blocks{
				{ID: block1},
			},
			storeSetResponses: []interface{}{
				// First attempt returns a client whose response does not include all expected blocks.
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{
						remoteAddr: "1.1.1.1", mockedSeriesResponses: nil,
					}: {block1},
				},
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{
						remoteAddr: "2.2.2.2", mockedSeriesResponses: newSeriesResponseBuilder().addBlocks(block1).build(),
					}: {block1},
				},
			},
			limits:       &blocksStoreLimitsMock{},
			queryLimiter: noOpQueryLimiter,
		},
		"two store-gateway instances returning no response causes consistency check to fail": {
			finderResult: bucketindex.Blocks{
				{ID: block1},
			},
			storeSetResponses: []interface{}{
				// First attempt returns a client whose response does not include all expected blocks.
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{
						remoteAddr: "1.1.1.1", mockedSeriesResponses: nil,
					}: {block1},
				},
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{
						remoteAddr: "2.2.2.2", mockedSeriesResponses: nil,
					}: {block1},
				},
				// Third attempt returns an error because there are no other store-gateways left.
				errors.New("no store-gateway remaining after exclude"),
			},
			limits:       &blocksStoreLimitsMock{},
			queryLimiter: noOpQueryLimiter,
			expectedErr:  newStoreConsistencyCheckFailedError([]ulid.ULID{block1}),
		},
		"a single store-gateway instance has some missing blocks (consistency check failed)": {
			finderResult: bucketindex.Blocks{
				{ID: block1},
				{ID: block2},
			},
			storeSetResponses: []interface{}{
				// First attempt returns a client whose response does not include all expected blocks.
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesResponses: newSeriesResponseBuilder().
						addValue(series1Label, minT, 1).
						addValue(series1Label, minT+1, 2).
						addBlocks(block1).
						build(),
					}: {block1},
				},
				// Second attempt returns an error because there are no other store-gateways left.
				errors.New("no store-gateway remaining after exclude"),
			},
			limits:       &blocksStoreLimitsMock{},
			queryLimiter: noOpQueryLimiter,
			expectedErr:  newStoreConsistencyCheckFailedError([]ulid.ULID{block2}),
			expectedMetrics: `
				# HELP cortex_querier_blocks_found_total Number of blocks found based on query time range.
				# TYPE cortex_querier_blocks_found_total counter
				cortex_querier_blocks_found_total 2

				# HELP cortex_querier_blocks_queried_total Number of blocks queried to satisfy query. Compared to blocks found, some blocks may have been filtered out thanks to query and compactor sharding.
				# TYPE cortex_querier_blocks_queried_total counter
				cortex_querier_blocks_queried_total 2

				# HELP cortex_querier_blocks_with_compactor_shard_but_incompatible_query_shard_total Blocks that couldn't be checked for query and compactor sharding optimization due to incompatible shard counts.
				# TYPE cortex_querier_blocks_with_compactor_shard_but_incompatible_query_shard_total counter
				cortex_querier_blocks_with_compactor_shard_but_incompatible_query_shard_total 0

				# HELP cortex_querier_query_storegateway_chunks_total Number of chunks received from store gateways at query time.
				# TYPE cortex_querier_query_storegateway_chunks_total counter
				cortex_querier_query_storegateway_chunks_total 0

				# HELP cortex_querier_storegateway_instances_hit_per_query Number of store-gateway instances hit for a single query.
				# TYPE cortex_querier_storegateway_instances_hit_per_query histogram
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="0"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="1"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="2"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="4"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="8"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="16"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="32"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="64"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="128"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="256"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="512"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="1024"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="2048"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="+Inf"} 0
				cortex_querier_storegateway_instances_hit_per_query_sum 0
				cortex_querier_storegateway_instances_hit_per_query_count 0

				# HELP cortex_querier_storegateway_refetches_per_query Number of re-fetches attempted while querying store-gateway instances due to missing blocks.
				# TYPE cortex_querier_storegateway_refetches_per_query histogram
				cortex_querier_storegateway_refetches_per_query_bucket{le="0"} 0
				cortex_querier_storegateway_refetches_per_query_bucket{le="1"} 0
				cortex_querier_storegateway_refetches_per_query_bucket{le="2"} 0
				cortex_querier_storegateway_refetches_per_query_bucket{le="+Inf"} 0
				cortex_querier_storegateway_refetches_per_query_sum 0
				cortex_querier_storegateway_refetches_per_query_count 0

				# HELP cortex_querier_blocks_consistency_checks_failed_total Total number of queries that failed consistency checks. A failed consistency check means that some of at least one block which had to be queried wasn't present in any of the store-gateways.
				# TYPE cortex_querier_blocks_consistency_checks_failed_total counter
				cortex_querier_blocks_consistency_checks_failed_total 1

				# HELP cortex_querier_blocks_consistency_checks_total Total number of queries that needed to run with consistency checks. A consistency check is required when querying blocks from store-gateways to make sure that all blocks are queried.
				# TYPE cortex_querier_blocks_consistency_checks_total counter
				cortex_querier_blocks_consistency_checks_total 1
			`,
		},
		"multiple store-gateway instances have some missing blocks (consistency check failed)": {
			finderResult: bucketindex.Blocks{
				{ID: block1},
				{ID: block2},
				{ID: block3},
				{ID: block4},
			},
			storeSetResponses: []interface{}{
				// First attempt returns a client whose response does not include all expected blocks.
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesResponses: newSeriesResponseBuilder().
						addValue(metricNameLabel, minT+1, 2).
						addBlocks(block1).
						build(),
					}: {block1},
					&storeGatewayClientMock{remoteAddr: "2.2.2.2", mockedSeriesResponses: newSeriesResponseBuilder().
						addValue(metricNameLabel, minT+1, 2).
						addBlocks(block2).
						build(),
					}: {block2},
				},
				// Second attempt returns an error because there are no other store-gateways left.
				errors.New("no store-gateway remaining after exclude"),
			},
			limits:       &blocksStoreLimitsMock{},
			queryLimiter: noOpQueryLimiter,
			expectedErr:  newStoreConsistencyCheckFailedError([]ulid.ULID{block3, block4}),
			expectedMetrics: `
				# HELP cortex_querier_blocks_found_total Number of blocks found based on query time range.
				# TYPE cortex_querier_blocks_found_total counter
				cortex_querier_blocks_found_total 4

				# HELP cortex_querier_blocks_queried_total Number of blocks queried to satisfy query. Compared to blocks found, some blocks may have been filtered out thanks to query and compactor sharding.
				# TYPE cortex_querier_blocks_queried_total counter
				cortex_querier_blocks_queried_total 4

				# HELP cortex_querier_blocks_with_compactor_shard_but_incompatible_query_shard_total Blocks that couldn't be checked for query and compactor sharding optimization due to incompatible shard counts.
				# TYPE cortex_querier_blocks_with_compactor_shard_but_incompatible_query_shard_total counter
				cortex_querier_blocks_with_compactor_shard_but_incompatible_query_shard_total 0

				# HELP cortex_querier_query_storegateway_chunks_total Number of chunks received from store gateways at query time.
				# TYPE cortex_querier_query_storegateway_chunks_total counter
				cortex_querier_query_storegateway_chunks_total 0

				# HELP cortex_querier_storegateway_instances_hit_per_query Number of store-gateway instances hit for a single query.
				# TYPE cortex_querier_storegateway_instances_hit_per_query histogram
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="0"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="1"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="2"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="4"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="8"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="16"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="32"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="64"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="128"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="256"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="512"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="1024"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="2048"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="+Inf"} 0
				cortex_querier_storegateway_instances_hit_per_query_sum 0
				cortex_querier_storegateway_instances_hit_per_query_count 0

				# HELP cortex_querier_storegateway_refetches_per_query Number of re-fetches attempted while querying store-gateway instances due to missing blocks.
				# TYPE cortex_querier_storegateway_refetches_per_query histogram
				cortex_querier_storegateway_refetches_per_query_bucket{le="0"} 0
				cortex_querier_storegateway_refetches_per_query_bucket{le="1"} 0
				cortex_querier_storegateway_refetches_per_query_bucket{le="2"} 0
				cortex_querier_storegateway_refetches_per_query_bucket{le="+Inf"} 0
				cortex_querier_storegateway_refetches_per_query_sum 0
				cortex_querier_storegateway_refetches_per_query_count 0

				# HELP cortex_querier_blocks_consistency_checks_failed_total Total number of queries that failed consistency checks. A failed consistency check means that some of at least one block which had to be queried wasn't present in any of the store-gateways.
				# TYPE cortex_querier_blocks_consistency_checks_failed_total counter
				cortex_querier_blocks_consistency_checks_failed_total 1

				# HELP cortex_querier_blocks_consistency_checks_total Total number of queries that needed to run with consistency checks. A consistency check is required when querying blocks from store-gateways to make sure that all blocks are queried.
				# TYPE cortex_querier_blocks_consistency_checks_total counter
				cortex_querier_blocks_consistency_checks_total 1
			`,
		},
		"multiple store-gateway instances have some missing blocks but queried from a replica during subsequent attempts": {
			finderResult: bucketindex.Blocks{
				{ID: block1},
				{ID: block2},
				{ID: block3},
				{ID: block4},
			},
			storeSetResponses: []interface{}{
				// First attempt returns a client whose response does not include all expected blocks.
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesResponses: newSeriesResponseBuilder().
						addValue(series1Label, minT, 1).
						addBlocks(block1).
						build(),
					}: {block1, block3},
					&storeGatewayClientMock{remoteAddr: "2.2.2.2", mockedSeriesResponses: newSeriesResponseBuilder().
						addValue(series2Label, minT, 2).
						addBlocks(block2).
						build(),
					}: {block2, block4},
				},
				// Second attempt returns 1 missing block.
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "3.3.3.3", mockedSeriesResponses: newSeriesResponseBuilder().
						addValue(series1Label, minT+1, 2).
						addBlocks(block3).
						build(),
					}: {block3, block4},
				},
				// Third attempt returns the last missing block.
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "4.4.4.4", mockedSeriesResponses: newSeriesResponseBuilder().
						addValue(series2Label, minT+1, 3).
						addBlocks(block4).
						build(),
					}: {block4},
				},
			},
			limits:       &blocksStoreLimitsMock{},
			queryLimiter: noOpQueryLimiter,
			expectedSeries: []seriesResult{
				{
					lbls: series1Label,
					values: []valueResult{
						{t: minT, v: 1},
						{t: minT + 1, v: 2},
					},
				}, {
					lbls: series2Label,
					values: []valueResult{
						{t: minT, v: 2},
						{t: minT + 1, v: 3},
					},
				},
			},
			expectedMetrics: `
				# HELP cortex_querier_storegateway_instances_hit_per_query Number of store-gateway instances hit for a single query.
				# TYPE cortex_querier_storegateway_instances_hit_per_query histogram
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="0"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="1"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="2"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="4"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="8"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="16"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="32"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="64"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="128"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="256"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="512"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="1024"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="2048"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="+Inf"} 1
				cortex_querier_storegateway_instances_hit_per_query_sum 4
				cortex_querier_storegateway_instances_hit_per_query_count 1

				# HELP cortex_querier_storegateway_refetches_per_query Number of re-fetches attempted while querying store-gateway instances due to missing blocks.
				# TYPE cortex_querier_storegateway_refetches_per_query histogram
				cortex_querier_storegateway_refetches_per_query_bucket{le="0"} 0
				cortex_querier_storegateway_refetches_per_query_bucket{le="1"} 0
				cortex_querier_storegateway_refetches_per_query_bucket{le="2"} 1
				cortex_querier_storegateway_refetches_per_query_bucket{le="+Inf"} 1
				cortex_querier_storegateway_refetches_per_query_sum 2
				cortex_querier_storegateway_refetches_per_query_count 1

				# HELP cortex_querier_blocks_found_total Number of blocks found based on query time range.
				# TYPE cortex_querier_blocks_found_total counter
				cortex_querier_blocks_found_total 4
				# HELP cortex_querier_blocks_queried_total Number of blocks queried to satisfy query. Compared to blocks found, some blocks may have been filtered out thanks to query and compactor sharding.
				# TYPE cortex_querier_blocks_queried_total counter
				cortex_querier_blocks_queried_total 4
				# HELP cortex_querier_blocks_with_compactor_shard_but_incompatible_query_shard_total Blocks that couldn't be checked for query and compactor sharding optimization due to incompatible shard counts.
				# TYPE cortex_querier_blocks_with_compactor_shard_but_incompatible_query_shard_total counter
				cortex_querier_blocks_with_compactor_shard_but_incompatible_query_shard_total 0
				# HELP cortex_querier_query_storegateway_chunks_total Number of chunks received from store gateways at query time.
				# TYPE cortex_querier_query_storegateway_chunks_total counter
				cortex_querier_query_storegateway_chunks_total 4

				# HELP cortex_querier_blocks_consistency_checks_failed_total Total number of queries that failed consistency checks. A failed consistency check means that some of at least one block which had to be queried wasn't present in any of the store-gateways.
				# TYPE cortex_querier_blocks_consistency_checks_failed_total counter
				cortex_querier_blocks_consistency_checks_failed_total 0

				# HELP cortex_querier_blocks_consistency_checks_total Total number of queries that needed to run with consistency checks. A consistency check is required when querying blocks from store-gateways to make sure that all blocks are queried.
				# TYPE cortex_querier_blocks_consistency_checks_total counter
				cortex_querier_blocks_consistency_checks_total 1
			`,
		},
		"more than 3 store-gateway instances have some missing blocks but queried from a replica during subsequent attempts": {
			finderResult: bucketindex.Blocks{
				{ID: block1},
			},
			storeSetResponses: []interface{}{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "1.1.1.1",
						mockedSeriesErr: errors.New("failed to receive from store-gateway"),
					}: {block1},
				},
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "2.2.2.2",
						mockedSeriesErr: errors.New("failed to receive from store-gateway"),
					}: {block1},
				},
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "3.3.3.3",
						mockedSeriesErr: errors.New("failed to receive from store-gateway"),
					}: {block1},
				},
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "4.4.4.4",
						mockedSeriesErr: errors.New("failed to receive from store-gateway"),
					}: {block1},
				},
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "5.5.5.5",
						mockedSeriesErr: errors.New("failed to receive from store-gateway"),
					}: {block1},
				},
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "6.6.6.6",
						mockedSeriesResponses: newSeriesResponseBuilder().
							addValue(series1Label, minT, 2).
							addBlocks(block1).
							build(),
					}: {block1},
				},
			},
			limits:       &blocksStoreLimitsMock{},
			queryLimiter: noOpQueryLimiter,
			expectedSeries: []seriesResult{
				{
					lbls: series1Label,
					values: []valueResult{
						{t: minT, v: 2},
					},
				},
			},
			expectedMetrics: `
				# HELP cortex_querier_storegateway_instances_hit_per_query Number of store-gateway instances hit for a single query.
				# TYPE cortex_querier_storegateway_instances_hit_per_query histogram
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="0"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="1"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="2"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="4"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="8"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="16"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="32"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="64"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="128"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="256"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="512"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="1024"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="2048"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="+Inf"} 1
				cortex_querier_storegateway_instances_hit_per_query_sum 6
				cortex_querier_storegateway_instances_hit_per_query_count 1

				# HELP cortex_querier_storegateway_refetches_per_query Number of re-fetches attempted while querying store-gateway instances due to missing blocks.
				# TYPE cortex_querier_storegateway_refetches_per_query histogram
				cortex_querier_storegateway_refetches_per_query_bucket{le="0"} 0
				cortex_querier_storegateway_refetches_per_query_bucket{le="1"} 0
				cortex_querier_storegateway_refetches_per_query_bucket{le="2"} 0
				cortex_querier_storegateway_refetches_per_query_bucket{le="+Inf"} 1
				cortex_querier_storegateway_refetches_per_query_sum 5
				cortex_querier_storegateway_refetches_per_query_count 1

				# HELP cortex_querier_blocks_found_total Number of blocks found based on query time range.
				# TYPE cortex_querier_blocks_found_total counter
				cortex_querier_blocks_found_total 1
				# HELP cortex_querier_blocks_queried_total Number of blocks queried to satisfy query. Compared to blocks found, some blocks may have been filtered out thanks to query and compactor sharding.
				# TYPE cortex_querier_blocks_queried_total counter
				cortex_querier_blocks_queried_total 1
				# HELP cortex_querier_blocks_with_compactor_shard_but_incompatible_query_shard_total Blocks that couldn't be checked for query and compactor sharding optimization due to incompatible shard counts.
				# TYPE cortex_querier_blocks_with_compactor_shard_but_incompatible_query_shard_total counter
				cortex_querier_blocks_with_compactor_shard_but_incompatible_query_shard_total 0
				# HELP cortex_querier_query_storegateway_chunks_total Number of chunks received from store gateways at query time.
				# TYPE cortex_querier_query_storegateway_chunks_total counter
				cortex_querier_query_storegateway_chunks_total 1

				# HELP cortex_querier_blocks_consistency_checks_failed_total Total number of queries that failed consistency checks. A failed consistency check means that some of at least one block which had to be queried wasn't present in any of the store-gateways.
				# TYPE cortex_querier_blocks_consistency_checks_failed_total counter
				cortex_querier_blocks_consistency_checks_failed_total 0

				# HELP cortex_querier_blocks_consistency_checks_total Total number of queries that needed to run with consistency checks. A consistency check is required when querying blocks from store-gateways to make sure that all blocks are queried.
				# TYPE cortex_querier_blocks_consistency_checks_total counter
				cortex_querier_blocks_consistency_checks_total 1
			`,
		},
		"max chunks per query limit greater then the number of chunks fetched": {
			finderResult: bucketindex.Blocks{
				{ID: block1},
				{ID: block2},
			},
			storeSetResponses: []interface{}{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesResponses: newSeriesResponseBuilder().
						addValue(series1Label, minT, 1).
						addValue(series1Label, minT+1, 2).
						addBlocks(block1, block2).
						build(),
					}: {block1, block2},
				},
			},
			limits:       &blocksStoreLimitsMock{maxChunksPerQuery: 3},
			queryLimiter: noOpQueryLimiter,
			expectedSeries: []seriesResult{
				{
					lbls: series1Label,
					values: []valueResult{
						{t: minT, v: 1},
						{t: minT + 1, v: 2},
					},
				},
			},
			expectedMetrics: `
				# HELP cortex_querier_blocks_found_total Number of blocks found based on query time range.
				# TYPE cortex_querier_blocks_found_total counter
				cortex_querier_blocks_found_total 2

				# HELP cortex_querier_blocks_queried_total Number of blocks queried to satisfy query. Compared to blocks found, some blocks may have been filtered out thanks to query and compactor sharding.
				# TYPE cortex_querier_blocks_queried_total counter
				cortex_querier_blocks_queried_total 2

				# HELP cortex_querier_blocks_with_compactor_shard_but_incompatible_query_shard_total Blocks that couldn't be checked for query and compactor sharding optimization due to incompatible shard counts.
				# TYPE cortex_querier_blocks_with_compactor_shard_but_incompatible_query_shard_total counter
				cortex_querier_blocks_with_compactor_shard_but_incompatible_query_shard_total 0

				# HELP cortex_querier_query_storegateway_chunks_total Number of chunks received from store gateways at query time.
				# TYPE cortex_querier_query_storegateway_chunks_total counter
				cortex_querier_query_storegateway_chunks_total 2

				# HELP cortex_querier_storegateway_instances_hit_per_query Number of store-gateway instances hit for a single query.
				# TYPE cortex_querier_storegateway_instances_hit_per_query histogram
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="0"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="1"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="2"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="4"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="8"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="16"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="32"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="64"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="128"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="256"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="512"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="1024"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="2048"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="+Inf"} 1
				cortex_querier_storegateway_instances_hit_per_query_sum 1
				cortex_querier_storegateway_instances_hit_per_query_count 1

				# HELP cortex_querier_storegateway_refetches_per_query Number of re-fetches attempted while querying store-gateway instances due to missing blocks.
				# TYPE cortex_querier_storegateway_refetches_per_query histogram
				cortex_querier_storegateway_refetches_per_query_bucket{le="0"} 1
				cortex_querier_storegateway_refetches_per_query_bucket{le="1"} 1
				cortex_querier_storegateway_refetches_per_query_bucket{le="2"} 1
				cortex_querier_storegateway_refetches_per_query_bucket{le="+Inf"} 1
				cortex_querier_storegateway_refetches_per_query_sum 0
				cortex_querier_storegateway_refetches_per_query_count 1

				# HELP cortex_querier_blocks_consistency_checks_failed_total Total number of queries that failed consistency checks. A failed consistency check means that some of at least one block which had to be queried wasn't present in any of the store-gateways.
				# TYPE cortex_querier_blocks_consistency_checks_failed_total counter
				cortex_querier_blocks_consistency_checks_failed_total 0

				# HELP cortex_querier_blocks_consistency_checks_total Total number of queries that needed to run with consistency checks. A consistency check is required when querying blocks from store-gateways to make sure that all blocks are queried.
				# TYPE cortex_querier_blocks_consistency_checks_total counter
				cortex_querier_blocks_consistency_checks_total 1
			`,
		},
		"max chunks per query limit hit while fetching chunks at first attempt": {
			finderResult: bucketindex.Blocks{
				{ID: block1},
				{ID: block2},
			},
			storeSetResponses: []interface{}{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesResponses: newSeriesResponseBuilder().
						addValue(series1Label, minT, 1).
						addValue(series1Label, minT+1, 2).
						addBlocks(block1, block2).
						build(),
					}: {block1, block2},
				},
			},
			limits:       &blocksStoreLimitsMock{},
			queryLimiter: limiter.NewQueryLimiter(0, 0, 1, 0, stats.NewQueryMetrics(prometheus.NewPedanticRegistry())),
			expectedErr:  limiter.NewMaxChunksPerQueryLimitError(1),
			expectedMetrics: `
				# HELP cortex_querier_blocks_found_total Number of blocks found based on query time range.
				# TYPE cortex_querier_blocks_found_total counter
				cortex_querier_blocks_found_total 2

				# HELP cortex_querier_blocks_queried_total Number of blocks queried to satisfy query. Compared to blocks found, some blocks may have been filtered out thanks to query and compactor sharding.
				# TYPE cortex_querier_blocks_queried_total counter
				cortex_querier_blocks_queried_total 2

				# HELP cortex_querier_blocks_with_compactor_shard_but_incompatible_query_shard_total Blocks that couldn't be checked for query and compactor sharding optimization due to incompatible shard counts.
				# TYPE cortex_querier_blocks_with_compactor_shard_but_incompatible_query_shard_total counter
				cortex_querier_blocks_with_compactor_shard_but_incompatible_query_shard_total 0

				# HELP cortex_querier_query_storegateway_chunks_total Number of chunks received from store gateways at query time.
				# TYPE cortex_querier_query_storegateway_chunks_total counter
				cortex_querier_query_storegateway_chunks_total 2

				# HELP cortex_querier_storegateway_instances_hit_per_query Number of store-gateway instances hit for a single query.
				# TYPE cortex_querier_storegateway_instances_hit_per_query histogram
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="0"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="1"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="2"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="4"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="8"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="16"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="32"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="64"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="128"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="256"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="512"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="1024"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="2048"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="+Inf"} 1
				cortex_querier_storegateway_instances_hit_per_query_sum 1
				cortex_querier_storegateway_instances_hit_per_query_count 1

				# HELP cortex_querier_storegateway_refetches_per_query Number of re-fetches attempted while querying store-gateway instances due to missing blocks.
				# TYPE cortex_querier_storegateway_refetches_per_query histogram
				cortex_querier_storegateway_refetches_per_query_bucket{le="0"} 1
				cortex_querier_storegateway_refetches_per_query_bucket{le="1"} 1
				cortex_querier_storegateway_refetches_per_query_bucket{le="2"} 1
				cortex_querier_storegateway_refetches_per_query_bucket{le="+Inf"} 1
				cortex_querier_storegateway_refetches_per_query_sum 0
				cortex_querier_storegateway_refetches_per_query_count 1

				# HELP cortex_querier_blocks_consistency_checks_failed_total Total number of queries that failed consistency checks. A failed consistency check means that some of at least one block which had to be queried wasn't present in any of the store-gateways.
				# TYPE cortex_querier_blocks_consistency_checks_failed_total counter
				cortex_querier_blocks_consistency_checks_failed_total 0

				# HELP cortex_querier_blocks_consistency_checks_total Total number of queries that needed to run with consistency checks. A consistency check is required when querying blocks from store-gateways to make sure that all blocks are queried.
				# TYPE cortex_querier_blocks_consistency_checks_total counter
				cortex_querier_blocks_consistency_checks_total 1
			`,
		},
		"max estimated chunks per query limit hit while fetching chunks": {
			finderResult: bucketindex.Blocks{
				{ID: block1},
				{ID: block2},
			},
			storeSetResponses: []interface{}{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesResponses: newSeriesResponseBuilder().
						addValue(series1Label, minT, 1).
						addValue(series1Label, minT+1, 2).
						addBlocks(block1, block2).
						build(),
					}: {block1, block2},
				},
			},
			limits:       &blocksStoreLimitsMock{},
			queryLimiter: limiter.NewQueryLimiter(0, 0, 0, 1, stats.NewQueryMetrics(prometheus.NewPedanticRegistry())),
			expectedErr:  limiter.NewMaxEstimatedChunksPerQueryLimitError(1),
			expectedMetrics: `
					# HELP cortex_querier_blocks_found_total Number of blocks found based on query time range.
					# TYPE cortex_querier_blocks_found_total counter
					cortex_querier_blocks_found_total 2

					# HELP cortex_querier_blocks_queried_total Number of blocks queried to satisfy query. Compared to blocks found, some blocks may have been filtered out thanks to query and compactor sharding.
					# TYPE cortex_querier_blocks_queried_total counter
					cortex_querier_blocks_queried_total 2

					# HELP cortex_querier_blocks_with_compactor_shard_but_incompatible_query_shard_total Blocks that couldn't be checked for query and compactor sharding optimization due to incompatible shard counts.
					# TYPE cortex_querier_blocks_with_compactor_shard_but_incompatible_query_shard_total counter
					cortex_querier_blocks_with_compactor_shard_but_incompatible_query_shard_total 0

					# HELP cortex_querier_query_storegateway_chunks_total Number of chunks received from store gateways at query time.
					# TYPE cortex_querier_query_storegateway_chunks_total counter
					cortex_querier_query_storegateway_chunks_total 0

					# HELP cortex_querier_storegateway_instances_hit_per_query Number of store-gateway instances hit for a single query.
					# TYPE cortex_querier_storegateway_instances_hit_per_query histogram
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="0"} 0
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="1"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="2"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="4"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="8"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="16"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="32"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="64"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="128"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="256"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="512"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="1024"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="2048"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="+Inf"} 1
					cortex_querier_storegateway_instances_hit_per_query_sum 1
					cortex_querier_storegateway_instances_hit_per_query_count 1

					# HELP cortex_querier_storegateway_refetches_per_query Number of re-fetches attempted while querying store-gateway instances due to missing blocks.
					# TYPE cortex_querier_storegateway_refetches_per_query histogram
					cortex_querier_storegateway_refetches_per_query_bucket{le="0"} 1
					cortex_querier_storegateway_refetches_per_query_bucket{le="1"} 1
					cortex_querier_storegateway_refetches_per_query_bucket{le="2"} 1
					cortex_querier_storegateway_refetches_per_query_bucket{le="+Inf"} 1
					cortex_querier_storegateway_refetches_per_query_sum 0
					cortex_querier_storegateway_refetches_per_query_count 1

					# HELP cortex_querier_blocks_consistency_checks_failed_total Total number of queries that failed consistency checks. A failed consistency check means that some of at least one block which had to be queried wasn't present in any of the store-gateways.
					# TYPE cortex_querier_blocks_consistency_checks_failed_total counter
					cortex_querier_blocks_consistency_checks_failed_total 0

					# HELP cortex_querier_blocks_consistency_checks_total Total number of queries that needed to run with consistency checks. A consistency check is required when querying blocks from store-gateways to make sure that all blocks are queried.
					# TYPE cortex_querier_blocks_consistency_checks_total counter
					cortex_querier_blocks_consistency_checks_total 1
				`,
		},
		"max chunks per query limit hit while fetching chunks during subsequent attempts": {
			finderResult: bucketindex.Blocks{
				{ID: block1},
				{ID: block2},
				{ID: block3},
				{ID: block4},
			},
			storeSetResponses: []interface{}{
				// First attempt returns a client whose response does not include all expected blocks.
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesResponses: newSeriesResponseBuilder().
						addValue(series1Label, minT, 1).
						addBlocks(block1).
						build(),
					}: {block1, block3},
					&storeGatewayClientMock{remoteAddr: "2.2.2.2", mockedSeriesResponses: newSeriesResponseBuilder().
						addValue(series2Label, minT, 2).
						addBlocks(block2).
						build(),
					}: {block2, block4},
				},
				// Second attempt returns 1 missing block.
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "3.3.3.3", mockedSeriesResponses: newSeriesResponseBuilder().
						addValue(series1Label, minT+1, 2).
						addBlocks(block3).
						build(),
					}: {block3, block4},
				},
				// Third attempt returns the last missing block.
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "4.4.4.4", mockedSeriesResponses: newSeriesResponseBuilder().
						addValue(series2Label, minT+1, 3).
						addBlocks(block4).
						build(),
					}: {block4},
				},
			},
			limits:       &blocksStoreLimitsMock{},
			queryLimiter: limiter.NewQueryLimiter(0, 0, 3, 0, stats.NewQueryMetrics(prometheus.NewPedanticRegistry())),
			expectedErr:  limiter.NewMaxChunksPerQueryLimitError(3),
		},
		"max series per query limit hit while fetching chunks": {
			finderResult: bucketindex.Blocks{
				{ID: block1},
				{ID: block2},
			},
			storeSetResponses: []interface{}{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesResponses: newSeriesResponseBuilder().
						addValue(series1Label, minT, 1).
						addValue(series2Label, minT+1, 2).
						addBlocks(block1, block2).
						build(),
					}: {block1, block2},
				},
			},
			limits:       &blocksStoreLimitsMock{},
			queryLimiter: limiter.NewQueryLimiter(1, 0, 0, 0, stats.NewQueryMetrics(prometheus.NewPedanticRegistry())),
			expectedErr:  limiter.NewMaxSeriesHitLimitError(1),
		},
		"max chunk bytes per query limit hit while fetching chunks": {
			finderResult: bucketindex.Blocks{
				{ID: block1},
				{ID: block2},
			},
			storeSetResponses: []interface{}{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesResponses: newSeriesResponseBuilder().
						addValue(series1Label, minT, 1).
						addValue(series1Label, minT+1, 2).
						addBlocks(block1, block2).
						build(),
					}: {block1, block2},
				},
			},
			limits:       &blocksStoreLimitsMock{maxChunksPerQuery: 1},
			queryLimiter: limiter.NewQueryLimiter(0, 8, 0, 0, stats.NewQueryMetrics(prometheus.NewPedanticRegistry())),
			expectedErr:  limiter.NewMaxChunkBytesHitLimitError(8),
		},
		"blocks with non-matching shard are filtered out": {
			finderResult: bucketindex.Blocks{
				{ID: block1, CompactorShardID: "1_of_4"},
				{ID: block2, CompactorShardID: "2_of_4"},
				{ID: block3, CompactorShardID: "3_of_4"},
				{ID: block4, CompactorShardID: "4_of_4"},
			},
			queryShardID: "2_of_4",
			storeSetResponses: []interface{}{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesResponses: newSeriesResponseBuilder().
						addValue(metricNameLabel, minT, 1).
						addValue(metricNameLabel, minT+1, 2).
						addBlocks(block2).
						build(),
					}: {block2}, // Only block2 will be queried
				},
			},
			limits:       &blocksStoreLimitsMock{},
			queryLimiter: noOpQueryLimiter,
			expectedSeries: []seriesResult{
				{
					lbls: metricNameLabel,
					values: []valueResult{
						{t: minT, v: 1},
						{t: minT + 1, v: 2},
					},
				},
			},
			expectedMetrics: `
					# HELP cortex_querier_blocks_found_total Number of blocks found based on query time range.
					# TYPE cortex_querier_blocks_found_total counter
					cortex_querier_blocks_found_total 4

					# HELP cortex_querier_blocks_queried_total Number of blocks queried to satisfy query. Compared to blocks found, some blocks may have been filtered out thanks to query and compactor sharding.
					# TYPE cortex_querier_blocks_queried_total counter
					cortex_querier_blocks_queried_total 1

					# HELP cortex_querier_blocks_with_compactor_shard_but_incompatible_query_shard_total Blocks that couldn't be checked for query and compactor sharding optimization due to incompatible shard counts.
					# TYPE cortex_querier_blocks_with_compactor_shard_but_incompatible_query_shard_total counter
					cortex_querier_blocks_with_compactor_shard_but_incompatible_query_shard_total 0

					# HELP cortex_querier_storegateway_instances_hit_per_query Number of store-gateway instances hit for a single query.
					# TYPE cortex_querier_storegateway_instances_hit_per_query histogram
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="0"} 0
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="1"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="2"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="4"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="8"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="16"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="32"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="64"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="128"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="256"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="512"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="1024"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="2048"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="+Inf"} 1
					cortex_querier_storegateway_instances_hit_per_query_sum 1
					cortex_querier_storegateway_instances_hit_per_query_count 1

					# HELP cortex_querier_storegateway_refetches_per_query Number of re-fetches attempted while querying store-gateway instances due to missing blocks.
					# TYPE cortex_querier_storegateway_refetches_per_query histogram
					cortex_querier_storegateway_refetches_per_query_bucket{le="0"} 1
					cortex_querier_storegateway_refetches_per_query_bucket{le="1"} 1
					cortex_querier_storegateway_refetches_per_query_bucket{le="2"} 1
					cortex_querier_storegateway_refetches_per_query_bucket{le="+Inf"} 1
					cortex_querier_storegateway_refetches_per_query_sum 0
					cortex_querier_storegateway_refetches_per_query_count 1
					# HELP cortex_querier_query_storegateway_chunks_total Number of chunks received from store gateways at query time.
					# TYPE cortex_querier_query_storegateway_chunks_total counter
					cortex_querier_query_storegateway_chunks_total 2

					# HELP cortex_querier_blocks_consistency_checks_failed_total Total number of queries that failed consistency checks. A failed consistency check means that some of at least one block which had to be queried wasn't present in any of the store-gateways.
					# TYPE cortex_querier_blocks_consistency_checks_failed_total counter
					cortex_querier_blocks_consistency_checks_failed_total 0

					# HELP cortex_querier_blocks_consistency_checks_total Total number of queries that needed to run with consistency checks. A consistency check is required when querying blocks from store-gateways to make sure that all blocks are queried.
					# TYPE cortex_querier_blocks_consistency_checks_total counter
					cortex_querier_blocks_consistency_checks_total 1
			`,
		},
		"all blocks are queried if shards don't match": {
			finderResult: bucketindex.Blocks{
				{ID: block1, CompactorShardID: "1_of_4"},
				{ID: block2, CompactorShardID: "2_of_4"},
				{ID: block3, CompactorShardID: "3_of_4"},
				{ID: block4, CompactorShardID: "4_of_4"},
			},
			queryShardID: "3_of_5",
			storeSetResponses: []interface{}{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesResponses: newSeriesResponseBuilder().
						addValue(metricNameLabel, minT, 1).
						addValue(metricNameLabel, minT+1, 2).
						addBlocks(block1, block2, block3, block4).
						build(),
					}: {block1, block2, block3, block4},
				},
			},
			limits:       &blocksStoreLimitsMock{},
			queryLimiter: noOpQueryLimiter,
			expectedSeries: []seriesResult{
				{
					lbls: metricNameLabel,
					values: []valueResult{
						{t: minT, v: 1},
						{t: minT + 1, v: 2},
					},
				},
			},
			expectedMetrics: `
					# HELP cortex_querier_blocks_found_total Number of blocks found based on query time range.
					# TYPE cortex_querier_blocks_found_total counter
					cortex_querier_blocks_found_total 4

					# HELP cortex_querier_blocks_queried_total Number of blocks queried to satisfy query. Compared to blocks found, some blocks may have been filtered out thanks to query and compactor sharding.
					# TYPE cortex_querier_blocks_queried_total counter
					cortex_querier_blocks_queried_total 4

					# HELP cortex_querier_blocks_with_compactor_shard_but_incompatible_query_shard_total Blocks that couldn't be checked for query and compactor sharding optimization due to incompatible shard counts.
					# TYPE cortex_querier_blocks_with_compactor_shard_but_incompatible_query_shard_total counter
					cortex_querier_blocks_with_compactor_shard_but_incompatible_query_shard_total 4

					# HELP cortex_querier_storegateway_instances_hit_per_query Number of store-gateway instances hit for a single query.
					# TYPE cortex_querier_storegateway_instances_hit_per_query histogram
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="0"} 0
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="1"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="2"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="4"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="8"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="16"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="32"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="64"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="128"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="256"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="512"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="1024"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="2048"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="+Inf"} 1
					cortex_querier_storegateway_instances_hit_per_query_sum 1
					cortex_querier_storegateway_instances_hit_per_query_count 1

					# HELP cortex_querier_storegateway_refetches_per_query Number of re-fetches attempted while querying store-gateway instances due to missing blocks.
					# TYPE cortex_querier_storegateway_refetches_per_query histogram
					cortex_querier_storegateway_refetches_per_query_bucket{le="0"} 1
					cortex_querier_storegateway_refetches_per_query_bucket{le="1"} 1
					cortex_querier_storegateway_refetches_per_query_bucket{le="2"} 1
					cortex_querier_storegateway_refetches_per_query_bucket{le="+Inf"} 1
					cortex_querier_storegateway_refetches_per_query_sum 0
					cortex_querier_storegateway_refetches_per_query_count 1
					# HELP cortex_querier_query_storegateway_chunks_total Number of chunks received from store gateways at query time.
					# TYPE cortex_querier_query_storegateway_chunks_total counter
					cortex_querier_query_storegateway_chunks_total 2

					# HELP cortex_querier_blocks_consistency_checks_failed_total Total number of queries that failed consistency checks. A failed consistency check means that some of at least one block which had to be queried wasn't present in any of the store-gateways.
					# TYPE cortex_querier_blocks_consistency_checks_failed_total counter
					cortex_querier_blocks_consistency_checks_failed_total 0

					# HELP cortex_querier_blocks_consistency_checks_total Total number of queries that needed to run with consistency checks. A consistency check is required when querying blocks from store-gateways to make sure that all blocks are queried.
					# TYPE cortex_querier_blocks_consistency_checks_total counter
					cortex_querier_blocks_consistency_checks_total 1
			`,
		},
		"multiple store-gateways have the block, but one of them fails to return": {
			finderResult: bucketindex.Blocks{
				{ID: block1},
			},
			storeSetResponses: []interface{}{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{
						remoteAddr:      "1.1.1.1",
						mockedSeriesErr: errors.New("failed to receive from store-gateway"),
					}: {block1},
				},
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "2.2.2.2", mockedSeriesResponses: newSeriesResponseBuilder().
						addValue(series1Label, minT, 2).
						addBlocks(block1).
						build(),
					}: {block1},
				},
			},
			limits:       &blocksStoreLimitsMock{},
			queryLimiter: noOpQueryLimiter,
			expectedSeries: []seriesResult{
				{
					lbls: series1Label,
					values: []valueResult{
						{t: minT, v: 2},
					},
				},
			},
			expectedMetrics: `
					# HELP cortex_querier_blocks_found_total Number of blocks found based on query time range.
					# TYPE cortex_querier_blocks_found_total counter
					cortex_querier_blocks_found_total 1

					# HELP cortex_querier_blocks_queried_total Number of blocks queried to satisfy query. Compared to blocks found, some blocks may have been filtered out thanks to query and compactor sharding.
					# TYPE cortex_querier_blocks_queried_total counter
					cortex_querier_blocks_queried_total 1

					# HELP cortex_querier_blocks_with_compactor_shard_but_incompatible_query_shard_total Blocks that couldn't be checked for query and compactor sharding optimization due to incompatible shard counts.
					# TYPE cortex_querier_blocks_with_compactor_shard_but_incompatible_query_shard_total counter
					cortex_querier_blocks_with_compactor_shard_but_incompatible_query_shard_total 0

					# HELP cortex_querier_storegateway_instances_hit_per_query Number of store-gateway instances hit for a single query.
					# TYPE cortex_querier_storegateway_instances_hit_per_query histogram
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="0"} 0
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="1"} 0
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="2"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="4"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="8"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="16"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="32"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="64"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="128"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="256"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="512"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="1024"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="2048"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="+Inf"} 1
					cortex_querier_storegateway_instances_hit_per_query_sum 2
					cortex_querier_storegateway_instances_hit_per_query_count 1

					# HELP cortex_querier_storegateway_refetches_per_query Number of re-fetches attempted while querying store-gateway instances due to missing blocks.
					# TYPE cortex_querier_storegateway_refetches_per_query histogram
					cortex_querier_storegateway_refetches_per_query_bucket{le="0"} 0
					cortex_querier_storegateway_refetches_per_query_bucket{le="1"} 1
					cortex_querier_storegateway_refetches_per_query_bucket{le="2"} 1
					cortex_querier_storegateway_refetches_per_query_bucket{le="+Inf"} 1
					cortex_querier_storegateway_refetches_per_query_sum 1
					cortex_querier_storegateway_refetches_per_query_count 1
					# HELP cortex_querier_query_storegateway_chunks_total Number of chunks received from store gateways at query time.
					# TYPE cortex_querier_query_storegateway_chunks_total counter
					cortex_querier_query_storegateway_chunks_total 1

					# HELP cortex_querier_blocks_consistency_checks_failed_total Total number of queries that failed consistency checks. A failed consistency check means that some of at least one block which had to be queried wasn't present in any of the store-gateways.
					# TYPE cortex_querier_blocks_consistency_checks_failed_total counter
					cortex_querier_blocks_consistency_checks_failed_total 0

					# HELP cortex_querier_blocks_consistency_checks_total Total number of queries that needed to run with consistency checks. A consistency check is required when querying blocks from store-gateways to make sure that all blocks are queried.
					# TYPE cortex_querier_blocks_consistency_checks_total counter
					cortex_querier_blocks_consistency_checks_total 1
			`,
		},
		"histograms with counter resets in overlapping chunks": {
			finderResult: bucketindex.Blocks{
				{ID: block1},
				{ID: block2},
			},
			storeSetResponses: []interface{}{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesResponses: newSeriesResponseBuilder().
						addFloatHistogramSamples(metricNameLabel,
							promql.HPoint{T: minT + 1, H: test.GenerateTestFloatHistogram(40)},
							promql.HPoint{T: minT + 7, H: test.GenerateTestFloatHistogram(50)},
						).
						addFloatHistogramSamples(metricNameLabel,
							promql.HPoint{T: minT + 2, H: test.GenerateTestFloatHistogram(20)},
							promql.HPoint{T: minT + 3, H: test.GenerateTestFloatHistogram(60)},
							promql.HPoint{T: minT + 4, H: test.GenerateTestFloatHistogram(70)},
							promql.HPoint{T: minT + 5, H: test.GenerateTestFloatHistogram(80)},
							promql.HPoint{T: minT + 6, H: test.GenerateTestFloatHistogram(90)},
						).
						addBlocks(block1, block2).
						addFetchedIndexBytes(50).
						build(),
					}: {block1, block2},
				},
			},
			limits:       &blocksStoreLimitsMock{},
			queryLimiter: noOpQueryLimiter,
			expectedSeries: []seriesResult{
				{
					lbls: metricNameLabel,
					values: []valueResult{
						{t: minT + 1, fh: test.GenerateTestFloatHistogram(40)},
						{t: minT + 2, fh: test.GenerateTestFloatHistogram(20)},
						{t: minT + 3, fh: tsdbutil.SetFloatHistogramNotCounterReset(test.GenerateTestFloatHistogram(60))},
						{t: minT + 4, fh: tsdbutil.SetFloatHistogramNotCounterReset(test.GenerateTestFloatHistogram(70))},
						{t: minT + 5, fh: tsdbutil.SetFloatHistogramNotCounterReset(test.GenerateTestFloatHistogram(80))},
						{t: minT + 6, fh: tsdbutil.SetFloatHistogramNotCounterReset(test.GenerateTestFloatHistogram(90))},
						{t: minT + 7, fh: test.GenerateTestFloatHistogram(50)},
					},
				},
			},
		},
		"histograms with counter resets with partially matching chunks": {
			finderResult: bucketindex.Blocks{
				{ID: block1},
				{ID: block2},
			},
			storeSetResponses: []interface{}{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesResponses: newSeriesResponseBuilder().
						addFloatHistogramSamples(metricNameLabel,
							promql.HPoint{T: minT + 1, H: test.GenerateTestFloatHistogram(40)},
						).
						addFloatHistogramSamples(metricNameLabel,
							promql.HPoint{T: minT + 3, H: test.GenerateTestFloatHistogram(20)},
						).
						addFloatHistogramSamples(metricNameLabel,
							promql.HPoint{T: minT + 1, H: test.GenerateTestFloatHistogram(40)},
						).
						addFloatHistogramSamples(metricNameLabel,
							promql.HPoint{T: minT + 2, H: test.GenerateTestFloatHistogram(30)},
						).
						addFloatHistogramSamples(metricNameLabel,
							promql.HPoint{T: minT + 3, H: test.GenerateTestFloatHistogram(20)},
						).
						addBlocks(block1, block2).
						addFetchedIndexBytes(50).
						build(),
					}: {block1, block2},
				},
			},
			limits:       &blocksStoreLimitsMock{},
			queryLimiter: noOpQueryLimiter,
			expectedSeries: []seriesResult{
				{
					lbls: metricNameLabel,
					values: []valueResult{
						{t: minT + 1, fh: test.GenerateTestFloatHistogram(40)},
						{t: minT + 2, fh: test.GenerateTestFloatHistogram(30)},
						{t: minT + 3, fh: test.GenerateTestFloatHistogram(20)},
					},
				},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()

			// Count the number of series and chunks to check the stats later.
			seriesCount, chunksCount := 0, 0
			for _, res := range testData.storeSetResponses {
				m, ok := res.(map[BlocksStoreClient][]ulid.ULID)
				if !ok {
					// If this isn't a success response we can't count series or chunks.
					continue
				}

				for k := range m {
					mockClient := k.(*storeGatewayClientMock)
					for _, sr := range mockClient.mockedSeriesResponses {
						if s := sr.GetStreamingSeries(); s != nil {
							seriesCount += len(s.Series)
						}

						if c := sr.GetStreamingChunksEstimate(); c != nil {
							chunksCount += int(c.EstimatedChunkCount)
						}
					}
				}
			}

			stores := &blocksStoreSetMock{mockedResponses: testData.storeSetResponses}
			finder := &blocksFinderMock{}
			finder.On("GetBlocks", mock.Anything, "user-1", minT, maxT).Return(testData.finderResult, &bucketindex.Metadata{}, testData.finderErr)

			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			ctx = limiter.AddQueryLimiterToContext(ctx, testData.queryLimiter)
			ctx = limiter.ContextWithNewUnlimitedMemoryConsumptionTracker(ctx)
			metrics := limiter.NewSeriesDeduplicatorMetrics(reg)
			ctx = limiter.ContextWithNewSeriesLabelsDeduplicator(ctx, metrics)
			st, ctx := stats.ContextWithEmptyStats(ctx)
			const tenantID = "user-1"
			ctx = user.InjectOrgID(ctx, tenantID)
			q := &blocksStoreQuerier{
				minT:               minT,
				maxT:               maxT,
				finder:             finder,
				stores:             stores,
				dynamicReplication: newDynamicReplication(),
				consistency:        NewBlocksConsistency(0, reg),
				logger:             log.NewNopLogger(),
				metrics:            newBlocksStoreQueryableMetrics(reg),
				limits:             testData.limits,
			}

			matchers := []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, metricName),
			}
			if testData.queryShardID != "" {
				matchers = append(matchers, labels.MustNewMatcher(labels.MatchEqual, sharding.ShardLabel, testData.queryShardID))
			}

			sp := &storage.SelectHints{Start: minT, End: maxT}
			set := q.Select(ctx, true, sp, matchers...)
			if testData.expectedErr != nil {
				if set.Err() == nil {
					// In case of streaming, the error can happen during iteration.
					var err error
					for set.Next() {
						it := set.At().Iterator(nil)
						for it.Next() != chunkenc.ValNone { // nolint
						}
						err = it.Err()
						if err != nil {
							break
						}
					}
					assert.ErrorIs(t, err, testData.expectedErr)
				} else {
					assert.ErrorContains(t, set.Err(), testData.expectedErr.Error())
					assert.IsType(t, set.Err(), testData.expectedErr)
					assert.False(t, set.Next())
					assert.Nil(t, set.Warnings())
				}
			} else {
				require.NoError(t, set.Err())
				assert.Len(t, set.Warnings(), 0)

				// Read all returned series and their values.
				var actualSeries []seriesResult
				var it chunkenc.Iterator
				for set.Next() {
					var actualValues []valueResult

					it = set.At().Iterator(it)
					for valType := it.Next(); valType != chunkenc.ValNone; valType = it.Next() {
						switch valType {
						case chunkenc.ValFloat:
							t, v := it.At()
							actualValues = append(actualValues, valueResult{
								t: t,
								v: v,
							})
						case chunkenc.ValFloatHistogram:
							t, fh := it.AtFloatHistogram(nil)
							actualValues = append(actualValues, valueResult{
								t:  t,
								fh: fh,
							})
						default:
							require.FailNow(t, "unhandled type")
						}
					}

					require.NoError(t, it.Err())

					actualSeries = append(actualSeries, seriesResult{
						lbls:   set.At().Labels(),
						values: actualValues,
					})
				}
				require.NoError(t, set.Err())
				assert.Equal(t, testData.expectedSeries, actualSeries)
				assert.Equal(t, seriesCount, int(st.FetchedSeriesCount))
				assert.Equal(t, chunksCount, int(st.FetchedChunksCount))
			}

			// Assert on metrics (optional, only for test cases defining it).
			if testData.expectedMetrics != "" {
				assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(testData.expectedMetrics),
					"cortex_querier_storegateway_instances_hit_per_query", "cortex_querier_storegateway_refetches_per_query",
					"cortex_querier_blocks_found_total", "cortex_querier_blocks_queried_total", "cortex_querier_blocks_with_compactor_shard_but_incompatible_query_shard_total",
					"cortex_querier_query_storegateway_chunks_total", "cortex_querier_blocks_consistency_checks_total", "cortex_querier_blocks_consistency_checks_failed_total"))
			}
		})
	}
}

func TestBlocksStoreQuerier_Select_ClosedBeforeSelectFinishes(t *testing.T) {
	const minT = int64(10)
	const maxT = int64(20)

	block := ulid.MustNew(1, nil)
	storeSetResponses := []interface{}{
		map[BlocksStoreClient][]ulid.ULID{
			&storeGatewayClientMock{
				remoteAddr: "1.1.1.1",
				mockedSeriesResponses: newSeriesResponseBuilder().
					addValue(labels.FromStrings(model.MetricNameLabel, "some_metric"), minT, 1).
					addBlocks(block).
					build(),
			}: {block},
		},
	}

	stores := &blocksStoreSetMock{mockedResponses: storeSetResponses}
	finder := &blocksFinderMock{}
	finderResult := bucketindex.Blocks{
		{ID: block},
	}
	finder.On("GetBlocks", mock.Anything, "user-1", minT, maxT).Return(finderResult, &bucketindex.Metadata{}, nil)

	reg := prometheus.NewPedanticRegistry()
	ctx := user.InjectOrgID(context.Background(), "user-1")
	ctx = limiter.ContextWithNewUnlimitedMemoryConsumptionTracker(ctx)
	metrics := limiter.NewSeriesDeduplicatorMetrics(reg)
	ctx = limiter.ContextWithNewSeriesLabelsDeduplicator(ctx, metrics)
	querier := &blocksStoreQuerier{
		minT:               minT,
		maxT:               maxT,
		finder:             finder,
		stores:             stores,
		dynamicReplication: newDynamicReplication(),
		consistency:        NewBlocksConsistency(0, reg),
		logger:             log.NewNopLogger(),
		metrics:            newBlocksStoreQueryableMetrics(reg),
		limits:             &blocksStoreLimitsMock{},
	}

	// For simplicity, we close the querier before issuing the Select call, but in the real world,
	// this would likely happen while the Select call is still in progress (eg. because the query was cancelled).
	require.NoError(t, querier.Close())

	seriesSet := querier.Select(ctx, true, &storage.SelectHints{Start: minT, End: maxT}, labels.MustNewMatcher(labels.MatchRegexp, model.MetricNameLabel, ".*"))
	require.EqualError(t, seriesSet.Err(), "querier already closed")

	// We also expect that the background goroutine launched by the stream reader is either never started or stopped correctly, and this should be
	// caught by VerifyNoLeak in TestMain().
}

func TestBlocksStoreQuerier_ShouldReturnContextCanceledIfContextWasCanceledWhileRunningRequestOnStoreGateway(t *testing.T) {
	const (
		tenantID   = "user-1"
		metricName = "test_metric"
		minT       = int64(math.MinInt64)
		maxT       = int64(math.MaxInt64)
	)

	var (
		block1 = ulid.MustNew(1, nil)
		logger = test.NewTestingLogger(t)
	)

	// Create an utility to easily run each test case in isolation.
	prepareTestCase := func(t *testing.T) (*mockStoreGatewayServer, *blocksStoreQuerier, *prometheus.Registry) {
		// Create a GRPC server used to query the mocked service.
		grpcServer := grpc.NewServer()
		t.Cleanup(grpcServer.GracefulStop)

		srv := &mockStoreGatewayServer{}
		storegatewaypb.RegisterStoreGatewayServer(grpcServer, srv)

		listener, err := net.Listen("tcp", "localhost:0")
		require.NoError(t, err)

		go func() {
			require.NoError(t, grpcServer.Serve(listener))
		}()

		// Mock the blocks finder.
		finder := &blocksFinderMock{}
		finder.On("GetBlocks", mock.Anything, tenantID, minT, maxT).Return(bucketindex.Blocks{{ID: block1}}, &bucketindex.Metadata{}, nil)

		// Create a real gRPC client connecting to the gRPC server we control in this test.
		clientCfg := grpcclient.Config{}
		flagext.DefaultValues(&clientCfg)

		client, err := dialStoreGatewayClient(
			clientCfg,
			ring.InstanceDesc{Addr: listener.Addr().String()},
			promauto.With(nil).NewHistogramVec(prometheus.HistogramOpts{}, []string{"route", "status_code"}),
			util.NewRequestInvalidClusterValidationLabelsTotalCounter(nil, "store-gateway", util.GRPCProtocol),
			promauto.With(nil).NewCounterVec(prometheus.CounterOpts{}, []string{"store_gateway_zone"}),
			log.NewNopLogger())

		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, client.Close())
		})

		// Mock the stores, returning a gRPC client connecting to the gRPC server controlled in this test.
		stores := &blocksStoreSetMock{mockedResponses: []interface{}{
			// These tests only require 1 mocked response, but we mock it multiple times to make debugging easier
			// when the tests fail because the request is retried (even if we expect not to be retried).
			map[BlocksStoreClient][]ulid.ULID{client: {block1}},
			map[BlocksStoreClient][]ulid.ULID{client: {block1}},
			map[BlocksStoreClient][]ulid.ULID{client: {block1}},
		}}

		reg := prometheus.NewPedanticRegistry()
		q := &blocksStoreQuerier{
			minT:               minT,
			maxT:               maxT,
			finder:             finder,
			stores:             stores,
			dynamicReplication: newDynamicReplication(),
			consistency:        NewBlocksConsistency(0, reg),
			logger:             logger,
			metrics:            newBlocksStoreQueryableMetrics(reg),
			limits:             &blocksStoreLimitsMock{},
		}

		return srv, q, reg
	}

	t.Run("Series()", func(t *testing.T) {
		// Mock the gRPC server to control the execution of Series().
		var (
			ctx, cancelCtx    = context.WithCancel(user.InjectOrgID(context.Background(), tenantID))
			numExecutions     = atomic.NewInt64(0)
			waitExecution     = make(chan struct{})
			continueExecution = make(chan struct{})
		)
		ctx = limiter.ContextWithNewUnlimitedMemoryConsumptionTracker(ctx)
		metrics := limiter.NewSeriesDeduplicatorMetrics(prometheus.NewPedanticRegistry())
		ctx = limiter.ContextWithNewSeriesLabelsDeduplicator(ctx, metrics)

		srv, q, reg := prepareTestCase(t)

		srv.onSeries = func(*storepb.SeriesRequest, storegatewaypb.StoreGateway_SeriesServer) error {
			if numExecutions.Inc() == 1 {
				close(waitExecution)
				<-continueExecution
			}
			return nil
		}

		go func() {
			// Cancel the context while Series() is executing.
			<-waitExecution
			cancelCtx()
			close(continueExecution)
		}()

		sp := &storage.SelectHints{Start: minT, End: maxT}
		set := q.Select(ctx, true, sp, labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, metricName))

		// We expect the returned error to be context.Canceled and not a gRPC error.
		assert.ErrorIs(t, set.Err(), context.Canceled)

		// Ensure the blocks store querier didn't retry requests to store-gateways.
		assert.Equal(t, int64(1), numExecutions.Load())

		// Ensure no consistency check failure is tracked when context is canceled.
		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_querier_blocks_consistency_checks_failed_total Total number of queries that failed consistency checks. A failed consistency check means that some of at least one block which had to be queried wasn't present in any of the store-gateways.
			# TYPE cortex_querier_blocks_consistency_checks_failed_total counter
			cortex_querier_blocks_consistency_checks_failed_total 0

			# HELP cortex_querier_blocks_consistency_checks_total Total number of queries that needed to run with consistency checks. A consistency check is required when querying blocks from store-gateways to make sure that all blocks are queried.
			# TYPE cortex_querier_blocks_consistency_checks_total counter
			cortex_querier_blocks_consistency_checks_total 0
			`),
			"cortex_querier_blocks_consistency_checks_total",
			"cortex_querier_blocks_consistency_checks_failed_total"))
	})

	t.Run("LabelNames()", func(t *testing.T) {
		// Mock the gRPC server to control the execution of LabelNames().
		var (
			ctx, cancelCtx    = context.WithCancel(user.InjectOrgID(context.Background(), tenantID))
			numExecutions     = atomic.NewInt64(0)
			waitExecution     = make(chan struct{})
			continueExecution = make(chan struct{})
		)

		srv, q, reg := prepareTestCase(t)

		srv.onLabelNames = func(context.Context, *storepb.LabelNamesRequest) (*storepb.LabelNamesResponse, error) {
			if numExecutions.Inc() == 1 {
				close(waitExecution)
				<-continueExecution
			}
			return &storepb.LabelNamesResponse{}, nil
		}

		go func() {
			// Cancel the context while Series() is executing.
			<-waitExecution
			cancelCtx()
			close(continueExecution)
		}()

		_, _, err := q.LabelNames(ctx, &storage.LabelHints{}, labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, metricName))

		// We expect the returned error to be context.Canceled and not a gRPC error.
		assert.ErrorIs(t, err, context.Canceled)

		// Ensure the blocks store querier didn't retry requests to store-gateways.
		assert.Equal(t, int64(1), numExecutions.Load())

		// Ensure no consistency check failure is tracked when context is canceled.
		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_querier_blocks_consistency_checks_failed_total Total number of queries that failed consistency checks. A failed consistency check means that some of at least one block which had to be queried wasn't present in any of the store-gateways.
			# TYPE cortex_querier_blocks_consistency_checks_failed_total counter
			cortex_querier_blocks_consistency_checks_failed_total 0

			# HELP cortex_querier_blocks_consistency_checks_total Total number of queries that needed to run with consistency checks. A consistency check is required when querying blocks from store-gateways to make sure that all blocks are queried.
			# TYPE cortex_querier_blocks_consistency_checks_total counter
			cortex_querier_blocks_consistency_checks_total 0
			`),
			"cortex_querier_blocks_consistency_checks_total",
			"cortex_querier_blocks_consistency_checks_failed_total"))
	})

	t.Run("LabelValues()", func(t *testing.T) {
		// Mock the gRPC server to control the execution of LabelValues().
		var (
			ctx, cancelCtx    = context.WithCancel(user.InjectOrgID(context.Background(), tenantID))
			numExecutions     = atomic.NewInt64(0)
			waitExecution     = make(chan struct{})
			continueExecution = make(chan struct{})
		)

		srv, q, reg := prepareTestCase(t)

		srv.onLabelValues = func(context.Context, *storepb.LabelValuesRequest) (*storepb.LabelValuesResponse, error) {
			if numExecutions.Inc() == 1 {
				close(waitExecution)
				<-continueExecution
			}
			return &storepb.LabelValuesResponse{}, nil
		}

		go func() {
			// Cancel the context while Series() is executing.
			<-waitExecution
			cancelCtx()
			close(continueExecution)
		}()

		_, _, err := q.LabelValues(ctx, model.MetricNameLabel, &storage.LabelHints{})

		// We expect the returned error to be context.Canceled and not a gRPC error.
		assert.ErrorIs(t, err, context.Canceled)

		// Ensure the blocks store querier didn't retry requests to store-gateways.
		assert.Equal(t, int64(1), numExecutions.Load())

		// Ensure no consistency check failure is tracked when context is canceled.
		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_querier_blocks_consistency_checks_failed_total Total number of queries that failed consistency checks. A failed consistency check means that some of at least one block which had to be queried wasn't present in any of the store-gateways.
			# TYPE cortex_querier_blocks_consistency_checks_failed_total counter
			cortex_querier_blocks_consistency_checks_failed_total 0

			# HELP cortex_querier_blocks_consistency_checks_total Total number of queries that needed to run with consistency checks. A consistency check is required when querying blocks from store-gateways to make sure that all blocks are queried.
			# TYPE cortex_querier_blocks_consistency_checks_total counter
			cortex_querier_blocks_consistency_checks_total 0
			`),
			"cortex_querier_blocks_consistency_checks_total",
			"cortex_querier_blocks_consistency_checks_failed_total"))
	})
}

func TestBlocksStoreQuerier_Select_cancelledContext(t *testing.T) {
	const (
		metricName = "test_metric"
		minT       = int64(10)
		maxT       = int64(20)
	)

	var (
		block            = ulid.MustNew(1, nil)
		noOpQueryLimiter = limiter.NewQueryLimiter(0, 0, 0, 0, nil)
	)

	canceledRequestTests := map[string]bool{
		"canceled request on series stream":           false,
		"canceled request on receiving series stream": true,
	}

	for testName, testData := range canceledRequestTests {
		t.Run(testName, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			ctx = limiter.AddQueryLimiterToContext(ctx, noOpQueryLimiter)
			ctx = limiter.ContextWithNewUnlimitedMemoryConsumptionTracker(ctx)
			reg := prometheus.NewPedanticRegistry()
			metrics := limiter.NewSeriesDeduplicatorMetrics(reg)
			ctx = limiter.ContextWithNewSeriesLabelsDeduplicator(ctx, metrics)

			const tenantID = "user-1"
			ctx = user.InjectOrgID(ctx, tenantID)

			storeGateway := &cancelerStoreGatewayClientMock{
				remoteAddr:    "1.1.1.1",
				produceSeries: testData,
				cancel:        cancel,
			}

			stores := &blocksStoreSetMock{mockedResponses: []interface{}{
				map[BlocksStoreClient][]ulid.ULID{storeGateway: {block}},
				errors.New("no store-gateway remaining after exclude"),
			}}

			finder := &blocksFinderMock{}
			finder.On("GetBlocks", mock.Anything, "user-1", minT, maxT).Return(bucketindex.Blocks{
				{ID: block},
			}, &bucketindex.Metadata{}, nil)

			q := &blocksStoreQuerier{
				minT:               minT,
				maxT:               maxT,
				finder:             finder,
				stores:             stores,
				dynamicReplication: newDynamicReplication(),
				consistency:        NewBlocksConsistency(0, nil),
				logger:             log.NewNopLogger(),
				metrics:            newBlocksStoreQueryableMetrics(reg),
				limits:             &blocksStoreLimitsMock{},
			}

			matchers := []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, metricName),
			}

			sp := &storage.SelectHints{Start: minT, End: maxT}
			set := q.Select(ctx, true, sp, matchers...)
			require.Error(t, set.Err())
			require.ErrorIs(t, set.Err(), context.Canceled)
		})
	}
}

func TestBlocksStoreQuerier_Labels(t *testing.T) {
	const (
		metricName = "test_metric"
		minT       = int64(10)
		maxT       = int64(20)
	)

	var (
		checkedMetrics = []string{"cortex_querier_storegateway_instances_hit_per_query", "cortex_querier_storegateway_refetches_per_query"}

		block1  = ulid.MustNew(1, nil)
		block2  = ulid.MustNew(2, nil)
		block3  = ulid.MustNew(3, nil)
		block4  = ulid.MustNew(4, nil)
		series1 = labels.FromMap(map[string]string{
			model.MetricNameLabel: metricName + "_1",
			"series1":             "1",
		})
		series2 = labels.FromMap(map[string]string{
			model.MetricNameLabel: metricName + "_2",
			"series2":             "1",
		})
	)

	tests := map[string]struct {
		finderResult        bucketindex.Blocks
		finderErr           error
		storeSetResponses   []interface{}
		expectedLabelNames  []string
		expectedLabelValues []string // For __name__
		expectedErrRegex    string
		expectedMetrics     string
	}{
		"no block in the storage matching the query time range": {
			finderResult:     nil,
			expectedErrRegex: "",
		},
		"error while finding blocks matching the query time range": {
			finderErr:        errors.New("unable to find blocks"),
			expectedErrRegex: "unable to find blocks",
		},
		"error while getting clients to query the store-gateway": {
			finderResult: bucketindex.Blocks{
				{ID: block1},
				{ID: block2},
			},
			storeSetResponses: []interface{}{
				errors.New("no client found"),
			},
			expectedErrRegex: "no client found",
		},
		"a single store-gateway instance holds the required blocks": {
			finderResult: bucketindex.Blocks{
				{ID: block1},
				{ID: block2},
			},
			storeSetResponses: []interface{}{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{
						remoteAddr: "1.1.1.1",
						mockedLabelNamesResponse: &storepb.LabelNamesResponse{
							Names:    namesFromSeries(series1, series2),
							Warnings: []string{},
							Hints:    mockNamesHints(block1, block2),
						},
						mockedLabelValuesResponse: &storepb.LabelValuesResponse{
							Values:   valuesFromSeries(model.MetricNameLabel, series1, series2),
							Warnings: []string{},
							Hints:    mockValuesHints(block1, block2),
						},
					}: {block1, block2},
				},
			},
			expectedLabelNames:  namesFromSeries(series1, series2),
			expectedLabelValues: valuesFromSeries(model.MetricNameLabel, series1, series2),
		},
		"multiple store-gateway instances holds the required blocks without overlapping series": {
			finderResult: bucketindex.Blocks{
				{ID: block1},
				{ID: block2},
			},
			storeSetResponses: []interface{}{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{
						remoteAddr: "1.1.1.1",
						mockedLabelNamesResponse: &storepb.LabelNamesResponse{
							Names:    namesFromSeries(series1),
							Warnings: []string{},
							Hints:    mockNamesHints(block1),
						},
						mockedLabelValuesResponse: &storepb.LabelValuesResponse{
							Values:   valuesFromSeries(model.MetricNameLabel, series1),
							Warnings: []string{},
							Hints:    mockValuesHints(block1),
						},
					}: {block1},
					&storeGatewayClientMock{
						remoteAddr: "2.2.2.2",
						mockedLabelNamesResponse: &storepb.LabelNamesResponse{
							Names:    namesFromSeries(series2),
							Warnings: []string{},
							Hints:    mockNamesHints(block2),
						},
						mockedLabelValuesResponse: &storepb.LabelValuesResponse{
							Values:   valuesFromSeries(model.MetricNameLabel, series2),
							Warnings: []string{},
							Hints:    mockValuesHints(block2),
						},
					}: {block2},
				},
			},
			expectedLabelNames:  namesFromSeries(series1, series2),
			expectedLabelValues: valuesFromSeries(model.MetricNameLabel, series1, series2),
		},
		"multiple store-gateway instances holds the required blocks with overlapping series (single returned series)": {
			finderResult: bucketindex.Blocks{
				{ID: block1},
				{ID: block2},
			},
			storeSetResponses: []interface{}{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{
						remoteAddr: "1.1.1.1",
						mockedLabelNamesResponse: &storepb.LabelNamesResponse{
							Names:    namesFromSeries(series1),
							Warnings: []string{},
							Hints:    mockNamesHints(block1),
						},
						mockedLabelValuesResponse: &storepb.LabelValuesResponse{
							Values:   valuesFromSeries(model.MetricNameLabel, series1),
							Warnings: []string{},
							Hints:    mockValuesHints(block1),
						},
					}: {block1},
					&storeGatewayClientMock{
						remoteAddr: "2.2.2.2",
						mockedLabelNamesResponse: &storepb.LabelNamesResponse{
							Names:    namesFromSeries(series1),
							Warnings: []string{},
							Hints:    mockNamesHints(block2),
						},
						mockedLabelValuesResponse: &storepb.LabelValuesResponse{
							Values:   valuesFromSeries(model.MetricNameLabel, series1),
							Warnings: []string{},
							Hints:    mockValuesHints(block2),
						},
					}: {block2},
				},
			},
			expectedLabelNames:  namesFromSeries(series1),
			expectedLabelValues: valuesFromSeries(model.MetricNameLabel, series1),
		},
		"multiple store-gateway instances holds the required blocks with overlapping series (multiple returned series)": {
			finderResult: bucketindex.Blocks{
				{ID: block1},
				{ID: block2},
			},
			// Block1 has series1 and series2
			// Block2 has only series1
			// Block3 has only series2
			storeSetResponses: []interface{}{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{
						remoteAddr: "1.1.1.1",
						mockedLabelNamesResponse: &storepb.LabelNamesResponse{
							Names:    namesFromSeries(series1, series2),
							Warnings: []string{},
							Hints:    mockNamesHints(block1),
						},
						mockedLabelValuesResponse: &storepb.LabelValuesResponse{
							Values:   valuesFromSeries(model.MetricNameLabel, series1, series2),
							Warnings: []string{},
							Hints:    mockValuesHints(block1),
						},
					}: {block1},
					&storeGatewayClientMock{
						remoteAddr: "2.2.2.2",
						mockedLabelNamesResponse: &storepb.LabelNamesResponse{
							Names:    namesFromSeries(series1),
							Warnings: []string{},
							Hints:    mockNamesHints(block2),
						},
						mockedLabelValuesResponse: &storepb.LabelValuesResponse{
							Values:   valuesFromSeries(model.MetricNameLabel, series1),
							Warnings: []string{},
							Hints:    mockValuesHints(block2),
						},
					}: {block2},
					&storeGatewayClientMock{
						remoteAddr: "3.3.3.3",
						mockedLabelNamesResponse: &storepb.LabelNamesResponse{
							Names:    namesFromSeries(series2),
							Warnings: []string{},
							Hints:    mockNamesHints(block3),
						},
						mockedLabelValuesResponse: &storepb.LabelValuesResponse{
							Values:   valuesFromSeries(model.MetricNameLabel, series2),
							Warnings: []string{},
							Hints:    mockValuesHints(block3),
						},
					}: {block3},
				},
			},
			expectedLabelNames:  namesFromSeries(series1, series2),
			expectedLabelValues: valuesFromSeries(model.MetricNameLabel, series1, series2),
			expectedMetrics: `
				# HELP cortex_querier_storegateway_instances_hit_per_query Number of store-gateway instances hit for a single query.
				# TYPE cortex_querier_storegateway_instances_hit_per_query histogram
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="0"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="1"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="2"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="4"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="8"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="16"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="32"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="64"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="128"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="256"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="512"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="1024"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="2048"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="+Inf"} 1
				cortex_querier_storegateway_instances_hit_per_query_sum 3
				cortex_querier_storegateway_instances_hit_per_query_count 1

				# HELP cortex_querier_storegateway_refetches_per_query Number of re-fetches attempted while querying store-gateway instances due to missing blocks.
				# TYPE cortex_querier_storegateway_refetches_per_query histogram
				cortex_querier_storegateway_refetches_per_query_bucket{le="0"} 1
				cortex_querier_storegateway_refetches_per_query_bucket{le="1"} 1
				cortex_querier_storegateway_refetches_per_query_bucket{le="2"} 1
				cortex_querier_storegateway_refetches_per_query_bucket{le="+Inf"} 1
				cortex_querier_storegateway_refetches_per_query_sum 0
				cortex_querier_storegateway_refetches_per_query_count 1
			`,
		},
		"a single store-gateway instance has some missing blocks (consistency check failed)": {
			finderResult: bucketindex.Blocks{
				{ID: block1},
				{ID: block2},
			},
			storeSetResponses: []interface{}{
				// First attempt returns a client whose response does not include all expected blocks.
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{
						remoteAddr: "1.1.1.1",
						mockedLabelNamesResponse: &storepb.LabelNamesResponse{
							Names:    namesFromSeries(series1),
							Warnings: []string{},
							Hints:    mockNamesHints(block1),
						},
						mockedLabelValuesResponse: &storepb.LabelValuesResponse{
							Values:   valuesFromSeries(model.MetricNameLabel, series1),
							Warnings: []string{},
							Hints:    mockValuesHints(block1),
						},
					}: {block1},
				},
				// Second attempt returns an error because there are no other store-gateways left.
				errors.New("no store-gateway remaining after exclude"),
			},
			expectedErrRegex: regexp.QuoteMeta(newStoreConsistencyCheckFailedError([]ulid.ULID{block2}).Error()),
		},
		"multiple store-gateway instances have some missing blocks (consistency check failed)": {
			finderResult: bucketindex.Blocks{
				{ID: block1},
				{ID: block2},
				{ID: block3},
				{ID: block4},
			},
			storeSetResponses: []interface{}{
				// First attempt returns a client whose response does not include all expected blocks.
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{
						remoteAddr: "1.1.1.1",
						mockedLabelNamesResponse: &storepb.LabelNamesResponse{
							Names:    namesFromSeries(series1),
							Warnings: []string{},
							Hints:    mockNamesHints(block1),
						},
						mockedLabelValuesResponse: &storepb.LabelValuesResponse{
							Values:   valuesFromSeries(model.MetricNameLabel, series1),
							Warnings: []string{},
							Hints:    mockValuesHints(block1),
						},
					}: {block1},
					&storeGatewayClientMock{
						remoteAddr: "2.2.2.2",
						mockedLabelNamesResponse: &storepb.LabelNamesResponse{
							Names:    namesFromSeries(series2),
							Warnings: []string{},
							Hints:    mockNamesHints(block2),
						},
						mockedLabelValuesResponse: &storepb.LabelValuesResponse{
							Values:   valuesFromSeries(model.MetricNameLabel, series2),
							Warnings: []string{},
							Hints:    mockValuesHints(block2),
						},
					}: {block2},
				},
				// Second attempt returns an error because there are no other store-gateways left.
				errors.New("no store-gateway remaining after exclude"),
			},
			expectedErrRegex: regexp.QuoteMeta(newStoreConsistencyCheckFailedError([]ulid.ULID{block3, block4}).Error()),
		},
		"multiple store-gateway instances have some missing blocks but queried from a replica during subsequent attempts": {
			// Block1 has series1
			// Block2 has series2
			// Block3 has series1 and series2
			// Block4 has no series (poor lonely block)
			finderResult: bucketindex.Blocks{
				{ID: block1},
				{ID: block2},
				{ID: block3},
				{ID: block4},
			},
			storeSetResponses: []interface{}{
				// First attempt returns a client whose response does not include all expected blocks.
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{
						remoteAddr: "1.1.1.1",
						mockedLabelNamesResponse: &storepb.LabelNamesResponse{
							Names:    namesFromSeries(series1),
							Warnings: []string{},
							Hints:    mockNamesHints(block1),
						},
						mockedLabelValuesResponse: &storepb.LabelValuesResponse{
							Values:   valuesFromSeries(model.MetricNameLabel, series1),
							Warnings: []string{},
							Hints:    mockValuesHints(block1),
						},
					}: {block1, block3},
					&storeGatewayClientMock{
						remoteAddr: "2.2.2.2",
						mockedLabelNamesResponse: &storepb.LabelNamesResponse{
							Names:    namesFromSeries(series2),
							Warnings: []string{},
							Hints:    mockNamesHints(block2),
						},
						mockedLabelValuesResponse: &storepb.LabelValuesResponse{
							Values:   valuesFromSeries(model.MetricNameLabel, series2),
							Warnings: []string{},
							Hints:    mockValuesHints(block2),
						},
					}: {block2, block4},
				},
				// Second attempt returns 1 missing block.
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{
						remoteAddr: "3.3.3.3",
						mockedLabelNamesResponse: &storepb.LabelNamesResponse{
							Names:    namesFromSeries(series1, series2),
							Warnings: []string{},
							Hints:    mockNamesHints(block3),
						},
						mockedLabelValuesResponse: &storepb.LabelValuesResponse{
							Values:   valuesFromSeries(model.MetricNameLabel, series1, series2),
							Warnings: []string{},
							Hints:    mockValuesHints(block3),
						},
					}: {block3, block4},
				},
				// Third attempt returns the last missing block.
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{
						remoteAddr: "4.4.4.4",
						mockedLabelNamesResponse: &storepb.LabelNamesResponse{
							Names:    []string{},
							Warnings: []string{},
							Hints:    mockNamesHints(block4),
						},
						mockedLabelValuesResponse: &storepb.LabelValuesResponse{
							Values:   []string{},
							Warnings: []string{},
							Hints:    mockValuesHints(block4),
						},
					}: {block4},
				},
			},
			expectedLabelNames:  namesFromSeries(series1, series2),
			expectedLabelValues: valuesFromSeries(model.MetricNameLabel, series1, series2),
			expectedMetrics: `
				# HELP cortex_querier_storegateway_instances_hit_per_query Number of store-gateway instances hit for a single query.
				# TYPE cortex_querier_storegateway_instances_hit_per_query histogram
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="0"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="1"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="2"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="4"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="8"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="16"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="32"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="64"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="128"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="256"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="512"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="1024"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="2048"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="+Inf"} 1
				cortex_querier_storegateway_instances_hit_per_query_sum 4
				cortex_querier_storegateway_instances_hit_per_query_count 1

				# HELP cortex_querier_storegateway_refetches_per_query Number of re-fetches attempted while querying store-gateway instances due to missing blocks.
				# TYPE cortex_querier_storegateway_refetches_per_query histogram
				cortex_querier_storegateway_refetches_per_query_bucket{le="0"} 0
				cortex_querier_storegateway_refetches_per_query_bucket{le="1"} 0
				cortex_querier_storegateway_refetches_per_query_bucket{le="2"} 1
				cortex_querier_storegateway_refetches_per_query_bucket{le="+Inf"} 1
				cortex_querier_storegateway_refetches_per_query_sum 2
				cortex_querier_storegateway_refetches_per_query_count 1
				# HELP cortex_querier_query_storegateway_chunks_total Number of chunks received from store gateways at query time.
				# TYPE cortex_querier_query_storegateway_chunks_total counter
				cortex_querier_query_storegateway_chunks_total 4
			`,
		},
		"multiple store-gateways have the block, but one of them fails to return": {
			finderResult: bucketindex.Blocks{
				{ID: block1},
			},
			storeSetResponses: []interface{}{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{
						remoteAddr:           "1.1.1.1",
						mockedLabelNamesErr:  errors.New("failed to receive from store-gateway"),
						mockedLabelValuesErr: errors.New("failed to receive from store-gateway"),
					}: {block1},
				},
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{
						remoteAddr: "2.2.2.2",
						mockedLabelNamesResponse: &storepb.LabelNamesResponse{
							Names:    namesFromSeries(series1),
							Warnings: []string{},
							Hints:    mockNamesHints(block1),
						},
						mockedLabelValuesResponse: &storepb.LabelValuesResponse{
							Values:   valuesFromSeries(model.MetricNameLabel, series1),
							Warnings: []string{},
							Hints:    mockValuesHints(block1),
						},
					}: {block1},
				},
			},
			expectedLabelNames:  namesFromSeries(series1),
			expectedLabelValues: valuesFromSeries(model.MetricNameLabel, series1),
			expectedMetrics: `
				# HELP cortex_querier_storegateway_instances_hit_per_query Number of store-gateway instances hit for a single query.
				# TYPE cortex_querier_storegateway_instances_hit_per_query histogram
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="0"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="1"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="2"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="4"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="8"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="16"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="32"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="64"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="128"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="256"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="512"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="1024"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="2048"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="+Inf"} 1
				cortex_querier_storegateway_instances_hit_per_query_sum 2
				cortex_querier_storegateway_instances_hit_per_query_count 1

				# HELP cortex_querier_storegateway_refetches_per_query Number of re-fetches attempted while querying store-gateway instances due to missing blocks.
				# TYPE cortex_querier_storegateway_refetches_per_query histogram
				cortex_querier_storegateway_refetches_per_query_bucket{le="0"} 0
				cortex_querier_storegateway_refetches_per_query_bucket{le="1"} 1
				cortex_querier_storegateway_refetches_per_query_bucket{le="2"} 1
				cortex_querier_storegateway_refetches_per_query_bucket{le="+Inf"} 1
				cortex_querier_storegateway_refetches_per_query_sum 1
				cortex_querier_storegateway_refetches_per_query_count 1
			`,
		},
		"unprocessable entity from store-gateway": {
			finderResult: bucketindex.Blocks{
				{ID: block1},
				{ID: block2},
			},
			storeSetResponses: []interface{}{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{
						remoteAddr:           "1.1.1.1",
						mockedLabelNamesErr:  status.Error(http.StatusUnprocessableEntity, "limit exceeded"),
						mockedLabelValuesErr: status.Error(http.StatusUnprocessableEntity, "limit exceeded"),
					}: {block1, block2},
				},
			},
			expectedErrRegex: "non-retriable error while fetching label (names|values) from store: rpc error: code = Code\\(422\\) desc = limit exceeded",
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			// Splitting it because we need a new registry for names and values.
			// And also the initial expectedErrRegex checking needs to be done for both.
			for _, testFunc := range []string{"LabelNames", "LabelValues"} {
				ctx := user.InjectOrgID(context.Background(), "user-1")
				reg := prometheus.NewPedanticRegistry()
				stores := &blocksStoreSetMock{mockedResponses: testData.storeSetResponses}
				finder := &blocksFinderMock{}
				finder.On("GetBlocks", mock.Anything, "user-1", minT, maxT).Return(testData.finderResult, &bucketindex.Metadata{}, testData.finderErr)

				q := &blocksStoreQuerier{
					minT:               minT,
					maxT:               maxT,
					finder:             finder,
					stores:             stores,
					dynamicReplication: newDynamicReplication(),
					consistency:        NewBlocksConsistency(0, nil),
					logger:             log.NewNopLogger(),
					metrics:            newBlocksStoreQueryableMetrics(reg),
					limits:             &blocksStoreLimitsMock{},
				}

				if testFunc == "LabelNames" {
					names, warnings, err := q.LabelNames(ctx, &storage.LabelHints{})
					if testData.expectedErrRegex != "" {
						require.Regexp(t, testData.expectedErrRegex, err.Error())
						continue
					}

					require.NoError(t, err)
					require.Equal(t, 0, len(warnings))
					require.Equal(t, testData.expectedLabelNames, names)

					// Assert on metrics (optional, only for test cases defining it).
					if testData.expectedMetrics != "" {
						assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(testData.expectedMetrics), checkedMetrics...))
					}
				}

				if testFunc == "LabelValues" {
					values, warnings, err := q.LabelValues(ctx, model.MetricNameLabel, &storage.LabelHints{})
					if testData.expectedErrRegex != "" {
						require.Regexp(t, testData.expectedErrRegex, err.Error())
						continue
					}

					require.NoError(t, err)
					require.Equal(t, 0, len(warnings))
					require.Equal(t, testData.expectedLabelValues, values)

					// Assert on metrics (optional, only for test cases defining it).
					if testData.expectedMetrics != "" {
						assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(testData.expectedMetrics), checkedMetrics...))
					}
				}
			}
		})
	}

	t.Run("canceled request", func(t *testing.T) {
		for _, testFunc := range []string{"LabelNames", "LabelValues"} {
			t.Run(testFunc, func(t *testing.T) {
				ctx, cancel := context.WithCancel(user.InjectOrgID(context.Background(), "user-1"))
				defer cancel()

				reg := prometheus.NewPedanticRegistry()

				storeGateway := &cancelerStoreGatewayClientMock{
					remoteAddr: "1.1.1.1",
					cancel:     cancel,
				}

				stores := &blocksStoreSetMock{mockedResponses: []interface{}{
					map[BlocksStoreClient][]ulid.ULID{storeGateway: {block1}},
					errors.New("no store-gateway remaining after exclude"),
				}}

				finder := &blocksFinderMock{}
				finder.On("GetBlocks", mock.Anything, "user-1", minT, maxT).Return(bucketindex.Blocks{
					{ID: block1},
				}, &bucketindex.Metadata{}, nil)

				q := &blocksStoreQuerier{
					minT:               minT,
					maxT:               maxT,
					finder:             finder,
					stores:             stores,
					dynamicReplication: newDynamicReplication(),
					consistency:        NewBlocksConsistency(0, nil),
					logger:             log.NewNopLogger(),
					metrics:            newBlocksStoreQueryableMetrics(reg),
					limits:             &blocksStoreLimitsMock{},
				}

				var err error
				switch testFunc {
				case "LabelNames":
					_, _, err = q.LabelNames(ctx, &storage.LabelHints{})
				case "LabelValues":
					_, _, err = q.LabelValues(ctx, model.MetricNameLabel, &storage.LabelHints{})
				}

				require.Error(t, err)
				require.ErrorIs(t, err, context.Canceled)
			})
		}
	})
}

func TestBlocksStoreQuerier_SelectSortedShouldHonorQueryStoreAfter(t *testing.T) {
	now := time.Now()
	ctx := context.Background()

	tests := map[string]struct {
		queryStoreAfter time.Duration
		queryMinT       int64
		queryMaxT       int64
		expectedMinT    int64
		expectedMaxT    int64
	}{
		"should not manipulate query time range if queryStoreAfter is disabled": {
			queryStoreAfter: 0,
			queryMinT:       util.TimeToMillis(now.Add(-100 * time.Minute)),
			queryMaxT:       util.TimeToMillis(now.Add(-30 * time.Minute)),
			expectedMinT:    util.TimeToMillis(now.Add(-100 * time.Minute)),
			expectedMaxT:    util.TimeToMillis(now.Add(-30 * time.Minute)),
		},
		"should not manipulate query time range if queryStoreAfter is enabled but query max time is older": {
			queryStoreAfter: time.Hour,
			queryMinT:       util.TimeToMillis(now.Add(-100 * time.Minute)),
			queryMaxT:       util.TimeToMillis(now.Add(-70 * time.Minute)),
			expectedMinT:    util.TimeToMillis(now.Add(-100 * time.Minute)),
			expectedMaxT:    util.TimeToMillis(now.Add(-70 * time.Minute)),
		},
		"should manipulate query time range if queryStoreAfter is enabled and query max time is recent": {
			queryStoreAfter: time.Hour,
			queryMinT:       util.TimeToMillis(now.Add(-100 * time.Minute)),
			queryMaxT:       util.TimeToMillis(now.Add(-30 * time.Minute)),
			expectedMinT:    util.TimeToMillis(now.Add(-100 * time.Minute)),
			expectedMaxT:    util.TimeToMillis(now.Add(-60 * time.Minute)),
		},
		"should skip the query if the query min time is more recent than queryStoreAfter": {
			queryStoreAfter: time.Hour,
			queryMinT:       util.TimeToMillis(now.Add(-50 * time.Minute)),
			queryMaxT:       util.TimeToMillis(now.Add(-20 * time.Minute)),
			expectedMinT:    0,
			expectedMaxT:    0,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			finder := &blocksFinderMock{}
			finder.On("GetBlocks", mock.Anything, "user-1", mock.Anything, mock.Anything).Return(bucketindex.Blocks(nil), &bucketindex.Metadata{}, error(nil))

			const tenantID = "user-1"
			ctx = user.InjectOrgID(ctx, tenantID)
			ctx = limiter.ContextWithNewUnlimitedMemoryConsumptionTracker(ctx)
			metrics := limiter.NewSeriesDeduplicatorMetrics(prometheus.NewPedanticRegistry())
			ctx = limiter.ContextWithNewSeriesLabelsDeduplicator(ctx, metrics)
			q := &blocksStoreQuerier{
				minT:               testData.queryMinT,
				maxT:               testData.queryMaxT,
				finder:             finder,
				stores:             &blocksStoreSetMock{},
				dynamicReplication: newDynamicReplication(),
				consistency:        NewBlocksConsistency(0, nil),
				logger:             log.NewNopLogger(),
				metrics:            newBlocksStoreQueryableMetrics(nil),
				limits:             &blocksStoreLimitsMock{},
				queryStoreAfter:    testData.queryStoreAfter,
			}

			sp := &storage.SelectHints{
				Start: testData.queryMinT,
				End:   testData.queryMaxT,
			}

			set := q.selectSorted(ctx, sp, tenantID)
			require.NoError(t, set.Err())

			if testData.expectedMinT == 0 && testData.expectedMaxT == 0 {
				assert.Len(t, finder.Calls, 0)
			} else {
				require.Len(t, finder.Calls, 1)
				assert.Equal(t, testData.expectedMinT, finder.Calls[0].Arguments.Get(2))
				assert.InDelta(t, testData.expectedMaxT, finder.Calls[0].Arguments.Get(3), float64(5*time.Second.Milliseconds()))
			}
		})
	}
}

func TestBlocksStoreQuerier_MaxLabelsQueryRange(t *testing.T) {
	const (
		thirtyDays = 30 * 24 * time.Hour
		sevenDays  = 7 * 24 * time.Hour
	)
	now := time.Now()

	tests := map[string]struct {
		maxLabelsQueryLength time.Duration
		queryMinT            int64
		queryMaxT            int64
		expectedMinT         int64
		expectedMaxT         int64
	}{
		"should not manipulate query time range if maxLabelsQueryLength is disabled": {
			maxLabelsQueryLength: 0,
			queryMinT:            util.TimeToMillis(now.Add(-thirtyDays)),
			queryMaxT:            util.TimeToMillis(now),
			expectedMinT:         util.TimeToMillis(now.Add(-thirtyDays)),
			expectedMaxT:         util.TimeToMillis(now),
		},
		"should not manipulate query time range if maxLabelsQueryLength is enabled but query fits within": {
			maxLabelsQueryLength: sevenDays,
			queryMinT:            util.TimeToMillis(now.Add(-100 * time.Minute)),
			queryMaxT:            util.TimeToMillis(now.Add(-30 * time.Minute)),
			expectedMinT:         util.TimeToMillis(now.Add(-100 * time.Minute)),
			expectedMaxT:         util.TimeToMillis(now.Add(-30 * time.Minute)),
		},
		"should manipulate query time range if maxLabelsQueryLength is enabled and query overlaps": {
			maxLabelsQueryLength: sevenDays,
			queryMinT:            util.TimeToMillis(now.Add(-thirtyDays)),
			queryMaxT:            util.TimeToMillis(now),
			expectedMinT:         util.TimeToMillis(now.Add(-sevenDays)),
			expectedMaxT:         util.TimeToMillis(now),
		},

		"should manipulate query on large time range over the limit": {
			maxLabelsQueryLength: thirtyDays,
			queryMinT:            util.TimeToMillis(now.Add(-thirtyDays).Add(-100 * time.Hour)),
			queryMaxT:            util.TimeToMillis(now),
			expectedMinT:         util.TimeToMillis(now.Add(-thirtyDays)),
			expectedMaxT:         util.TimeToMillis(now),
		},
		"should manipulate the start of a query without start time": {
			maxLabelsQueryLength: thirtyDays,
			queryMinT:            util.TimeToMillis(v1.MinTime),
			queryMaxT:            util.TimeToMillis(now),
			expectedMinT:         util.TimeToMillis(now.Add(-thirtyDays)),
			expectedMaxT:         util.TimeToMillis(now),
		},
		"should not manipulate query without end time, we allow querying arbitrarily into the future": {
			maxLabelsQueryLength: thirtyDays,
			queryMinT:            util.TimeToMillis(now.Add(-time.Hour)),
			queryMaxT:            util.TimeToMillis(v1.MaxTime),
			expectedMinT:         util.TimeToMillis(now.Add(-time.Hour)),
			expectedMaxT:         util.TimeToMillis(v1.MaxTime),
		},
		"should manipulate the start of a query without start or end time, we allow querying arbitrarily into the future, but not the past": {
			maxLabelsQueryLength: thirtyDays,
			queryMinT:            util.TimeToMillis(v1.MinTime),
			queryMaxT:            util.TimeToMillis(v1.MaxTime),
			expectedMinT:         util.TimeToMillis(now.Add(-thirtyDays)),
			expectedMaxT:         util.TimeToMillis(v1.MaxTime),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			finder := &blocksFinderMock{}
			finder.On("GetBlocks", mock.Anything, "user-1", mock.Anything, mock.Anything).Return(bucketindex.Blocks(nil), &bucketindex.Metadata{}, error(nil))

			ctx := user.InjectOrgID(context.Background(), "user-1")
			q := &blocksStoreQuerier{
				minT:               testData.queryMinT,
				maxT:               testData.queryMaxT,
				finder:             finder,
				stores:             &blocksStoreSetMock{},
				dynamicReplication: newDynamicReplication(),
				consistency:        NewBlocksConsistency(0, nil),
				logger:             log.NewNopLogger(),
				metrics:            newBlocksStoreQueryableMetrics(nil),
				limits: &blocksStoreLimitsMock{
					maxLabelsQueryLength: testData.maxLabelsQueryLength,
				},
			}

			assertCalledWithMinMaxTime := func() {
				const delta = float64(5000)
				require.Len(t, finder.Calls, 1)
				gotStartMillis := finder.Calls[0].Arguments.Get(2).(int64)
				assert.InDeltaf(t, testData.expectedMinT, gotStartMillis, delta, "expected start %s, got %s", util.TimeFromMillis(testData.expectedMinT).UTC(), util.TimeFromMillis(gotStartMillis).UTC())
				gotEndMillis := finder.Calls[0].Arguments.Get(3).(int64)
				assert.InDeltaf(t, testData.expectedMaxT, gotEndMillis, delta, "expected end %s, got %s", util.TimeFromMillis(testData.expectedMinT).UTC(), util.TimeFromMillis(gotEndMillis).UTC())
				finder.Calls = finder.Calls[1:]
			}

			// Assert on the time range of the actual executed query (5s delta).
			t.Run("LabelNames", func(t *testing.T) {
				_, _, err := q.LabelNames(ctx, &storage.LabelHints{})
				require.NoError(t, err)
				assertCalledWithMinMaxTime()
			})

			t.Run("LabelValues", func(t *testing.T) {
				_, _, err := q.LabelValues(ctx, "foo", &storage.LabelHints{})
				require.NoError(t, err)
				assertCalledWithMinMaxTime()
			})
		})
	}
}

func newDynamicReplication() *storegateway.MaxTimeDynamicReplication {
	cfg := storegateway.Config{}
	flagext.DefaultValues(&cfg)
	cfg.DynamicReplication.Enabled = true
	return storegateway.NewMaxTimeDynamicReplication(cfg, time.Hour)
}

func TestBlocksStoreQuerier_PromQLExecution(t *testing.T) {
	// Prepare series fixtures.
	series1 := labels.FromStrings("__name__", "metric_1")
	series2 := labels.FromStrings("__name__", "metric_2")
	series3 := labels.FromStrings("__name__", "metric_3_ooo")
	series4 := labels.FromStrings("__name__", "metric_4_ooo_and_overlapping")

	block1 := ulid.MustNew(1, nil)
	block2 := ulid.MustNew(2, nil)

	generateSeriesSamples := func(value float64) []promql.FPoint {
		return []promql.FPoint{
			{T: 1589759955000, F: value},
			{T: 1589759970000, F: value},
			{T: 1589759985000, F: value},
			{T: 1589760000000, F: value},
			{T: 1589760015000, F: value},
			{T: 1589760030000, F: value},
		}
	}

	series1Samples := generateSeriesSamples(1)
	series2Samples := generateSeriesSamples(2)
	series3Samples := generateSeriesSamples(3)
	series4Samples := generateSeriesSamples(4)
	queryStart := time.Unix(0, series1Samples[0].T*int64(time.Millisecond))
	queryEnd := time.Unix(0, series1Samples[len(series1Samples)-1].T*int64(time.Millisecond))

	tests := map[string]struct {
		query                  string
		storeGateway1Responses []*storepb.SeriesResponse
		storeGateway2Responses []*storepb.SeriesResponse
		expected               promql.Matrix
	}{
		"should query metrics with chunks in the right order": {
			query: `{__name__=~"metric_(1|2)"}`,
			storeGateway1Responses: newSeriesResponseBuilder().
				addSamples(series1, series1Samples[:3]...). // First half.
				addSamples(series2, series2Samples[:3]...). // First half.
				addBlocks(block1).
				build(),
			storeGateway2Responses: newSeriesResponseBuilder().
				addSamples(series1, series1Samples[3:]...). // Second half.
				addSamples(series2, series2Samples[3:]...). // Second half.
				addBlocks(block2).
				build(),
			expected: promql.Matrix{
				{Metric: series1, Floats: series1Samples},
				{Metric: series2, Floats: series2Samples},
			},
		},
		"should query metrics with out-of-order chunks": {
			query: `{__name__=~".*ooo.*"}`,
			storeGateway1Responses: newSeriesResponseBuilder().
				addChunks(series3, []storepb.AggrChunk{
					createAggrChunkWithSamples(series3Samples[2:4]...),
					createAggrChunkWithSamples(series3Samples[0:2]...), // Out of order.
				}...).
				addChunks(series4, []storepb.AggrChunk{
					createAggrChunkWithSamples(series4Samples[2:4]...),
					createAggrChunkWithSamples(series4Samples[0:3]...), // Out of order and overlapping.
				}...).
				addBlocks(block1).
				build(),

			storeGateway2Responses: newSeriesResponseBuilder().
				addSamples(series3, series3Samples[4:6]...).
				addSamples(series4, series4Samples[4:6]...).
				addBlocks(block2).
				build(),
			expected: promql.Matrix{
				{Metric: series3, Floats: series3Samples},
				{Metric: series4, Floats: series4Samples},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {

			ctx := context.Background()
			ctx = limiter.ContextWithNewUnlimitedMemoryConsumptionTracker(ctx)
			metrics := limiter.NewSeriesDeduplicatorMetrics(prometheus.NewPedanticRegistry())
			ctx = limiter.ContextWithNewSeriesLabelsDeduplicator(ctx, metrics)

			// Mock the finder to simulate we need to query two blocks.
			finder := &blocksFinderMock{
				Service: services.NewIdleService(nil, nil),
			}
			finder.On("GetBlocks", mock.Anything, "user-1", mock.Anything, mock.Anything).Return(bucketindex.Blocks{
				{ID: block1},
				{ID: block2},
			}, &bucketindex.Metadata{}, error(nil))

			// Mock the store-gateway response, to simulate the case each block is queried from a different gateway.
			gateway1 := &storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesResponses: testData.storeGateway1Responses}
			gateway2 := &storeGatewayClientMock{remoteAddr: "2.2.2.2", mockedSeriesResponses: testData.storeGateway2Responses}

			stores := &blocksStoreSetMock{
				Service: services.NewIdleService(nil, nil),
				mockedResponses: []interface{}{
					map[BlocksStoreClient][]ulid.ULID{
						gateway1: {block1},
						gateway2: {block2},
					},
				},
			}

			// Instantiate the querier that will be executed to run the query.
			logger := log.NewNopLogger()
			queryable, err := NewBlocksStoreQueryable(stores, storegateway.NewNopDynamicReplication(3), finder, NewBlocksConsistency(0, nil), &blocksStoreLimitsMock{}, 0, 0, logger, nil)
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), queryable))
			defer services.StopAndAwaitTerminated(context.Background(), queryable) // nolint:errcheck

			engine := promql.NewEngine(promql.EngineOpts{
				Logger:     util_log.SlogFromGoKit(logger),
				Timeout:    10 * time.Second,
				MaxSamples: 1e6,
			})

			// Query metrics.
			ctx = user.InjectOrgID(ctx, "user-1")
			q, err := engine.NewRangeQuery(ctx, queryable, nil, testData.query, queryStart, queryEnd, 15*time.Second)
			require.NoError(t, err)

			res := q.Exec(ctx)
			require.NoError(t, err)
			require.NoError(t, res.Err)

			matrix, err := res.Matrix()
			require.NoError(t, err)
			assert.Equal(t, testData.expected, matrix)
		})
	}
}

func TestCanBlockWithCompactorShardIdContainQueryShard(t *testing.T) {
	const numSeries = 1000
	const maxShards = 512

	hashes := make([]uint64, numSeries)
	for ix := 0; ix < numSeries; ix++ {
		hashes[ix] = rand.Uint64()
	}

	for compactorShards := uint64(1); compactorShards <= maxShards; compactorShards++ {
		for queryShards := uint64(1); queryShards <= maxShards; queryShards++ {
			for _, seriesHash := range hashes {
				// Compute the query shard index for the given series.
				queryShardIndex := seriesHash % queryShards

				// Compute the compactor shard index where the series really is.
				compactorShardIndex := seriesHash % compactorShards

				// This must always be true when querying correct compactor shard.
				res, _ := canBlockWithCompactorShardIndexContainQueryShard(queryShardIndex, queryShards, compactorShardIndex, compactorShards)
				if !res {
					t.Fatalf("series hash: %d, queryShards: %d, queryIndex: %d, compactorShards: %d, compactorIndex: %d", seriesHash, queryShards, queryShardIndex, compactorShards, compactorShardIndex)
				}
			}
		}
	}
}

func TestFilterBlocksByShard(t *testing.T) {
	block1 := &bucketindex.Block{ID: ulid.MustNew(ulid.Now(), crand.Reader), MinTime: 0, MaxTime: 100, CompactorShardID: "1_of_4"}
	block2 := &bucketindex.Block{ID: ulid.MustNew(ulid.Now(), crand.Reader), MinTime: 0, MaxTime: 100, CompactorShardID: "2_of_4"}
	block3 := &bucketindex.Block{ID: ulid.MustNew(ulid.Now(), crand.Reader), MinTime: 0, MaxTime: 100, CompactorShardID: "3_of_4"}
	block4 := &bucketindex.Block{ID: ulid.MustNew(ulid.Now(), crand.Reader), MinTime: 0, MaxTime: 100, CompactorShardID: "4_of_4"}

	block5 := &bucketindex.Block{ID: ulid.MustNew(ulid.Now(), crand.Reader), MinTime: 100, MaxTime: 200, CompactorShardID: "1_of_4"}
	block6 := &bucketindex.Block{ID: ulid.MustNew(ulid.Now(), crand.Reader), MinTime: 100, MaxTime: 200, CompactorShardID: "2_of_4"}
	block7 := &bucketindex.Block{ID: ulid.MustNew(ulid.Now(), crand.Reader), MinTime: 100, MaxTime: 200, CompactorShardID: "3_of_4"}
	block8 := &bucketindex.Block{ID: ulid.MustNew(ulid.Now(), crand.Reader), MinTime: 100, MaxTime: 200, CompactorShardID: "4_of_4"}

	allBlocks := []*bucketindex.Block{block1, block2, block3, block4, block5, block6, block7, block8}

	for name, testcase := range map[string]struct {
		queryShardID       string
		expectedBlocks     bucketindex.Blocks
		incompatibleBlocks int
	}{
		"equal number of query shards": {
			queryShardID:   "1_of_4",
			expectedBlocks: bucketindex.Blocks{block1, block5},
		},
		"less query shards than compactor shards 1": {
			queryShardID:   "1_of_2",
			expectedBlocks: bucketindex.Blocks{block1, block3, block5, block7},
		},
		"less query shards than compactor shards 2": {
			queryShardID:   "2_of_2",
			expectedBlocks: bucketindex.Blocks{block2, block4, block6, block8},
		},
		"double the equal number of query shards 1": {
			queryShardID:   "3_of_8",
			expectedBlocks: bucketindex.Blocks{block3, block7},
		},
		"double the equal number of query shards 2": {
			queryShardID:   "5_of_8",
			expectedBlocks: bucketindex.Blocks{block1, block5},
		},
		"non-divisible number of shards (less than compactor shards)": {
			queryShardID:       "3_of_7",
			expectedBlocks:     allBlocks,
			incompatibleBlocks: 8,
		},
		"non-divisible number of shards (higher than compactor shards)": {
			queryShardID:       "3_of_9",
			expectedBlocks:     allBlocks,
			incompatibleBlocks: 8,
		},
		"query shard using shard count which isn't power of 2": {
			queryShardID:   "5_of_12",
			expectedBlocks: bucketindex.Blocks{block1, block5},
		},
		"query shard using shard count which isn't power of 2 (2nd test)": {
			queryShardID:   "14_of_20",
			expectedBlocks: bucketindex.Blocks{block2, block6},
		},
	} {
		t.Run(name, func(t *testing.T) {
			queryShardIndex, queryShardCount, err := sharding.ParseShardIDLabelValue(testcase.queryShardID)
			require.NoError(t, err)

			blocksCopy := append([]*bucketindex.Block(nil), allBlocks...)

			result, incompatible := filterBlocksByShard(blocksCopy, queryShardIndex, queryShardCount)

			require.Equal(t, testcase.expectedBlocks, result)
			require.Equal(t, testcase.incompatibleBlocks, incompatible)
		})
	}
}

type blocksStoreSetMock struct {
	services.Service

	mockedResponses []interface{}
	nextResult      int
}

func (m *blocksStoreSetMock) GetClientsFor(_ string, _ bucketindex.Blocks, _ map[ulid.ULID][]string) (map[BlocksStoreClient][]ulid.ULID, error) {
	if m.nextResult >= len(m.mockedResponses) {
		panic("not enough mocked results")
	}

	res := m.mockedResponses[m.nextResult]
	m.nextResult++

	if err, ok := res.(error); ok {
		return nil, err
	}
	if clients, ok := res.(map[BlocksStoreClient][]ulid.ULID); ok {
		return clients, nil
	}

	return nil, errors.New("unknown data type in the mocked result")
}

type blocksFinderMock struct {
	services.Service
	mock.Mock
}

func (m *blocksFinderMock) GetBlocks(ctx context.Context, userID string, minT, maxT int64) (bucketindex.Blocks, *bucketindex.Metadata, error) {
	args := m.Called(ctx, userID, minT, maxT)
	return args.Get(0).(bucketindex.Blocks), args.Get(1).(*bucketindex.Metadata), args.Error(2)
}

type storeGatewayClientMock struct {
	remoteAddr                string
	remoteZone                string
	mockedSeriesResponses     []*storepb.SeriesResponse
	mockedSeriesErr           error
	mockedLabelNamesResponse  *storepb.LabelNamesResponse
	mockedLabelNamesErr       error
	mockedLabelValuesResponse *storepb.LabelValuesResponse
	mockedLabelValuesErr      error
}

func (m *storeGatewayClientMock) Series(ctx context.Context, _ *storepb.SeriesRequest, _ ...grpc.CallOption) (storegatewaypb.StoreGateway_SeriesClient, error) {
	// Make an independent copy of the responses, to avoid data races during tests.
	seriesResponses := slices.Clone(m.mockedSeriesResponses)

	seriesClient := &storeGatewaySeriesClientMock{
		ClientStream:    grpcClientStreamMock{ctx: ctx}, // Required to not panic.
		mockedResponses: seriesResponses,
	}

	return seriesClient, m.mockedSeriesErr
}

func (m *storeGatewayClientMock) LabelNames(context.Context, *storepb.LabelNamesRequest, ...grpc.CallOption) (*storepb.LabelNamesResponse, error) {
	return m.mockedLabelNamesResponse, m.mockedLabelNamesErr
}

func (m *storeGatewayClientMock) LabelValues(context.Context, *storepb.LabelValuesRequest, ...grpc.CallOption) (*storepb.LabelValuesResponse, error) {
	return m.mockedLabelValuesResponse, m.mockedLabelValuesErr
}

func (m *storeGatewayClientMock) RemoteAddress() string {
	return m.remoteAddr
}

func (m *storeGatewayClientMock) RemoteZone() string {
	return m.remoteZone
}

type storeGatewaySeriesClientMock struct {
	grpc.ClientStream

	mockedResponses []*storepb.SeriesResponse
}

func (m *storeGatewaySeriesClientMock) Recv() (*storepb.SeriesResponse, error) {
	// Ensure some concurrency occurs.
	time.Sleep(10 * time.Millisecond)

	if len(m.mockedResponses) == 0 {
		return nil, io.EOF
	}

	res := m.mockedResponses[0]
	m.mockedResponses = m.mockedResponses[1:]
	return res, nil
}

type grpcClientStreamMock struct {
	ctx context.Context
}

func (grpcClientStreamMock) Header() (metadata.MD, error) { return nil, nil }
func (grpcClientStreamMock) Trailer() metadata.MD         { return nil }
func (grpcClientStreamMock) CloseSend() error             { return nil }
func (m grpcClientStreamMock) Context() context.Context   { return m.ctx }
func (grpcClientStreamMock) SendMsg(interface{}) error    { return nil }
func (grpcClientStreamMock) RecvMsg(interface{}) error    { return nil }

type cancelerStoreGatewaySeriesClientMock struct {
	storeGatewaySeriesClientMock
	ctx    context.Context
	cancel func()
}

func (m *cancelerStoreGatewaySeriesClientMock) Recv() (*storepb.SeriesResponse, error) {
	m.cancel()
	return nil, m.ctx.Err()
}

type cancelerStoreGatewayClientMock struct {
	remoteAddr    string
	remoteZone    string
	produceSeries bool
	cancel        func()
}

func (m *cancelerStoreGatewayClientMock) Series(ctx context.Context, _ *storepb.SeriesRequest, _ ...grpc.CallOption) (storegatewaypb.StoreGateway_SeriesClient, error) {
	if m.produceSeries {
		series := &cancelerStoreGatewaySeriesClientMock{
			ctx:    ctx,
			cancel: m.cancel,
			storeGatewaySeriesClientMock: storeGatewaySeriesClientMock{
				ClientStream: grpcClientStreamMock{ctx: ctx},
			},
		}
		return series, nil
	}
	m.cancel()
	return nil, ctx.Err()
}

func (m *cancelerStoreGatewayClientMock) LabelNames(ctx context.Context, _ *storepb.LabelNamesRequest, _ ...grpc.CallOption) (*storepb.LabelNamesResponse, error) {
	m.cancel()
	return nil, ctx.Err()
}

func (m *cancelerStoreGatewayClientMock) LabelValues(ctx context.Context, _ *storepb.LabelValuesRequest, _ ...grpc.CallOption) (*storepb.LabelValuesResponse, error) {
	m.cancel()
	return nil, ctx.Err()
}

func (m *cancelerStoreGatewayClientMock) RemoteAddress() string {
	return m.remoteAddr
}

func (m *cancelerStoreGatewayClientMock) RemoteZone() string {
	return m.remoteZone
}

type blocksStoreLimitsMock struct {
	maxLabelsQueryLength               time.Duration
	maxChunksPerQuery                  int
	storeGatewayTenantShardSize        int
	storeGatewayTenantShardSizePerZone int
	storeGatewayExpandedReplication    bool
}

func (m *blocksStoreLimitsMock) MaxLabelsQueryLength(_ string) time.Duration {
	return m.maxLabelsQueryLength
}

func (m *blocksStoreLimitsMock) MaxChunksPerQuery(_ string) int {
	return m.maxChunksPerQuery
}

func (m *blocksStoreLimitsMock) StoreGatewayTenantShardSize(_ string) int {
	return m.storeGatewayTenantShardSize
}

func (m *blocksStoreLimitsMock) StoreGatewayTenantShardSizePerZone(_ string) int {
	return m.storeGatewayTenantShardSizePerZone
}

func (m *blocksStoreLimitsMock) StoreGatewayExpandedReplication(_ string) bool {
	return m.storeGatewayExpandedReplication
}

func (m *blocksStoreLimitsMock) S3SSEType(_ string) string {
	return ""
}

func (m *blocksStoreLimitsMock) S3SSEKMSKeyID(_ string) string {
	return ""
}

func (m *blocksStoreLimitsMock) S3SSEKMSEncryptionContext(_ string) string {
	return ""
}

// seriesResponseBuilder builds up series and chunks to return as part of a
// streaming response from a mock store-gateway client. Each call to addValue,
// addSamples, or addFloatHistogramSamples adds a new chunk to the response while
// addChunks adds all the provided chunks. The ULIDs of blocks queried must be
// set via addBlocks for the response to be valid.
type seriesResponseBuilder struct {
	series     []labels.Labels
	chunks     [][]storepb.AggrChunk
	blocks     []ulid.ULID
	indexBytes uint64
}

func newSeriesResponseBuilder() *seriesResponseBuilder {
	return &seriesResponseBuilder{}
}

func (b *seriesResponseBuilder) addValue(lbls labels.Labels, timeMillis int64, value float64) *seriesResponseBuilder {
	b.series = append(b.series, lbls)
	b.chunks = append(b.chunks, []storepb.AggrChunk{createAggrChunkWithSamples(promql.FPoint{T: timeMillis, F: value})})
	return b
}

func (b *seriesResponseBuilder) addSamples(lbls labels.Labels, samples ...promql.FPoint) *seriesResponseBuilder {
	b.series = append(b.series, lbls)
	b.chunks = append(b.chunks, []storepb.AggrChunk{createAggrChunkWithSamples(samples...)})
	return b
}

func (b *seriesResponseBuilder) addFloatHistogramSamples(lbls labels.Labels, samples ...promql.HPoint) *seriesResponseBuilder {
	b.series = append(b.series, lbls)
	b.chunks = append(b.chunks, []storepb.AggrChunk{createAggrChunkWithFloatHistogramSamples(samples...)})
	return b
}

func (b *seriesResponseBuilder) addChunks(lbls labels.Labels, chunks ...storepb.AggrChunk) *seriesResponseBuilder {
	b.series = append(b.series, lbls)
	b.chunks = append(b.chunks, chunks)
	return b
}

func (b *seriesResponseBuilder) addBlocks(ids ...ulid.ULID) *seriesResponseBuilder {
	b.blocks = append(b.blocks, ids...)
	return b
}

func (b *seriesResponseBuilder) addFetchedIndexBytes(bytes uint64) *seriesResponseBuilder {
	b.indexBytes += bytes
	return b
}

func (b *seriesResponseBuilder) build() []*storepb.SeriesResponse {
	var chunksEstimate uint64
	var series, chunks, final []*storepb.SeriesResponse

	for i, s := range b.series {
		rawChunks := b.chunks[i]
		chunksEstimate += uint64(len(rawChunks))
		series = append(series, mockStreamingSeriesBatchResponse(false, mimirpb.FromLabelsToLabelAdapters(s)))
		chunks = append(chunks, mockStreamingSeriesChunksResponse(uint64(len(series)-1), rawChunks))
	}

	// Turn our streaming series and chunks responses into a complete set of streaming
	// responses in the expected order.
	// * block hints
	// * streaming series
	// * stats
	// * end-of-stream streaming series
	// * chunks estimate
	// * chunks
	final = append(final, mockHintsResponse(b.blocks...))
	final = append(final, series...)
	final = append(final, mockStatsResponse(b.indexBytes))
	final = append(final, mockStreamingSeriesBatchResponse(true))
	final = append(final, storepb.NewStreamingChunksEstimate(chunksEstimate))
	final = append(final, chunks...)
	return final
}

func mockStreamingSeriesBatchResponse(endOfStream bool, lbls ...[]mimirpb.LabelAdapter) *storepb.SeriesResponse {
	res := &storepb.StreamingSeriesBatch{}
	for _, l := range lbls {
		res.Series = append(res.Series, &storepb.StreamingSeries{Labels: l})
	}
	res.IsEndOfSeriesStream = endOfStream
	return &storepb.SeriesResponse{
		Result: &storepb.SeriesResponse_StreamingSeries{
			StreamingSeries: res,
		},
	}
}

func mockStreamingSeriesChunksResponse(index uint64, chks []storepb.AggrChunk) *storepb.SeriesResponse {
	return &storepb.SeriesResponse{
		Result: &storepb.SeriesResponse_StreamingChunks{
			StreamingChunks: &storepb.StreamingChunksBatch{
				Series: []*storepb.StreamingChunks{
					{
						SeriesIndex: index,
						Chunks:      chks,
					},
				},
			},
		},
	}
}

func mockStatsResponse(fetchedIndexBytes uint64) *storepb.SeriesResponse {
	return &storepb.SeriesResponse{
		Result: &storepb.SeriesResponse_Stats{
			Stats: &storepb.Stats{FetchedIndexBytes: fetchedIndexBytes},
		},
	}
}

func mockHintsResponse(ids ...ulid.ULID) *storepb.SeriesResponse {
	hints := &hintspb.SeriesResponseHints{}
	for _, id := range ids {
		hints.AddQueriedBlock(id)
	}

	marshalled, err := types.MarshalAny(hints)
	if err != nil {
		panic(err)
	}

	return &storepb.SeriesResponse{
		Result: &storepb.SeriesResponse_Hints{
			Hints: marshalled,
		},
	}
}

func mockNamesHints(ids ...ulid.ULID) *types.Any {
	hints := &hintspb.LabelNamesResponseHints{}
	for _, id := range ids {
		hints.AddQueriedBlock(id)
	}

	marshalled, err := types.MarshalAny(hints)
	if err != nil {
		panic(err)
	}

	return marshalled
}

func mockValuesHints(ids ...ulid.ULID) *types.Any {
	hints := &hintspb.LabelValuesResponseHints{}
	for _, id := range ids {
		hints.AddQueriedBlock(id)
	}

	marshalled, err := types.MarshalAny(hints)
	if err != nil {
		panic(err)
	}

	return marshalled
}

func namesFromSeries(series ...labels.Labels) []string {
	namesMap := map[string]struct{}{}
	for _, s := range series {
		s.Range(func(l labels.Label) {
			namesMap[l.Name] = struct{}{}
		})
	}

	names := []string{}
	for name := range namesMap {
		names = append(names, name)
	}

	slices.Sort(names)
	return names
}

func valuesFromSeries(name string, series ...labels.Labels) []string {
	valuesMap := map[string]struct{}{}
	for _, s := range series {
		if value := s.Get(name); value != "" {
			valuesMap[value] = struct{}{}
		}
	}

	values := []string{}
	for name := range valuesMap {
		values = append(values, name)
	}

	slices.Sort(values)
	return values
}

func TestStoreConsistencyCheckFailedErr(t *testing.T) {
	t.Run("Error() should return an human readable error message", func(t *testing.T) {
		err := newStoreConsistencyCheckFailedError([]ulid.ULID{ulid.MustNew(1, nil)})
		assert.Equal(t, `failed to fetch some blocks (err-mimir-store-consistency-check-failed). The failed blocks are: 00000000010000000000000000`, err.Error())
	})

	t.Run("should support errors.Is()", func(t *testing.T) {
		err := newStoreConsistencyCheckFailedError([]ulid.ULID{ulid.MustNew(1, nil)})
		assert.True(t, errors.Is(err, &storeConsistencyCheckFailedErr{}))

		wrapped := errors.Wrap(err, "wrapped")
		assert.True(t, errors.Is(wrapped, &storeConsistencyCheckFailedErr{}))
	})

	t.Run("should support errors.As()", func(t *testing.T) {
		var target *storeConsistencyCheckFailedErr

		err := newStoreConsistencyCheckFailedError([]ulid.ULID{ulid.MustNew(1, nil)})
		assert.ErrorAs(t, err, &target)

		wrapped := errors.Wrap(err, "wrapped")
		assert.ErrorAs(t, wrapped, &target)
	})
}

func TestShouldRetry(t *testing.T) {
	tests := map[string]struct {
		err      error
		expected bool
	}{
		"should not retry on context canceled": {
			err:      context.Canceled,
			expected: false,
		},
		"should not retry on wrapped context canceled": {
			err:      errors.Wrap(context.Canceled, "test"),
			expected: false,
		},
		"should not retry on deadline exceeded": {
			err:      context.DeadlineExceeded,
			expected: false,
		},
		"should not retry on wrapped deadline exceeded": {
			err:      errors.Wrap(context.DeadlineExceeded, "test"),
			expected: false,
		},
		"should not retry on gRPC error with status code = 422": {
			err:      status.Error(http.StatusUnprocessableEntity, "test"),
			expected: false,
		},
		"should not retry on wrapped gRPC error with status code = 422": {
			err:      errors.Wrap(status.Error(http.StatusUnprocessableEntity, "test"), "test"),
			expected: false,
		},
		"should not retry on gogo error with status code = 422": {
			err:      gogoStatus.Error(http.StatusUnprocessableEntity, "test"),
			expected: false,
		},
		"should not retry on wrapped gogo error with status code = 422": {
			err:      errors.Wrap(gogoStatus.Error(http.StatusUnprocessableEntity, "test"), "test"),
			expected: false,
		},
		"should retry on gRPC error with status code != 422": {
			err:      status.Error(http.StatusInternalServerError, "test"),
			expected: true,
		},
		"should retry on grpc.ErrClientConnClosing": {
			// Ignore deprecation warning for now
			// nolint:staticcheck
			err:      grpc.ErrClientConnClosing,
			expected: true,
		},
		"should retry on wrapped grpc.ErrClientConnClosing": {
			// Ignore deprecation warning for now
			// nolint:staticcheck
			err:      globalerror.WrapGRPCErrorWithContextError(context.Background(), grpc.ErrClientConnClosing),
			expected: true,
		},
		"should retry on generic error": {
			err:      errors.New("test"),
			expected: true,
		},
		"should retry stop query on store-gateway instance limit": {
			err:      globalerror.WrapErrorWithGRPCStatus(errors.New("instance limit"), codes.Aborted, &mimirpb.ErrorDetails{Cause: mimirpb.ERROR_CAUSE_INSTANCE_LIMIT}).Err(),
			expected: true,
		},
		"should retry on store-gateway instance limit; shouldn't look at the gRPC code, only Mimir error cause": {
			err:      globalerror.WrapErrorWithGRPCStatus(errors.New("instance limit"), codes.Internal, &mimirpb.ErrorDetails{Cause: mimirpb.ERROR_CAUSE_INSTANCE_LIMIT}).Err(),
			expected: true,
		},
		"should retry on any other mimirpb error": {
			err:      globalerror.WrapErrorWithGRPCStatus(errors.New("instance limit"), codes.Internal, &mimirpb.ErrorDetails{Cause: mimirpb.ERROR_CAUSE_TOO_BUSY}).Err(),
			expected: true,
		},
		"should retry on any unknown error detail": {
			err: func() error {
				st, createErr := status.New(codes.Internal, "test").WithDetails(&hintspb.Block{Id: "123"})
				require.NoError(t, createErr)
				return st.Err()
			}(),
			expected: true,
		},
		"should retry on multiple error details": {
			err: func() error {
				st, createErr := status.New(codes.Internal, "test").WithDetails(&hintspb.Block{Id: "123"}, &mimirpb.ErrorDetails{Cause: mimirpb.ERROR_CAUSE_INSTANCE_LIMIT})
				require.NoError(t, createErr)
				return st.Err()
			}(),
			expected: true,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testData.expected, shouldRetry(testData.err))
		})
	}
}
