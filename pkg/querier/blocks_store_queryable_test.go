// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/blocks_store_queryable_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querier

import (
	"context"
	crand "crypto/rand"
	"fmt"
	"io"
	"math/rand"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/types"
	"github.com/grafana/dskit/services"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"
	"google.golang.org/grpc"

	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/storegateway/hintspb"
	"github.com/grafana/mimir/pkg/storegateway/labelpb"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/grafana/mimir/pkg/util/validation"
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
		metricNameLabel  = labels.Label{Name: labels.MetricName, Value: metricName}
		series1Label     = labels.Label{Name: "series", Value: "1"}
		series2Label     = labels.Label{Name: "series", Value: "2"}
		noOpQueryLimiter = limiter.NewQueryLimiter(0, 0, 0)
	)

	type valueResult struct {
		t int64
		v float64
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
					&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesResponses: []*storepb.SeriesResponse{
						mockSeriesResponse(labels.Labels{metricNameLabel}, minT, 1),
						mockSeriesResponse(labels.Labels{metricNameLabel}, minT+1, 2),
						mockHintsResponse(block1, block2),
					}}: {block1, block2},
				},
			},
			limits:       &blocksStoreLimitsMock{},
			queryLimiter: noOpQueryLimiter,
			expectedSeries: []seriesResult{
				{
					lbls: labels.New(metricNameLabel),
					values: []valueResult{
						{t: minT, v: 1},
						{t: minT + 1, v: 2},
					},
				},
			},
		},
		"a single store-gateway instance holds the required blocks (multiple returned series)": {
			finderResult: bucketindex.Blocks{
				{ID: block1},
				{ID: block2},
			},
			storeSetResponses: []interface{}{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesResponses: []*storepb.SeriesResponse{
						mockSeriesResponse(labels.Labels{metricNameLabel, series1Label}, minT, 1),
						mockSeriesResponse(labels.Labels{metricNameLabel, series1Label}, minT+1, 2),
						mockSeriesResponse(labels.Labels{metricNameLabel, series2Label}, minT, 3),
						mockHintsResponse(block1, block2),
					}}: {block1, block2},
				},
			},
			limits:       &blocksStoreLimitsMock{},
			queryLimiter: noOpQueryLimiter,
			expectedSeries: []seriesResult{
				{
					lbls: labels.New(metricNameLabel, series1Label),
					values: []valueResult{
						{t: minT, v: 1},
						{t: minT + 1, v: 2},
					},
				}, {
					lbls: labels.New(metricNameLabel, series2Label),
					values: []valueResult{
						{t: minT, v: 3},
					},
				},
			},
		},
		"multiple store-gateway instances holds the required blocks without overlapping series (single returned series)": {
			finderResult: bucketindex.Blocks{
				{ID: block1},
				{ID: block2},
			},
			storeSetResponses: []interface{}{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesResponses: []*storepb.SeriesResponse{
						mockSeriesResponse(labels.Labels{metricNameLabel}, minT, 1),
						mockHintsResponse(block1),
					}}: {block1},
					&storeGatewayClientMock{remoteAddr: "2.2.2.2", mockedSeriesResponses: []*storepb.SeriesResponse{
						mockSeriesResponse(labels.Labels{metricNameLabel}, minT+1, 2),
						mockHintsResponse(block2),
					}}: {block2},
				},
			},
			limits:       &blocksStoreLimitsMock{},
			queryLimiter: noOpQueryLimiter,
			expectedSeries: []seriesResult{
				{
					lbls: labels.New(metricNameLabel),
					values: []valueResult{
						{t: minT, v: 1},
						{t: minT + 1, v: 2},
					},
				},
			},
		},
		"multiple store-gateway instances holds the required blocks with overlapping series (single returned series)": {
			finderResult: bucketindex.Blocks{
				{ID: block1},
				{ID: block2},
			},
			storeSetResponses: []interface{}{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesResponses: []*storepb.SeriesResponse{
						mockSeriesResponse(labels.Labels{metricNameLabel}, minT+1, 2),
						mockHintsResponse(block1),
					}}: {block1},
					&storeGatewayClientMock{remoteAddr: "2.2.2.2", mockedSeriesResponses: []*storepb.SeriesResponse{
						mockSeriesResponse(labels.Labels{metricNameLabel}, minT, 1),
						mockSeriesResponse(labels.Labels{metricNameLabel}, minT+1, 2),
						mockHintsResponse(block2),
					}}: {block2},
				},
			},
			limits:       &blocksStoreLimitsMock{},
			queryLimiter: noOpQueryLimiter,
			expectedSeries: []seriesResult{
				{
					lbls: labels.New(metricNameLabel),
					values: []valueResult{
						{t: minT, v: 1},
						{t: minT + 1, v: 2},
					},
				},
			},
		},
		"multiple store-gateway instances holds the required blocks with overlapping series (multiple returned series)": {
			finderResult: bucketindex.Blocks{
				{ID: block1},
				{ID: block2},
			},
			storeSetResponses: []interface{}{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesResponses: []*storepb.SeriesResponse{
						mockSeriesResponse(labels.Labels{metricNameLabel, series1Label}, minT+1, 2),
						mockSeriesResponse(labels.Labels{metricNameLabel, series2Label}, minT, 1),
						mockHintsResponse(block1),
					}}: {block1},
					&storeGatewayClientMock{remoteAddr: "2.2.2.2", mockedSeriesResponses: []*storepb.SeriesResponse{
						mockSeriesResponse(labels.Labels{metricNameLabel, series1Label}, minT, 1),
						mockSeriesResponse(labels.Labels{metricNameLabel, series1Label}, minT+1, 2),
						mockHintsResponse(block2),
					}}: {block2},
					&storeGatewayClientMock{remoteAddr: "3.3.3.3", mockedSeriesResponses: []*storepb.SeriesResponse{
						mockSeriesResponse(labels.Labels{metricNameLabel, series2Label}, minT, 1),
						mockSeriesResponse(labels.Labels{metricNameLabel, series2Label}, minT+1, 3),
						mockHintsResponse(block3),
					}}: {block3},
				},
			},
			limits:       &blocksStoreLimitsMock{},
			queryLimiter: noOpQueryLimiter,
			expectedSeries: []seriesResult{
				{
					lbls: labels.New(metricNameLabel, series1Label),
					values: []valueResult{
						{t: minT, v: 1},
						{t: minT + 1, v: 2},
					},
				}, {
					lbls: labels.New(metricNameLabel, series2Label),
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
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="3"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="4"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="5"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="6"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="7"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="8"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="9"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="10"} 1
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
					&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesResponses: []*storepb.SeriesResponse{
						mockSeriesResponse(labels.Labels{metricNameLabel, series1Label}, minT, 1),
						mockSeriesResponse(labels.Labels{metricNameLabel, series1Label}, minT+1, 2),
						mockHintsResponse(block1),
					}}: {block1},
				},
				// Second attempt returns an error because there are no other store-gateways left.
				errors.New("no store-gateway remaining after exclude"),
			},
			limits:       &blocksStoreLimitsMock{},
			queryLimiter: noOpQueryLimiter,
			expectedErr:  newStoreConsistencyCheckFailedError([]ulid.ULID{block2}),
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
					&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesResponses: []*storepb.SeriesResponse{
						mockSeriesResponse(labels.Labels{metricNameLabel}, minT+1, 2),
						mockHintsResponse(block1),
					}}: {block1},
					&storeGatewayClientMock{remoteAddr: "2.2.2.2", mockedSeriesResponses: []*storepb.SeriesResponse{
						mockSeriesResponse(labels.Labels{metricNameLabel}, minT+1, 2),
						mockHintsResponse(block2),
					}}: {block2},
				},
				// Second attempt returns an error because there are no other store-gateways left.
				errors.New("no store-gateway remaining after exclude"),
			},
			limits:       &blocksStoreLimitsMock{},
			queryLimiter: noOpQueryLimiter,
			expectedErr:  newStoreConsistencyCheckFailedError([]ulid.ULID{block3, block4}),
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
					&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesResponses: []*storepb.SeriesResponse{
						mockSeriesResponse(labels.Labels{metricNameLabel, series1Label}, minT, 1),
						mockHintsResponse(block1),
					}}: {block1, block3},
					&storeGatewayClientMock{remoteAddr: "2.2.2.2", mockedSeriesResponses: []*storepb.SeriesResponse{
						mockSeriesResponse(labels.Labels{metricNameLabel, series2Label}, minT, 2),
						mockHintsResponse(block2),
					}}: {block2, block4},
				},
				// Second attempt returns 1 missing block.
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "3.3.3.3", mockedSeriesResponses: []*storepb.SeriesResponse{
						mockSeriesResponse(labels.Labels{metricNameLabel, series1Label}, minT+1, 2),
						mockHintsResponse(block3),
					}}: {block3, block4},
				},
				// Third attempt returns the last missing block.
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "4.4.4.4", mockedSeriesResponses: []*storepb.SeriesResponse{
						mockSeriesResponse(labels.Labels{metricNameLabel, series2Label}, minT+1, 3),
						mockHintsResponse(block4),
					}}: {block4},
				},
			},
			limits:       &blocksStoreLimitsMock{},
			queryLimiter: noOpQueryLimiter,
			expectedSeries: []seriesResult{
				{
					lbls: labels.New(metricNameLabel, series1Label),
					values: []valueResult{
						{t: minT, v: 1},
						{t: minT + 1, v: 2},
					},
				}, {
					lbls: labels.New(metricNameLabel, series2Label),
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
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="3"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="4"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="5"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="6"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="7"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="8"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="9"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="10"} 1
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
			`,
		},
		"max chunks per query limit greater then the number of chunks fetched": {
			finderResult: bucketindex.Blocks{
				{ID: block1},
				{ID: block2},
			},
			storeSetResponses: []interface{}{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesResponses: []*storepb.SeriesResponse{
						mockSeriesResponse(labels.Labels{metricNameLabel, series1Label}, minT, 1),
						mockSeriesResponse(labels.Labels{metricNameLabel, series1Label}, minT+1, 2),
						mockHintsResponse(block1, block2),
					}}: {block1, block2},
				},
			},
			limits:       &blocksStoreLimitsMock{maxChunksPerQuery: 3},
			queryLimiter: noOpQueryLimiter,
			expectedSeries: []seriesResult{
				{
					lbls: labels.New(metricNameLabel, series1Label),
					values: []valueResult{
						{t: minT, v: 1},
						{t: minT + 1, v: 2},
					},
				},
			},
		},
		"max chunks per query limit hit while fetching chunks at first attempt": {
			finderResult: bucketindex.Blocks{
				{ID: block1},
				{ID: block2},
			},
			storeSetResponses: []interface{}{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesResponses: []*storepb.SeriesResponse{
						mockSeriesResponse(labels.Labels{metricNameLabel, series1Label}, minT, 1),
						mockSeriesResponse(labels.Labels{metricNameLabel, series1Label}, minT+1, 2),
						mockHintsResponse(block1, block2),
					}}: {block1, block2},
				},
			},
			limits:       &blocksStoreLimitsMock{maxChunksPerQuery: 1},
			queryLimiter: noOpQueryLimiter,
			expectedErr:  validation.LimitError(fmt.Sprintf(maxChunksPerQueryLimitMsgFormat, fmt.Sprintf("{__name__=%q}", metricName), 1)),
		},
		"max chunks per query limit hit while fetching chunks at first attempt - global limit": {
			finderResult: bucketindex.Blocks{
				{ID: block1},
				{ID: block2},
			},
			storeSetResponses: []interface{}{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesResponses: []*storepb.SeriesResponse{
						mockSeriesResponse(labels.Labels{metricNameLabel, series1Label}, minT, 1),
						mockSeriesResponse(labels.Labels{metricNameLabel, series1Label}, minT+1, 2),
						mockHintsResponse(block1, block2),
					}}: {block1, block2},
				},
			},
			limits:       &blocksStoreLimitsMock{},
			queryLimiter: limiter.NewQueryLimiter(0, 0, 1),
			expectedErr:  validation.LimitError(fmt.Sprintf(limiter.MaxChunksPerQueryLimitMsgFormat, 1)),
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
					&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesResponses: []*storepb.SeriesResponse{
						mockSeriesResponse(labels.Labels{metricNameLabel, series1Label}, minT, 1),
						mockHintsResponse(block1),
					}}: {block1, block3},
					&storeGatewayClientMock{remoteAddr: "2.2.2.2", mockedSeriesResponses: []*storepb.SeriesResponse{
						mockSeriesResponse(labels.Labels{metricNameLabel, series2Label}, minT, 2),
						mockHintsResponse(block2),
					}}: {block2, block4},
				},
				// Second attempt returns 1 missing block.
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "3.3.3.3", mockedSeriesResponses: []*storepb.SeriesResponse{
						mockSeriesResponse(labels.Labels{metricNameLabel, series1Label}, minT+1, 2),
						mockHintsResponse(block3),
					}}: {block3, block4},
				},
				// Third attempt returns the last missing block.
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "4.4.4.4", mockedSeriesResponses: []*storepb.SeriesResponse{
						mockSeriesResponse(labels.Labels{metricNameLabel, series2Label}, minT+1, 3),
						mockHintsResponse(block4),
					}}: {block4},
				},
			},
			limits:       &blocksStoreLimitsMock{maxChunksPerQuery: 3},
			queryLimiter: noOpQueryLimiter,
			expectedErr:  validation.LimitError(fmt.Sprintf(maxChunksPerQueryLimitMsgFormat, fmt.Sprintf("{__name__=%q}", metricName), 3)),
		},
		"max chunks per query limit hit while fetching chunks during subsequent attempts - global": {
			finderResult: bucketindex.Blocks{
				{ID: block1},
				{ID: block2},
				{ID: block3},
				{ID: block4},
			},
			storeSetResponses: []interface{}{
				// First attempt returns a client whose response does not include all expected blocks.
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesResponses: []*storepb.SeriesResponse{
						mockSeriesResponse(labels.Labels{metricNameLabel, series1Label}, minT, 1),
						mockHintsResponse(block1),
					}}: {block1, block3},
					&storeGatewayClientMock{remoteAddr: "2.2.2.2", mockedSeriesResponses: []*storepb.SeriesResponse{
						mockSeriesResponse(labels.Labels{metricNameLabel, series2Label}, minT, 2),
						mockHintsResponse(block2),
					}}: {block2, block4},
				},
				// Second attempt returns 1 missing block.
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "3.3.3.3", mockedSeriesResponses: []*storepb.SeriesResponse{
						mockSeriesResponse(labels.Labels{metricNameLabel, series1Label}, minT+1, 2),
						mockHintsResponse(block3),
					}}: {block3, block4},
				},
				// Third attempt returns the last missing block.
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "4.4.4.4", mockedSeriesResponses: []*storepb.SeriesResponse{
						mockSeriesResponse(labels.Labels{metricNameLabel, series2Label}, minT+1, 3),
						mockHintsResponse(block4),
					}}: {block4},
				},
			},
			limits:       &blocksStoreLimitsMock{},
			queryLimiter: limiter.NewQueryLimiter(0, 0, 3),
			expectedErr:  validation.LimitError(fmt.Sprintf(limiter.MaxChunksPerQueryLimitMsgFormat, 3)),
		},
		"max series per query limit hit while fetching chunks": {
			finderResult: bucketindex.Blocks{
				{ID: block1},
				{ID: block2},
			},
			storeSetResponses: []interface{}{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesResponses: []*storepb.SeriesResponse{
						mockSeriesResponse(labels.Labels{metricNameLabel, series1Label}, minT, 1),
						mockSeriesResponse(labels.Labels{metricNameLabel, series2Label}, minT+1, 2),
						mockHintsResponse(block1, block2),
					}}: {block1, block2},
				},
			},
			limits:       &blocksStoreLimitsMock{},
			queryLimiter: limiter.NewQueryLimiter(1, 0, 0),
			expectedErr:  validation.LimitError(fmt.Sprintf(limiter.MaxSeriesHitMsgFormat, 1)),
		},
		"max chunk bytes per query limit hit while fetching chunks": {
			finderResult: bucketindex.Blocks{
				{ID: block1},
				{ID: block2},
			},
			storeSetResponses: []interface{}{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesResponses: []*storepb.SeriesResponse{
						mockSeriesResponse(labels.Labels{metricNameLabel, series1Label}, minT, 1),
						mockSeriesResponse(labels.Labels{metricNameLabel, series1Label}, minT+1, 2),
						mockHintsResponse(block1, block2),
					}}: {block1, block2},
				},
			},
			limits:       &blocksStoreLimitsMock{maxChunksPerQuery: 1},
			queryLimiter: limiter.NewQueryLimiter(0, 8, 0),
			expectedErr:  validation.LimitError(fmt.Sprintf(limiter.MaxChunkBytesHitMsgFormat, 8)),
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
					&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesResponses: []*storepb.SeriesResponse{
						mockSeriesResponse(labels.Labels{metricNameLabel}, minT, 1),
						mockSeriesResponse(labels.Labels{metricNameLabel}, minT+1, 2),
						mockHintsResponse(block2),
					}}: {block2}, // Only block2 will be queried
				},
			},
			limits:       &blocksStoreLimitsMock{},
			queryLimiter: noOpQueryLimiter,
			expectedSeries: []seriesResult{
				{
					lbls: labels.New(metricNameLabel),
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
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="3"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="4"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="5"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="6"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="7"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="8"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="9"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="10"} 1
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
					&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesResponses: []*storepb.SeriesResponse{
						mockSeriesResponse(labels.Labels{metricNameLabel}, minT, 1),
						mockSeriesResponse(labels.Labels{metricNameLabel}, minT+1, 2),
						mockHintsResponse(block1, block2, block3, block4),
					}}: {block1, block2, block3, block4},
				},
			},
			limits:       &blocksStoreLimitsMock{},
			queryLimiter: noOpQueryLimiter,
			expectedSeries: []seriesResult{
				{
					lbls: labels.New(metricNameLabel),
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
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="3"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="4"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="5"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="6"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="7"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="8"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="9"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="10"} 1
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
					&storeGatewayClientMock{remoteAddr: "2.2.2.2", mockedSeriesResponses: []*storepb.SeriesResponse{
						mockSeriesResponse(labels.Labels{metricNameLabel, series1Label}, minT, 2),
						mockHintsResponse(block1),
					}}: {block1},
				},
			},
			limits:       &blocksStoreLimitsMock{},
			queryLimiter: noOpQueryLimiter,
			expectedSeries: []seriesResult{
				{
					lbls: labels.New(metricNameLabel, series1Label),
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
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="3"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="4"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="5"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="6"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="7"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="8"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="9"} 1
					cortex_querier_storegateway_instances_hit_per_query_bucket{le="10"} 1
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
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			ctx := limiter.AddQueryLimiterToContext(context.Background(), testData.queryLimiter)
			reg := prometheus.NewPedanticRegistry()
			stores := &blocksStoreSetMock{mockedResponses: testData.storeSetResponses}
			finder := &blocksFinderMock{}
			finder.On("GetBlocks", mock.Anything, "user-1", minT, maxT).Return(testData.finderResult, map[ulid.ULID]*bucketindex.BlockDeletionMark(nil), testData.finderErr)

			q := &blocksStoreQuerier{
				ctx:         ctx,
				minT:        minT,
				maxT:        maxT,
				userID:      "user-1",
				finder:      finder,
				stores:      stores,
				consistency: NewBlocksConsistencyChecker(0, 0, log.NewNopLogger(), nil),
				logger:      log.NewNopLogger(),
				metrics:     newBlocksStoreQueryableMetrics(reg),
				limits:      testData.limits,
			}

			matchers := []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, metricName),
			}
			if testData.queryShardID != "" {
				matchers = append(matchers, labels.MustNewMatcher(labels.MatchEqual, sharding.ShardLabel, testData.queryShardID))
			}

			sp := &storage.SelectHints{Start: minT, End: maxT}
			set := q.Select(true, sp, matchers...)
			if testData.expectedErr != nil {
				assert.ErrorContains(t, set.Err(), testData.expectedErr.Error())
				assert.IsType(t, set.Err(), testData.expectedErr)
				assert.False(t, set.Next())
				assert.Nil(t, set.Warnings())
				return
			}

			require.NoError(t, set.Err())
			assert.Len(t, set.Warnings(), 0)

			// Read all returned series and their values.
			var actualSeries []seriesResult
			for set.Next() {
				var actualValues []valueResult

				it := set.At().Iterator()
				for it.Next() {
					t, v := it.At()
					actualValues = append(actualValues, valueResult{
						t: t,
						v: v,
					})
				}

				require.NoError(t, it.Err())

				actualSeries = append(actualSeries, seriesResult{
					lbls:   set.At().Labels(),
					values: actualValues,
				})
			}
			require.NoError(t, set.Err())
			assert.Equal(t, testData.expectedSeries, actualSeries)

			// Assert on metrics (optional, only for test cases defining it).
			if testData.expectedMetrics != "" {
				assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(testData.expectedMetrics),
					"cortex_querier_storegateway_instances_hit_per_query", "cortex_querier_storegateway_refetches_per_query",
					"cortex_querier_blocks_found_total", "cortex_querier_blocks_queried_total", "cortex_querier_blocks_with_compactor_shard_but_incompatible_query_shard_total"))
			}
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
			labels.MetricName: metricName + "_1",
			"series1":         "1",
		})
		series2 = labels.FromMap(map[string]string{
			labels.MetricName: metricName + "_2",
			"series2":         "1",
		})
	)

	tests := map[string]struct {
		finderResult        bucketindex.Blocks
		finderErr           error
		storeSetResponses   []interface{}
		expectedLabelNames  []string
		expectedLabelValues []string // For __name__
		expectedErr         string
		expectedMetrics     string
	}{
		"no block in the storage matching the query time range": {
			finderResult: nil,
			expectedErr:  "",
		},
		"error while finding blocks matching the query time range": {
			finderErr:   errors.New("unable to find blocks"),
			expectedErr: "unable to find blocks",
		},
		"error while getting clients to query the store-gateway": {
			finderResult: bucketindex.Blocks{
				{ID: block1},
				{ID: block2},
			},
			storeSetResponses: []interface{}{
				errors.New("no client found"),
			},
			expectedErr: "no client found",
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
							Values:   valuesFromSeries(labels.MetricName, series1, series2),
							Warnings: []string{},
							Hints:    mockValuesHints(block1, block2),
						},
					}: {block1, block2},
				},
			},
			expectedLabelNames:  namesFromSeries(series1, series2),
			expectedLabelValues: valuesFromSeries(labels.MetricName, series1, series2),
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
							Values:   valuesFromSeries(labels.MetricName, series1),
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
							Values:   valuesFromSeries(labels.MetricName, series2),
							Warnings: []string{},
							Hints:    mockValuesHints(block2),
						},
					}: {block2},
				},
			},
			expectedLabelNames:  namesFromSeries(series1, series2),
			expectedLabelValues: valuesFromSeries(labels.MetricName, series1, series2),
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
							Values:   valuesFromSeries(labels.MetricName, series1),
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
							Values:   valuesFromSeries(labels.MetricName, series1),
							Warnings: []string{},
							Hints:    mockValuesHints(block2),
						},
					}: {block2},
				},
			},
			expectedLabelNames:  namesFromSeries(series1),
			expectedLabelValues: valuesFromSeries(labels.MetricName, series1),
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
							Values:   valuesFromSeries(labels.MetricName, series1, series2),
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
							Values:   valuesFromSeries(labels.MetricName, series1),
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
							Values:   valuesFromSeries(labels.MetricName, series2),
							Warnings: []string{},
							Hints:    mockValuesHints(block3),
						},
					}: {block3},
				},
			},
			expectedLabelNames:  namesFromSeries(series1, series2),
			expectedLabelValues: valuesFromSeries(labels.MetricName, series1, series2),
			expectedMetrics: `
				# HELP cortex_querier_storegateway_instances_hit_per_query Number of store-gateway instances hit for a single query.
				# TYPE cortex_querier_storegateway_instances_hit_per_query histogram
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="0"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="1"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="2"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="3"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="4"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="5"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="6"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="7"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="8"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="9"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="10"} 1
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
							Values:   valuesFromSeries(labels.MetricName, series1),
							Warnings: []string{},
							Hints:    mockValuesHints(block1),
						},
					}: {block1},
				},
				// Second attempt returns an error because there are no other store-gateways left.
				errors.New("no store-gateway remaining after exclude"),
			},
			expectedErr: newStoreConsistencyCheckFailedError([]ulid.ULID{block2}).Error(),
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
							Values:   valuesFromSeries(labels.MetricName, series1),
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
							Values:   valuesFromSeries(labels.MetricName, series2),
							Warnings: []string{},
							Hints:    mockValuesHints(block2),
						},
					}: {block2},
				},
				// Second attempt returns an error because there are no other store-gateways left.
				errors.New("no store-gateway remaining after exclude"),
			},
			expectedErr: newStoreConsistencyCheckFailedError([]ulid.ULID{block3, block4}).Error(),
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
							Values:   valuesFromSeries(labels.MetricName, series1),
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
							Values:   valuesFromSeries(labels.MetricName, series2),
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
							Values:   valuesFromSeries(labels.MetricName, series1, series2),
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
			expectedLabelValues: valuesFromSeries(labels.MetricName, series1, series2),
			expectedMetrics: `
				# HELP cortex_querier_storegateway_instances_hit_per_query Number of store-gateway instances hit for a single query.
				# TYPE cortex_querier_storegateway_instances_hit_per_query histogram
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="0"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="1"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="2"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="3"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="4"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="5"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="6"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="7"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="8"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="9"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="10"} 1
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
							Values:   valuesFromSeries(labels.MetricName, series1),
							Warnings: []string{},
							Hints:    mockValuesHints(block1),
						},
					}: {block1},
				},
			},
			expectedLabelNames:  namesFromSeries(series1),
			expectedLabelValues: valuesFromSeries(labels.MetricName, series1),
			expectedMetrics: `
				# HELP cortex_querier_storegateway_instances_hit_per_query Number of store-gateway instances hit for a single query.
				# TYPE cortex_querier_storegateway_instances_hit_per_query histogram
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="0"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="1"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="2"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="3"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="4"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="5"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="6"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="7"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="8"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="9"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="10"} 1
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
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			// Splitting it because we need a new registry for names and values.
			// And also the initial expectedErr checking needs to be done for both.
			for _, testFunc := range []string{"LabelNames", "LabelValues"} {
				ctx := user.InjectOrgID(context.Background(), "user-1")
				reg := prometheus.NewPedanticRegistry()
				stores := &blocksStoreSetMock{mockedResponses: testData.storeSetResponses}
				finder := &blocksFinderMock{}
				finder.On("GetBlocks", mock.Anything, "user-1", minT, maxT).Return(testData.finderResult, map[ulid.ULID]*bucketindex.BlockDeletionMark(nil), testData.finderErr)

				q := &blocksStoreQuerier{
					ctx:         ctx,
					minT:        minT,
					maxT:        maxT,
					userID:      "user-1",
					finder:      finder,
					stores:      stores,
					consistency: NewBlocksConsistencyChecker(0, 0, log.NewNopLogger(), nil),
					logger:      log.NewNopLogger(),
					metrics:     newBlocksStoreQueryableMetrics(reg),
					limits:      &blocksStoreLimitsMock{},
				}

				if testFunc == "LabelNames" {
					names, warnings, err := q.LabelNames()
					if testData.expectedErr != "" {
						require.Equal(t, testData.expectedErr, err.Error())
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
					values, warnings, err := q.LabelValues(labels.MetricName)
					if testData.expectedErr != "" {
						require.Equal(t, testData.expectedErr, err.Error())
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
}

func TestBlocksStoreQuerier_SelectSortedShouldHonorQueryStoreAfter(t *testing.T) {
	now := time.Now()

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
			finder.On("GetBlocks", mock.Anything, "user-1", mock.Anything, mock.Anything).Return(bucketindex.Blocks(nil), map[ulid.ULID]*bucketindex.BlockDeletionMark(nil), error(nil))

			q := &blocksStoreQuerier{
				ctx:             context.Background(),
				minT:            testData.queryMinT,
				maxT:            testData.queryMaxT,
				userID:          "user-1",
				finder:          finder,
				stores:          &blocksStoreSetMock{},
				consistency:     NewBlocksConsistencyChecker(0, 0, log.NewNopLogger(), nil),
				logger:          log.NewNopLogger(),
				metrics:         newBlocksStoreQueryableMetrics(nil),
				limits:          &blocksStoreLimitsMock{},
				queryStoreAfter: testData.queryStoreAfter,
			}

			sp := &storage.SelectHints{
				Start: testData.queryMinT,
				End:   testData.queryMaxT,
			}

			set := q.selectSorted(sp)
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
		engineLookbackDelta = 5 * time.Minute
		thirtyDays          = 30 * 24 * time.Hour
		sevenDays           = 7 * 24 * time.Hour
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
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			finder := &blocksFinderMock{}
			finder.On("GetBlocks", mock.Anything, "user-1", mock.Anything, mock.Anything).Return(bucketindex.Blocks(nil), map[ulid.ULID]*bucketindex.BlockDeletionMark(nil), error(nil))

			q := &blocksStoreQuerier{
				ctx:         user.InjectOrgID(context.Background(), "user-1"),
				minT:        testData.queryMinT,
				maxT:        testData.queryMaxT,
				userID:      "user-1",
				finder:      finder,
				stores:      &blocksStoreSetMock{},
				consistency: NewBlocksConsistencyChecker(0, 0, log.NewNopLogger(), nil),
				logger:      log.NewNopLogger(),
				metrics:     newBlocksStoreQueryableMetrics(nil),
				limits: &blocksStoreLimitsMock{
					maxLabelsQueryLength: testData.maxLabelsQueryLength,
				},
			}

			_, _, err := q.LabelNames()
			require.NoError(t, err)
			require.Len(t, finder.Calls, 1)
			assert.Equal(t, testData.expectedMinT, finder.Calls[0].Arguments.Get(2))
			assert.Equal(t, testData.expectedMaxT, finder.Calls[0].Arguments.Get(3))

			_, _, err = q.LabelValues("foo")
			require.Len(t, finder.Calls, 2)
			require.NoError(t, err)
			assert.Equal(t, testData.expectedMinT, finder.Calls[1].Arguments.Get(2))
			assert.Equal(t, testData.expectedMaxT, finder.Calls[1].Arguments.Get(3))
		})
	}
}

func TestBlocksStoreQuerier_PromQLExecution(t *testing.T) {
	// Prepare series fixtures.
	series1 := labels.Labels{{Name: "__name__", Value: "metric_1"}}
	series2 := labels.Labels{{Name: "__name__", Value: "metric_2"}}
	series3 := labels.Labels{{Name: "__name__", Value: "metric_3_ooo"}}
	series4 := labels.Labels{{Name: "__name__", Value: "metric_4_ooo_and_overlapping"}}

	generateSeriesSamples := func(value float64) []promql.Point {
		return []promql.Point{
			{T: 1589759955000, V: value},
			{T: 1589759970000, V: value},
			{T: 1589759985000, V: value},
			{T: 1589760000000, V: value},
			{T: 1589760015000, V: value},
			{T: 1589760030000, V: value},
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
			storeGateway1Responses: []*storepb.SeriesResponse{
				mockSeriesResponseWithSamples(series1, series1Samples[:3]...), // First half.
				mockSeriesResponseWithSamples(series2, series2Samples[:3]...), // First half.
			},
			storeGateway2Responses: []*storepb.SeriesResponse{
				mockSeriesResponseWithSamples(series1, series1Samples[3:]...), // Second half.
				mockSeriesResponseWithSamples(series2, series2Samples[3:]...), // Second half.
			},
			expected: promql.Matrix{
				{Metric: series1, Points: series1Samples},
				{Metric: series2, Points: series2Samples},
			},
		},
		"should query metrics with out-of-order chunks": {
			query: `{__name__=~".*ooo.*"}`,
			storeGateway1Responses: []*storepb.SeriesResponse{
				mockSeriesResponseWithChunks(series3,
					createAggrChunkWithSamples(series3Samples[2:4]...),
					createAggrChunkWithSamples(series3Samples[0:2]...), // Out of order.
				),
				mockSeriesResponseWithChunks(series4,
					createAggrChunkWithSamples(series4Samples[2:4]...),
					createAggrChunkWithSamples(series4Samples[0:3]...), // Out of order and overlapping.
				),
			},
			storeGateway2Responses: []*storepb.SeriesResponse{
				mockSeriesResponseWithSamples(series3, series3Samples[4:6]...),
				mockSeriesResponseWithSamples(series4, series4Samples[4:6]...),
			},
			expected: promql.Matrix{
				{Metric: series3, Points: series3Samples},
				{Metric: series4, Points: series4Samples},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			block1 := ulid.MustNew(1, nil)
			block2 := ulid.MustNew(2, nil)

			// Mock the finder to simulate we need to query two blocks.
			finder := &blocksFinderMock{
				Service: services.NewIdleService(nil, nil),
			}
			finder.On("GetBlocks", mock.Anything, "user-1", mock.Anything, mock.Anything).Return(bucketindex.Blocks{
				{ID: block1},
				{ID: block2},
			}, map[ulid.ULID]*bucketindex.BlockDeletionMark(nil), error(nil))

			// Mock the store-gateway response, to simulate the case each block is queried from a different gateway.
			gateway1 := &storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesResponses: append(testData.storeGateway1Responses, mockHintsResponse(block1))}
			gateway2 := &storeGatewayClientMock{remoteAddr: "2.2.2.2", mockedSeriesResponses: append(testData.storeGateway2Responses, mockHintsResponse(block2))}

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
			queryable, err := NewBlocksStoreQueryable(stores, finder, NewBlocksConsistencyChecker(0, 0, logger, nil), &blocksStoreLimitsMock{}, 0, logger, nil)
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), queryable))
			defer services.StopAndAwaitTerminated(context.Background(), queryable) // nolint:errcheck

			engine := promql.NewEngine(promql.EngineOpts{
				Logger:     logger,
				Timeout:    10 * time.Second,
				MaxSamples: 1e6,
			})

			// Query metrics.
			q, err := engine.NewRangeQuery(queryable, nil, testData.query, queryStart, queryEnd, 15*time.Second)
			require.NoError(t, err)

			ctx := user.InjectOrgID(context.Background(), "user-1")
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

	rand.Seed(time.Now().UnixNano())
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

func (m *blocksStoreSetMock) GetClientsFor(_ string, _ []ulid.ULID, _ map[ulid.ULID][]string) (map[BlocksStoreClient][]ulid.ULID, error) {
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

func (m *blocksFinderMock) GetBlocks(ctx context.Context, userID string, minT, maxT int64) (bucketindex.Blocks, map[ulid.ULID]*bucketindex.BlockDeletionMark, error) {
	args := m.Called(ctx, userID, minT, maxT)
	return args.Get(0).(bucketindex.Blocks), args.Get(1).(map[ulid.ULID]*bucketindex.BlockDeletionMark), args.Error(2)
}

type storeGatewayClientMock struct {
	remoteAddr                string
	mockedSeriesResponses     []*storepb.SeriesResponse
	mockedSeriesErr           error
	mockedLabelNamesResponse  *storepb.LabelNamesResponse
	mockedLabelNamesErr       error
	mockedLabelValuesResponse *storepb.LabelValuesResponse
	mockedLabelValuesErr      error
}

func (m *storeGatewayClientMock) Series(ctx context.Context, in *storepb.SeriesRequest, opts ...grpc.CallOption) (storegatewaypb.StoreGateway_SeriesClient, error) {
	seriesClient := &storeGatewaySeriesClientMock{
		mockedResponses: m.mockedSeriesResponses,
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

type blocksStoreLimitsMock struct {
	maxLabelsQueryLength        time.Duration
	maxChunksPerQuery           int
	storeGatewayTenantShardSize int
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

func (m *blocksStoreLimitsMock) S3SSEType(_ string) string {
	return ""
}

func (m *blocksStoreLimitsMock) S3SSEKMSKeyID(_ string) string {
	return ""
}

func (m *blocksStoreLimitsMock) S3SSEKMSEncryptionContext(_ string) string {
	return ""
}

func mockSeriesResponse(lbls labels.Labels, timeMillis int64, value float64) *storepb.SeriesResponse {
	return mockSeriesResponseWithSamples(lbls, promql.Point{T: timeMillis, V: value})
}

func mockSeriesResponseWithSamples(lbls labels.Labels, samples ...promql.Point) *storepb.SeriesResponse {
	return mockSeriesResponseWithChunks(lbls, createAggrChunkWithSamples(samples...))
}

func mockSeriesResponseWithChunks(lbls labels.Labels, chunks ...storepb.AggrChunk) *storepb.SeriesResponse {
	return &storepb.SeriesResponse{
		Result: &storepb.SeriesResponse_Series{
			Series: &storepb.Series{
				Labels: labelpb.ZLabelsFromPromLabels(lbls),
				Chunks: chunks,
			},
		},
	}
}

func mockHintsResponse(ids ...ulid.ULID) *storepb.SeriesResponse {
	hints := &hintspb.SeriesResponseHints{}
	for _, id := range ids {
		hints.AddQueriedBlock(id)
	}

	any, err := types.MarshalAny(hints)
	if err != nil {
		panic(err)
	}

	return &storepb.SeriesResponse{
		Result: &storepb.SeriesResponse_Hints{
			Hints: any,
		},
	}
}

func mockNamesHints(ids ...ulid.ULID) *types.Any {
	hints := &hintspb.LabelNamesResponseHints{}
	for _, id := range ids {
		hints.AddQueriedBlock(id)
	}

	any, err := types.MarshalAny(hints)
	if err != nil {
		panic(err)
	}

	return any
}

func mockValuesHints(ids ...ulid.ULID) *types.Any {
	hints := &hintspb.LabelValuesResponseHints{}
	for _, id := range ids {
		hints.AddQueriedBlock(id)
	}

	any, err := types.MarshalAny(hints)
	if err != nil {
		panic(err)
	}

	return any
}

func namesFromSeries(series ...labels.Labels) []string {
	namesMap := map[string]struct{}{}
	for _, s := range series {
		for _, l := range s {
			namesMap[l.Name] = struct{}{}
		}
	}

	names := []string{}
	for name := range namesMap {
		names = append(names, name)
	}

	sort.Strings(names)
	return names
}

func valuesFromSeries(name string, series ...labels.Labels) []string {
	valuesMap := map[string]struct{}{}
	for _, s := range series {
		for _, l := range s {
			if l.Name == name {
				valuesMap[l.Value] = struct{}{}
			}
		}
	}

	values := []string{}
	for name := range valuesMap {
		values = append(values, name)
	}

	sort.Strings(values)
	return values
}

func TestBlocksStoreQueryableErrMsgs(t *testing.T) {
	tests := map[string]struct {
		err error
		msg string
	}{
		"newStoreConsistencyCheckFailedError": {
			err: newStoreConsistencyCheckFailedError([]ulid.ULID{ulid.MustNew(1, nil)}),
			msg: `the consistency check failed because some blocks were not queried (err-mimir-store-consistency-check-failed). The non-queried blocks are: 00000000010000000000000000`,
		},
	}

	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, tc.msg, tc.err.Error())
		})
	}
}
