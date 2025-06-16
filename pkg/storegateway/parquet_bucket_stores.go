// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storegateway/bucket_stores.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package storegateway

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/gate"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
)

type ParquetBucketStores struct {
	services.Service

	logger log.Logger
	reg    prometheus.Registerer

	limits *validation.Overrides // nolint:unused
	// Tenants that are specifically enabled or disabled via configuration
	allowedTenants *util.AllowList

	bucket objstore.Bucket
	// Metrics specific to bucket store operations
	bucketStoreMetrics *BucketStoreMetrics     // nolint:unused    // TODO: Create ParquetBucketStoreMetrics
	metaFetcherMetrics *MetadataFetcherMetrics // nolint:unused
	shardingStrategy   ShardingStrategy        // nolint:unused
	syncBackoffConfig  backoff.Config          // nolint:unused

	// Gate used to limit concurrency on loading index-headers across all tenants.
	lazyLoadingGate gate.Gate // nolint:unused

	// Keeps a bucket store for each tenant.
	storesMu sync.RWMutex
	stores   map[string]*ParquetBucketStore

	// Block sync metrics.
	syncTimes         prometheus.Histogram // nolint:unused
	syncLastSuccess   prometheus.Gauge     // nolint:unused
	tenantsDiscovered prometheus.Gauge     // nolint:unused
	tenantsSynced     prometheus.Gauge     // nolint:unused
	blocksLoaded      *prometheus.Desc     // nolint:unused
}

// NewParquetBucketStores initializes a Parquet implementation of the Stores interface.
func NewParquetBucketStores(
	logger log.Logger,
	reg prometheus.Registerer,
) (*ParquetBucketStores, error) {

	stores := &ParquetBucketStores{
		logger: logger,
		reg:    reg,
	}
	stores.Service = services.NewIdleService(nil, nil)

	return stores, nil
}

// Series implements the storegatewaypb.StoreGatewayServer interface, making a series request to the underlying user bucket store.
func (ss *ParquetBucketStores) Series(req *storepb.SeriesRequest, srv storegatewaypb.StoreGateway_SeriesServer) error {
	spanLog, spanCtx := spanlogger.New(srv.Context(), ss.logger, tracer, "ParquetBucketStores.Series")
	defer spanLog.Finish()

	userID := getUserIDFromGRPCContext(spanCtx)
	if userID == "" {
		return fmt.Errorf("no userID")
	}

	store := ss.getStore(userID)
	if store == nil {
		return nil
	}

	return store.Series(req, spanSeriesServer{
		StoreGateway_SeriesServer: srv,
		ctx:                       spanCtx,
	})
}

// LabelNames implements the storegatewaypb.StoreGatewayServer interface.
func (ss *ParquetBucketStores) LabelNames(ctx context.Context, req *storepb.LabelNamesRequest) (*storepb.LabelNamesResponse, error) {
	spanLog, spanCtx := spanlogger.New(ctx, ss.logger, tracer, "ParquetBucketStores.LabelNames")
	defer spanLog.Finish()

	userID := getUserIDFromGRPCContext(spanCtx)
	if userID == "" {
		return nil, fmt.Errorf("no userID")
	}

	store := ss.getStore(userID)
	if store == nil {
		return &storepb.LabelNamesResponse{}, nil
	}

	return store.LabelNames(ctx, req)
}

// LabelValues implements the storegatewaypb.StoreGatewayServer interface.
func (ss *ParquetBucketStores) LabelValues(ctx context.Context, req *storepb.LabelValuesRequest) (*storepb.LabelValuesResponse, error) {
	spanLog, spanCtx := spanlogger.New(ctx, ss.logger, tracer, "ParquetBucketStores.LabelValues")
	defer spanLog.Finish()

	userID := getUserIDFromGRPCContext(spanCtx)
	if userID == "" {
		return nil, fmt.Errorf("no userID")
	}

	store := ss.getStore(userID)
	if store == nil {
		return &storepb.LabelValuesResponse{}, nil
	}

	return store.LabelValues(ctx, req)
}

func (ss *ParquetBucketStores) SyncBlocks(ctx context.Context) error {
	// TODO implement me
	panic("implement me")
}

// scanUsers in the bucket and return the list of found users, respecting any specifically
// enabled or disabled users.
func (ss *ParquetBucketStores) scanUsers(ctx context.Context) ([]string, error) {
	users, err := tsdb.ListUsers(ctx, ss.bucket)
	if err != nil {
		return nil, err
	}

	filtered := make([]string, 0, len(users))
	for _, user := range users {
		if ss.allowedTenants.IsAllowed(user) {
			filtered = append(filtered, user)
		}
	}

	return filtered, nil
}

func (ss *ParquetBucketStores) getStore(userID string) *ParquetBucketStore {
	ss.storesMu.RLock()
	defer ss.storesMu.RUnlock()
	return ss.stores[userID]
}
