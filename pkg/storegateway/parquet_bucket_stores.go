// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

type ParquetBucketStores struct {
	services.Service

	logger log.Logger
	reg    prometheus.Registerer
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

func (ss ParquetBucketStores) Series(req *storepb.SeriesRequest, srv storegatewaypb.StoreGateway_SeriesServer) error {
	//TODO implement me
	panic("implement me")
}

func (ss ParquetBucketStores) LabelNames(ctx context.Context, req *storepb.LabelNamesRequest) (*storepb.LabelNamesResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (ss ParquetBucketStores) LabelValues(ctx context.Context, req *storepb.LabelValuesRequest) (*storepb.LabelValuesResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (ss ParquetBucketStores) SyncBlocks(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (ss ParquetBucketStores) scanUsers(ctx context.Context) ([]string, error) {
	//TODO implement me
	panic("implement me")
}
