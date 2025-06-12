package storegateway

import (
	"context"

	"github.com/grafana/dskit/services"

	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

type ParquetBucketStores struct {
	services.Service
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
