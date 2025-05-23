// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"

	"github.com/grafana/dskit/services"

	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

type Stores interface {
	services.Service
	Series(req *storepb.SeriesRequest, srv storegatewaypb.StoreGateway_SeriesServer) error
	LabelNames(ctx context.Context, req *storepb.LabelNamesRequest) (*storepb.LabelNamesResponse, error)
	LabelValues(ctx context.Context, req *storepb.LabelValuesRequest) (*storepb.LabelValuesResponse, error)
	SyncBlocks(ctx context.Context) error

	scanUsers(context.Context) ([]string, error)
}
