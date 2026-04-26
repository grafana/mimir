// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

// SearchLabelNames implements the storegatewaypb.StoreGatewayServer interface.
// Full implementation arrives in the next task.
func (s *BucketStore) SearchLabelNames(_ *storepb.SearchLabelNamesRequest, _ storegatewaypb.StoreGateway_SearchLabelNamesServer) error {
	return status.Errorf(codes.Unimplemented, "method SearchLabelNames not implemented")
}

// SearchLabelValues implements the storegatewaypb.StoreGatewayServer interface.
// Full implementation arrives in the next task.
func (s *BucketStore) SearchLabelValues(_ *storepb.SearchLabelValuesRequest, _ storegatewaypb.StoreGateway_SearchLabelValuesServer) error {
	return status.Errorf(codes.Unimplemented, "method SearchLabelValues not implemented")
}
