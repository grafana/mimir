// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"github.com/gogo/protobuf/proto"

	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

const backendTypeStoreGateway = "store-gateway"

// newStoreGatewayCodec returns the codec for the gatewaypb.StoreGateway service.
func newStoreGatewayCodec() MessageCodec {
	return methodTableCodec{methods: map[string]methodTypes{
		"/gatewaypb.StoreGateway/Series": {
			newRequest:  func() proto.Message { return &storepb.SeriesRequest{} },
			newResponse: func() proto.Message { return &storepb.SeriesResponse{} },
		},
		"/gatewaypb.StoreGateway/LabelNames": {
			newRequest:  func() proto.Message { return &storepb.LabelNamesRequest{} },
			newResponse: func() proto.Message { return &storepb.LabelNamesResponse{} },
		},
		"/gatewaypb.StoreGateway/LabelValues": {
			newRequest:  func() proto.Message { return &storepb.LabelValuesRequest{} },
			newResponse: func() proto.Message { return &storepb.LabelValuesResponse{} },
		},
		"/gatewaypb.StoreGateway/SearchLabelNames": {
			newRequest:  func() proto.Message { return &storepb.SearchLabelNamesRequest{} },
			newResponse: func() proto.Message { return &storepb.SearchResultBatch{} },
		},
		"/gatewaypb.StoreGateway/SearchLabelValues": {
			newRequest:  func() proto.Message { return &storepb.SearchLabelValuesRequest{} },
			newResponse: func() proto.Message { return &storepb.SearchResultBatch{} },
		},
	}}
}
