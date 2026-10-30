// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"github.com/gogo/protobuf/proto"

	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/tools/grpc-tee/backend"
)

func newStoreGatewayCodecTable() *backend.MethodCodecTable {
	codecTable := backend.NewMethodCodecTable()

	codecTable.RegisterMethodCodec(
		"/gatewaypb.StoreGateway/Series",
		backend.MethodTypes{
			NewReq: func() proto.Message { return &storepb.SeriesRequest{} },
			NewRes: func() proto.Message { return &storepb.SeriesResponse{} },
		},
	)
	codecTable.RegisterMethodCodec(
		"/gatewaypb.StoreGateway/LabelNames",
		backend.MethodTypes{
			NewReq: func() proto.Message { return &storepb.LabelNamesRequest{} },
			NewRes: func() proto.Message { return &storepb.LabelNamesResponse{} },
		},
	)
	codecTable.RegisterMethodCodec(
		"/gatewaypb.StoreGateway/LabelValues",
		backend.MethodTypes{
			NewReq: func() proto.Message { return &storepb.LabelValuesRequest{} },
			NewRes: func() proto.Message { return &storepb.LabelValuesResponse{} },
		},
	)
	codecTable.RegisterMethodCodec(
		"/gatewaypb.StoreGateway/SearchLabelNames",
		backend.MethodTypes{
			NewReq: func() proto.Message { return &storepb.SearchLabelNamesRequest{} },
			NewRes: func() proto.Message { return &storepb.SearchResultBatch{} },
		},
	)
	codecTable.RegisterMethodCodec(
		"/gatewaypb.StoreGateway/SearchLabelValues",
		backend.MethodTypes{
			NewReq: func() proto.Message { return &storepb.SearchLabelValuesRequest{} },
			NewRes: func() proto.Message { return &storepb.SearchResultBatch{} },
		},
	)

	return codecTable
}
