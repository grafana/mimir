package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/gogo/protobuf/types"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/user"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	grpc_metadata "google.golang.org/grpc/metadata"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storegateway"
	"github.com/grafana/mimir/pkg/storegateway/hintspb"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

func main() {
	conn, err := grpc.Dial("localhost:9095", grpc.WithUnaryInterceptor(middleware.ClientUserHeaderInterceptor), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	client := storegatewaypb.NewStoreGatewayClient(conn)

	hints := &hintspb.SeriesRequestHints{
		BlockMatchers: []storepb.LabelMatcher{
			{
				Type: storepb.LabelMatcher_RE,
				Name: block.BlockIDLabel,
				Value: strings.Join([]string{
					//"01H908VJB3SCD71DABS921A4WW",
					//"01H91SE65CNWZA1R6ZVP76HA9V",
					"01H91C8FHB4Q89DMMVFN2GPWSF",
				}, "|"),
			},
		},
	}

	anyHints, err := types.MarshalAny(hints)
	if err != nil {
		panic(err)
	}
	req := &storepb.SeriesRequest{
		MinTime: time.Now().Add(-2 * 24 * time.Hour).UnixMilli(),
		MaxTime: time.Now().UnixMilli(),
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: "__query_shard__", Value: "30_of_32"},
			{Type: storepb.LabelMatcher_EQ, Name: "__name__", Value: "node_cpu_seconds_total"},
			{Type: storepb.LabelMatcher_EQ, Name: "cluster", Value: "prod-eu-west-0"},
		},
		Hints:                    anyHints,
		StreamingChunksBatchSize: 100,
		SkipChunks:               false,
	}

	ctx := grpc_metadata.AppendToOutgoingContext(user.InjectOrgID(context.Background(), "10428"), storegateway.GrpcContextMetadataTenantID, "10428")
	ctx, err = user.InjectIntoGRPCRequest(ctx)
	if err != nil {
		panic(err)
	}

	stream, err := client.Series(ctx, req)
	if err != nil {
		panic(err)
	}

	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			panic(err)
		}
		fmt.Println("resp", resp.Size())
	}
	fmt.Println("done")
}
