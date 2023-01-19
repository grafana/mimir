package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/weaveworks/common/server"
	"google.golang.org/grpc/grpclog"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	util_log "github.com/grafana/mimir/pkg/util/log"
)

type testServer struct {
	serv *server.Server
}

func newServer() (*testServer, error) {
	cfg := server.Config{}
	flagext.DefaultValues(&cfg)
	cfg.GRPCListenAddress = "localhost"
	cfg.GRPCListenPort = 9200
	cfg.LogLevel.Set("debug")
	util_log.InitLogger(&cfg)
	grpclog.SetLoggerV2(grpclog.NewLoggerV2WithVerbosity(os.Stdout, os.Stdout, os.Stdout, 1000))

	serv, err := server.New(cfg)
	if err != nil {
		return nil, err
	}

	s := &testServer{
		serv: serv,
	}

	storegatewaypb.RegisterStoreGatewayServer(s.serv.GRPC, s)

	go func() {
		log.Fatal(s.serv.Run())
	}()

	return s, nil
}

func (s *testServer) Series(req *storepb.SeriesRequest, srv storegatewaypb.StoreGateway_SeriesServer) error {
	for i := 0; i < 10; i++ {
		series := &storepb.Series{
			Labels: []mimirpb.LabelAdapter{{Name: "__name__", Value: fmt.Sprintf("series_%d", i)}},
			Chunks: []storepb.AggrChunk{
				{Raw: &storepb.Chunk{Data: make([]byte, 16*1024)}},
			},
		}

		if err := srv.Send(storepb.NewSeriesResponse(series)); err != nil {
			return err
		}

		fmt.Println(time.Now().String(), "Server sent:", series.Labels)
	}

	return nil
}

func (s *testServer) LabelNames(context.Context, *storepb.LabelNamesRequest) (*storepb.LabelNamesResponse, error) {
	return nil, nil
}

func (s *testServer) LabelValues(context.Context, *storepb.LabelValuesRequest) (*storepb.LabelValuesResponse, error) {
	return nil, nil
}
