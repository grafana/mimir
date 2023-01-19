package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/grpcclient"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/weaveworks/common/user"

	"github.com/grafana/mimir/pkg/querier"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

type testClient struct {
	client storegatewaypb.StoreGatewayClient
}

func newClient() (*testClient, error) {
	cfg := grpcclient.Config{}
	flagext.DefaultValues(&cfg)

	requestDuration := promauto.With(nil).NewHistogramVec(prometheus.HistogramOpts{
		Namespace:   "cortex",
		Name:        "storegateway_client_request_duration_seconds",
		Help:        "Time spent executing requests to the store-gateway.",
		Buckets:     prometheus.ExponentialBuckets(0.008, 4, 7),
		ConstLabels: prometheus.Labels{"client": "querier"},
	}, []string{"operation", "status_code"})

	client, err := querier.DialStoreGatewayClient(cfg, "localhost:9200", requestDuration)
	if err != nil {
		return nil, err
	}

	return &testClient{
		client: client,
	}, nil
}

func (c *testClient) runRequest() error {
	ctx := user.InjectOrgID(context.Background(), "test")

	stream, err := c.client.Series(ctx, &storepb.SeriesRequest{})
	if err != nil {
		return err
	}

	for {
		res, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}

		// TODO introduce a slow down
		time.Sleep(250 * time.Millisecond)

		fmt.Println(time.Now().String(), "Client received:", res.GetSeries().Labels)
	}

	return nil
}
