package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/grafana/dskit/concurrency"
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
	client  storegatewaypb.StoreGatewayClient
	ballast []byte
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
		client:  client,
		ballast: make([]byte, 1024*1024*1024), // 1GB
	}, nil
}

func (c *testClient) runRequests(totalRequests, concurrencyLimit int) error {
	fmt.Println("Num requests:", totalRequests, "Concurrency:", concurrencyLimit)

	// Keep track of timing.
	var (
		durationsMx sync.Mutex
		durations   []time.Duration
	)

	err := concurrency.ForEachJob(context.Background(), totalRequests, concurrencyLimit, func(ctx context.Context, idx int) error {
		startTime := time.Now()

		defer func() {
			elapsedTime := time.Since(startTime)
			fmt.Println(fmt.Sprintf("Request #%d took %s", idx, elapsedTime.String()))

			durationsMx.Lock()
			durations = append(durations, elapsedTime)
			durationsMx.Unlock()
		}()

		return c.runRequest()
	})

	// Print stats.
	printStats(durations)

	return err
}

func (c *testClient) runRequest() error {
	ctx := user.InjectOrgID(context.Background(), "test")

	stream, err := c.client.Series(ctx, &storepb.SeriesRequest{})
	if err != nil {
		return err
	}

	for {
		_, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}

		// TODO introduce a slow down
		//time.Sleep(250 * time.Millisecond)

		//fmt.Println(time.Now().String(), "Client received:", res.GetSeries().Labels)
	}

	return nil
}

func printStats(durations []time.Duration) {
	minDuration := durations[0]
	maxDuration := durations[0]
	sumDuration := time.Duration(0)

	for _, value := range durations {
		sumDuration += value

		if value < minDuration {
			minDuration = value
		}
		if value > maxDuration {
			maxDuration = value
		}
	}

	fmt.Println(fmt.Sprintf("Min: %s Max: %s Avg: %s", minDuration, maxDuration, sumDuration/time.Duration(len(durations))))
}
