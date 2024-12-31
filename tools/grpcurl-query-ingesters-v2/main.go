package main

import (
	"context"
	"io"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/ring"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"

	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
)

func main() {

}

func queryIngester(ctx context.Context, addr string, from, to model.Time, matchers ...*labels.Matcher) (returnErr error) {
	req, err := ingester_client.ToQueryRequest(from, to, matchers)
	if err != nil {
		return err
	}

	// To keep it simple, create a gRPC client each time.
	clientMetrics := ingester_client.NewMetrics(nil)
	clientConfig := ingester_client.Config{}
	flagext.DefaultValues(&clientConfig)

	client, err := ingester_client.MakeIngesterClient(ring.InstanceDesc{Addr: addr}, clientConfig, clientMetrics)
	if err != nil {
		return err
	}

	// Ensure to close the client once done.
	defer func() {
		if closeErr := client.Close(); closeErr != nil && returnErr == nil {
			returnErr = closeErr
		}
	}()

	stream, err := client.QueryStream(ctx, req)
	if err != nil {
		return err
	}

	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return err
		}

		if len(resp.Timeseries) > 0 {
			panic("Not expected to receive timeseries")
		} else if len(resp.StreamingSeries) > 0 {
			panic("Not expected to receive streaming series")
		} else if len(resp.Chunkseries) > 0 {
			for _, series := range resp.Chunkseries {
				// TODO do something
			}
		}
	}

}
