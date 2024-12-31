package main

import (
	"context"
	"fmt"
	"io"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/ring"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"

	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
)

func main() {

}

func queryIngesterAndCheckMatchersCorrectness(ctx context.Context, addr string, from, to model.Time, matchers ...*labels.Matcher) error {
	fetchedSeries, err := queryIngester(ctx, addr, from, to, matchers...)
	if err != nil {
		return err
	}

	for _, series := range fetchedSeries {
		seriesLabels := mimirpb.FromLabelAdaptersToLabels(series.Labels)

		// Ensure the matchers match all series labels.
		for _, m := range matchers {
			val := seriesLabels.Get(m.Name)
			if !m.Matches(val) {
				return fmt.Errorf("received series %s but it doesn't match the matcher %s", seriesLabels.String(), m.String())
			}
		}
	}

	return nil
}

func queryIngester(ctx context.Context, addr string, from, to model.Time, matchers ...*labels.Matcher) (_ map[string]ingester_client.TimeSeriesChunk, returnErr error) {
	req, err := ingester_client.ToQueryRequest(from, to, matchers)
	if err != nil {
		return nil, err
	}

	// To keep it simple, create a gRPC client each time.
	clientMetrics := ingester_client.NewMetrics(nil)
	clientConfig := ingester_client.Config{}
	flagext.DefaultValues(&clientConfig)

	client, err := ingester_client.MakeIngesterClient(ring.InstanceDesc{Addr: addr}, clientConfig, clientMetrics)
	if err != nil {
		return nil, err
	}

	// Ensure to close the client once done.
	defer func() {
		if closeErr := client.Close(); closeErr != nil && returnErr == nil {
			returnErr = closeErr
		}
	}()

	stream, err := client.QueryStream(ctx, req)
	if err != nil {
		return nil, err
	}

	// Fetch all series.
	fetchedSeries := make(map[string]ingester_client.TimeSeriesChunk)

	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, err
		}

		if len(resp.Timeseries) > 0 {
			panic("Not expected to receive timeseries")
		} else if len(resp.StreamingSeries) > 0 {
			panic("Not expected to receive streaming series")
		} else if len(resp.Chunkseries) > 0 {
			for _, series := range resp.Chunkseries {
				// Serialize the series labels and use it as map key.
				key := mimirpb.FromLabelAdaptersToString(series.Labels)

				data, ok := fetchedSeries[key]
				if !ok {
					fetchedSeries[key] = series
				} else {
					data = fetchedSeries[key]
					data.Chunks = append(fetchedSeries[key].Chunks, series.Chunks...)
					fetchedSeries[key] = data
				}
			}
		}
	}

	return fetchedSeries, nil
}
