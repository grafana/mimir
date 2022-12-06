package main

import (
	"context"
	"io"
	"sort"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/weaveworks/common/user"
	"golang.org/x/sync/errgroup"
	grpc_metadata "google.golang.org/grpc/metadata"

	"github.com/grafana/mimir/pkg/querier"
	"github.com/grafana/mimir/pkg/storegateway"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

type tester struct {
	userID   string
	finder   querier.BlocksFinder
	selector *storeGatewaySelector
	logger   log.Logger
}

func newTester(userID string, finder querier.BlocksFinder, selector *storeGatewaySelector, logger log.Logger) *tester {
	return &tester{
		userID:   userID,
		finder:   finder,
		selector: selector,
		logger:   logger,
	}
}

func (t *tester) sendRequestToAllStoreGatewayZonesAndCompareResults(ctx context.Context, minT int64, maxT int64, matchers []storepb.LabelMatcher, compareResults bool) error {
	perZoneSeries, err := t.sendRequestToAllStoreGatewayZones(ctx, minT, maxT, matchers, compareResults)
	if err != nil {
		return err
	}

	// TODO run the comparison
	if compareResults {
		for zone, zoneSeries := range perZoneSeries {
			level.Info(t.logger).Log("msg", "response", "zone", zone, "num_series", len(zoneSeries))
		}
	}

	return nil
}

func (t *tester) sendRequestToAllStoreGatewayZones(ctx context.Context, minT int64, maxT int64, matchers []storepb.LabelMatcher, keepResults bool) (map[string][]*storepb.Series, error) {
	// Find the list of blocks we need to query given the time range.
	knownBlocks, _, err := t.finder.GetBlocks(ctx, t.userID, minT, maxT)
	if err != nil {
		return nil, err
	}
	if len(knownBlocks) == 0 {
		level.Warn(t.logger).Log("msg", "no blocks to query")
		return nil, nil
	}

	perZoneClients, err := t.selector.GetZonalClientsFor(t.userID, knownBlocks.GetULIDs())
	if err != nil {
		return nil, err
	}

	// Inject the user ID in the context.
	ctx = user.InjectOrgID(ctx, t.userID)
	ctx = grpc_metadata.AppendToOutgoingContext(ctx, storegateway.GrpcContextMetadataTenantID, t.userID)

	var (
		g, gCtx         = errgroup.WithContext(ctx)
		perZoneSeriesMx sync.Mutex
		perZoneSeries   = map[string]map[string]*storepb.Series{}
	)

	// Concurrently send all requests to all store-gateways (in all zones),
	// and collect the per-zone results.
	for zone, clients := range perZoneClients {
		zone := zone

		// Initialize perZoneSeries, so we don't have to worry later about it.
		perZoneSeries[zone] = map[string]*storepb.Series{}

		for client, blockIDs := range clients {
			client := client
			blockIDs := blockIDs

			g.Go(func() error {
				skipChunks := false

				req, err := querier.CreateSeriesRequest(minT, maxT, matchers, skipChunks, blockIDs)
				if err != nil {
					return errors.Wrapf(err, "failed to create series request")
				}

				receivedSeries, err := t.sendRequestToStoreGateway(gCtx, client, req, keepResults)
				if err != nil {
					return err
				}

				// Keep track of the received series.
				if keepResults {
					perZoneSeriesMx.Lock()
					defer perZoneSeriesMx.Unlock()

					for _, series := range receivedSeries {
						// In order to avoid having to deal with hashing collisions, we use the whole
						// labels set as "series ID".
						seriesID := series.PromLabels().String()

						if _, ok := perZoneSeries[zone][seriesID]; !ok {
							perZoneSeries[zone][seriesID] = series
						} else {
							perZoneSeries[zone][seriesID].Chunks = append(perZoneSeries[zone][seriesID].Chunks, series.Chunks...)
						}
					}
				}

				return nil
			})
		}
	}

	// Wait until all requests have done.
	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Convert per-zone series into a list.
	perZoneSeriesList := map[string][]*storepb.Series{}
	for zone, zoneSeries := range perZoneSeries {
		perZoneSeriesList[zone] = make([]*storepb.Series, 0, len(zoneSeries))

		for _, series := range zoneSeries {
			perZoneSeriesList[zone] = append(perZoneSeriesList[zone], series)
		}
	}

	// Sort all series and chunks.
	for _, zoneSeries := range perZoneSeriesList {
		// Sort series.
		sort.Slice(zoneSeries, func(i, j int) bool {
			return labels.Compare(zoneSeries[i].PromLabels(), zoneSeries[j].PromLabels()) < 0
		})

		// Sort chunks.
		for _, series := range zoneSeries {
			sort.Slice(series.Chunks, func(i, j int) bool {
				if series.Chunks[i].MinTime < series.Chunks[j].MinTime {
					return true
				}
				if series.Chunks[i].MinTime > series.Chunks[j].MinTime {
					return false
				}

				// If min time is equal then we sort on max time.
				return series.Chunks[i].MaxTime < series.Chunks[j].MaxTime
			})
		}
	}

	return perZoneSeriesList, nil
}

func (t *tester) sendRequestToStoreGateway(ctx context.Context, client querier.BlocksStoreClient, req *storepb.SeriesRequest, keepResults bool) ([]*storepb.Series, error) {
	var receivedSeries []*storepb.Series

	stream, err := client.Series(ctx, req)
	if err != nil {
		return nil, err
	}

	for {
		// Ensure the context hasn't been canceled in the meanwhile).
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}

		if keepResults {
			if series := resp.GetSeries(); series != nil {
				// Remove all chunks data. We only want to keep track of chunks refs.
				for _, chunk := range series.Chunks {
					chunk.Raw = nil
				}

				receivedSeries = append(receivedSeries, series)
			}
		}
	}

	return receivedSeries, nil
}
