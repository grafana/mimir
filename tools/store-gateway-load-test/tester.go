package main

import (
	"context"
	"fmt"
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
	userID                      string
	finder                      querier.BlocksFinder
	selector                    *storeGatewaySelector
	logger                      log.Logger
	comparisonAuthoritativeZone string
}

func newTester(userID string, finder querier.BlocksFinder, selector *storeGatewaySelector, comparisonAuthoritativeZone string, logger log.Logger) *tester {
	return &tester{
		userID:                      userID,
		finder:                      finder,
		selector:                    selector,
		logger:                      logger,
		comparisonAuthoritativeZone: comparisonAuthoritativeZone,
	}
}

func (t *tester) sendRequestToAllStoreGatewayZonesAndCompareResults(ctx context.Context, minT int64, maxT int64, matchers []storepb.LabelMatcher, compareResults bool) error {
	perZoneSeries, err := t.sendRequestToAllStoreGatewayZones(ctx, minT, maxT, matchers, compareResults)
	if err != nil {
		return err
	}

	if compareResults {
		errs := comparePerZoneSeries(perZoneSeries, t.comparisonAuthoritativeZone)
		if len(errs) > 0 {
			for _, err := range errs {
				level.Warn(t.logger).Log("msg", "per-zone response comparison failed", "err", err)
			}
		} else {
			// Find the number of series received per zone (given comparison succeeded,
			// the number of series is equal in each zone).
			numSeries := 0
			for _, series := range perZoneSeries {
				numSeries = len(series)
				break
			}

			level.Info(t.logger).Log("msg", "per-zone response comparison succeeded", "num_zones", len(perZoneSeries), "num_series", numSeries)
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

func comparePerZoneSeries(perZoneSeries map[string][]*storepb.Series, authoritativeZone string) []error {
	var errs []error

	// Get the list of zones.
	zones := make([]string, 0, len(perZoneSeries))
	for zone := range perZoneSeries {
		zones = append(zones, zone)
	}

	// Compare each zone against the authoritative one.
	for _, zone := range zones {
		if zone == authoritativeZone {
			continue
		}

		if err := compareSeries(perZoneSeries[zone], perZoneSeries[authoritativeZone], zone, authoritativeZone); err != nil {
			errs = append(errs, err)
		}
	}

	return errs
}

func compareSeries(first, second []*storepb.Series, firstZone, secondZone string) error {
	// Ensure the number of series is the same.
	if len(first) != len(second) {
		return fmt.Errorf("the number of series don't match: %s has %d series, while %s has %d series", firstZone, len(first), secondZone, len(second))
	}

	// Ensure all series match.
	for i := 0; i < len(first); i++ {
		if !labels.Equal(first[i].PromLabels(), second[i].PromLabels()) {
			return fmt.Errorf("the series labels don't match at position %d: %s has series %s, while %s has series %s", i, firstZone, first[i].PromLabels().String(), secondZone, second[i].PromLabels().String())
		}
	}

	// Ensure all chunks match.
	for i := 0; i < len(first); i++ {
		if len(first[i].Chunks) != len(second[i].Chunks) {
			return fmt.Errorf("the number of chunks don't match for series at position %d (%s): %s has %d chunks, while %s has %d chunks", i, first[i].PromLabels().String(), firstZone, len(first[i].Chunks), secondZone, len(second[i].Chunks))
		}

		for c := 0; c < len(first[i].Chunks); c++ {
			if !first[i].Chunks[c].Equal(second[i].Chunks[c]) {
				return fmt.Errorf("the chunks don't match for series at position %d (%s): %s has chunk %s at position %d, while %s has chunk %s", i, first[i].PromLabels().String(), firstZone, first[i].Chunks[c].String(), c, secondZone, second[i].Chunks[c].String())
			}
		}
	}

	return nil
}
