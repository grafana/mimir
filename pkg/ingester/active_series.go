// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"fmt"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"

	"github.com/grafana/mimir/pkg/ingester/activeseries"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

const activeSeriesMaxSizeBytes = 1 * 1024 * 1024

// ActiveSeries implements the ActiveSeries RPC. It returns a stream of active
// series that match the given matchers.
func (i *Ingester) ActiveSeries(request *client.ActiveSeriesRequest, stream client.Ingester_ActiveSeriesServer) error {
	if err := i.checkRunning(); err != nil {
		return err
	}
	if err := i.checkReadOverloaded(); err != nil {
		return err
	}

	spanlog, ctx := spanlogger.NewWithLogger(stream.Context(), i.logger, "Ingester.ActiveSeries")
	defer spanlog.Finish()

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return err
	}

	matchers, err := client.FromLabelMatchers(request.GetMatchers())
	if err != nil {
		return fmt.Errorf("error parsing label matchers: %w", err)
	}

	db := i.getTSDB(userID)
	if db == nil {
		level.Debug(i.logger).Log("msg", "no TSDB for user", "userID", userID)
		return nil
	}

	series, err := listActiveSeries(ctx, db, matchers)
	if err != nil {
		return fmt.Errorf("error listing active series: %w", err)
	}

	resp := &client.ActiveSeriesResponse{}
	currentSize := 0
	for series.Next() {
		m := &mimirpb.Metric{Labels: mimirpb.FromLabelsToLabelAdapters(series.At())}
		mSize := m.Size()
		if currentSize+mSize > activeSeriesMaxSizeBytes {
			if err := client.SendActiveSeriesResponse(stream, resp); err != nil {
				return fmt.Errorf("error sending response: %w", err)
			}
			resp = &client.ActiveSeriesResponse{}
			currentSize = 0
		}
		resp.Metric = append(resp.Metric, m)
		currentSize += mSize
	}
	if err := series.Err(); err != nil {
		return fmt.Errorf("error iterating over series: %w", err)
	}

	if len(resp.Metric) > 0 {
		if err := client.SendActiveSeriesResponse(stream, resp); err != nil {
			return fmt.Errorf("error sending response: %w", err)
		}
	}

	return nil
}

// listActiveSeries returns an iterator over the active series matching the given matchers.
func listActiveSeries(ctx context.Context, db *userTSDB, matchers []*labels.Matcher) (series *Series, err error) {
	idx, err := db.Head().Index()
	if err != nil {
		return nil, fmt.Errorf("error getting index: %w", err)
	}

	if db.activeSeries == nil {
		return nil, fmt.Errorf("active series tracker is not initialized")
	}

	postings, err := tsdb.PostingsForMatchers(ctx, idx, matchers...)
	if err != nil {
		return nil, fmt.Errorf("error getting postings: %w", err)
	}

	return NewSeries(activeseries.NewPostings(db.activeSeries, postings), idx), nil
}
