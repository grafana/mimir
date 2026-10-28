// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"

	"github.com/grafana/mimir/pkg/ingester/client"
)

const (
	// labelChurnRecordInterval is how often the ingester records the newest series reference of each tenant.
	labelChurnRecordInterval = time.Minute

	// labelChurnRecords is the number of records that the ingester keeps for each tenant.
	// The records cover a time range of labelChurnRecords multiplied by labelChurnRecordInterval.
	labelChurnRecords = 120

	// labelChurnSampledSeriesPerValue is the number of series that the ingester samples for each new label value.
	// The metric names of the sampled series show which metrics a label with a high score belongs to.
	labelChurnSampledSeriesPerValue = 5

	// labelChurnMetricNamesPerLabel is the maximum number of metric names in the result for each label name.
	labelChurnMetricNamesPerLabel = 20
)

type seriesRefRecord struct {
	timestampMs int64
	seriesRef   storage.SeriesRef
}

// seriesRefHistory is a ring buffer of the newest series reference of a head at different times.
type seriesRefHistory struct {
	mtx     sync.Mutex
	records []seriesRefRecord
	// oldestRecordIndex is the index of the oldest record when the buffer is full.
	oldestRecordIndex int
}

func (h *seriesRefHistory) record(timestampMs int64, seriesRef storage.SeriesRef, maxRecords int) {
	h.mtx.Lock()
	defer h.mtx.Unlock()

	newRecord := seriesRefRecord{timestampMs: timestampMs, seriesRef: seriesRef}
	if len(h.records) < maxRecords {
		h.records = append(h.records, newRecord)
		return
	}
	h.records[h.oldestRecordIndex] = newRecord
	h.oldestRecordIndex = (h.oldestRecordIndex + 1) % len(h.records)
}

// firstRecordNotBefore returns the oldest record with a timestamp at or after timestampMs.
func (h *seriesRefHistory) firstRecordNotBefore(timestampMs int64) (seriesRefRecord, bool) {
	h.mtx.Lock()
	defer h.mtx.Unlock()

	var result seriesRefRecord
	found := false
	for _, r := range h.records {
		if r.timestampMs >= timestampMs && (!found || r.timestampMs < result.timestampMs) {
			result = r
			found = true
		}
	}
	return result, found
}

// recordLabelChurnSeriesRefs records the newest series reference of each tenant.
func (i *Ingester) recordLabelChurnSeriesRefs(now time.Time) {
	for _, userID := range i.getTSDBUsers() {
		db := i.getTSDB(userID)
		if db == nil {
			continue
		}
		db.seriesRefHistory.record(now.UnixMilli(), db.Head().LastSeriesRef(), labelChurnRecords)
	}
}

// LabelChurn implements client.IngesterServer.
func (i *Ingester) LabelChurn(ctx context.Context, req *client.LabelChurnRequest) (resp *client.LabelChurnResponse, err error) {
	defer func() { err = i.mapReadErrorToErrorWithStatus(err) }()

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}

	if err := i.enforceReadConsistency(ctx, userID); err != nil {
		return nil, err
	}

	db := i.getTSDB(userID)
	if db == nil {
		return &client.LabelChurnResponse{}, nil
	}

	return db.labelChurn(ctx, req.SinceTimestampMs, time.Now().UnixMilli())
}

// labelChurn returns a score for each label name of the head.
// The score is the number of series that have a value of the label which did not exist before sinceMs.
func (u *userTSDB) labelChurn(ctx context.Context, sinceMs, nowMs int64) (*client.LabelChurnResponse, error) {
	// When the head removes all old series of a label value, the value looks new.
	// Thus the time range starts after the last series removal, with a margin.
	if lastSeriesRemovalMs := u.lastSeriesRemovalMs.Load(); lastSeriesRemovalMs > 0 {
		sinceMs = max(sinceMs, lastSeriesRemovalMs+(nowMs-lastSeriesRemovalMs)/2)
	}

	watermark, found := u.seriesRefHistory.firstRecordNotBefore(sinceMs)
	if !found {
		return &client.LabelChurnResponse{}, nil
	}

	head := u.db.Head()
	idx, err := head.Index()
	if err != nil {
		return nil, err
	}
	defer idx.Close()

	labelNames, err := idx.LabelNames(ctx)
	if err != nil {
		return nil, err
	}

	response := &client.LabelChurnResponse{SinceTimestampMs: watermark.timestampMs}
	var sampledSeriesRefs []storage.SeriesRef
	seriesLabelsBuilder := labels.NewScratchBuilder(0)
	for _, labelName := range labelNames {
		labelValues, err := idx.LabelValues(ctx, labelName, nil)
		if err != nil {
			return nil, err
		}

		var score uint64
		sampledSeriesPerMetricName := map[string]uint64{}
		for _, labelValue := range labelValues {
			postings, err := idx.Postings(ctx, labelName, labelValue)
			if err != nil {
				return nil, err
			}
			// The postings are sorted. When the first series reference is above the watermark, all series of the value are new.
			if !postings.Next() || postings.At() <= watermark.seriesRef {
				continue
			}

			score += uint64(head.PostingsLength(labelName, labelValue))

			sampledSeriesRefs = head.AppendRandomPostingsRefs(labelName, labelValue, labelChurnSampledSeriesPerValue, sampledSeriesRefs[:0])
			for _, seriesRef := range sampledSeriesRefs {
				if err := idx.Series(seriesRef, &seriesLabelsBuilder, nil); err != nil {
					if errors.Is(err, storage.ErrNotFound) {
						continue
					}
					return nil, err
				}
				sampledSeriesPerMetricName[seriesLabelsBuilder.Labels().Get(model.MetricNameLabel)]++
			}
		}

		if score > 0 {
			response.Scores = append(response.Scores, client.LabelChurnScore{
				LabelName:   labelName,
				Score:       score,
				MetricNames: client.TopLabelChurnMetricNames(sampledSeriesPerMetricName, labelChurnMetricNamesPerLabel),
			})
		}
	}
	return response, nil
}
