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
	// labelChurnRecords is the number of records that the ingester keeps for each tenant.
	// The records cover a time range of labelChurnRecords multiplied by client.LabelChurnRecordInterval.
	labelChurnRecords = 120

	// labelChurnSampledSeriesPerValue is the number of series that the ingester samples for each new label value.
	// The metric names of the sampled series show which metrics a label with a high score belongs to.
	labelChurnSampledSeriesPerValue = 5
)

var errLabelChurnDisabled = errors.New("label churn detection is disabled, enable it with -ingester.label-churn-enabled")

type seriesRefRecord struct {
	timestampMs int64
	seriesRef   storage.SeriesRef
}

// seriesRefHistory is a ring buffer of the newest series reference of a head at different times.
type seriesRefHistory struct {
	mtx     sync.Mutex
	records []seriesRefRecord
	// nextRecordIndex is the index that the next record overwrites when the buffer is full.
	nextRecordIndex int
}

func newSeriesRefHistory(maxRecords int) *seriesRefHistory {
	return &seriesRefHistory{records: make([]seriesRefRecord, 0, maxRecords)}
}

func (h *seriesRefHistory) record(timestampMs int64, seriesRef storage.SeriesRef) {
	h.mtx.Lock()
	defer h.mtx.Unlock()

	newRecord := seriesRefRecord{timestampMs: timestampMs, seriesRef: seriesRef}
	if len(h.records) < cap(h.records) {
		h.records = append(h.records, newRecord)
		return
	}
	h.records[h.nextRecordIndex] = newRecord
	h.nextRecordIndex = (h.nextRecordIndex + 1) % len(h.records)
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
		db.seriesRefHistory.record(now.UnixMilli(), db.Head().LastSeriesRef())
	}
}

// LabelChurn implements client.IngesterServer.
func (i *Ingester) LabelChurn(ctx context.Context, req *client.LabelChurnRequest) (resp *client.LabelChurnResponse, err error) {
	defer func() { err = i.mapReadErrorToErrorWithStatus(err) }()

	if !i.cfg.LabelChurnEnabled {
		return nil, errLabelChurnDisabled
	}

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}

	// The score approximates series already in the head. Skip read consistency so a lagging
	// partition still returns that approximation instead of failing the call.
	db := i.getTSDB(userID)
	if db == nil {
		return &client.LabelChurnResponse{}, nil
	}

	return db.labelChurn(ctx, req.SinceTimestampMs)
}

// labelChurn returns a score for each label name of the head.
// The score is the number of series that have a value of the label which did not exist before sinceMs.
func (u *userTSDB) labelChurn(ctx context.Context, sinceMs int64) (*client.LabelChurnResponse, error) {
	// Get the watermark, which is the newest series reference at the start of the time range.
	// When the head removes the old series of a label value, records from before the removal show the value as new.
	// Thus the time range starts after the last compaction that removed series.
	// No record is valid while a compaction removes series.
	if u.pendingSeriesRemovals.Load() > 0 {
		return &client.LabelChurnResponse{Status: client.LABEL_CHURN_COMPACTED_TOO_RECENTLY}, nil
	}
	lastSeriesRemovalMs := u.lastSeriesRemovalMs.Load()
	watermark, found := u.seriesRefHistory.firstRecordNotBefore(max(sinceMs, lastSeriesRemovalMs))
	if !found {
		if lastSeriesRemovalMs > sinceMs {
			return &client.LabelChurnResponse{Status: client.LABEL_CHURN_COMPACTED_TOO_RECENTLY}, nil
		}
		return &client.LabelChurnResponse{Status: client.LABEL_CHURN_NO_RECORD}, nil
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
			// The head sorts the postings by series reference.
			// Thus the value is new when its first series reference is above the watermark.
			if !postings.Next() || postings.At() <= watermark.seriesRef {
				continue
			}

			// Add the number of series of the new value to the score of the label.
			score += uint64(head.PostingsLength(labelName, labelValue))

			// Count the metric names of a few random series of the new value.
			// The metric names show some examples metrics the new values belong to.
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

		// Report the label only when it has at least one new value.
		if score > 0 {
			response.Scores = append(response.Scores, client.LabelChurnScore{
				LabelName:   labelName,
				Score:       score,
				MetricNames: client.TopLabelChurnMetricNames(sampledSeriesPerMetricName, client.LabelChurnMetricNamesPerLabel),
			})
		}
	}
	return response, nil
}
