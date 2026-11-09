// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"cmp"
	"context"
	"errors"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"

	"github.com/grafana/mimir/pkg/util"
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

	defaultLabelChurnLookback = 30 * time.Minute
	defaultLabelChurnLimit    = 30
)

var (
	errLabelChurnDisabled             = errors.New("label churn detection is disabled, enable it with -ingester.label-churn-enabled")
	errLabelChurnCompactedTooRecently = errors.New("a compaction removed series after the start of the lookback window, retry later or use a shorter lookback")
	errLabelChurnNoRecord             = errors.New("the ingester has no record in the lookback window, the ingester records once per minute after it starts")
)

type labelChurnResponse struct {
	// Since is the start of the time range that the scores are for. It can be later than the requested start.
	Since time.Time `json:"since"`
	// Scores are in descending order.
	Scores []labelChurnScore `json:"scores"`
}

type labelChurnScore struct {
	LabelName string `json:"label_name"`
	Score     uint64 `json:"score"`
	// MetricNames are the metric names that occur most frequently in a sample of the new series of the label.
	MetricNames []labelChurnMetricName `json:"metric_names"`
}

type labelChurnMetricName struct {
	MetricName    string `json:"metric_name"`
	SampledSeries uint64 `json:"sampled_series"`
}

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

// LabelChurnHandler returns the label churn scores of the tenant in the path.
// The optional "lookback" parameter sets the start of the time range. The default is 30m.
// The optional "limit" parameter sets the maximum number of scores. The default is 30. 0 returns all scores.
func (i *Ingester) LabelChurnHandler(w http.ResponseWriter, r *http.Request) {
	if !i.cfg.LabelChurnEnabled {
		http.Error(w, errLabelChurnDisabled.Error(), http.StatusNotFound)
		return
	}

	lookback := defaultLabelChurnLookback
	if lookbackParameter := r.FormValue("lookback"); lookbackParameter != "" {
		parsedLookback, err := model.ParseDuration(lookbackParameter)
		if err != nil || parsedLookback <= 0 {
			http.Error(w, "the lookback parameter must be a positive duration", http.StatusBadRequest)
			return
		}
		lookback = time.Duration(parsedLookback)
	}

	limit := defaultLabelChurnLimit
	if limitParameter := r.FormValue("limit"); limitParameter != "" {
		parsedLimit, err := strconv.Atoi(limitParameter)
		if err != nil || parsedLimit < 0 {
			http.Error(w, "the limit parameter must be a non-negative integer", http.StatusBadRequest)
			return
		}
		limit = parsedLimit
	}

	tenantID := mux.Vars(r)["tenant"]
	db := i.getTSDB(tenantID)
	if db == nil {
		http.Error(w, "the ingester has no TSDB for tenant "+tenantID, http.StatusNotFound)
		return
	}

	response, err := db.labelChurn(r.Context(), time.Now().Add(-lookback).UnixMilli(), limit)
	switch {
	case errors.Is(err, errLabelChurnCompactedTooRecently) || errors.Is(err, errLabelChurnNoRecord):
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
	case err != nil:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	default:
		util.WriteJSONResponse(w, response)
	}
}

// labelChurn returns the limit highest scores of the label names of the head, in descending order.
// The score is the number of series that have a value of the label which did not exist before sinceMs.
// A limit of 0 returns all scores.
func (u *userTSDB) labelChurn(ctx context.Context, sinceMs int64, limit int) (*labelChurnResponse, error) {
	// Get the watermark, which is the newest series reference at the start of the time range.
	// When the head removes the old series of a label value, records from before the removal show the value as new.
	// Thus the time range starts after the last compaction that removed series.
	// No record is valid while a compaction removes series.
	if u.pendingSeriesRemovals.Load() > 0 {
		return nil, errLabelChurnCompactedTooRecently
	}
	lastSeriesRemovalMs := u.lastSeriesRemovalMs.Load()
	watermark, found := u.seriesRefHistory.firstRecordNotBefore(max(sinceMs, lastSeriesRemovalMs))
	if !found {
		if lastSeriesRemovalMs > sinceMs {
			return nil, errLabelChurnCompactedTooRecently
		}
		return nil, errLabelChurnNoRecord
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

	response := &labelChurnResponse{Since: time.UnixMilli(watermark.timestampMs).UTC(), Scores: []labelChurnScore{}}
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
			response.Scores = append(response.Scores, labelChurnScore{
				LabelName:   labelName,
				Score:       score,
				MetricNames: topLabelChurnMetricNames(sampledSeriesPerMetricName),
			})
		}
	}

	// A compaction that removed series during the scan can make old values look new.
	// Load the pending removals before the last series removal, because updateLastSeriesRemoval stores them in the opposite order.
	if u.pendingSeriesRemovals.Load() > 0 || u.lastSeriesRemovalMs.Load() != lastSeriesRemovalMs {
		return nil, errLabelChurnCompactedTooRecently
	}

	slices.SortFunc(response.Scores, func(a, b labelChurnScore) int {
		return cmp.Or(cmp.Compare(b.Score, a.Score), cmp.Compare(a.LabelName, b.LabelName))
	})
	if limit > 0 && len(response.Scores) > limit {
		response.Scores = response.Scores[:limit]
	}
	return response, nil
}

// topLabelChurnMetricNames returns the metric names with the most sampled series, in descending order.
func topLabelChurnMetricNames(sampledSeriesPerMetricName map[string]uint64) []labelChurnMetricName {
	metricNames := make([]labelChurnMetricName, 0, len(sampledSeriesPerMetricName))
	for metricName, sampledSeries := range sampledSeriesPerMetricName {
		metricNames = append(metricNames, labelChurnMetricName{MetricName: metricName, SampledSeries: sampledSeries})
	}
	slices.SortFunc(metricNames, func(a, b labelChurnMetricName) int {
		return cmp.Or(cmp.Compare(b.SampledSeries, a.SampledSeries), cmp.Compare(a.MetricName, b.MetricName))
	})
	return metricNames[:min(labelChurnMetricNamesPerLabel, len(metricNames))]
}
