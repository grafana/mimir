// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/ingester/client"
	mimirstorage "github.com/grafana/mimir/pkg/storage"
	"github.com/grafana/mimir/pkg/streaminglabelvalues"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

const (
	searchLabelValues = "Ingester.SearchLabelValues"
	searchLabelNames  = "Ingester.SearchLabelNames"
)

func initSpanLog(spanlog *spanlogger.SpanLogger, userId string, mint, maxt int64, hints *storage.LabelHints, matchers []*labels.Matcher, filter *client.SearchLabelValuesFilter) {
	spanlog.SetSpanAndLogTag("user", userId)
	spanlog.SetSpanAndLogTag("minT", mint)
	spanlog.SetSpanAndLogTag("maxT", maxt)

	if hints != nil && hints.Limit > 0 {
		spanlog.SetSpanAndLogTag("limit", hints.Limit)
	}

	if len(matchers) > 0 {
		spanlog.SetSpanAndLogTag("matchers", util.LabelMatchersToString(matchers))

	}

	if filter != nil {
		if len(filter.SearchTerms) > 0 {
			spanlog.SetSpanAndLogTag("searchTerms", strings.Join(filter.SearchTerms, ","))
			spanlog.SetSpanAndLogTag("fuzz_threshold", filter.FuzzThreshold)
		}
		if filter.SortBy != 0 {
			spanlog.SetSpanAndLogTag("sort_by", filter.SortBy)
			spanlog.SetSpanAndLogTag("sort_dir", filter.SortOrder)
		}
	}
}

// SearchLabelNames implements client.IngesterServer.
// It is identical to LabelNames but applies the SearchLabelValuesFilter before the limit,
// and streams results back in batches.
func (i *Ingester) SearchLabelNames(req *client.SearchLabelValuesRequest, stream client.Ingester_SearchLabelNamesServer) (err error) {
	return i.doSearchLabelValues(req, stream, searchLabelNames)
}

// SearchLabelValues implements client.IngesterServer.
// It is identical to LabelValues but applies the SearchLabelValuesFilter before the limit,
// and streams results back in batches.
func (i *Ingester) SearchLabelValues(req *client.SearchLabelValuesRequest, stream client.Ingester_SearchLabelValuesServer) (err error) {
	return i.doSearchLabelValues(req, stream, searchLabelValues)
}

func (i *Ingester) doSearchLabelValues(req *client.SearchLabelValuesRequest, stream client.Ingester_SearchLabelValuesServer, method string) (err error) {
	defer func() { err = i.mapReadErrorToErrorWithStatus(err) }()

	spanlog, ctx := spanlogger.New(stream.Context(), i.logger, tracer, method)
	defer spanlog.Finish()

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		level.Error(spanlog).Log("msg", "unable to determine tenant from context", "err", err)
		return err
	}

	// The max bytes of labels/values which can be buffered during the search.
	maxBytesLimit := i.limits.IngesterSearchLabelsValuesMaxSizeBytes(userID)

	// extract the relevant params from the search request
	// noting that labelName is only used for label values queries
	labelName, mint, maxt, hints, matchers, err := client.FromSearchRequest(req)
	if err != nil {
		level.Error(spanlog).Log("msg", "unable to parse search label values request", "err", err)
		return err
	}

	initSpanLog(spanlog, userID, mint, maxt, hints, matchers, req.Filter)

	if labelName == "" && method == searchLabelValues {
		err = fmt.Errorf("missing label name")
		level.Error(spanlog).Log("msg", "unable to parse search label values request", "err", err)
		return err
	} else if labelName != "" {
		spanlog.SetSpanAndLogTag("label", labelName)
	}

	if err := i.enforceReadConsistency(ctx, userID); err != nil {
		level.Error(spanlog).Log("msg", "unable to enforce read consistency", "err", err)
		return err
	}

	db := i.getTSDB(userID)
	if db == nil {
		level.Error(spanlog).Log("msg", "unable to access TSDB for user", "err", err)
		return nil
	}

	searchFilter, err := buildSearchFilter(req.Filter)
	if err != nil {
		level.Error(spanlog).Log("msg", "unable to build search filter", "err", err)
		return err
	}

	var produce func(ch chan<- mimirstorage.SearchResult) (annotations.Annotations, error)
	switch method {
	case searchLabelValues:
		produce = func(ch chan<- mimirstorage.SearchResult) (annotations.Annotations, error) {
			return produceLabelValues(ctx, db.db, mint, maxt, labelName, matchers, searchFilter, ch)
		}
	case searchLabelNames:
		q, err := db.Querier(mint, maxt)
		if err != nil {
			level.Error(spanlog).Log("msg", "unable to access Querier", "err", err)
			return err
		}
		produce = func(ch chan<- mimirstorage.SearchResult) (annotations.Annotations, error) {
			return produceLabelNames(ctx, q, matchers, searchFilter, ch)
		}
	default:
		return nil
	}

	// The querier clamps req.Limit to the per-tenant maximum via clampToMaxLimit before
	// fan-out; the ingester honours whatever limit arrives in the request.
	limit := int(req.Limit)

	bufSize := i.cfg.SearchLabelValuesStreamingBatchSize
	if limit > 0 {
		bufSize = min(limit, i.cfg.SearchLabelValuesStreamingBatchSize)
	}

	// slice of pointers since this will be passed into the gRPC stream.Send
	batch := make([]*client.SearchLabelValuesResult, bufSize)
	for k := range batch {
		batch[k] = &client.SearchLabelValuesResult{}
	}

	// Note that the req.Filter is used to extract the sort options. The actual filtering is performed within the producer.
	iter := newingesterSearcherValueSet(produce, req.Filter, limit, maxBytesLimit, nil)
	defer iter.Close()

	batchIdx := 0
	for iter.Next() {
		r := iter.At()
		*batch[batchIdx] = client.SearchLabelValuesResult{Value: r.Value, Score: r.Score}
		batchIdx++
		if batchIdx == len(batch) {
			if err := stream.Send(&client.SearchLabelValuesResponse{Results: batch}); err != nil {
				return err
			}
			batchIdx = 0
		}
	}

	if err := iter.Err(); err != nil {
		return err
	}

	if batchIdx > 0 {
		if err := stream.Send(&client.SearchLabelValuesResponse{Results: batch[:batchIdx]}); err != nil {
			return err
		}
	}
	return sendWarnings(stream, iter.Warnings())
}

// sendWarnings sends any accumulated warnings (if any) as a final response message with no results.
func sendWarnings(stream client.Ingester_SearchLabelValuesServer, warns annotations.Annotations) error {
	if len(warns) == 0 {
		return nil
	}
	warningStrs := make([]string, 0, len(warns))
	for _, err := range warns {
		warningStrs = append(warningStrs, err.Error())
	}
	return stream.Send(&client.SearchLabelValuesResponse{Warnings: warningStrs})
}

// produceLabelNames fetches all label names from q, applies searchFilter, and sends each
// accepted name to ch. Sends are cancelled via ctx. The querier is closed before returning.
func produceLabelNames(
	ctx context.Context,
	q storage.Querier,
	matchers []*labels.Matcher,
	searchFilter *streaminglabelvalues.FilterChains,
	ch chan<- mimirstorage.SearchResult,
) (annotations.Annotations, error) {
	defer q.Close() //nolint:errcheck
	// TODO: TSDB has no streaming LabelNames iterator (unlike LabelValuesFor).
	// To avoid buffering all label names, Prometheus would need to add
	// LabelNamesFor(postings index.Postings) storage.LabelValues to its IndexReader interface.
	// Pass nil hints so TSDB returns all names; the limit is enforced downstream by SearchValueSet
	// after filtering. Passing hints.Limit here would truncate before filtering, causing
	// filter+limit combinations to return fewer results than the user expects.
	vals, warnings, err := q.LabelNames(ctx, nil, matchers...)
	if err != nil {
		return warnings, err
	}

	var score float64
	var accepted bool
	for _, v := range vals {
		if searchFilter != nil {
			accepted, score = searchFilter.Accept(v)
			if !accepted {
				continue
			}
		}
		select {
		case ch <- mimirstorage.SearchResult{Value: v, Score: score}:
		case <-ctx.Done():
			return warnings, ctx.Err()
		}
	}
	return warnings, nil
}

// produceLabelValues reads label values from a TSDB querier covering [mint, maxt],
// applies searchFilter, and sends each accepted value to ch.
// Using a querier (rather than iterating tsdb.DB.Blocks() directly) ensures all block
// index readers are acquired atomically under the DB read lock, preventing a race where
// a block is deleted by compaction between Blocks() and b.Index().
func produceLabelValues(
	ctx context.Context,
	tsdbDB *tsdb.DB,
	mint, maxt int64,
	name string,
	matchers []*labels.Matcher,
	searchFilter *streaminglabelvalues.FilterChains,
	ch chan<- mimirstorage.SearchResult,
) (annotations.Annotations, error) {
	q, err := tsdbDB.Querier(mint, maxt)
	if err != nil {
		return nil, err
	}
	defer q.Close() //nolint:errcheck

	vals, warnings, err := q.LabelValues(ctx, name, nil, matchers...)
	if err != nil {
		return warnings, err
	}

	var score float64
	var accepted bool
	for _, v := range vals {
		if searchFilter != nil {
			accepted, score = searchFilter.Accept(v)
			if !accepted {
				continue
			}
		}
		select {
		case ch <- mimirstorage.SearchResult{Value: v, Score: score}:
		case <-ctx.Done():
			return warnings, ctx.Err()
		}
	}
	return warnings, nil
}

// buildSearchFilter converts a proto SearchLabelValuesFilter into a FilterChains ready for use.
// Returns nil if sf is nil or has no search terms.
func buildSearchFilter(sf *client.SearchLabelValuesFilter) (*streaminglabelvalues.FilterChains, error) {
	if sf == nil || len(sf.SearchTerms) == 0 {
		return nil, nil
	}
	return streaminglabelvalues.BuildFilterChains(sf.SearchTerms, sf.CaseInsensitive, sf.FuzzAlg, sf.FuzzThreshold)
}
