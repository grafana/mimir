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
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/streaminglabelvalues"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

const (
	// the size of the internal channel where we pass labels/values from the producer to consumer side. We keep this small to avoid potentially needing to deal with memory growth in this buffer
	internalChannelSize = 100
	// the size of the batches that we stream back to the querier. // TODO - what is a good sane value here? Should these become set via config?
	streamingBatchSize = 1024
	searchLabelValues  = "Ingester.SearchLabelValues"
	searchLabelNames   = "Ingester.SearchLabelNames"
)

// streamingLabelValuesReader is satisfied by *headIndexReader and *index.Reader,
// which expose LabelValuesFor for lazy iteration over label values.
// Neither type includes this method on the public tsdb.IndexReader interface.
type streamingLabelValuesReader interface {
	LabelValuesFor(postings index.Postings, name string) storage.LabelValues
}

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
			spanlog.SetSpanAndLogTag("operator", filter.Operator)
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
	// TODO - note these are -querier.label-names-and-values-results-max-size-bytes - so we need to update something to allow this in the ingester
	maxBytesLimit := i.limits.LabelNamesAndValuesResultsMaxSizeBytes(userID)

	// extract the relevant params from the search request
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

	searchFilter := buildSearchFilter(req.Filter)

	var produce func(ch chan<- string) (annotations.Annotations, error)
	switch method {
	case searchLabelValues:
		// Label values are obtained from the TSDB head and then any TSDB files
		// We provide a channel so that we can enforce flow control between the reading of the TSDB files and
		// the batching/streaming of results back to the querier.
		// This avoids the need to accumulate all results in memory before streaming.
		produce = func(ch chan<- string) (annotations.Annotations, error) {
			return produceLabelValues(ctx, db.db, mint, maxt, labelName, matchers, searchFilter, ch)
		}
	case searchLabelNames:
		q, err := db.Querier(mint, maxt)
		if err != nil {
			level.Error(spanlog).Log("msg", "unable to access Querier", "err", err)
			return err
		}
		// TODO - once we have the prometheus side changes, this production of label names should become streaming compatible.
		produce = func(ch chan<- string) (annotations.Annotations, error) {
			return produceLabelNames(ctx, q, hints, matchers, searchFilter, ch)
		}
	default:
		return nil
	}

	limit := int(req.Limit)
	bufSize := streamingBatchSize
	if limit > 0 {
		bufSize = min(limit, streamingBatchSize)
	}

	batch := make([]*client.SearchLabelValuesResult, bufSize)
	for k := range batch {
		batch[k] = &client.SearchLabelValuesResult{}
	}

	iter := newingesterSearcherValueSet(produce, req.Filter, limit, maxBytesLimit)
	defer iter.Close()

	batchIdx := 0
	for iter.Next() {
		*batch[batchIdx] = iter.At()
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
	hints *storage.LabelHints,
	matchers []*labels.Matcher,
	searchFilter *streaminglabelvalues.FilterChains,
	ch chan<- string,
) (annotations.Annotations, error) {
	defer q.Close() //nolint:errcheck
	// TODO: TSDB has no streaming LabelNames iterator (unlike LabelValuesFor).
	// To avoid buffering all label names, Prometheus would need to add
	// LabelNamesFor(postings index.Postings) storage.LabelValues to its IndexReader interface.
	vals, warnings, err := q.LabelNames(ctx, hints, matchers...)
	if err != nil {
		return warnings, err
	}
	vals = streaminglabelvalues.ApplyFilterChains(vals, searchFilter)

	for _, v := range vals {
		select {
		case ch <- v:
		case <-ctx.Done():
			return warnings, ctx.Err()
		}
	}
	return warnings, nil
}

// produceLabelValues reads label values from the TSDB head and all overlapping blocks,
// filtering via searchFilter, and sends each accepted value to ch.
// The caller must drain ch even if an error is returned.
func produceLabelValues(
	ctx context.Context,
	tsdbDB *tsdb.DB,
	mint, maxt int64,
	name string,
	matchers []*labels.Matcher,
	searchFilter *streaminglabelvalues.FilterChains,
	ch chan<- string,
) (annotations.Annotations, error) {
	// Head covers in-memory (recent) series; skip if its time range doesn't overlap [mint, maxt].
	head := tsdbDB.Head()
	if head.MaxTime() >= mint && head.MinTime() <= maxt {
		headIR, err := head.Index()
		if err != nil {
			return nil, err
		}
		if err = produceLabelValuesFromReader(ctx, headIR, name, matchers, searchFilter, ch); err != nil {
			return nil, err
		}
	}

	// Persisted blocks overlapping [mint, maxt].
	for _, b := range tsdbDB.Blocks() {
		if b.MaxTime() < mint || b.MinTime() > maxt {
			continue
		}
		ir, err := b.Index()
		if err != nil {
			return nil, err
		}
		if err = produceLabelValuesFromReader(ctx, ir, name, matchers, searchFilter, ch); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

// produceLabelValuesFromReader reads filtered label values from a single IndexReader and
// sends each to ch. Sends are cancelled via ctx. The reader is closed before returning.
func produceLabelValuesFromReader(
	ctx context.Context,
	r tsdb.IndexReader,
	name string,
	matchers []*labels.Matcher,
	searchFilter *streaminglabelvalues.FilterChains,
	ch chan<- string,
) error {
	defer r.Close() //nolint:errcheck

	var (
		postings index.Postings
		err      error
	)
	if len(matchers) == 0 {
		// PostingsForMatchers with no matchers returns EmptyPostings (Intersect of nothing).
		// Use the AllPostings key instead to get every series.
		k, v := index.AllPostingsKey()
		postings, err = r.Postings(ctx, k, v)
	} else {
		postings, err = r.PostingsForMatchers(ctx, false, matchers...)
	}
	if err != nil {
		return err
	}

	send := func(v string) error {
		select {
		case ch <- v:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if sr, ok := r.(streamingLabelValuesReader); ok {
		iter := sr.LabelValuesFor(postings, name)
		defer iter.Close() //nolint:errcheck
		for iter.Next() {
			v := iter.At()
			if searchFilter != nil {
				if accepted, _ := searchFilter.Accept(v); !accepted {
					continue
				}
			}
			if err := send(v); err != nil {
				return err
			}
		}
		return iter.Err()
	}

	// Fallback for any IndexReader that doesn't implement LabelValuesFor - not expected to happen
	// TODO - hopefully this can be replaced to avoid allocating all these values
	vals, err := r.LabelValues(ctx, name, nil, matchers...)
	if err != nil {
		return err
	}
	vals = streaminglabelvalues.ApplyFilterChains(vals, searchFilter)

	for _, v := range vals {
		if err := send(v); err != nil {
			return err
		}
	}
	return nil
}

// buildSearchFilter converts a proto SearchLabelValuesFilter into a FilterChains ready for use.
// Returns nil if sf is nil or has no search terms.
func buildSearchFilter(sf *client.SearchLabelValuesFilter) *streaminglabelvalues.FilterChains {
	if sf == nil || len(sf.SearchTerms) == 0 {
		return nil
	}
	op := streaminglabelvalues.Or
	if sf.Operator == client.AND {
		op = streaminglabelvalues.And
	}
	return streaminglabelvalues.BuildFilterChains(sf.SearchTerms, sf.CaseInsensitive, op, sf.FuzzThreshold)
}
