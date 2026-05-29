// SPDX-License-Identifier: AGPL-3.0-only

// Architecture: see docs/internal/streaming-label-value-search/README.md.

package ingester

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/streaminglabelvalues"
)

// searchBatchSize bounds the number of results emitted per SearchResultBatch
// stream send. A small number keeps memory bounded and surfaces results to
// the client incrementally; a larger number reduces gRPC framing overhead.
const searchBatchSize = 256

// SearchLabelNames streams label names matching the search filter.
func (i *Ingester) SearchLabelNames(req *client.SearchLabelNamesRequest, stream client.Ingester_SearchLabelNamesServer) (err error) {
	// Validate user input ahead of the deferred read-error mapper so request
	// errors surface as codes.InvalidArgument rather than being re-tagged as
	// codes.Internal — matching the store-gateway side (bucket_search.go).
	hints, matchers, err := buildSearchHints(req.Filter, req.Ordering, req.Limit, req.Matchers)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	defer func() { err = i.mapReadErrorToErrorWithStatus(err) }()

	ctx := stream.Context()
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return err
	}
	if err := i.enforceReadConsistency(ctx, userID); err != nil {
		return err
	}

	db := i.getTSDB(userID)
	if db == nil {
		return nil
	}
	q, err := db.Querier(req.StartTimestampMs, req.EndTimestampMs)
	if err != nil {
		return fmt.Errorf("open querier: %w", err)
	}
	defer q.Close()
	searcher, ok := q.(storage.Searcher)
	if !ok {
		return fmt.Errorf("ingester querier does not implement storage.Searcher")
	}

	rs := searcher.SearchLabelNames(ctx, hints, matchers...)
	defer rs.Close()
	return streamSearchResults(ctx, rs, stream.Send, nil)
}

// SearchLabelValues streams label values for req.Name matching the search filter.
func (i *Ingester) SearchLabelValues(req *client.SearchLabelValuesRequest, stream client.Ingester_SearchLabelValuesServer) (err error) {
	// See SearchLabelNames for why validation runs ahead of the deferred mapper.
	hints, matchers, err := buildSearchHints(req.Filter, req.Ordering, req.Limit, req.Matchers)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	defer func() { err = i.mapReadErrorToErrorWithStatus(err) }()

	ctx := stream.Context()
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return err
	}
	if err := i.enforceReadConsistency(ctx, userID); err != nil {
		return err
	}

	db := i.getTSDB(userID)
	if db == nil {
		return nil
	}
	q, err := db.Querier(req.StartTimestampMs, req.EndTimestampMs)
	if err != nil {
		return fmt.Errorf("open querier: %w", err)
	}
	defer q.Close()
	searcher, ok := q.(storage.Searcher)
	if !ok {
		return fmt.Errorf("ingester querier does not implement storage.Searcher")
	}

	rs := searcher.SearchLabelValues(ctx, req.Name, hints, matchers...)
	defer rs.Close()
	return streamSearchResults(ctx, rs, stream.Send, i.newMetadataBatchDecoratorFunc(userID, req))
}

// newMetadataBatchDecoratorFunc returns a metadataBatchDecoratorFunc, or
// nil when enrichment is not applicable.
//
// A decorator is returned only when (1) the request asked for
// include_metadata=true, (2) the requested label is __name__ (only metric
// names carry MetricMetadata), and (3) the tenant has any recorded
// metadata. Returning nil in every other case lets streamSearchResults
// skip the per-batch decoration step entirely.
//
// Snapshot semantics: the *userMetricsMetadata pointer is captured once at
// decorator construction and re-used for every batch. Per-batch reads run
// under mm.mtx.RLock(), so a concurrent deleteUserMetadata for the same
// tenant leaves the decorator holding a detached-but-race-safe snapshot
// for the remainder of the RPC. Acceptable because RPC lifetimes are
// bounded, the caller is the tenant whose data is being served, and the
// alternative (re-acquire per batch) trades a negligible cost for live
// state we don't otherwise need here.
func (i *Ingester) newMetadataBatchDecoratorFunc(userID string, req *client.SearchLabelValuesRequest) metadataBatchDecoratorFunc {
	if req == nil || !req.IncludeMetadata {
		return nil
	}
	if req.Name != model.MetricNameLabel {
		return nil
	}
	mm := i.getUserMetadata(userID)
	if mm == nil {
		return nil
	}

	return func(batch *client.SearchResultBatch) {
		if batch == nil || len(batch.Results) == 0 {
			return
		}
		mm.mtx.RLock()
		defer mm.mtx.RUnlock()
		for idx := range batch.Results {
			set, ok := mm.metricToMetadata[batch.Results[idx].Value]
			if !ok || len(set) == 0 {
				continue
			}
			// Note - although we return the most recent record on this ingester, it is possible that
			// during a k-way merge in the ingester fan-out an older ingester record may win.
			// There is no guarantee that the most recent metadata record across all ingesters is returned.
			var (
				bestM mimirpb.MetricMetadata
				bestT time.Time
				found bool
			)
			for m, t := range set {
				if !found || t.After(bestT) {
					bestM, bestT, found = m, t, true
				}
			}
			if found {
				// Defensive copies: Help/Unit are owned by the long-lived
				// metadata store, so we clone them to keep the wire batch
				// independent of any later mutation of the source map.
				batch.Results[idx].Metadata = &mimirpb.MetricMetadata{
					Type: bestM.Type,
					Help: strings.Clone(bestM.Help),
					Unit: strings.Clone(bestM.Unit),
				}
			}
		}
	}
}

// buildSearchHints constructs storage.SearchHints from the wire request.
// limit follows the Prometheus PR #18573 convention: 0 means no limit;
// negatives are rejected as malformed input (the querier never sends them);
// values above math.MaxInt are clamped to math.MaxInt so the int(limit)
// cast cannot wrap on 32-bit builds.
func buildSearchHints(wf *client.SearchFilter, ord client.SearchOrdering, limit int64, wireMatchers []*client.LabelMatcher) (*storage.SearchHints, []*labels.Matcher, error) {
	matchers, err := client.FromLabelMatchers(wireMatchers)
	if err != nil {
		return nil, nil, err
	}
	params, err := protoToParams(wf)
	if err != nil {
		return nil, nil, err
	}
	filter, err := streaminglabelvalues.BuildFilter(params)
	if err != nil {
		return nil, nil, err
	}
	if limit < 0 {
		return nil, nil, fmt.Errorf("limit must be >= 0, got %d", limit)
	}
	hintsLimit := int(limit)
	if limit > int64(math.MaxInt) {
		hintsLimit = math.MaxInt
	}
	hints := &storage.SearchHints{
		Filter:  filter,
		OrderBy: protoToOrdering(ord),
		Limit:   hintsLimit,
	}
	return hints, matchers, nil
}

// protoToParams converts a wire SearchFilter into a validated
// streaminglabelvalues.Params via NewParams. A nil input returns (nil, nil).
func protoToParams(wf *client.SearchFilter) (*streaminglabelvalues.Params, error) {
	if wf == nil {
		return nil, nil
	}
	alg := streaminglabelvalues.FuzzAlgSubsequence
	if wf.FuzzAlg == client.FUZZ_ALG_JARO_WINKLER {
		alg = streaminglabelvalues.FuzzAlgJaroWinkler
	}
	return streaminglabelvalues.NewParams(wf.Terms, !wf.CaseInsensitive, alg, int(wf.FuzzThreshold))
}

// protoToOrdering maps the wire SearchOrdering enum onto the Prometheus
// storage.Ordering enum.
func protoToOrdering(o client.SearchOrdering) storage.Ordering {
	switch o {
	case client.ORDER_BY_VALUE_DESC:
		return storage.OrderByValueDesc
	case client.ORDER_BY_SCORE_DESC:
		return storage.OrderByScoreDesc
	default:
		return storage.OrderByValueAsc
	}
}

// searchResultSender is the minimal interface satisfied by both
// Ingester_SearchLabelNamesServer and Ingester_SearchLabelValuesServer.
type searchResultSender func(*client.SearchResultBatch) error

// metadataBatchDecoratorFunc enriches a wire batch in place with metric
// metadata. Called at most once per outgoing batch so the implementation
// can amortise lock acquisition across all results in the batch.
type metadataBatchDecoratorFunc func(batch *client.SearchResultBatch)

// streamSearchResults reads from rs in batches of searchBatchSize and emits
// each batch via send. Any warnings accumulated by rs are attached to the
// final batch (or sent alone if no results were produced). Returns rs.Err()
// at termination. The stream context is checked before iteration starts and
// at each batch boundary so a cancelled client stops the loop promptly even
// when rs.Next() does not yet honour context cancellation itself.
//
// When decorate is non-nil it is invoked exactly once per outgoing batch
// with len(batch.Results) > 0, just before send. Warnings-only batches do
// not invoke the decorator — there's nothing to enrich.
func streamSearchResults(ctx context.Context, rs storage.SearchResultSet, send searchResultSender, decorate metadataBatchDecoratorFunc) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	batch := &client.SearchResultBatch{Results: make([]client.SearchResultBatch_Result, 0, searchBatchSize)}
	for rs.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		v := rs.At()
		batch.Results = append(batch.Results, client.SearchResultBatch_Result{Value: v.Value, Score: v.Score})
		if len(batch.Results) >= searchBatchSize {
			if decorate != nil {
				decorate(batch)
			}
			if err := send(batch); err != nil {
				return err
			}
			// stream.Send is synchronous (gogoproto marshals before
			// returning) so the backing array can be reset in place.
			batch.Results = batch.Results[:0]
		}
	}
	if err := rs.Err(); err != nil {
		return err
	}
	batch.Warnings = warningsToStrings(rs.Warnings())
	if len(batch.Results) > 0 || len(batch.Warnings) > 0 {
		if decorate != nil && len(batch.Results) > 0 {
			decorate(batch)
		}
		if err := send(batch); err != nil {
			return err
		}
	}
	return nil
}

// warningsToStrings flattens annotations into a string slice for wire transport.
// Returns nil for empty input so the proto field is omitted on the wire.
func warningsToStrings(a annotations.Annotations) []string {
	if len(a) == 0 {
		return nil
	}
	out := make([]string, 0, len(a))
	for _, w := range a {
		out = append(out, w.Error())
	}
	return out
}
