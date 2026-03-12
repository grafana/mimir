// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/cespare/xxhash/v2"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/types"
	"github.com/grafana/dskit/tenant"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"golang.org/x/sync/errgroup"

	mimirstorage "github.com/grafana/mimir/pkg/storage"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/storegateway/hintspb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/streaminglabelvalues"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// labelSearchStream is a SearcherValueSet backed by a channel populated by a background goroutine.
// The producer sends FilteredResults to ch and then closes it.
// If an error occurs the producer sets err before closing ch.
// The err, warnings, and limitReached fields are written before close(ch) and read only after
// Next() observes channel close, so no mutex is needed.
//
// When compare is set, Next() buffers all results on the first call, sorts them, applies
// the limit, and then serves them in order. Otherwise results stream out as they arrive.
type labelSearchStream struct {
	ch           <-chan mimirstorage.FilteredResult
	current      mimirstorage.FilteredResult
	ctx          context.Context
	cancel       context.CancelFunc
	err          error
	warnings     annotations.Annotations
	limitReached bool // set by the dedup goroutine before it cancels; suppresses context.Canceled in Err()
	hints        *mimirstorage.MimirSearchHints
	compare      mimirstorage.Comparator
	// sorted* fields are used only when compare != nil.
	sorted     []mimirstorage.FilteredResult
	sortedPos  int
	sortedInit bool
}

func (s *labelSearchStream) Next() bool {
	if s.compare != nil {
		return s.nextSorted()
	}
	// Non-blocking check first: prefer already-buffered values over a cancelled context
	// so that all values sent before a limit-triggered cancellation are visible to the caller.
	select {
	case v, ok := <-s.ch:
		if !ok {
			return false
		}
		s.current = v
		return true
	default:
	}
	// Block until a value arrives, the channel is closed, or the context is done.
	select {
	case v, ok := <-s.ch:
		if !ok {
			return false
		}
		s.current = v
		return true
	case <-s.ctx.Done():
		return false
	}
}

// nextSorted is called when compare != nil. On the first invocation it drains the
// channel, sorts all results using the comparator, applies the limit, and then serves
// items sequentially from the sorted slice.
func (s *labelSearchStream) nextSorted() bool {
	if !s.sortedInit {
		s.drainAndSort()
		s.sortedInit = true
	}
	if s.sortedPos >= len(s.sorted) {
		return false
	}
	s.current = s.sorted[s.sortedPos]
	s.sortedPos++
	return true
}

// drainAndSort drains ch into s.sorted, then sorts and limits the slice.
// All items must be collected before sorting so that the comparator can
// determine the correct top-K; the limit is applied by sortAndTruncate.
func (s *labelSearchStream) drainAndSort() {
	for {
		select {
		case v, ok := <-s.ch:
			if !ok {
				s.sortAndTruncate()
				return
			}
			s.sorted = append(s.sorted, v)
		case <-s.ctx.Done():
			s.sortAndTruncate()
			return
		}
	}
}

// sortAndTruncate sorts s.sorted in-place and truncates it to hints.Limit.
func (s *labelSearchStream) sortAndTruncate() {
	if len(s.sorted) == 0 {
		return
	}
	sort.Slice(s.sorted, func(i, j int) bool {
		return s.compare.Compare(s.sorted[i], s.sorted[j]) < 0
	})
	if s.hints != nil && s.hints.Limit > 0 && len(s.sorted) > s.hints.Limit {
		s.sorted = s.sorted[:s.hints.Limit]
		s.limitReached = true
		s.warnings = s.warnings.Add(NewMaxLimitError(0, s.hints.Limit, "search result limit"))
	}
}

func (s *labelSearchStream) At() mimirstorage.FilteredResult   { return s.current }
func (s *labelSearchStream) Warnings() annotations.Annotations { return s.warnings }
func (s *labelSearchStream) Err() error {
	// When the limit was reached the background goroutine cancelled the context itself
	// as a stop signal; that is not a caller-visible error.
	if s.limitReached {
		return nil
	}
	if s.err != nil {
		return s.err
	}
	return s.ctx.Err()
}

// Close cancels the producer goroutine and drains any remaining items so the producer is not blocked.
func (s *labelSearchStream) Close() {
	s.cancel()
	for range s.ch { //nolint:revive // intentional drain
	}
}

// dedupSink deduplicates values sent to an output channel, enforcing an eager limit on the
// unsorted path. It is not goroutine-safe and is intended to run inside a single goroutine.
// seen stores xxhash fingerprints of observed values rather than the full strings, keeping
// per-entry memory at a fixed 8 bytes regardless of label name/value length.
type dedupSink struct {
	seen   map[uint64]struct{}
	count  int
	outCh  chan<- mimirstorage.FilteredResult
	hints  *mimirstorage.MimirSearchHints
	filter *streaminglabelvalues.FilterChains
	stream *labelSearchStream
	cancel context.CancelFunc
	ctx    context.Context
}

// buildSearchFilter builds a FilterChains from hints, or returns nil when no filtering is needed.
func buildSearchFilter(hints *mimirstorage.MimirSearchHints) *streaminglabelvalues.FilterChains {
	if hints == nil || len(hints.Search) == 0 {
		return nil
	}
	return streaminglabelvalues.BuildFilterChains(hints.Search, hints.CaseInsensitive, streaminglabelvalues.Operator(hints.Operator), hints.FuzzThreshold)
}

func newDedupSink(outCh chan<- mimirstorage.FilteredResult, hints *mimirstorage.MimirSearchHints, filter *streaminglabelvalues.FilterChains, stream *labelSearchStream, cancel context.CancelFunc, ctx context.Context) *dedupSink {
	return &dedupSink{
		seen:   make(map[uint64]struct{}),
		outCh:  outCh,
		hints:  hints,
		filter: filter,
		stream: stream,
		cancel: cancel,
		ctx:    ctx,
	}
}

// add deduplicates value, applies the filter if set, enforces the eager limit on the
// unsorted path, and forwards accepted values to outCh.
// Returns false when the caller should stop iterating: either the limit was reached or
// the context is done. Returns true if the caller may continue.
func (d *dedupSink) add(value string) bool {
	h := xxhash.Sum64String(value)
	if _, exists := d.seen[h]; exists {
		return true
	}
	d.seen[h] = struct{}{}

	score := -1.0
	if d.filter != nil {
		accepted, s := d.filter.Accept(value)
		if !accepted {
			return true
		}
		score = s
	}

	if d.hints != nil && d.hints.SortBy == 0 && d.hints.Limit > 0 && d.count >= d.hints.Limit {
		d.stream.limitReached = true
		d.cancel()
		return false
	}

	select {
	case d.outCh <- mimirstorage.FilteredResult{Value: value, Score: score}:
		d.count++
		return true
	case <-d.ctx.Done():
		return false
	}
}

// comparatorFromHints builds a Comparator from the sort fields of a MimirSearchHints.
// Returns nil when no sorting is requested (SortBy == 0).
func comparatorFromHints(h *mimirstorage.MimirSearchHints) mimirstorage.Comparator {
	if h == nil || h.SortBy == 0 {
		return nil
	}
	if h.SortBy == 1 { // Alpha
		if h.SortOrder == 1 { // Desc
			return &streaminglabelvalues.ComparerAlphaDesc{}
		}
		return &streaminglabelvalues.ComparerAlpha{}
	}
	if h.SortBy == 2 { // Score
		return streaminglabelvalues.NewCompareScore(h.Search)
	}
	return nil
}

// SearchLabelNames implements mimirstorage.MimirSearcher.
// It returns a SearcherValueSet immediately; fetching and deduplication run in the background.
// As each store-gateway responds its label names are deduplicated and streamed to the caller.
func (q *blocksStoreQuerier) SearchLabelNames(ctx context.Context, hints *mimirstorage.MimirSearchHints, matchers ...*labels.Matcher) (mimirstorage.SearcherValueSet, annotations.Annotations, error) {
	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, nil, err
	}

	minT, maxT := q.minT, q.maxT

	// Clamp minT synchronously before returning — same semantics as LabelNames.
	// TODO - removed for testing
	//maxQueryLength := q.limits.MaxLabelsQueryLength(tenantID)
	//if maxQueryLength != 0 {
	//	minT = clampMinT(minT, maxT, maxQueryLength.Milliseconds())
	//}

	ctx, cancel := context.WithCancel(ctx)
	outCh := make(chan mimirstorage.FilteredResult, 256)
	// compare is always nil here: sorted output is produced by the collect+merge path below.
	stream := &labelSearchStream{ch: outCh, ctx: ctx, cancel: cancel, hints: hints}

	go func() {
		defer close(outCh)

		spanLog, ctx := spanlogger.New(ctx, q.logger, tracer, "blocksStoreQuerier.SearchLabelNames")
		defer spanLog.Finish()

		convertedMatchers := convertMatchersToLabelMatcher(matchers)
		storeHints := searchHintsToLabelHints(hints)
		sf := mimirHintsToStoreSearchFilter(hints)

		// sgCh carries sorted []string slices from each store-gateway as it finishes.
		// Buffer of 32 lets store-gateways publish without waiting for the downstream goroutine.
		sgCh := make(chan []string, 32)

		queryF := func(clients map[BlocksStoreClient][]ulid.ULID, minT, maxT int64, indexMeta *bucketindex.Metadata) ([]ulid.ULID, error) {
			return q.fetchLabelNamesFromStoreStreaming(ctx, clients, minT, maxT, tenantID, storeHints, convertedMatchers, sf, indexMeta, sgCh)
		}

		if hints != nil && hints.SortBy != 0 {
			// Sorted path: collect all per-gateway sorted slices, merge them, then emit in order.
			var collected [][]string
			collectDone := make(chan struct{})
			go func() {
				defer close(collectDone)
				for names := range sgCh {
					collected = append(collected, names)
				}
			}()

			if err := q.queryWithConsistencyCheck(ctx, spanLog, minT, maxT, tenantID, nil, queryF); err != nil {
				close(sgCh)
				<-collectDone
				if !stream.limitReached {
					stream.err = err
				}
				return
			}
			close(sgCh)
			<-collectDone

			names := mergeAndSort(collected, hints, buildSearchFilter(hints))
			for _, name := range names {
				select {
				case outCh <- mimirstorage.FilteredResult{Value: name, Score: -1}:
				case <-ctx.Done():
					return
				}
			}
		} else {
			// Unsorted path: dedup eagerly as results arrive.
			dedupDone := make(chan struct{})
			go func() {
				defer close(dedupDone)
				sink := newDedupSink(outCh, hints, buildSearchFilter(hints), stream, cancel, ctx)
				for names := range sgCh {
					for _, name := range names {
						if !sink.add(name) {
							return
						}
					}
				}
			}()

			if err := q.queryWithConsistencyCheck(ctx, spanLog, minT, maxT, tenantID, nil, queryF); err != nil {
				close(sgCh)
				<-dedupDone
				if !stream.limitReached {
					stream.err = err
				}
				return
			}
			close(sgCh)
			<-dedupDone
		}
	}()

	return stream, nil, nil
}

// SearchLabelValues implements mimirstorage.MimirSearcher.
// It returns a SearcherValueSet immediately; fetching and deduplication run in the background.
// As each store-gateway responds its label values are deduplicated and streamed to the caller.
func (q *blocksStoreQuerier) SearchLabelValues(ctx context.Context, name string, hints *mimirstorage.MimirSearchHints, matchers ...*labels.Matcher) (mimirstorage.SearcherValueSet, annotations.Annotations, error) {
	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, nil, err
	}

	minT, maxT := q.minT, q.maxT

	// TODO - disable max query length for testing
	//maxQueryLength := q.limits.MaxLabelsQueryLength(tenantID)
	//if maxQueryLength != 0 {
	//	minT = clampMinT(minT, maxT, maxQueryLength.Milliseconds())
	//}

	ctx, cancel := context.WithCancel(ctx)

	// This is where filtered + de-duplicated results are sent
	outCh := make(chan mimirstorage.FilteredResult, 256)
	// compare is always nil here: sorted output is produced by the collect+merge path below.
	stream := &labelSearchStream{ch: outCh, ctx: ctx, cancel: cancel, hints: hints}

	go func() {
		defer close(outCh)

		spanLog, ctx := spanlogger.New(ctx, q.logger, tracer, "blocksStoreQuerier.SearchLabelValues")
		defer spanLog.Finish()

		storeHints := searchHintsToLabelHints(hints)
		sf := mimirHintsToStoreSearchFilter(hints)

		// sgCh carries sorted []string slices from each store-gateway as it finishes.
		// Buffer of 32 lets store-gateways publish without waiting for the downstream goroutine.
		sgCh := make(chan []string, 32)

		queryF := func(clients map[BlocksStoreClient][]ulid.ULID, minT, maxT int64, indexMeta *bucketindex.Metadata) ([]ulid.ULID, error) {
			return q.fetchLabelValuesFromStoreStreaming(ctx, name, clients, minT, maxT, tenantID, storeHints, matchers, sf, indexMeta, sgCh)
		}

		if hints != nil && hints.SortBy != 0 {
			// Sorted path: collect all per-gateway sorted slices, merge them, then emit in order.
			var collected [][]string
			collectDone := make(chan struct{})
			go func() {
				defer close(collectDone)
				for values := range sgCh {
					collected = append(collected, values)
				}
			}()

			if err := q.queryWithConsistencyCheck(ctx, spanLog, minT, maxT, tenantID, nil, queryF); err != nil {
				close(sgCh)
				<-collectDone
				if !stream.limitReached {
					stream.err = err
				}
				return
			}
			close(sgCh)
			<-collectDone

			values := mergeAndSort(collected, hints, buildSearchFilter(hints))
			for _, v := range values {
				select {
				case outCh <- mimirstorage.FilteredResult{Value: v, Score: -1}:
				case <-ctx.Done():
					return
				}
			}
		} else {
			// Unsorted path: dedup eagerly as results arrive.
			dedupDone := make(chan struct{})
			go func() {
				defer close(dedupDone)
				sink := newDedupSink(outCh, hints, buildSearchFilter(hints), stream, cancel, ctx)
				for values := range sgCh {
					for _, v := range values {
						if !sink.add(v) {
							return
						}
					}
				}
			}()

			if err := q.queryWithConsistencyCheck(ctx, spanLog, minT, maxT, tenantID, nil, queryF); err != nil {
				close(sgCh)
				<-dedupDone
				if !stream.limitReached {
					stream.err = err
				}
				return
			}
			close(sgCh)
			<-dedupDone
		}
	}()

	return stream, nil, nil
}

// mimirHintsToStoreSearchFilter converts MimirSearchHints to a storepb.SearchFilter so the
// store-gateway can apply filter and sort server-side before transmitting values over the wire.
// Returns nil when there is nothing useful to send (no search terms and no sort).
func mimirHintsToStoreSearchFilter(h *mimirstorage.MimirSearchHints) *storepb.SearchFilter {
	if h == nil || (len(h.Search) == 0 && h.SortBy == 0) {
		return nil
	}
	op := storepb.OR
	if h.Operator == 1 {
		op = storepb.AND
	}
	return &storepb.SearchFilter{
		SearchTerms:     h.Search,
		CaseInsensitive: h.CaseInsensitive,
		Operator:        op,
		FuzzThreshold:   h.FuzzThreshold,
		SortBy:          storepb.SortBy(h.SortBy),
		SortOrder:       storepb.SortOrder(h.SortOrder),
	}
}

// createSearchLabelNamesRequest builds a SearchLabelNamesRequest scoped to the given block IDs and
// carrying an optional SearchFilter for server-side filtering.
func createSearchLabelNamesRequest(minT, maxT int64, blockIDs []ulid.ULID, hints *storage.LabelHints, matchers []storepb.LabelMatcher, sf *storepb.SearchFilter) *storepb.SearchLabelNamesRequest {
	var limit int64
	if hints != nil && hints.Limit > 0 {
		limit = int64(hints.Limit)
	}
	return &storepb.SearchLabelNamesRequest{
		Start:    minT,
		End:      maxT,
		Matchers: matchers,
		Limit:    limit,
		RequestHints: &storepb.LabelNamesRequestHints{
			BlockMatchers: []storepb.LabelMatcher{
				{
					Type:  storepb.LabelMatcher_RE,
					Name:  block.BlockIDLabel,
					Value: strings.Join(convertULIDsToString(blockIDs), "|"),
				},
			},
		},
		SearchFilter: sf,
	}
}

// createSearchLabelValuesRequest builds a SearchLabelValuesRequest scoped to the given block IDs and
// carrying an optional SearchFilter for server-side filtering.
func createSearchLabelValuesRequest(minT, maxT int64, label string, blockIDs []ulid.ULID, hints *storage.LabelHints, matchers []*labels.Matcher, sf *storepb.SearchFilter) *storepb.SearchLabelValuesRequest {
	var limit int64
	if hints != nil && hints.Limit > 0 {
		limit = int64(hints.Limit)
	}
	return &storepb.SearchLabelValuesRequest{
		Label:    label,
		Start:    minT,
		End:      maxT,
		Matchers: convertMatchersToLabelMatcher(matchers),
		Limit:    limit,
		RequestHints: &storepb.LabelValuesRequestHints{
			BlockMatchers: []storepb.LabelMatcher{
				{
					Type:  storepb.LabelMatcher_RE,
					Name:  block.BlockIDLabel,
					Value: strings.Join(convertULIDsToString(blockIDs), "|"),
				},
			},
		},
		SearchFilter: sf,
	}
}

// fetchLabelNamesFromStoreStreaming fans out label-names requests to the given store-gateway
// clients concurrently. As each client responds its sorted Names slice is sent to resultCh.
// It mirrors fetchLabelNamesFromStore but pushes results eagerly rather than collecting them.
func (q *blocksStoreQuerier) fetchLabelNamesFromStoreStreaming(
	ctx context.Context,
	clients map[BlocksStoreClient][]ulid.ULID,
	minT int64,
	maxT int64,
	tenantID string,
	hints *storage.LabelHints,
	matchers []storepb.LabelMatcher,
	sf *storepb.SearchFilter,
	indexMeta *bucketindex.Metadata,
	resultCh chan<- []string,
) ([]ulid.ULID, error) {
	reqCtx := grpcContextWithBucketStoreRequestMeta(ctx, tenantID, indexMeta)

	var (
		g, gCtx       = errgroup.WithContext(reqCtx)
		mtx           sync.Mutex
		queriedBlocks []ulid.ULID
		spanLog       = spanlogger.FromContext(ctx, q.logger)
	)

	for c, blockIDs := range clients {
		g.Go(func() error {
			req := createSearchLabelNamesRequest(minT, maxT, blockIDs, hints, matchers, sf)

			namesResp, err := c.SearchLabelNames(gCtx, req)
			if err != nil {
				if shouldRetry(err) {
					level.Warn(spanLog).Log("msg", "failed to fetch label names; error is retriable", "remote", c.RemoteAddress(), "err", err)
					return nil
				}
				return fmt.Errorf("non-retriable error while fetching label names from store: %w", err)
			}

			myQueriedBlocks, err := parseLabelNamesQueriedBlocks(namesResp)
			if err != nil {
				return err
			}

			spanLog.DebugLog("msg", "received label names from store-gateway",
				"instance", c,
				"num labels", len(namesResp.Names),
				"requested blocks", strings.Join(convertULIDsToString(blockIDs), " "),
				"queried blocks", strings.Join(convertULIDsToString(myQueriedBlocks), " "))

			// Send this store-gateway's sorted names to the dedup goroutine.
			select {
			case resultCh <- namesResp.Names:
			case <-gCtx.Done():
				return gCtx.Err()
			}

			mtx.Lock()
			queriedBlocks = append(queriedBlocks, myQueriedBlocks...)
			mtx.Unlock()

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return queriedBlocks, nil
}

// fetchLabelValuesFromStoreStreaming fans out label-values requests to the given store-gateway
// clients concurrently. As each client responds its sorted Values slice is sent to resultCh.
// It mirrors fetchLabelValuesFromStore but pushes results eagerly rather than collecting them.
func (q *blocksStoreQuerier) fetchLabelValuesFromStoreStreaming(
	ctx context.Context,
	name string,
	clients map[BlocksStoreClient][]ulid.ULID,
	minT int64,
	maxT int64,
	tenantID string,
	hints *storage.LabelHints,
	matchers []*labels.Matcher,
	sf *storepb.SearchFilter,
	indexMeta *bucketindex.Metadata,
	resultCh chan<- []string,
) ([]ulid.ULID, error) {
	reqCtx := grpcContextWithBucketStoreRequestMeta(ctx, tenantID, indexMeta)

	var (
		g, gCtx       = errgroup.WithContext(reqCtx)
		mtx           sync.Mutex
		queriedBlocks []ulid.ULID
		spanLog       = spanlogger.FromContext(ctx, q.logger)
	)

	for c, blockIDs := range clients {
		g.Go(func() error {
			req := createSearchLabelValuesRequest(minT, maxT, name, blockIDs, hints, matchers, sf)

			valuesResp, err := c.SearchLabelValues(gCtx, req)
			if err != nil {
				if shouldRetry(err) {
					level.Warn(spanLog).Log("msg", "failed to fetch label values; error is retriable", "remote", c.RemoteAddress(), "err", err)
					return nil
				}
				return fmt.Errorf("non-retriable error while fetching label values from store: %w", err)
			}

			myQueriedBlocks, err := parseLabelValuesQueriedBlocks(valuesResp)
			if err != nil {
				return err
			}

			spanLog.DebugLog("msg", "received label values from store-gateway",
				"instance", c.RemoteAddress(),
				"num values", len(valuesResp.Values),
				"requested blocks", strings.Join(convertULIDsToString(blockIDs), " "),
				"queried blocks", strings.Join(convertULIDsToString(myQueriedBlocks), " "))

			select {
			case resultCh <- valuesResp.Values:
			case <-gCtx.Done():
				return gCtx.Err()
			}

			mtx.Lock()
			queriedBlocks = append(queriedBlocks, myQueriedBlocks...)
			mtx.Unlock()

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return queriedBlocks, nil
}

// parseLabelNamesQueriedBlocks extracts the queried block IDs from a LabelNamesResponse,
// supporting both the current non-opaque ResponseHints and the legacy opaque Hints field.
func parseLabelNamesQueriedBlocks(resp *storepb.LabelNamesResponse) ([]ulid.ULID, error) {
	if resp.ResponseHints != nil {
		return convertBlockHintsToULIDs(resp.ResponseHints.QueriedBlocks)
	}
	if resp.Hints != nil { //nolint:staticcheck // Ignore SA1019. This use will be removed in Mimir 3.2
		resHints := hintspb.LabelNamesResponseHints{}
		//nolint:staticcheck // Ignore SA1019. This use will be removed in Mimir 3.2
		if err := types.UnmarshalAny(resp.Hints, &resHints); err != nil {
			return nil, fmt.Errorf("failed to unmarshal label names response hints: %w", err)
		}
		return convertBlockHintsToULIDsOpaque(resHints.QueriedBlocks)
	}
	return nil, nil
}

// parseLabelValuesQueriedBlocks extracts the queried block IDs from a LabelValuesResponse,
// supporting both the current non-opaque ResponseHints and the legacy opaque Hints field.
func parseLabelValuesQueriedBlocks(resp *storepb.LabelValuesResponse) ([]ulid.ULID, error) {
	if resp.ResponseHints != nil {
		return convertBlockHintsToULIDs(resp.ResponseHints.QueriedBlocks)
	}
	if resp.Hints != nil { //nolint:staticcheck // Ignore SA1019. This use will be removed in Mimir 3.2
		resHints := hintspb.LabelValuesResponseHints{}
		//nolint:staticcheck // Ignore SA1019. This use will be removed in Mimir 3.2
		if err := types.UnmarshalAny(resp.Hints, &resHints); err != nil {
			return nil, fmt.Errorf("failed to unmarshal label values response hints: %w", err)
		}
		return convertBlockHintsToULIDsOpaque(resHints.QueriedBlocks)
	}
	return nil, nil
}

// clampMinT returns minT clamped so the query window does not exceed maxQueryLength milliseconds.
// This is a pure function equivalent of clampToMaxLabelQueryLength, used here to avoid requiring
// a spanLogger before the background goroutine starts.
func clampMinT(minT, maxT, maxQueryLength int64) int64 {
	if maxQueryLength > 0 {
		clamped := maxT - maxQueryLength
		if clamped > minT {
			return clamped
		}
	}
	return minT
}

// mergeAndSort combines sorted slices from multiple store-gateways and applies the
// requested sort order. For alpha sort it performs a k-way sorted merge (util.MergeSlices);
// for score sort it deduplicates across all slices and re-scores using MergeSlicesAndSortByScore.
// The limit is applied after sorting.
func mergeAndSort(collected [][]string, hints *mimirstorage.MimirSearchHints, filter *streaminglabelvalues.FilterChains) []string {
	var result []string
	if hints.SortBy == 2 { // score
		result = streaminglabelvalues.MergeSlicesAndSortByScore(collected, filter, hints.SortOrder != 0)
	} else {
		// alpha (SortBy == 1): each slice is already alpha-sorted; k-way merge preserves order.
		result = util.MergeSlices(collected...)
		if hints.SortOrder == 1 { // desc
			slices.Reverse(result)
		}
	}
	if hints.Limit > 0 && len(result) > hints.Limit {
		result = result[:hints.Limit]
	}
	return result
}

// searchHintsToLabelHints converts a MimirSearchHints to a storage.LabelHints for use with
// store-gateway requests. No limit is pushed when sorting is requested, because all results
// must be returned to apply the global sort correctly.
func searchHintsToLabelHints(hints *mimirstorage.MimirSearchHints) *storage.LabelHints {
	if hints == nil || hints.SortBy != 0 {
		return nil
	}
	return &storage.LabelHints{Limit: hints.Limit}
}
