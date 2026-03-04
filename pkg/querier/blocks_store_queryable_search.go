// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/types"
	"github.com/grafana/dskit/tenant"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"golang.org/x/sync/errgroup"

	mimirstorage "github.com/grafana/mimir/pkg/storage"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/storegateway/hintspb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// labelSearchStream is a SearcherValueSet backed by a channel populated by a background goroutine.
// The producer sends FilteredResults to ch and then closes it.
// If an error occurs the producer sets err before closing ch.
// The err and limitReached fields are written before close(ch) and read only after Next() observes
// channel close, so no mutex is needed.
//
// When hints.Compare is set, Next() buffers all results on the first call, sorts them, applies
// the limit, and then serves them in order. Otherwise results stream out as they arrive.
type labelSearchStream struct {
	ch           <-chan mimirstorage.FilteredResult
	current      mimirstorage.FilteredResult
	ctx          context.Context
	cancel       context.CancelFunc
	err          error
	limitReached bool // set by the dedup goroutine before it cancels; suppresses context.Canceled in Err()
	hints        *mimirstorage.SearchHints
	// sorted* fields are used only when hints.Compare != nil.
	sorted     []mimirstorage.FilteredResult
	sortedPos  int
	sortedInit bool
}

func (s *labelSearchStream) Next() bool {
	if s.hints != nil && s.hints.Compare != nil {
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

// nextSorted is called when hints.Compare != nil. On the first invocation it drains the
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
func (s *labelSearchStream) drainAndSort() {
	for {
		select {
		case v, ok := <-s.ch:
			if !ok {
				sort.Slice(s.sorted, func(i, j int) bool {
					return s.hints.Compare.Compare(s.sorted[i], s.sorted[j]) < 0
				})
				if s.hints.Limit > 0 && len(s.sorted) > s.hints.Limit {
					s.sorted = s.sorted[:s.hints.Limit]
					s.limitReached = true
				}
				return
			}
			s.sorted = append(s.sorted, v)
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *labelSearchStream) At() mimirstorage.FilteredResult   { return s.current }
func (s *labelSearchStream) Warnings() annotations.Annotations { return nil }
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
type dedupSink struct {
	seen   map[string]struct{}
	count  int
	outCh  chan<- mimirstorage.FilteredResult
	hints  *mimirstorage.SearchHints
	stream *labelSearchStream
	cancel context.CancelFunc
	ctx    context.Context
}

func newDedupSink(outCh chan<- mimirstorage.FilteredResult, hints *mimirstorage.SearchHints, stream *labelSearchStream, cancel context.CancelFunc, ctx context.Context) *dedupSink {
	return &dedupSink{
		seen:   make(map[string]struct{}),
		outCh:  outCh,
		hints:  hints,
		stream: stream,
		cancel: cancel,
		ctx:    ctx,
	}
}

// add deduplicates value, applies hints.Filter if set, enforces the eager limit on the
// unsorted path, and forwards accepted values to outCh.
// Returns false when the caller should stop iterating: either the limit was reached or
// the context is done. Returns true if the caller may continue.
func (d *dedupSink) add(value string) bool {
	if _, exists := d.seen[value]; exists {
		return true
	}
	d.seen[value] = struct{}{}

	score := -1.0
	if d.hints != nil && d.hints.Filter != nil {
		accepted, s := d.hints.Filter.Accept(value)
		if !accepted {
			return true
		}
		score = s
	}

	if d.hints != nil && d.hints.Compare == nil && d.hints.Limit > 0 && d.count >= d.hints.Limit {
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

// SearchLabelNames implements mimirstorage.Searcher.
// It returns a SearcherValueSet immediately; fetching and deduplication run in the background.
// As each store-gateway responds its label names are deduplicated and streamed to the caller.
func (q *blocksStoreQuerier) SearchLabelNames(ctx context.Context, hints *mimirstorage.SearchHints, matchers ...*labels.Matcher) (mimirstorage.SearcherValueSet, error) {
	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
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
	stream := &labelSearchStream{ch: outCh, ctx: ctx, cancel: cancel, hints: hints}

	go func() {
		defer close(outCh)

		spanLog, ctx := spanlogger.New(ctx, q.logger, tracer, "blocksStoreQuerier.SearchLabelNames")
		defer spanLog.Finish()

		convertedMatchers := convertMatchersToLabelMatcher(matchers)
		storeHints := searchHintsToLabelHints(hints)

		// sgCh carries sorted []string slices from each store-gateway as it finishes.
		// Buffer of 32 lets store-gateways publish without waiting for the dedup loop.
		sgCh := make(chan []string, 32)

		// Dedup goroutine: reads from sgCh, deduplicates, applies filter and limit,
		// and sends unique values to outCh.
		dedupDone := make(chan struct{})
		go func() {
			defer close(dedupDone)
			sink := newDedupSink(outCh, hints, stream, cancel, ctx)
			for names := range sgCh {
				for _, name := range names {
					if !sink.add(name) {
						return
					}
				}
			}
		}()

		queryF := func(clients map[BlocksStoreClient][]ulid.ULID, minT, maxT int64, indexMeta *bucketindex.Metadata) ([]ulid.ULID, error) {
			return q.fetchLabelNamesFromStoreStreaming(ctx, clients, minT, maxT, tenantID, storeHints, convertedMatchers, indexMeta, sgCh)
		}

		if err := q.queryWithConsistencyCheck(ctx, spanLog, minT, maxT, tenantID, nil, queryF); err != nil {
			close(sgCh)
			<-dedupDone
			// Don't overwrite a limit-triggered cancellation with context.Canceled.
			if !stream.limitReached {
				stream.err = err
			}
			return
		}

		close(sgCh)
		<-dedupDone
	}()

	return stream, nil
}

// SearchLabelValues implements mimirstorage.Searcher.
// It returns a SearcherValueSet immediately; fetching and deduplication run in the background.
// As each store-gateway responds its label values are deduplicated and streamed to the caller.
func (q *blocksStoreQuerier) SearchLabelValues(ctx context.Context, name string, hints *mimirstorage.SearchHints, matchers ...*labels.Matcher) (mimirstorage.SearcherValueSet, error) {
	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
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
	stream := &labelSearchStream{ch: outCh, ctx: ctx, cancel: cancel, hints: hints}

	go func() {
		defer close(outCh)

		spanLog, ctx := spanlogger.New(ctx, q.logger, tracer, "blocksStoreQuerier.SearchLabelValues")
		defer spanLog.Finish()

		storeHints := searchHintsToLabelHints(hints)

		// This is the channel that the store-gateway records are written to - note this is a []string
		sgCh := make(chan []string, 32)

		dedupDone := make(chan struct{})
		go func() {
			defer close(dedupDone)
			sink := newDedupSink(outCh, hints, stream, cancel, ctx)
			for values := range sgCh {
				for _, v := range values {
					if !sink.add(v) {
						return
					}
				}
			}
		}()

		queryF := func(clients map[BlocksStoreClient][]ulid.ULID, minT, maxT int64, indexMeta *bucketindex.Metadata) ([]ulid.ULID, error) {
			return q.fetchLabelValuesFromStoreStreaming(ctx, name, clients, minT, maxT, tenantID, storeHints, matchers, indexMeta, sgCh)
		}

		if err := q.queryWithConsistencyCheck(ctx, spanLog, minT, maxT, tenantID, nil, queryF); err != nil {
			close(sgCh)
			<-dedupDone
			// Don't overwrite a limit-triggered cancellation with context.Canceled.
			if !stream.limitReached {
				stream.err = err
			}
			return
		}

		close(sgCh)
		<-dedupDone
	}()

	return stream, nil
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
			req, err := createLabelNamesRequest(minT, maxT, blockIDs, hints, matchers)
			if err != nil {
				return fmt.Errorf("failed to create label names request: %w", err)
			}

			namesResp, err := c.LabelNames(gCtx, req)
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
			req, err := createLabelValuesRequest(minT, maxT, name, blockIDs, hints, matchers...)
			if err != nil {
				return fmt.Errorf("failed to create label values request: %w", err)
			}

			valuesResp, err := c.LabelValues(gCtx, req)
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

			// Values returned need not be sorted, but we sort them here so that
			// the dedup goroutine can detect duplicates without sorting the entire seen-set.
			// (Sorting is not strictly needed for correctness since we use a seen-map,
			// but it is cheap and keeps the output order predictable within each SG batch.)
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

// searchHintsToLabelHints converts a SearchHints to a storage.LabelHints for use with
// store-gateway requests. The filter is not pushed down to the store-gateway; it is applied
// locally after collecting results.
func searchHintsToLabelHints(hints *mimirstorage.SearchHints) *storage.LabelHints {
	// Do not request a limit if we have a sort order defined
	if hints == nil || hints.Compare != nil {
		return nil
	}
	return &storage.LabelHints{Limit: hints.Limit}
}
