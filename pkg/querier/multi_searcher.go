// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"

	"github.com/prometheus/prometheus/util/annotations"

	mimirstorage "github.com/grafana/mimir/pkg/storage"
)

// labelSearchStream is a SearchResultSet backed by a channel populated by a background goroutine.
// The producer sends FilteredResults to ch and then closes it.
// If an error occurs the producer sets err before closing ch.
// The err, warnings, and limitReached fields are written before close(ch) and read only after
// Next() observes channel close, so no mutex is needed.
type labelSearchStream struct {
	ch           <-chan mimirstorage.SearchResult
	current      mimirstorage.SearchResult
	ctx          context.Context
	cancel       context.CancelFunc
	err          error
	warnings     annotations.Annotations
	limitReached bool // set by the merge goroutine before it cancels; suppresses context.Canceled in Err()
}

// LimitReached reports whether the result limit was reached.
func (s *labelSearchStream) LimitReached() bool { return s.limitReached }

func (s *labelSearchStream) Next() bool {
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

func (s *labelSearchStream) At() mimirstorage.SearchResult     { return s.current }
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
func (s *labelSearchStream) Close() error {
	s.cancel()
	for range s.ch { //nolint:revive // intentional drain
	}
	return nil
}

// emptySearcherValueSet returns a SearchResultSet that immediately ends with no results.
func emptySearcherValueSet(ctx context.Context) mimirstorage.SearchResultSet {
	ctx, cancel := context.WithCancel(ctx)
	// Cancel immediately: there is no background goroutine, so we do not need the
	// derived context to remain live. Calling cancel() here avoids a context leak
	// if the caller never calls Close() on the returned set.
	cancel()
	ch := make(chan mimirstorage.SearchResult)
	close(ch)
	return &labelSearchStream{ch: ch, ctx: ctx, cancel: cancel}
}

// fanOutSearch fans out a search call to multiple MimirSearchers in parallel.
//
// When sorting is requested (hints.SortBy != 0), a k-way sorted merge is performed:
// full hints (including SortBy/SortOrder) are pushed to every sub-Searcher so each
// returns pre-sorted results. The merge delegates to mimirstorage.KWayMergeValueSets
// which performs an O(k) per-step heap merge — no global sort buffer.
//
// When no sorting is requested, mimirstorage.UnsortedDedupValueSets fans out all
// sub-Searchers concurrently, deduplicates by hash, and enforces the limit eagerly.
func fanOutSearch(
	ctx context.Context,
	hints *mimirstorage.MimirSearchHints,
	searchers []mimirstorage.MimirSearcher,
	call func(context.Context, mimirstorage.MimirSearcher, *mimirstorage.MimirSearchHints) (mimirstorage.SearchResultSet, annotations.Annotations),
) (mimirstorage.SearchResultSet, annotations.Annotations) {
	ctx, cancel := context.WithCancel(ctx)
	outCh := make(chan mimirstorage.SearchResult, 256)
	stream := &labelSearchStream{ch: outCh, ctx: ctx, cancel: cancel}

	cmp := hints.Comparator()

	go func() {
		defer close(outCh)

		var allWarns annotations.Annotations

		// Open all sub-streams sequentially before starting the merge. Sequential
		// opening lets us collect call-time warnings (e.g. sub-RPC errors that are
		// surfaced as annotations rather than Go errors) without additional
		// synchronisation. Each sub-Searcher RPC is assumed to be fast to initiate
		// even if the underlying stream is lazy.
		iters := make([]mimirstorage.SearchResultSet, 0, len(searchers))
		for _, s := range searchers {
			vs, subWarns := call(ctx, s, hints)
			allWarns = allWarns.Merge(subWarns)
			iters = append(iters, vs)
		}

		var mergeErr error
		if cmp != nil {
			// KWayMergeValueSets requires that every iterator in iters already yields
			// items in the order described by cmp — i.e. sub-Searchers must have been
			// called with the same SortBy/SortOrder hints. fanOutSearch passes the full
			// hints (including sort fields) to every sub-Searcher above.
			mergeErr = mimirstorage.KWayMergeValueSets(ctx, cancel, iters, hints, cmp, outCh, &stream.limitReached)
		} else {
			mergeErr = mimirstorage.UnsortedDedupValueSets(ctx, cancel, iters, hints, outCh, &stream.limitReached)
		}
		if mergeErr != nil && !stream.limitReached {
			stream.err = mergeErr
		}

		// All sub-goroutines have exited by this point; no concurrent access to allWarns.
		// Note: warnings collected from sub-Searcher call sites (above) are merged here,
		// but per-item warnings emitted by individual sub-streams during iteration are
		// not yet plumbed through (see TODO in streamResults).
		if allWarns != nil {
			stream.warnings = stream.warnings.Merge(allWarns)
		}
	}()

	return stream, nil
}
