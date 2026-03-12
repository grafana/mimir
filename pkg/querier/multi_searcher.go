// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"sync"

	"github.com/cespare/xxhash/v2"
	"github.com/prometheus/prometheus/util/annotations"
	"golang.org/x/sync/errgroup"

	mimirstorage "github.com/grafana/mimir/pkg/storage"
)

// emptySearcherValueSet returns a SearcherValueSet that immediately ends with no results.
func emptySearcherValueSet(ctx context.Context) mimirstorage.SearcherValueSet {
	ctx, cancel := context.WithCancel(ctx)
	ch := make(chan mimirstorage.FilteredResult)
	close(ch)
	return &labelSearchStream{ch: ch, ctx: ctx, cancel: cancel}
}

// fanOutSearch fans out a search call to multiple MimirSearchers in parallel.
//
// When sorting is requested (hints.SortBy != 0), a k-way sorted merge is performed:
// full hints (including SortBy/SortOrder) are pushed to every sub-Searcher so each
// returns pre-sorted results. The querier merges the k sorted streams with a linear-
// scan heap — O(k) memory per step, no global sort buffer.  All sub-streams must be
// open simultaneously, so latency is bounded by the slowest sub-stream.
//
// When no sorting is requested, results arrive in arbitrary order and are fed through
// a dedup goroutine that enforces the limit eagerly.
func fanOutSearch(
	ctx context.Context,
	hints *mimirstorage.MimirSearchHints,
	searchers []mimirstorage.MimirSearcher,
	call func(context.Context, mimirstorage.MimirSearcher, *mimirstorage.MimirSearchHints) (mimirstorage.SearcherValueSet, annotations.Annotations, error),
) (mimirstorage.SearcherValueSet, annotations.Annotations, error) {
	ctx, cancel := context.WithCancel(ctx)
	outCh := make(chan mimirstorage.FilteredResult, 256)
	// compare is always nil: sorted ordering is managed inside the goroutine below
	// (k-way merge for sorted, arbitrary arrival order for unsorted).
	stream := &labelSearchStream{ch: outCh, ctx: ctx, cancel: cancel, hints: hints}

	cmp := comparatorFromHints(hints)

	go func() {
		defer close(outCh)

		var (
			warnsMu  sync.Mutex
			allWarns annotations.Annotations
		)
		addWarns := func(w annotations.Annotations) {
			if w == nil {
				return
			}
			warnsMu.Lock()
			allWarns = allWarns.Merge(w)
			warnsMu.Unlock()
		}

		if cmp != nil {
			kWayMerge(ctx, hints, cmp, outCh, stream, cancel, searchers, call, addWarns)
		} else {
			unsortedFanOut(ctx, hints, outCh, stream, cancel, searchers, call, addWarns)
		}

		// All sub-goroutines have exited by this point; no concurrent access to allWarns.
		if allWarns != nil {
			stream.warnings = stream.warnings.Merge(allWarns)
		}
	}()

	return stream, nil, nil
}

// kWayMerge performs a sorted k-way merge across multiple sub-Searchers.
//
// Each sub-Searcher is called with the full hints (including SortBy/SortOrder) and
// is expected to return results in that sorted order.  The merge starts one goroutine
// per sub-Searcher that advances the iterator and buffers the next value in a channel
// of capacity 1.  The merge loop waits for an initial value from every sub-stream, then
// repeatedly picks the globally best head value, deduplicates, applies the limit, and
// emits to outCh.  Filtering is the sub-Searcher's responsibility.
func kWayMerge(
	ctx context.Context,
	hints *mimirstorage.MimirSearchHints,
	cmp mimirstorage.Comparator,
	outCh chan<- mimirstorage.FilteredResult,
	stream *labelSearchStream,
	cancel context.CancelFunc,
	searchers []mimirstorage.MimirSearcher,
	call func(context.Context, mimirstorage.MimirSearcher, *mimirstorage.MimirSearchHints) (mimirstorage.SearcherValueSet, annotations.Annotations, error),
	addWarns func(annotations.Annotations),
) {
	if len(searchers) == 0 {
		return
	}

	type mergeHead struct {
		ch        <-chan mimirstorage.FilteredResult
		errHolder *error
		current   mimirstorage.FilteredResult
		exhausted bool
	}

	heads := make([]mergeHead, 0, len(searchers))

	// Drain all advancement goroutine channels when we return for any reason,
	// ensuring no goroutine is left blocked trying to send.
	defer func() {
		for i := range heads {
			for range heads[i].ch { //nolint:revive // intentional drain
			}
		}
	}()

	// Start each sub-searcher and its advancement goroutine.
	for _, s := range searchers {
		vs, subWarns, err := call(ctx, s, hints)
		addWarns(subWarns)
		if err != nil {
			if !stream.limitReached {
				stream.err = err
			}
			cancel()
			return // defer drain handles cleanup
		}
		ch := make(chan mimirstorage.FilteredResult, 1)
		e := new(error)
		heads = append(heads, mergeHead{ch: ch, errHolder: e})
		go func() {
			defer vs.Close()
			defer close(ch)
			for vs.Next() {
				select {
				case ch <- vs.At():
				case <-ctx.Done():
					return
				}
			}
			if iterErr := vs.Err(); iterErr != nil {
				*e = iterErr
			}
		}()
	}

	// Load the initial head from each sub-stream (blocks until each produces a value or closes).
	for i := range heads {
		v, ok := <-heads[i].ch
		if ok {
			heads[i].current = v
		} else {
			heads[i].exhausted = true
		}
	}

	limit := 0
	if hints != nil {
		limit = hints.Limit
	}

	seen := make(map[uint64]struct{})
	count := 0

	for {
		// Pick the sub-stream whose current value ranks best according to the comparator.
		best := -1
		for i := range heads {
			if heads[i].exhausted {
				continue
			}
			if best == -1 || cmp.Compare(heads[i].current, heads[best].current) < 0 {
				best = i
			}
		}
		if best == -1 {
			break // all sub-streams exhausted
		}

		res := heads[best].current

		// Dedup.
		h := xxhash.Sum64String(res.Value)
		if _, dup := seen[h]; !dup {
			seen[h] = struct{}{}

			// Limit (applied inline since output is already in sorted order).
			if limit > 0 && count >= limit {
				stream.limitReached = true
				// TODO - I don't think we need to apply this warning here. The has_more will be set in the response
				//stream.warnings = stream.warnings.Add(NewMaxLimitError(0, limit, "search result limit"))
				cancel()
				return // defer drain handles cleanup
			}
			select {
			case outCh <- res:
				count++
			case <-ctx.Done():
				return
			}
		}

		// Advance the chosen sub-stream.
		v, ok := <-heads[best].ch
		if ok {
			heads[best].current = v
		} else {
			heads[best].exhausted = true
		}
	}

	// Propagate the first iteration error seen, if any.
	for i := range heads {
		if *heads[i].errHolder != nil && !stream.limitReached {
			stream.err = *heads[i].errHolder
			return
		}
	}
}

// unsortedFanOut is the fan-out path for queries with no sort order.
// Results from all sub-Searchers are funnelled into a shared channel, deduplicated,
// filtered, and forwarded to outCh. The global limit is enforced eagerly as values arrive.
func unsortedFanOut(
	ctx context.Context,
	hints *mimirstorage.MimirSearchHints,
	outCh chan<- mimirstorage.FilteredResult,
	stream *labelSearchStream,
	cancel context.CancelFunc,
	searchers []mimirstorage.MimirSearcher,
	call func(context.Context, mimirstorage.MimirSearcher, *mimirstorage.MimirSearchHints) (mimirstorage.SearcherValueSet, annotations.Annotations, error),
	addWarns func(annotations.Annotations),
) {
	sgCh := make(chan mimirstorage.FilteredResult, 256)

	dedupDone := make(chan struct{})
	go func() {
		defer close(dedupDone)
		sink := newDedupSink(outCh, hints, nil, stream, cancel, ctx)
		for result := range sgCh {
			if !sink.add(result.Value) {
				return
			}
		}
	}()

	g, gCtx := errgroup.WithContext(ctx)
	for _, s := range searchers {
		g.Go(func() error {
			vs, subWarns, err := call(gCtx, s, hints)
			addWarns(subWarns)
			if err != nil {
				return err
			}
			defer vs.Close()
			for vs.Next() {
				select {
				case sgCh <- vs.At():
				case <-gCtx.Done():
					return gCtx.Err()
				}
			}
			return vs.Err()
		})
	}

	err := g.Wait()
	close(sgCh)
	<-dedupDone

	if err != nil && !stream.limitReached {
		stream.err = err
	}
}
