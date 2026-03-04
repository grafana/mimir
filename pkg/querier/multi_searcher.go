// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"

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

// fanOutSearch fans out a search call to multiple Searchers in parallel.
// Sub-Searchers only receive the Filter from hints; dedup, sort, and limit are
// applied globally here so results are correct across querier boundaries.
func fanOutSearch(
	ctx context.Context,
	hints *mimirstorage.SearchHints,
	searchers []mimirstorage.Searcher,
	call func(context.Context, mimirstorage.Searcher, *mimirstorage.SearchHints) (mimirstorage.SearcherValueSet, error),
) (mimirstorage.SearcherValueSet, error) {
	// Sub-hints carry only the Filter; Compare and Limit are applied globally
	// after deduplication across all sub-Searchers.
	var subHints *mimirstorage.SearchHints
	if hints != nil && hints.Filter != nil {
		subHints = &mimirstorage.SearchHints{Filter: hints.Filter}
	}

	// If we have no sort order to apply we can push the limit out to the edge
	if hints != nil && hints.Limit > 0 && hints.Compare == nil {
		subHints.Limit = hints.Limit
	}

	ctx, cancel := context.WithCancel(ctx)
	outCh := make(chan mimirstorage.FilteredResult, 256)
	stream := &labelSearchStream{ch: outCh, ctx: ctx, cancel: cancel, hints: hints}

	go func() {
		defer close(outCh)

		// sgCh carries per-Searcher results to the dedup goroutine.
		sgCh := make(chan mimirstorage.FilteredResult, 256)

		dedupDone := make(chan struct{})
		go func() {
			defer close(dedupDone)
			sink := newDedupSink(outCh, hints, stream, cancel, ctx)
			for result := range sgCh {
				if !sink.add(result.Value) {
					return
				}
			}
		}()

		g, gCtx := errgroup.WithContext(ctx)
		for _, s := range searchers {
			g.Go(func() error {
				vs, err := call(gCtx, s, subHints)
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
	}()

	return stream, nil
}
