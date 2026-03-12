// SPDX-License-Identifier: AGPL-3.0-only

package storage

import (
	"context"

	"github.com/cespare/xxhash/v2"
	"golang.org/x/sync/errgroup"
)

// KWayMergeValueSets performs a sorted k-way merge over pre-opened SearcherValueSet iterators.
// Each iterator must already return results in the order defined by cmp.
// Deduplicates by xxhash. Emits merged results to outCh.
// Sets *limitReached = true and cancels ctx when hints.Limit is reached.
// Returns the first iteration error encountered, if any.
func KWayMergeValueSets(
	ctx context.Context,
	cancel context.CancelFunc,
	iters []SearcherValueSet,
	hints *MimirSearchHints,
	cmp Comparator,
	outCh chan<- FilteredResult,
	limitReached *bool,
) error {
	if len(iters) == 0 {
		return nil
	}

	type mergeHead struct {
		ch        <-chan FilteredResult
		errHolder *error
		current   FilteredResult
		exhausted bool
	}

	heads := make([]mergeHead, 0, len(iters))

	// Drain all advancement goroutine channels on return to unblock goroutines.
	defer func() {
		for i := range heads {
			for range heads[i].ch { //nolint:revive // intentional drain
			}
		}
	}()

	// Start one goroutine per iterator.
	for _, vs := range iters {
		ch := make(chan FilteredResult, 1)
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

	// Load the initial head from each sub-stream.
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
			break
		}

		res := heads[best].current
		h := xxhash.Sum64String(res.Value)
		if _, dup := seen[h]; !dup {
			seen[h] = struct{}{}
			if limit > 0 && count >= limit {
				*limitReached = true
				cancel()
				return nil
			}
			select {
			case outCh <- res:
				count++
			case <-ctx.Done():
				return nil
			}
		}

		v, ok := <-heads[best].ch
		if ok {
			heads[best].current = v
		} else {
			heads[best].exhausted = true
		}
	}

	for i := range heads {
		if *heads[i].errHolder != nil {
			return *heads[i].errHolder
		}
	}
	return nil
}

// UnsortedDedupValueSets fans out all iterators concurrently, deduplicates results
// by xxhash, and enforces hints.Limit (if set). Writes to outCh until all iterators
// are exhausted or ctx is cancelled.
func UnsortedDedupValueSets(
	ctx context.Context,
	cancel context.CancelFunc,
	iters []SearcherValueSet,
	hints *MimirSearchHints,
	outCh chan<- FilteredResult,
	limitReached *bool,
) error {
	if len(iters) == 0 {
		return nil
	}

	limit := 0
	if hints != nil {
		limit = hints.Limit
	}

	rawCh := make(chan FilteredResult, 256)

	dedupDone := make(chan error, 1)
	go func() {
		seen := make(map[uint64]struct{})
		count := 0
		for res := range rawCh {
			h := xxhash.Sum64String(res.Value)
			if _, dup := seen[h]; dup {
				continue
			}
			seen[h] = struct{}{}
			if limit > 0 && count >= limit {
				*limitReached = true
				cancel()
				dedupDone <- nil
				return
			}
			select {
			case outCh <- res:
				count++
			case <-ctx.Done():
				dedupDone <- nil
				return
			}
		}
		dedupDone <- nil
	}()

	g, gCtx := errgroup.WithContext(ctx)
	for _, vs := range iters {
		g.Go(func() error {
			defer vs.Close()
			for vs.Next() {
				select {
				case rawCh <- vs.At():
				case <-gCtx.Done():
					return gCtx.Err()
				}
			}
			return vs.Err()
		})
	}

	err := g.Wait()
	close(rawCh)
	<-dedupDone

	if err != nil && !*limitReached {
		return err
	}
	return nil
}
