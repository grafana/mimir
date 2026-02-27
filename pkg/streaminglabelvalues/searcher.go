package streaminglabelvalues

import (
	"context"

	"github.com/prometheus/prometheus/model/labels"
	storage2 "github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/storage"
)

// StreamingSearch wraps a LabelQuerier and implements storage.Searcher using a
// producer goroutine that streams results through a channel.
type StreamingSearch struct {
	querier storage2.LabelQuerier
}

func NewStreamingSearch(querier storage2.LabelQuerier) *StreamingSearch {
	return &StreamingSearch{querier: querier}
}

// chanSearcherValueSet is a storage.SearcherValueSet backed by a channel produced
// by a background goroutine. Callers must call Close() to release resources.
type chanSearcherValueSet struct {
	ch      <-chan storage.FilteredResult
	current storage.FilteredResult
	ctx     context.Context
	cancel  context.CancelFunc
	// err holds a non-context error from the producer (e.g. a query failure). It is
	// written before close(ch) and read only after Next() returns false via channel
	// close, so no mutex is required. Context cancellation errors are not stored here;
	// Err() derives them from ctx.Err() directly to avoid a data race.
	err error
}

func (c *chanSearcherValueSet) Next() bool {
	select {
	case v, ok := <-c.ch:
		if !ok {
			return false
		}
		c.current = v
		return true
	case <-c.ctx.Done():
		return false
	}
}

func (c *chanSearcherValueSet) At() storage.FilteredResult { return c.current }

func (c *chanSearcherValueSet) Warnings() annotations.Annotations { return nil }

// Err returns any error set by the producer, or the context error if the context
// was cancelled. Callers should check Err() after Next() returns false.
func (c *chanSearcherValueSet) Err() error {
	if c.err != nil {
		return c.err
	}
	return c.ctx.Err()
}

// Close cancels the producer goroutine and drains any remaining channel items so
// that the producer is not stuck trying to send.
func (c *chanSearcherValueSet) Close() {
	c.cancel()
	for range c.ch { //nolint:revive // intentional drain
	}
}

// SearchLabelNames starts a producer goroutine that retrieves label names from the
// underlying querier, applies hints.Filter (if set), and streams accepted names
// through a buffered channel. The returned SearcherValueSet reads from that channel.
func (s *StreamingSearch) SearchLabelNames(ctx context.Context, hints *storage.SearchHints, matchers ...*labels.Matcher) (storage.SearcherValueSet, error) {
	ctx, cancel := context.WithCancel(ctx)
	ch := make(chan storage.FilteredResult, 256)
	vs := &chanSearcherValueSet{ch: ch, ctx: ctx, cancel: cancel}

	go func() {
		defer close(ch)

		names, _, err := s.querier.LabelNames(ctx, &storage2.LabelHints{}, matchers...)
		if err != nil {
			vs.err = err
			return
		}

		accepted := false
		score := -1.0
		for _, name := range names {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if hints != nil && hints.Filter != nil {
				accepted, score = hints.Filter.Accept(name)
				if !accepted {
					continue
				}
			}
			select {
			case ch <- storage.FilteredResult{Value: name, Score: score}:
			case <-ctx.Done():
				return
			}
		}
	}()

	return vs, nil
}

// SearchLabelValues starts a producer goroutine that retrieves values for the given
// label name from the underlying querier, applies hints.Filter (if set), and streams
// accepted values through a buffered channel. The returned SearcherValueSet reads
// from that channel.
func (s *StreamingSearch) SearchLabelValues(ctx context.Context, name string, hints *storage.SearchHints, matchers ...*labels.Matcher) (storage.SearcherValueSet, error) {
	ctx, cancel := context.WithCancel(ctx)
	ch := make(chan storage.FilteredResult, 256)
	vs := &chanSearcherValueSet{ch: ch, ctx: ctx, cancel: cancel}

	go func() {
		defer close(ch)

		values, _, err := s.querier.LabelValues(ctx, name, &storage2.LabelHints{}, matchers...)
		if err != nil {
			vs.err = err
			return
		}

		accepted := false
		score := -1.0
		for _, v := range values {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if hints != nil && hints.Filter != nil {
				accepted, score = hints.Filter.Accept(v)
				if !accepted {
					continue
				}
			}
			select {
			case ch <- storage.FilteredResult{Value: v, Score: score}:
			case <-ctx.Done():
				return
			}
		}
	}()

	return vs, nil
}
