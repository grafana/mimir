// SPDX-License-Identifier: AGPL-3.0-only

package storage

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

// staticResultSet is a deterministic in-memory SearchResultSet used to drive
// the prefetcher and the merger from tests.
type staticResultSet struct {
	results []storage.SearchResult
	idx     int
	closed  bool
	warns   annotations.Annotations
	err     error
}

func (s *staticResultSet) Next() bool {
	if s.idx >= len(s.results) {
		return false
	}
	s.idx++
	return true
}
func (s *staticResultSet) At() storage.SearchResult          { return s.results[s.idx-1] }
func (s *staticResultSet) Warnings() annotations.Annotations { return s.warns }
func (s *staticResultSet) Err() error                        { return s.err }
func (s *staticResultSet) Close() error                      { s.closed = true; return nil }

func TestConcurrentSearchResultSet_ForwardsInOrder(t *testing.T) {
	src := &staticResultSet{results: []storage.SearchResult{
		{Value: "a", Score: 1.0},
		{Value: "b", Score: 0.9},
		{Value: "c", Score: 0.5},
	}}
	c := NewConcurrentSearchResultSet(context.Background(), src, 4)
	defer c.Close()

	var got []storage.SearchResult
	for c.Next() {
		got = append(got, c.At())
	}
	require.NoError(t, c.Err())
	assert.Equal(t, []storage.SearchResult{
		{Value: "a", Score: 1.0},
		{Value: "b", Score: 0.9},
		{Value: "c", Score: 0.5},
	}, got)
}

func TestConcurrentSearchResultSet_ClosesChild(t *testing.T) {
	src := &staticResultSet{results: []storage.SearchResult{{Value: "x", Score: 1.0}}}
	c := NewConcurrentSearchResultSet(context.Background(), src, 4)
	for c.Next() {
	}
	require.NoError(t, c.Close())
	assert.True(t, src.closed, "child must be closed by the wrapper's Close")
}

func TestConcurrentSearchResultSet_CancellationUnblocks(t *testing.T) {
	// Producer with more values than buffer size; consumer never reads.
	// Cancelling the context must let the producer exit promptly, and
	// Close must then return without hanging.
	src := &staticResultSet{results: make([]storage.SearchResult, 1000)}
	for i := range src.results {
		src.results[i] = storage.SearchResult{Value: "x", Score: 1.0}
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := NewConcurrentSearchResultSet(ctx, src, 4)
	cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for c.Next() {
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Next did not unblock after context cancel within 2s")
	}
	require.NoError(t, c.Close())
}

func TestConcurrentSearchResultSet_PropagatesError(t *testing.T) {
	want := errors.New("boom")
	src := &staticResultSet{results: nil, err: want}
	c := NewConcurrentSearchResultSet(context.Background(), src, 4)
	for c.Next() {
	}
	require.ErrorIs(t, c.Err(), want)
	require.NoError(t, c.Close())
}

func TestConcurrentSearchResultSet_PropagatesWarnings(t *testing.T) {
	src := &staticResultSet{
		results: []storage.SearchResult{{Value: "a", Score: 1.0}},
		warns:   addAnnotation(nil, "child-warn"),
	}
	c := NewConcurrentSearchResultSet(context.Background(), src, 4)
	for c.Next() {
	}
	require.NoError(t, c.Err())
	msgs := make([]string, 0, 1)
	for _, w := range c.Warnings() {
		msgs = append(msgs, w.Error())
	}
	assert.Equal(t, []string{"child-warn"}, msgs)
	require.NoError(t, c.Close())
}

func TestConcurrentSearchResultSet_CloseIsIdempotent(t *testing.T) {
	src := &staticResultSet{results: []storage.SearchResult{{Value: "x", Score: 1.0}}}
	c := NewConcurrentSearchResultSet(context.Background(), src, 4)
	for c.Next() {
	}
	require.NoError(t, c.Close())
	require.NoError(t, c.Close(), "second Close must not panic or block")
}

// blockingResultSet models a child (e.g. a gRPC stream) whose Next blocks on
// a remote call and only unblocks when Close is invoked. The wrapper must
// close the child before waiting for the producer; otherwise Close hangs.
type blockingResultSet struct {
	closeCh chan struct{}
	once    sync.Once
	closed  atomic.Bool
}

func newBlockingResultSet() *blockingResultSet {
	return &blockingResultSet{closeCh: make(chan struct{})}
}

func (b *blockingResultSet) Next() bool {
	<-b.closeCh
	return false
}
func (b *blockingResultSet) At() storage.SearchResult          { return storage.SearchResult{} }
func (b *blockingResultSet) Warnings() annotations.Annotations { return nil }
func (b *blockingResultSet) Err() error                        { return nil }
func (b *blockingResultSet) Close() error {
	b.once.Do(func() {
		b.closed.Store(true)
		close(b.closeCh)
	})
	return nil
}

func TestConcurrentSearchResultSet_PreservesWarningsAcrossEarlyClose(t *testing.T) {
	// Producer is blocked in `select { case ch <- r: case <-ctx.Done() }`
	// when Close fires. The terminal capture of child.Warnings() must
	// still happen — otherwise warnings accumulated by the child up to
	// the cancel point are silently lost.
	src := &staticResultSet{
		results: []storage.SearchResult{{Value: "a"}, {Value: "b"}},
		warns:   addAnnotation(nil, "child-warn"),
	}
	c := NewConcurrentSearchResultSet(context.Background(), src, 1)
	// Don't drain. With buf=1 and 2 results, the producer pushes "a",
	// then blocks trying to push "b" because the consumer never pulls.
	// Sleep long enough for the producer goroutine to enter the select.
	time.Sleep(100 * time.Millisecond)
	require.NoError(t, c.Close())
	msgs := make([]string, 0, 1)
	for _, w := range c.Warnings() {
		msgs = append(msgs, w.Error())
	}
	assert.Equal(t, []string{"child-warn"}, msgs, "child warnings must survive early Close")
}

func TestConcurrentSearchResultSet_CloseUnblocksProducerBlockedInChildNext(t *testing.T) {
	src := newBlockingResultSet()
	c := NewConcurrentSearchResultSet(context.Background(), src, 4)

	done := make(chan error, 1)
	go func() { done <- c.Close() }()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Close hung waiting for producer goroutine; child was not closed first")
	}
	assert.True(t, src.closed.Load(), "child must be closed by the wrapper's Close")
}

// addAnnotation is a small helper shared with searchresult_merge_test.go.
func addAnnotation(a annotations.Annotations, msg string) annotations.Annotations {
	return a.Add(errors.New(msg))
}
