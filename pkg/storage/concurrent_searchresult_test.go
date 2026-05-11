// SPDX-License-Identifier: AGPL-3.0-only

package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

// addAnnotation is a small helper shared with searchresult_merge_test.go.
func addAnnotation(a annotations.Annotations, msg string) annotations.Annotations {
	return a.Add(errors.New(msg))
}
