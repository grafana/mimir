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
)

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

func TestConcurrentSearchResultSetForwardsInOrder(t *testing.T) {
	src := &staticResultSet{results: []storage.SearchResult{
		{Value: "a", Score: 1.0},
		{Value: "b", Score: 0.9},
		{Value: "c", Score: 0.5},
	}}
	c := NewConcurrentSearchResultSet(context.Background(), src, 4)
	defer c.Close()

	var got []string
	for c.Next() {
		got = append(got, c.At().Value)
	}
	require.NoError(t, c.Err())
	assert.Equal(t, []string{"a", "b", "c"}, got)
}

func TestConcurrentSearchResultSetClosesChild(t *testing.T) {
	src := &staticResultSet{results: []storage.SearchResult{{Value: "x", Score: 1.0}}}
	c := NewConcurrentSearchResultSet(context.Background(), src, 4)
	for c.Next() {
	}
	require.NoError(t, c.Close())
	assert.True(t, src.closed, "child was not closed")
}

func TestConcurrentSearchResultSetCancellationDrains(t *testing.T) {
	src := &staticResultSet{results: make([]storage.SearchResult, 1000)}
	for i := range src.results {
		src.results[i] = storage.SearchResult{Value: string(rune('a' + i%26)), Score: 1.0}
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := NewConcurrentSearchResultSet(ctx, src, 4)
	cancel()
	deadline := time.After(2 * time.Second)
	done := make(chan struct{})
	go func() {
		for c.Next() {
		}
		close(done)
	}()
	select {
	case <-done:
	case <-deadline:
		t.Fatal("Next did not unblock after context cancel")
	}
	require.NoError(t, c.Close())
}

func TestConcurrentSearchResultSetClonesValues(t *testing.T) {
	buf := []byte("aaaa")
	src := &staticResultSet{results: []storage.SearchResult{{Value: string(buf), Score: 1.0}}}
	c := NewConcurrentSearchResultSet(context.Background(), src, 4)
	var got []string
	for c.Next() {
		got = append(got, c.At().Value)
	}
	require.NoError(t, c.Close())
	buf[0] = 'b'
	require.Len(t, got, 1)
	assert.Equal(t, "aaaa", got[0], "value must be cloned, not aliased")
	var wg sync.WaitGroup
	wg.Add(0)
	wg.Wait()
}

func TestConcurrentSearchResultSetPropagatesError(t *testing.T) {
	want := errors.New("boom")
	src := &staticResultSet{results: nil, err: want}
	c := NewConcurrentSearchResultSet(context.Background(), src, 4)
	for c.Next() {
	}
	require.ErrorIs(t, c.Err(), want)
	require.NoError(t, c.Close())
}
