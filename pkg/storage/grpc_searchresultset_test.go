// SPDX-License-Identifier: AGPL-3.0-only

package storage

import (
	"errors"
	"io"
	"testing"

	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeBatch struct {
	pairs []storage.SearchResult
	warns []string
}

func (b *fakeBatch) Len() int { return len(b.pairs) }
func (b *fakeBatch) At(i int) (string, float64) {
	return b.pairs[i].Value, b.pairs[i].Score
}
func (b *fakeBatch) BatchWarnings() []string { return b.warns }

type fakeStream struct {
	batches []*fakeBatch
	idx     int
	err     error
}

func (s *fakeStream) Recv() (*fakeBatch, error) {
	if s.idx >= len(s.batches) {
		if s.err != nil {
			return nil, s.err
		}
		return nil, io.EOF
	}
	b := s.batches[s.idx]
	s.idx++
	return b, nil
}

func TestGRPCStreamSearchResultSet_AtIsIdempotent(t *testing.T) {
	// At must return the same value across multiple calls between Next
	// calls — the SearchResultSet contract requires it.
	stream := &fakeStream{batches: []*fakeBatch{
		{pairs: []storage.SearchResult{{Value: "a", Score: 1.0}, {Value: "b", Score: 0.9}}},
	}}
	rs := NewGRPCStreamSearchResultSet[*fakeBatch](stream, func() {})
	require.True(t, rs.Next())
	first := rs.At()
	second := rs.At()
	third := rs.At()
	assert.Equal(t, first, second)
	assert.Equal(t, first, third)
	assert.Equal(t, "a", first.Value)

	require.True(t, rs.Next())
	assert.Equal(t, "b", rs.At().Value)
}

func TestGRPCStreamSearchResultSet_BatchBoundaries(t *testing.T) {
	stream := &fakeStream{batches: []*fakeBatch{
		{pairs: []storage.SearchResult{{Value: "a", Score: 1.0}, {Value: "b", Score: 0.9}}},
		{pairs: []storage.SearchResult{{Value: "c", Score: 0.8}}},
	}}
	rs := NewGRPCStreamSearchResultSet[*fakeBatch](stream, func() {})
	var got []storage.SearchResult
	for rs.Next() {
		got = append(got, rs.At())
	}
	require.NoError(t, rs.Err())
	assert.Equal(t, []storage.SearchResult{
		{Value: "a", Score: 1.0},
		{Value: "b", Score: 0.9},
		{Value: "c", Score: 0.8},
	}, got)
}

func TestGRPCStreamSearchResultSet_EmptyStream(t *testing.T) {
	stream := &fakeStream{}
	rs := NewGRPCStreamSearchResultSet[*fakeBatch](stream, func() {})
	assert.False(t, rs.Next())
	assert.NoError(t, rs.Err())
}

func TestGRPCStreamSearchResultSet_WarningsAccumulate(t *testing.T) {
	stream := &fakeStream{batches: []*fakeBatch{
		{pairs: []storage.SearchResult{{Value: "a"}}, warns: []string{"first"}},
		{pairs: []storage.SearchResult{{Value: "b"}}, warns: []string{"second", "third"}},
	}}
	rs := NewGRPCStreamSearchResultSet[*fakeBatch](stream, func() {})
	for rs.Next() {
		_ = rs.At()
	}
	require.NoError(t, rs.Err())
	msgs := make([]string, 0, 3)
	for _, w := range rs.Warnings() {
		msgs = append(msgs, w.Error())
	}
	assert.ElementsMatch(t, []string{"first", "second", "third"}, msgs)
}

func TestGRPCStreamSearchResultSet_WarningOnlyTrailer(t *testing.T) {
	stream := &fakeStream{batches: []*fakeBatch{
		{pairs: []storage.SearchResult{{Value: "a"}}},
		{warns: []string{"trailer-warn"}},
	}}
	rs := NewGRPCStreamSearchResultSet[*fakeBatch](stream, func() {})
	var got []storage.SearchResult
	for rs.Next() {
		got = append(got, rs.At())
	}
	require.NoError(t, rs.Err())
	assert.Equal(t, []storage.SearchResult{{Value: "a"}}, got)
	msgs := make([]string, 0, 1)
	for _, w := range rs.Warnings() {
		msgs = append(msgs, w.Error())
	}
	assert.Equal(t, []string{"trailer-warn"}, msgs)
}

func TestGRPCStreamSearchResultSet_StreamError(t *testing.T) {
	wantErr := errors.New("rpc broke")
	stream := &fakeStream{
		batches: []*fakeBatch{
			{pairs: []storage.SearchResult{{Value: "a"}}},
		},
		err: wantErr,
	}
	rs := NewGRPCStreamSearchResultSet[*fakeBatch](stream, func() {})
	assert.True(t, rs.Next())
	_ = rs.At()
	assert.False(t, rs.Next())
	assert.ErrorIs(t, rs.Err(), wantErr)
}

func TestGRPCStreamSearchResultSet_CloseCancels(t *testing.T) {
	var cancelled bool
	cancel := func() { cancelled = true }
	stream := &fakeStream{batches: []*fakeBatch{
		{pairs: []storage.SearchResult{{Value: "a"}}},
	}}
	rs := NewGRPCStreamSearchResultSet[*fakeBatch](stream, cancel)
	require.NoError(t, rs.Close())
	assert.True(t, cancelled, "Close must invoke the cancel func")
	require.NoError(t, rs.Close())
}
