// SPDX-License-Identifier: AGPL-3.0-only

package storage

import (
	"errors"
	"io"

	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
)

// SearchStreamBatch is the minimal shape both ingester and store-gateway
// SearchResultBatch protos satisfy through a thin adapter.
type SearchStreamBatch interface {
	Len() int
	At(i int) (value string, score float64)
	BatchWarnings() []string
}

// gRPCStreamSearcher is the receive-side of a server-streaming gRPC search
// RPC. Implementations are the generated *_Recv-shaped clients.
type gRPCStreamSearcher[B SearchStreamBatch] interface {
	Recv() (B, error)
}

// gRPCStreamSearchResultSet adapts a server-streaming gRPC client to
// storage.SearchResultSet. Value strings come from gRPC proto unmarshal —
// independent of any request-pool buffer (no unsafeMutableString aliasing).
// Not safe for concurrent use.
type gRPCStreamSearchResultSet[B SearchStreamBatch] struct {
	stream gRPCStreamSearcher[B]
	cancel func()

	batch    B
	batchLen int
	idx      int

	warnings annotations.Annotations
	err      error
	done     bool
}

// NewGRPCStreamSearchResultSet wraps a stream client; cancel runs on Close.
func NewGRPCStreamSearchResultSet[B SearchStreamBatch](stream gRPCStreamSearcher[B], cancel func()) storage.SearchResultSet {
	return &gRPCStreamSearchResultSet[B]{stream: stream, cancel: cancel}
}

func (s *gRPCStreamSearchResultSet[B]) Next() bool {
	if s.done || s.err != nil {
		return false
	}
	if s.idx < s.batchLen {
		return true
	}
	for {
		batch, err := s.stream.Recv()
		if errors.Is(err, io.EOF) {
			s.done = true
			return false
		}
		if err != nil {
			s.err = err
			return false
		}
		for _, w := range batch.BatchWarnings() {
			s.warnings.Add(errors.New(w))
		}
		s.batch = batch
		s.batchLen = batch.Len()
		s.idx = 0
		if s.batchLen > 0 {
			return true
		}
		// Warning-only batch — keep pulling.
	}
}

func (s *gRPCStreamSearchResultSet[B]) At() storage.SearchResult {
	v, sc := s.batch.At(s.idx)
	s.idx++
	return storage.SearchResult{Value: v, Score: sc}
}

func (s *gRPCStreamSearchResultSet[B]) Warnings() annotations.Annotations {
	return s.warnings
}

func (s *gRPCStreamSearchResultSet[B]) Err() error {
	return s.err
}

func (s *gRPCStreamSearchResultSet[B]) Close() error {
	if s.cancel != nil {
		s.cancel()
		s.cancel = nil
	}
	return nil
}
