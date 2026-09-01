// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"errors"
	"io"
	"sync"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/storage/remote"
)

// MergeChunkedReader merges multiple remote.ChunkedReader into one, returning from Next()
// whichever reader's next chunk becomes available first, instead of draining readers strictly
// in order like MultiChunkedReader does. Chunks are never split, but chunks from different
// readers can be interleaved in the order they become ready.
type MergeChunkedReader struct {
	permits   []chan struct{}
	results   chan mergeChunkedReaderResult
	done      chan struct{}
	closeOnce sync.Once

	remaining int
	err       error // Sticky error/io.EOF returned by Next() once set.
}

type mergeChunkedReaderResult struct {
	idx  int
	data []byte
	err  error
}

func NewMergeChunkedReader(readers ...*remote.ChunkedReader) *MergeChunkedReader {
	m := &MergeChunkedReader{
		permits:   make([]chan struct{}, len(readers)),
		results:   make(chan mergeChunkedReaderResult, len(readers)),
		done:      make(chan struct{}),
		remaining: len(readers),
	}

	for i, r := range readers {
		m.permits[i] = make(chan struct{}, 1)
		m.permits[i] <- struct{}{}
		go m.readLoop(i, r)
	}

	return m
}

func (m *MergeChunkedReader) readLoop(i int, r *remote.ChunkedReader) {
	for {
		select {
		case <-m.permits[i]:
		case <-m.done:
			return
		}

		rec, err := r.Next()
		var data []byte
		if err == nil {
			// Next()'s returned slice is only valid until the next call, so it must be copied
			// before handing it off to a Next() caller on another goroutine.
			data = append([]byte(nil), rec...)
		}

		select {
		case m.results <- mergeChunkedReaderResult{idx: i, data: data, err: err}:
		case <-m.done:
			return
		}
		if err != nil {
			return
		}
	}
}

func (m *MergeChunkedReader) Next() ([]byte, error) {
	if m.err != nil {
		return nil, m.err
	}

	for m.remaining > 0 {
		res := <-m.results
		if res.err != nil {
			if errors.Is(res.err, io.EOF) {
				m.remaining--
				continue
			}
			m.err = res.err
			m.closeOnce.Do(func() { close(m.done) })
			return nil, m.err
		}

		m.permits[res.idx] <- struct{}{}
		return res.data, nil
	}

	m.err = io.EOF
	return nil, io.EOF
}

func (m *MergeChunkedReader) NextProto(pb proto.Message) error {
	rec, err := m.Next()
	if err != nil {
		return err
	}
	return proto.Unmarshal(rec, pb)
}
