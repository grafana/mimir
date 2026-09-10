// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"bytes"
	"context"
	"errors"
	"io"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/storage/remote"
)

type streamedChunkReader struct {
	reader *remote.ChunkedReader
	closer io.Closer
	cancel context.CancelCauseFunc
}

func newEmptyStreamedChunkReader() *streamedChunkReader {
	emptyReader := &bytes.Buffer{}
	return &streamedChunkReader{
		reader: remote.NewChunkedReader(emptyReader, 1, nil),
		cancel: nil,
		closer: io.NopCloser(emptyReader),
	}
}

func (r *streamedChunkReader) Next() ([]byte, error) {
	return r.reader.Next()
}

func (r *streamedChunkReader) NextProto(pb proto.Message) error {
	return r.reader.NextProto(pb)
}

func (r *streamedChunkReader) Close() error {
	if r.cancel != nil {
		r.cancel(nil)
	}
	return r.closer.Close()
}

// MergeChunkedReader merges multiple remote.ChunkedReader into one, draining each reader in order.
type MergeChunkedReader struct {
	readers []*streamedChunkReader
	current int
	err     error // Sticky error/io.EOF returned by Next() once set.
}

func NewMergeChunkedReader(readers ...*streamedChunkReader) *MergeChunkedReader {
	return &MergeChunkedReader{readers: readers}
}

func (m *MergeChunkedReader) Next() ([]byte, int, error) {
	if m.err != nil {
		return nil, 0, m.err
	}

	for m.current < len(m.readers) {
		rec, err := m.readers[m.current].Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				if err = m.readers[m.current].Close(); err != nil {
					m.err = err
					return nil, 0, m.err
				}
				m.current++
				continue
			}
			m.err = err
			return nil, 0, m.err
		}

		return rec, m.current, nil
	}

	m.err = io.EOF
	return nil, 0, m.err
}

func (m *MergeChunkedReader) NextProto(pb proto.Message) (int, error) {
	rec, rIdx, err := m.Next()
	if err != nil {
		return 0, err
	}
	if err = proto.Unmarshal(rec, pb); err != nil {
		return 0, err
	}
	return rIdx, nil
}

func (m *MergeChunkedReader) Close() error {
	var joinedErr error
	for _, reader := range m.readers[m.current:] {
		if err := reader.Close(); err != nil {
			joinedErr = errors.Join(joinedErr, err)
		}
	}
	return joinedErr
}
