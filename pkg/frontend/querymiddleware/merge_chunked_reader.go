// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"errors"
	"io"

	"github.com/gogo/protobuf/proto"
	"github.com/grafana/dskit/cancellation"
	"github.com/prometheus/prometheus/storage/remote"
)

var errStreamedChunksDrained = cancellation.NewErrorf("streamed remote read chunks drained")

type streamedChunkReader struct {
	reader *remote.ChunkedReader
	closer io.Closer
	cancel context.CancelCauseFunc
}

func (r *streamedChunkReader) Next() ([]byte, error) {
	return r.reader.Next()
}

func (r *streamedChunkReader) NextProto(pb proto.Message) error {
	return r.reader.NextProto(pb)
}

func (r *streamedChunkReader) cancelRequest(cause error) {
	if r.cancel != nil {
		r.cancel(cause)
	}
}

func (r *streamedChunkReader) Close() error {
	r.cancelRequest(errStreamedChunksDrained)
	return r.closer.Close()
}

// mergeChunkedReader is the logical concatenation of several streamedChunkReaders.
// They're read sequentially. Once all inputs have returned EOF, Read will return EOF.
// If any of the readers return a non-nil, non-EOF error, Read will return that error.
type mergeChunkedReader struct {
	readers []*streamedChunkReader
	// Index of the reader being drained. Readers before it are already closed, which is why
	// Close only closes readers[current:].
	current int
}

func newMergeChunkedReader(readers ...*streamedChunkReader) *mergeChunkedReader {
	return &mergeChunkedReader{readers: readers}
}

// Next returns the next record along with the index of the reader it was read from.
func (m *mergeChunkedReader) Next() ([]byte, int, error) {
	for m.current < len(m.readers) {
		reader := m.readers[m.current]
		rec, err := reader.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				m.current++
				// We can ignore the error since we already got the confirmation
				// the all stream was read.
				_ = reader.Close()
				continue
			}
			return nil, 0, err
		}

		return rec, m.current, nil
	}

	return nil, 0, io.EOF
}

func (m *mergeChunkedReader) NextProto(pb proto.Message) (int, error) {
	rec, rIdx, err := m.Next()
	if err != nil {
		return 0, err
	}
	if err = proto.Unmarshal(rec, pb); err != nil {
		return 0, err
	}
	return rIdx, nil
}

func (m *mergeChunkedReader) CancelAll(cause error) {
	for _, r := range m.readers {
		r.cancelRequest(cause)
	}
}

func (m *mergeChunkedReader) Close() error {
	var joinedErr error
	for _, reader := range m.readers[m.current:] {
		if err := reader.Close(); err != nil {
			joinedErr = errors.Join(joinedErr, err)
		}
	}
	return joinedErr
}
