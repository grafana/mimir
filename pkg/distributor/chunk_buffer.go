package distributor

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/grafana/mimir/pkg/mimirpb"
)

var (
	ErrChunkClosed    = fmt.Errorf("the chunk is closed")
	ErrChunkNotClosed = fmt.Errorf("the chunk has not been closed yet")
)

// ChunkBuffer holds a chunk of data not written to the object storage yet.
// ChunkBuffer is concurrency safe.
// TODO rename "chunk" to something else, to avoid misunderstandings with TSDB chunks.
type ChunkBuffer struct {
	// mx protects closed and partitions.
	mx         sync.Mutex
	closed     bool
	partitions map[int]*bytes.Buffer

	commitErr  error
	commitDone chan struct{}
}

func NewChunkBuffer() *ChunkBuffer {
	return &ChunkBuffer{
		partitions: map[int]*bytes.Buffer{},
		commitDone: make(chan struct{}),
	}
}

func (b *ChunkBuffer) Append(ctx context.Context, partitionID int, userID string, timeseries []mimirpb.PreallocTimeseries, metadata []*mimirpb.MetricMetadata, source mimirpb.WriteRequest_SourceEnum) error {
	// Serialise the input data.
	entry := &mimirpb.WriteRequest{
		Timeseries: timeseries,
		Metadata:   metadata,
		Source:     source,
	}

	data, err := entry.Marshal()
	if err != nil {
		return err
	}

	b.mx.Lock()
	defer b.mx.Unlock()

	// Ensure the chunk has not been closed in the meanwhile.
	if b.closed {
		return ErrChunkClosed
	}

	// Lazily create the per-partition buffer.
	partitionBuffer, ok := b.partitions[partitionID]
	if !ok {
		partitionBuffer = bytes.NewBuffer(nil) // TODO pooling
		b.partitions[partitionID] = partitionBuffer
	}

	// Write the next entry length.
	if err := binary.Write(partitionBuffer, binary.LittleEndian, int64(len(data))); err != nil {
		return err
	}

	// Write the next entry content.
	_, err = partitionBuffer.Write(data)
	return err
}

// Close the chunk. This function is idempotent.
func (b *ChunkBuffer) Close() {
	b.mx.Lock()
	defer b.mx.Unlock()

	b.closed = true
}

// NotifyCommit that the commit operation is completed. If the input err is valued, it means the operation
// failed otherwise it succeeded.
func (b *ChunkBuffer) NotifyCommit(err error) {
	b.commitErr = err
	close(b.commitDone)
}

// WaitCommit until the chunk is committed to the object storage or an error occurred.
func (b *ChunkBuffer) WaitCommit(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()

	case <-b.commitDone:
		return b.commitErr
	}
}

// Marshal returns an io.Reader that can be used to read the serialised chunk.
func (b *ChunkBuffer) Marshal() (io.Reader, error) {
	// Ensure the chunk is closed (otherwise it's not safe to marshal).
	b.mx.Lock()
	closed := b.closed
	b.mx.Unlock()

	if !closed {
		return nil, ErrChunkNotClosed
	}

	// TODO ensure it's closed, otherwise it's an error

	return &ChunkBufferMarshaller{
		partitions: b.partitions,
	}, nil
}

type ChunkBufferMarshaller struct {
	partitions map[int]*bytes.Buffer
}

func (m *ChunkBufferMarshaller) Read(p []byte) (n int, err error) {
	// TODO
	return 0, nil
}
