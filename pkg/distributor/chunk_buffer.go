package distributor

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"slices"
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
	// mx protects closed and partitions. Once closed, it's safe to read from partitions
	// without acquiring the lock, because it's guaranteed no more data will be written to it.
	mx         sync.Mutex
	closed     bool
	partitions map[uint32]*bytes.Buffer

	commitErr  error
	commitDone chan struct{}
}

func NewChunkBuffer() *ChunkBuffer {
	return &ChunkBuffer{
		partitions: map[uint32]*bytes.Buffer{},
		commitDone: make(chan struct{}),
	}
}

// TODO store the userID too
func (b *ChunkBuffer) Append(ctx context.Context, partitionID uint32, userID string, timeseries []mimirpb.PreallocTimeseries, metadata []*mimirpb.MetricMetadata, source mimirpb.WriteRequest_SourceEnum) error {
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

	return NewChunkBufferMarshaller(b.partitions), nil
}

type ChunkBufferMarshaller struct {
	// content is an ordered list of byte slices that get marshalled.
	content [][]byte

	// currContentOffset is the read offset in the current content slice
	// (the current content is the one at index 0).
	currContentOffset int
}

func NewChunkBufferMarshaller(partitions map[uint32]*bytes.Buffer) *ChunkBufferMarshaller {
	// Get the list of partitions IDs.
	partitionIDs := make([]uint32, 0, len(partitions))
	for id := range partitions {
		partitionIDs = append(partitionIDs, id)
	}

	// Spec requires to sort partition IDs.
	slices.Sort(partitionIDs)

	// Add TOC.
	content := make([][]byte, 0, 1+len(partitions))
	content = append(content, NewChunkBufferTOC(partitions).Bytes())

	// Add partitions.
	for _, partitionID := range partitionIDs {
		content = append(content, partitions[partitionID].Bytes())
	}

	return &ChunkBufferMarshaller{
		content: content,
	}
}

// Read implements io.Reader.
func (m *ChunkBufferMarshaller) Read(p []byte) (n int, err error) {
	// Ensure we haven't reached the end of the current slice.
	if len(m.content) > 0 && m.currContentOffset >= len(m.content[0]) {
		m.content = m.content[1:]
		m.currContentOffset = 0
	}

	// Ensure we haven't reached the end of the content.
	if len(m.content) == 0 {
		return 0, io.EOF
	}

	// Ensure the input buffer has some space.
	if len(p) == 0 {
		return 0, nil
	}

	n = min(len(p), len(m.content[0])-m.currContentOffset)
	copy(p[0:n], m.content[0][m.currContentOffset:m.currContentOffset+n])
	m.currContentOffset += n

	return n, nil
}

// ChunkBufferTOC holds the TOC of a serialised ChunkBuffer.
type ChunkBufferTOC struct {
	// TODO magic
	// TODO version

	partitionsLength uint32
	partitions       []ChunkBufferTOCPartition
}

func NewChunkBufferTOC(partitions map[uint32]*bytes.Buffer) ChunkBufferTOC {
	toc := ChunkBufferTOC{}

	// Get the list of partitions IDs.
	partitionIDs := make([]uint32, 0, len(partitions))
	for id := range partitions {
		partitionIDs = append(partitionIDs, id)
	}

	// Spec requires to sort partition IDs.
	slices.Sort(partitionIDs)

	// Compute the TOC size (in bytes). This is required to know the starting offset of partitions.
	// TODO use Size()
	offset := 4 + uint64(len(partitions)*ChunkBufferTOCPartitionSize)

	// Add partitions to TOC.
	toc.partitions = make([]ChunkBufferTOCPartition, 0, len(partitions))
	for id, buffer := range partitions {
		// TODO we may want to write the length at the beginning of each partition, so we could also read it in a streaming way

		toc.partitions = append(toc.partitions, ChunkBufferTOCPartition{
			partitionID: id,
			offset:      offset,
			length:      uint64(buffer.Len()),
		})

		offset += uint64(buffer.Len())
	}

	return toc
}

// Size returns the serialised TOC size, in bytes.
func (t ChunkBufferTOC) Size() int {
	return 4 + (len(t.partitions) * ChunkBufferTOCPartitionSize)
}

func (t ChunkBufferTOC) Bytes() []byte {
	buffer := bytes.NewBuffer(nil)
	buffer.Grow(t.Size())

	// Partitions.
	_ = binary.Write(buffer, binary.BigEndian, uint32(len(t.partitions)))

	for _, partition := range t.partitions {
		_ = binary.Write(buffer, binary.BigEndian, partition.partitionID)
		_ = binary.Write(buffer, binary.BigEndian, partition.offset)
		_ = binary.Write(buffer, binary.BigEndian, partition.length)
	}

	return buffer.Bytes()
}

// ChunkBufferTOCPartitionSize is the size, in bytes, of a serialized ChunkBufferTOCPartition entry.
const ChunkBufferTOCPartitionSize = 4 + 8 + 8

type ChunkBufferTOCPartition struct {
	partitionID uint32
	offset      uint64
	length      uint64
}
