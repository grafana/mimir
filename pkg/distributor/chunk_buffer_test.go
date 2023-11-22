package distributor

import (
	"bytes"
	"io"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewChunkBufferTOC(t *testing.T) {
	tests := map[string]struct {
		partitions  map[uint32]*bytes.Buffer
		expectedTOC ChunkBufferTOC
	}{
		"TOC with no partitions": {
			partitions: map[uint32]*bytes.Buffer{},
			expectedTOC: ChunkBufferTOC{
				partitionsLength: 0,
				partitions:       []ChunkBufferTOCPartition{},
			},
		},
		"TOC with one partition": {
			partitions: map[uint32]*bytes.Buffer{
				1: bytes.NewBufferString("partition-1"),
			},
			expectedTOC: func() ChunkBufferTOC {
				tocSize := ChunkBufferTOCSize(1)

				return ChunkBufferTOC{
					partitionsLength: 1,
					partitions: []ChunkBufferTOCPartition{
						{
							partitionID: 1,
							offset:      tocSize,
							length:      11,
						},
					},
				}
			}(),
		},
		"TOC with multiple partitions": {
			partitions: map[uint32]*bytes.Buffer{
				1: bytes.NewBufferString("partition-1"),
				2: bytes.NewBufferString("partition-2"),
				3: bytes.NewBufferString("partition-3"),
			},
			expectedTOC: func() ChunkBufferTOC {
				tocSize := ChunkBufferTOCSize(3)

				return ChunkBufferTOC{
					partitionsLength: 3,
					partitions: []ChunkBufferTOCPartition{
						{
							partitionID: 1,
							offset:      tocSize,
							length:      11,
						}, {
							partitionID: 2,
							offset:      tocSize + 11,
							length:      11,
						}, {
							partitionID: 3,
							offset:      tocSize + (2 * 11),
							length:      11,
						},
					},
				}
			}(),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testData.expectedTOC, NewChunkBufferTOC(testData.partitions))
		})
	}
}

func TestNewChunkBufferTOCFromBytes(t *testing.T) {
	tests := map[string]struct {
		toc ChunkBufferTOC
	}{
		"TOC with no partitions": {
			toc: ChunkBufferTOC{
				partitionsLength: 0,
				partitions:       nil,
			},
		},
		"TOC with one partition": {
			toc: func() ChunkBufferTOC {
				tocSize := ChunkBufferTOCSize(1)

				return ChunkBufferTOC{
					partitionsLength: 1,
					partitions: []ChunkBufferTOCPartition{
						{
							partitionID: 1,
							offset:      tocSize,
							length:      11,
						},
					},
				}
			}(),
		},
		"TOC with multiple partitions": {
			toc: func() ChunkBufferTOC {
				tocSize := ChunkBufferTOCSize(3)

				return ChunkBufferTOC{
					partitionsLength: 3,
					partitions: []ChunkBufferTOCPartition{
						{
							partitionID: 1,
							offset:      tocSize,
							length:      11,
						}, {
							partitionID: 2,
							offset:      tocSize + 11,
							length:      11,
						}, {
							partitionID: 3,
							offset:      tocSize + (2 * 11),
							length:      11,
						},
					},
				}
			}(),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			marshalled := testData.toc.Bytes()
			unmarshalled, err := NewChunkBufferTOCFromBytes(marshalled)
			require.NoError(t, err)
			assert.Equal(t, testData.toc, unmarshalled)
		})
	}
}

func TestNewChunkBufferTOCFromBytes_Corrupted(t *testing.T) {
	t.Run("should return error if input is empty", func(t *testing.T) {
		_, err := NewChunkBufferTOCFromBytes(nil)
		require.Error(t, err)
	})

	t.Run("should return error if input is too short", func(t *testing.T) {
		_, err := NewChunkBufferTOCFromBytes([]byte{1})
		require.Error(t, err)
	})
}

func TestChunkBufferMarshaller_Read(t *testing.T) {
	// TODO run many times, change seed each time and log it

	partitions := map[uint32]*bytes.Buffer{
		1: bytes.NewBufferString("partition-1"),
		2: bytes.NewBufferString("partition-2"),
		3: bytes.NewBufferString("partition-3"),
	}

	marshaller := NewChunkBufferMarshaller(partitions)
	buffer := bytes.NewBuffer(nil)

	// Read using random sizes.
	for {
		readBuffer := make([]byte, rand.Intn(10))
		readSize, err := marshaller.Read(readBuffer)
		if err == io.EOF {
			break
		}

		require.NoError(t, err)
		require.Greater(t, readSize, 0)

		buffer.Write(readBuffer[0:readSize])
	}

	marshalled := buffer.Bytes()

	// Check the TOC.
	expectedTOC := NewChunkBufferTOC(partitions).Bytes()
	assert.Equal(t, expectedTOC, marshalled[0:len(expectedTOC)])

	// TODO assert the rest of the content
}
