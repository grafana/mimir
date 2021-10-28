package chunkenc

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

const (
	// When serialized, the XORPartialChunk has a fixed header:
	// - numSamples: 2 bytes
	// - lastSamples: 4 * 16 bytes
	xorPartialChunkHeaderBytes = 2 + (4 * 16)
)

type Sample struct {
	T int64
	V float64
}

// XORPartialChunk holds a snapshot of an XORChunk data, created while still appending it.
// This struct is read-only.
type XORPartialChunk struct {
	// Number of samples that can be iterated on.
	numSamples int

	// (Up to) last 4 samples appended to XORChunk.
	lastSamples [4]Sample

	// Snapshot of XORChunk underlying bytes when the XORPartialChunk was created.
	xorChunk []byte
}

func NewXORPartialChunkFromXORChunk(partial *XORChunk, lastSamples [4]Sample) *XORPartialChunk {
	return &XORPartialChunk{
		numSamples:  partial.NumSamples(),
		xorChunk:    partial.Bytes(),
		lastSamples: lastSamples,
	}
}

func NewXORPartialChunkFromWire(raw []byte) (*XORPartialChunk, error) {
	c := &XORPartialChunk{}
	offset := 0

	if len(raw) < xorPartialChunkHeaderBytes {
		return nil, fmt.Errorf("the serialized XORPartialChunk is %d bytes but should be at least %d bytes", len(raw), xorPartialChunkHeaderBytes)
	}

	// Read numSamples.
	c.numSamples = int(binary.BigEndian.Uint16(raw[offset:]))
	offset += 2

	// Read lastSamples.
	for idx := range c.lastSamples {
		c.lastSamples[idx].T = int64(binary.BigEndian.Uint64(raw[offset:]))
		offset += 8

		c.lastSamples[idx].V = math.Float64frombits(binary.BigEndian.Uint64(raw[offset:]))
		offset += 8
	}

	// Read XORChunk underlying bytes.
	c.xorChunk = raw[offset:]

	return c, nil
}

// Bytes returns the encoded XORPartialChunk safe to be transferred on wire.
func (c *XORPartialChunk) Bytes() []byte {
	serialized := make([]byte, xorPartialChunkHeaderBytes+len(c.xorChunk))
	offset := 0

	// Write numSamples.
	binary.BigEndian.PutUint16(serialized[offset:], uint16(c.numSamples))
	offset += 2

	// Write lastSamples.
	for _, sample := range c.lastSamples {
		binary.BigEndian.PutUint64(serialized[offset:], uint64(sample.T))
		offset += 8

		binary.BigEndian.PutUint64(serialized[offset:], math.Float64bits(sample.V))
		offset += 8
	}

	// Copy XORChunk underlying bytes.
	copy(serialized[offset:], c.xorChunk)

	return serialized
}

// Encoding returns the encoding type of the chunk.
func (c *XORPartialChunk) Encoding() Encoding {
	return EncXORPartial
}

// Appender returns an appender to append samples to the chunk.
func (c *XORPartialChunk) Appender() (Appender, error) {
	return nil, errors.New("XORPartialChunk is read-only")
}

// The iterator passed as argument is for re-use.
// Depending on implementation, the iterator can
// be re-used or a new iterator can be allocated.
// TODO support Iterator reusing
func (c *XORPartialChunk) Iterator(_ Iterator) Iterator {
	xorChunk, err := FromData(EncXOR, c.xorChunk)
	if err != nil {
		// Current can't return error, so we panic just in case anything will change in future.
		panic(err.Error())
	}

	return &xorPartialChunkIterator{
		Iterator:    xorChunk.Iterator(nil),
		total:       c.numSamples,
		lastSamples: c.lastSamples,
		i:           -1,
	}
}

// NumSamples returns the number of samples in the chunk.
func (c *XORPartialChunk) NumSamples() int {
	return c.numSamples
}

// Compact is called whenever a chunk is expected to be complete (no more
// samples appended) and the underlying implementation can eventually
// optimize the chunk.
// There's no strong guarantee that no samples will be appended once
// Compact() is called. Implementing this function is optional.
func (c *XORPartialChunk) Compact() {
	// Nothing to compact.
}

type xorPartialChunkIterator struct {
	// Underlying XORChunk iterator.
	Iterator

	i           int
	total       int
	lastSamples [4]Sample
}

func (it *xorPartialChunkIterator) Seek(t int64) bool {
	if it.Err() != nil {
		return false
	}

	ts, _ := it.At()

	for t > ts || it.i == -1 {
		if !it.Next() {
			return false
		}
		ts, _ = it.At()
	}

	return true
}

func (it *xorPartialChunkIterator) Next() bool {
	if it.i+1 >= it.total {
		return false
	}
	it.i++
	if it.total-it.i > 4 {
		return it.Iterator.Next()
	}
	return true
}

func (it *xorPartialChunkIterator) At() (int64, float64) {
	if it.total-it.i > 4 {
		return it.Iterator.At()
	}
	s := it.lastSamples[4-(it.total-it.i)]
	return s.T, s.V
}
