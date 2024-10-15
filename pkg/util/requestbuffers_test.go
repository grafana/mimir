// SPDX-License-Identifier: AGPL-3.0-only
package util

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRequestBuffers(t *testing.T) {
	const maxBufferSize = 32 * 1024

	rb := NewRequestBuffers(&fakePool{maxBufferSize: maxBufferSize})
	t.Cleanup(rb.CleanUp)

	b := rb.Get(1024)
	require.NotNil(t, b)
	assert.Equal(t, 1024, b.Cap())
	assert.Zero(t, b.Len())
	// Make sure that the buffer gets reset upon next Get
	_, err := b.Write([]byte("test"))
	require.NoError(t, err)

	rb.CleanUp()
	assert.Nil(t, rb.buffersBacking[0])

	// Retrieve a new buffer of the same size after cleanup
	// to test if it reuses the previously returned buffer.
	b1 := rb.Get(1024)
	assert.Same(t, unsafe.SliceData(b1.Bytes()), unsafe.SliceData(b.Bytes()))
	assert.Equal(t, 1024, b1.Cap())
	assert.Zero(t, b1.Len())

	// Retrieve a buffer larger than maxBufferSize to ensure
	// it doesn't get reused.
	b2 := rb.Get(maxBufferSize + 1)
	assert.Equal(t, maxBufferSize+1, b2.Cap())
	assert.Zero(t, b2.Len())

	rb.CleanUp()

	b3 := rb.Get(maxBufferSize + 1)
	assert.NotSame(t, unsafe.SliceData(b2.Bytes()), unsafe.SliceData(b3.Bytes()))

	t.Run("as nil pointer", func(t *testing.T) {
		var rb *RequestBuffers
		b := rb.Get(1024)
		require.NotNil(t, b)
		assert.Equal(t, 1024, b.Cap())
		assert.Zero(t, b.Len())
	})
	t.Run("as nil p", func(t *testing.T) {
		rb := &RequestBuffers{}
		b := rb.Get(1024)
		require.NotNil(t, b)
		assert.Equal(t, 1024, b.Cap())
		assert.Zero(t, b.Len())
	})
}

type fakePool struct {
	maxBufferSize int
	buffers       [][]byte
}

func (p *fakePool) Get(sz int) []byte {
	if sz <= p.maxBufferSize {
		for i, b := range p.buffers {
			if cap(b) < sz {
				continue
			}
			p.buffers = append(p.buffers[:i], p.buffers[i+1:]...)
			return b
		}
	}
	return make([]byte, 0, sz)
}

func (p *fakePool) Put(s []byte) {
	p.buffers = append(p.buffers, s[:0])
}
