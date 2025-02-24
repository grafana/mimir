// SPDX-License-Identifier: AGPL-3.0-only
package util

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRequestBuffers(t *testing.T) {
	rb := NewRequestBuffers(&fakePool{})
	t.Cleanup(rb.CleanUp)

	b := rb.Get(1024)
	require.NotNil(t, b)
	assert.GreaterOrEqual(t, b.Cap(), 1024)
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
	assert.GreaterOrEqual(t, b1.Cap(), 1024)
	assert.Zero(t, b1.Len())

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

func TestRequestsBuffersMaxPoolBufferSize(t *testing.T) {
	const maxPoolBufferCap = defaultPoolBufferCap

	t.Run("pool buffer is reused when size is less or equal to maxBufferSize", func(t *testing.T) {
		rb := NewRequestBuffers(&fakePool{maxBufferCap: maxPoolBufferCap})
		t.Cleanup(rb.CleanUp)

		b0 := rb.Get(maxPoolBufferCap)
		require.NotNil(t, b0)
		assert.Zero(t, b0.Len())
		assert.GreaterOrEqual(t, b0.Cap(), maxPoolBufferCap)

		rb.CleanUp()

		b1 := rb.Get(maxPoolBufferCap)
		assert.Same(t, unsafe.SliceData(b0.Bytes()), unsafe.SliceData(b1.Bytes()))
	})
	t.Run("pool buffer is not reused when size is greater than maxBufferSize", func(t *testing.T) {
		rb := NewRequestBuffers(NewBufferPool(maxPoolBufferCap))
		t.Cleanup(rb.CleanUp)

		b0 := rb.Get(maxPoolBufferCap + 1)
		require.NotNil(t, b0)
		assert.Zero(t, b0.Len())
		assert.GreaterOrEqual(t, b0.Cap(), maxPoolBufferCap+1)

		rb.CleanUp()

		b1 := rb.Get(maxPoolBufferCap + 1)
		assert.NotSame(t, unsafe.SliceData(b0.Bytes()), unsafe.SliceData(b1.Bytes()))
	})
}

type fakePool struct {
	maxBufferCap int
	buffers      [][]byte
}

func (p *fakePool) Get() []byte {
	if len(p.buffers) > 0 {
		buf := p.buffers[0]
		p.buffers = p.buffers[1:]
		return buf
	}
	return make([]byte, 0, defaultPoolBufferCap)
}

func (p *fakePool) Put(s []byte) {
	if p.maxBufferCap > 0 && cap(s) > p.maxBufferCap {
		return
	}
	p.buffers = append(p.buffers, s[:0])
}
