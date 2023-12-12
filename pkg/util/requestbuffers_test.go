// SPDX-License-Identifier: AGPL-3.0-only
package util

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRequestBuffers(t *testing.T) {
	rb := NewRequestBuffers(&fakePool{})
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

	b1 := rb.Get(2048)
	assert.Same(t, b1, b)
	assert.Equal(t, 2048, b1.Cap())
	assert.Zero(t, b1.Len())

	t.Run("as nil pointer", func(t *testing.T) {
		var rb *RequestBuffers
		b := rb.Get(1024)
		require.NotNil(t, b)
		assert.Equal(t, 1024, b.Cap())
		assert.Zero(t, b.Len())
	})
}

type fakePool struct {
	buffers []*bytes.Buffer
}

func (p *fakePool) Get() any {
	if len(p.buffers) > 0 {
		b := p.buffers[0]
		p.buffers = p.buffers[1:]
		return b
	}

	return bytes.NewBuffer(nil)
}

func (p *fakePool) Put(x any) {
	p.buffers = append(p.buffers, x.(*bytes.Buffer))
}
