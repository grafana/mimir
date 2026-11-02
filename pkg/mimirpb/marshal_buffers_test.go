// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDirtyBufferPool(t *testing.T) {
	var pool dirtyBufferPool
	buf := pool.Get(3000)
	require.Len(t, *buf, 3000)
	require.Equal(t, 4096, cap(*buf), "the next power of two")
	for i := range *buf {
		(*buf)[i] = 0xff
	}
	pool.Put(buf)
	// The buffer comes back as it was left, which is why the codec checks a message wrote all of its size.
	again := pool.Get(2500)
	require.Len(t, *again, 2500)
	require.Equal(t, 4096, cap(*again))

	require.Equal(t, 1<<minDirtyBufferShift, cap(*pool.Get(1)), "small sizes share the smallest class")
	large := pool.Get(1<<maxDirtyBufferShift + 1)
	require.Len(t, *large, 1<<maxDirtyBufferShift+1)
	// Not a pooled class: dropped, not kept.
	pool.Put(large)
	odd := make([]byte, 3000)
	pool.Put(&odd)
	require.Equal(t, 4096, cap(*pool.Get(3000)), "a buffer of another capacity isn't pooled")
}

// shortMessage claims a size it then doesn't write all of.
type shortMessage struct{ size, written int }

func (m shortMessage) Size() int { return m.size }

func (m shortMessage) MarshalToSizedBuffer(b []byte) (int, error) {
	for i := len(b) - m.written; i < len(b); i++ {
		b[i] = 1
	}
	return m.written, nil
}

func TestCodecV2RefusesMessagesShorterThanTheirSize(t *testing.T) {
	codec := &codecV2{}
	_, err := codec.Marshal(shortMessage{size: 4000, written: 3000})
	require.EqualError(t, err, "proto: marshaled 3000 bytes of a mimirpb.shortMessage sized 4000")

	data, err := codec.Marshal(shortMessage{size: 4000, written: 4000})
	require.NoError(t, err)
	require.Equal(t, 4000, data.Len())
	data.Free()
}
