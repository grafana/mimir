// SPDX-License-Identifier: AGPL-3.0-only

//go:build !nopools

package mimirpb

import (
	"google.golang.org/grpc/mem"
)

// mem.DefaultBufferPool() only has five sizes: 256 B, 4 KB, 16 KB, 32 KB and 1 MB.
// This means that for messages between 32 KB and 1 MB, we may over-allocate by up to 992 KB,
// or ~97%. If we have a lot of messages in this range, we can waste a lot of memory.
// So instead, we create our own buffer pool with more sizes to reduce this wasted space, and
// also include pools for larger sizes up to 256 MB.
var unmarshalSlicePool = newUnmarshalSlicePool()

func newUnmarshalSlicePool() mem.BufferPool {
	var exponents []uint8
	for exponent := uint8(8); exponent <= 28; exponent++ {
		exponents = append(exponents, exponent)
	}

	pool, err := mem.NewBinaryTieredBufferPool(exponents...)
	if err != nil {
		// The fixed exponents above are valid on all supported architectures.
		panic(err)
	}
	return pool
}

func materializeBufferSlice(data mem.BufferSlice) mem.Buffer {
	return data.MaterializeToBuffer(unmarshalSlicePool)
}
