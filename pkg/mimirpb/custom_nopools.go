// SPDX-License-Identifier: AGPL-3.0-only

//go:build nopools

package mimirpb

import (
	"google.golang.org/grpc/mem"
)

func materializeBufferSlice(data mem.BufferSlice) mem.Buffer {
	return mem.SliceBuffer(data.Materialize())
}
