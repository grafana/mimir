// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/chunk/util/parallel_chunk_fetch_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package util

import (
	"context"
	"testing"

	"github.com/grafana/mimir/pkg/chunk"
)

func BenchmarkGetParallelChunks(b *testing.B) {
	ctx := context.Background()
	in := make([]chunk.Chunk, 1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := GetParallelChunks(ctx, in,
			func(_ context.Context, d *chunk.DecodeContext, c chunk.Chunk) (chunk.Chunk, error) {
				return c, nil
			})
		if err != nil {
			b.Fatal(err)
		}
		if len(res) != len(in) {
			b.Fatal("unexpected number of chunk returned")
		}
	}
}
