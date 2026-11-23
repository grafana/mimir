// SPDX-License-Identifier: AGPL-3.0-only

package util

import (
	"bytes"
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"
)

func TestParseProtoReader_ZstdDoesNotLeakGoroutines(t *testing.T) {
	enc, err := zstd.NewWriter(nil)
	require.NoError(t, err)
	// Large, compressible payload that exceeds the max size, so that decoding stops before the end of the stream.
	body := enc.EncodeAll(bytes.Repeat([]byte("a"), 8<<20), nil)

	before := runtime.NumGoroutine()
	for i := 0; i < 20; i++ {
		_, err := ParseProtoReader(context.Background(), bytes.NewReader(body), 0, 1024, NewRequestBuffers(nil), &prompb.WriteRequest{}, Zstd)
		require.Error(t, err)
	}

	require.Eventually(t, func() bool {
		return runtime.NumGoroutine() <= before+2
	}, 5*time.Second, 50*time.Millisecond, "zstd decoders leaked goroutines: before=%d after=%d", before, runtime.NumGoroutine())
}
