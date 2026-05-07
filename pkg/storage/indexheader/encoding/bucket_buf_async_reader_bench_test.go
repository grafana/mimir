// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
)

// latentBucket wraps a BucketReader and injects a fixed delay into every GetRange call,
// simulating the round-trip latency of a remote object store. Read latency is unaffected
// once the body stream is opened — the cost is paid once per range request, which is
// what dominates real-world performance and what readahead is meant to hide.
type latentBucket struct {
	objstore.InstrumentedBucketReader
	latency time.Duration
}

func (b *latentBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(b.latency):
	}
	return b.InstrumentedBucketReader.GetRange(ctx, name, off, length)
}

func newLatentBucket(b *testing.B, objectData []byte, latency time.Duration) *latentBucket {
	b.Helper()
	inmem := objstore.NewInMemBucket()
	require.NoError(b, inmem.Upload(context.Background(), testBucketObjectName, bytes.NewReader(objectData)))
	return &latentBucket{
		InstrumentedBucketReader: objstore.WithNoopInstr(inmem),
		latency:                  latency,
	}
}

// benchBufPoolSize is sized to the async chunk so the bufio buffer drains in roughly
// one chunk and we don't add a second layer of small reads on top of the async pipeline.
const benchBufPoolSize = asyncReadAheadChunkSize

var benchBufPool = sync.Pool{
	New: func() any {
		return bufio.NewReaderSize(nil, benchBufPoolSize)
	},
}

// benchObjectData returns a deterministic byte slice of the requested size.
func benchObjectData(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = testBucketContents[i%len(testBucketContents)]
	}
	return data
}

// benchReadSizes covers ranges from 1 MiB up to 64 MiB.
var benchReadSizes = []int{
	1 << 20,
	4 << 20,
	16 << 20,
	64 << 20,
}

const benchLatency = 10 * time.Millisecond

// benchReadChunk is the per-call read size used to drive the readers — small enough that
// many calls are required, so the difference between sync and async fetches is visible.
const benchReadChunk = 128 << 10

func BenchmarkBucketBufReader_Sequential(b *testing.B) {
	for _, size := range benchReadSizes {
		b.Run(fmt.Sprintf("size=%dMiB", size>>20), func(b *testing.B) {
			data := benchObjectData(size)
			bkt := newLatentBucket(b, data, benchLatency)
			ctx := context.Background()

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				r := newBucketBufReader(ctx, &benchBufPool, bkt, testBucketObjectName, 0, size)
				readAll(b, r.ReadInto, size)
				_ = r.Close()
			}
		})
	}
}

func BenchmarkBucketBufAsyncReader_Sequential(b *testing.B) {
	for _, size := range benchReadSizes {
		b.Run(fmt.Sprintf("size=%dMiB", size>>20), func(b *testing.B) {
			data := benchObjectData(size)
			bkt := newLatentBucket(b, data, benchLatency)
			ctx := context.Background()

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				r := newBucketBufAsyncReader(ctx, &benchBufPool, bkt, testBucketObjectName, 0, size)
				readAll(b, r.ReadInto, size)
				_ = r.Close()
			}
		})
	}
}

// readAll drains a reader of total bytes by repeatedly calling readInto with a fixed
// chunk size. The last read is sized to the remaining bytes so we don't overshoot.
func readAll(b *testing.B, readInto func([]byte) error, total int) {
	b.Helper()
	buf := make([]byte, benchReadChunk)
	remaining := total
	for remaining > 0 {
		n := benchReadChunk
		if n > remaining {
			n = remaining
			buf = buf[:n]
		}
		if err := readInto(buf); err != nil {
			b.Fatalf("read failed at remaining=%d: %v", remaining, err)
		}
		remaining -= n
	}
}
