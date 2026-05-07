// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"runtime"
	"runtime/debug"
	"sync"
	"testing"
	"time"

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

func newLatentBucket(objectData []byte, latency time.Duration) *latentBucket {
	inmem := objstore.NewInMemBucket()
	if err := inmem.Upload(context.Background(), testBucketObjectName, bytes.NewReader(objectData)); err != nil {
		panic(err)
	}
	return &latentBucket{
		InstrumentedBucketReader: objstore.WithNoopInstr(inmem),
		latency:                  latency,
	}
}

// We pick pool sizes so both impls keep the same total bytes "in flight" per logical
// fetch unit (1 MiB):
//   - the sync reader does one GetRange per bufio fill, so it needs a 1 MiB buffer to
//     match the async reader's pipelined working set.
//   - the async reader's own pool is 128 KiB × asyncReadAheadMaxInFlight (8) = 1 MiB,
//     and the wrapping bufio buffer just needs to be large enough to consume one async
//     chunk per fill, so we size it to match the async chunk size.
const (
	benchSyncBufPoolSize  = 1 << 20
	benchAsyncBufPoolSize = asyncReadAheadChunkSize
)

var (
	benchSyncBufPool = sync.Pool{
		New: func() any {
			return bufio.NewReaderSize(nil, benchSyncBufPoolSize)
		},
	}
	benchAsyncBufPool = sync.Pool{
		New: func() any {
			return bufio.NewReaderSize(nil, benchAsyncBufPoolSize)
		},
	}
)

// benchReadSizes covers ranges from 1 MiB up to 1 GiB.
var benchReadSizes = []int{
	1 << 20,
	4 << 20,
	16 << 20,
	64 << 20,
	256 << 20,
	//1 << 30,
}

const benchLatency = 200 * time.Millisecond

// benchBuckets holds a pre-built latent bucket per benchmark read size, so the cost of
// allocating the underlying object data and uploading to the in-memory bucket does not
// pollute benchmark profiles.
var benchBuckets = map[int]*latentBucket{}

func init() {
	maxSize := 0
	for _, s := range benchReadSizes {
		if s > maxSize {
			maxSize = s
		}
	}
	data := make([]byte, maxSize)
	for i := range data {
		data[i] = testBucketContents[i%len(testBucketContents)]
	}
	for _, s := range benchReadSizes {
		benchBuckets[s] = newLatentBucket(data[:s], benchLatency)
	}
}

// disableGC turns off garbage collection for the duration of a benchmark so GC pauses do
// not show up in CPU profiles. It runs a final GC up front to start from a clean slate
// and restores the previous GOGC on cleanup.
func disableGC(b *testing.B) {
	b.Helper()
	runtime.GC()
	prev := debug.SetGCPercent(-1)
	b.Cleanup(func() { debug.SetGCPercent(prev) })
}

func BenchmarkBucketBufReader_Sequential(b *testing.B) {
	for _, size := range benchReadSizes {
		b.Run(fmt.Sprintf("size=%dMiB", size>>20), func(b *testing.B) {
			bkt := benchBuckets[size]
			ctx := context.Background()

			disableGC(b)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				r := newBucketBufReader(ctx, &benchSyncBufPool, bkt, testBucketObjectName, 0, size)
				readAll(b, r.ReadInto, size)
				_ = r.Close()
			}
		})
	}
}

func BenchmarkBucketBufAsyncReader_Sequential(b *testing.B) {
	for _, size := range benchReadSizes {
		b.Run(fmt.Sprintf("size=%dMiB", size>>20), func(b *testing.B) {
			bkt := benchBuckets[size]
			ctx := context.Background()

			disableGC(b)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				r := newBucketBufAsyncReader(ctx, &benchAsyncBufPool, bkt, testBucketObjectName, 0, size)
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
	buf := make([]byte, 128) // TODO do something like decbuf UnsafeUvarintBytes where it Peeks, then Skips
	remaining := total
	for remaining > 0 {
		n := 128
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
