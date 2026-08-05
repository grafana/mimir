// SPDX-License-Identifier: AGPL-3.0-only

// To run against a real S3 bucket, set S3_BENCH_BUCKET and S3_BENCH_REGION, and
// and populate benchBlocks in a git-ignored file Go file in this package.
//
//		AWS_PROFILE=<your-profile> S3_BENCH_BUCKET=<your-s3-bucket> S3_BENCH_REGION=<region> \
//		  go test ./pkg/storage/bucket/s3/
//	   	  -test.v -test.run='^$' -test.bench='^\QBenchmarkGetRangeS3\E$' -test.benchtime=1x
//
// Auth uses the standard AWS credential chain (env vars, ~/.aws/credentials, IAM role).
package s3

import (
	"context"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
	thanoss3 "github.com/thanos-io/objstore/providers/s3"
	"go.uber.org/atomic"
)

type indexFile struct {
	path      string
	sizeBytes int64
}

// benchBlocks is populated by bench_blocks_test.go (not committed).
// If empty, the benchmark skips. See bench_blocks_test.go.example for the format.
var benchBlocks []indexFile

const (
	readSize    = 1 << 20 // 1 MiB
	totalCalls  = 10_000  // ~150 seconds run time
	concurrency = 32

	// maxConnsPerHost caps the number of TCP connections to S3. 0 = unlimited.
	// 0 is the default on thanosgcs.DefaultConfig.HTTPConfig.
	maxConnsPerHost = 0
)

// countingTransport counts new TCP connections via a wrapped DialContext.
type countingTransport struct {
	base     http.RoundTripper
	newConns atomic.Int64
}

func (c *countingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return c.base.RoundTrip(r)
}

type benchCall struct {
	file indexFile
	off  int64
}

type benchResult struct {
	header time.Duration
}

// BenchmarkGetRangeS3 measures random 1 MiB GetRange header latency against real S3
// index files. It makes totalCalls requests across concurrency goroutines and reports:
//   - hdr latency:   time until GetRange returns (server response, any retry backoff)
//   - new_tcp_conns: TCP connections opened during the run
//
// Skips unless S3_BENCH_BUCKET is set and benchBlocks is non-empty.
func BenchmarkGetRangeS3(b *testing.B) {
	bucket := os.Getenv("S3_BENCH_BUCKET")
	if bucket == "" {
		b.Skip("set S3_BENCH_BUCKET to run against a real S3 bucket")
	}
	if len(benchBlocks) == 0 {
		b.Skip("benchBlocks is empty — fill in block paths in bench_blocks_test.go")
	}

	ctx := context.Background()

	region := os.Getenv("S3_BENCH_REGION")
	if region == "" {
		region = "us-east-1"
	}
	// Thanos's S3 provider (minio-go underneath) always requires an explicit endpoint.
	endpoint := os.Getenv("S3_BENCH_ENDPOINT")
	if endpoint == "" {
		endpoint = "s3." + region + ".amazonaws.com"
	}

	ct := &countingTransport{}
	httpCfg := thanoss3.DefaultConfig.HTTPConfig
	httpCfg.MaxConnsPerHost = maxConnsPerHost

	bucketCfg := thanoss3.Config{
		Bucket:     bucket,
		Endpoint:   endpoint,
		Region:     region,
		AWSSDKAuth: true, // standard AWS credential chain
		HTTPConfig: httpCfg,
	}
	bkt, err := thanoss3.NewBucketWithConfig(log.NewNopLogger(), bucketCfg, "bench",
		func(rt http.RoundTripper) http.RoundTripper {
			// Wrap DialContext on the base transport to count new TCP connections.
			// This fires once per TCP handshake, not per HTTP request or stream.
			if t, ok := rt.(*http.Transport); ok {
				orig := t.DialContext
				t.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
					ct.newConns.Add(1)
					return orig(ctx, network, addr)
				}
			}
			ct.base = rt
			return ct
		},
	)
	require.NoError(b, err)
	b.Cleanup(func() { _ = bkt.Close() })

	rng := rand.New(rand.NewSource(42))
	calls := make([]benchCall, totalCalls)
	for i := range calls {
		f := benchBlocks[rng.Intn(len(benchBlocks))]
		calls[i] = benchCall{f, rng.Int63n(f.sizeBytes - readSize)}
	}

	// drainCh receives response bodies from measurement goroutines immediately after
	// header time is stamped. Background drainers consume them so connections are
	// returned to the idle pool and subsequent requests don't dial.
	//
	// Buffer is bounded to concurrency so measurement goroutines block when all
	// drainers are busy, keeping simultaneous open connections bounded to
	// ~2×concurrency. An unbounded buffer lets measurement goroutines race ahead,
	// accumulating open connections and creating artificial S3 load.
	drainCh := make(chan io.ReadCloser, concurrency)
	var drainWg sync.WaitGroup
	for range concurrency {
		drainWg.Add(1)
		go func() {
			defer drainWg.Done()
			for rc := range drainCh {
				_, _ = io.Copy(io.Discard, rc)
				_ = rc.Close()
			}
		}()
	}

	results := make([]benchResult, totalCalls)
	work := make(chan int, totalCalls)
	for i := range calls {
		work <- i
	}
	close(work)

	b.ResetTimer()

	var wg sync.WaitGroup
	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range work {
				c := calls[i]

				start := time.Now()
				rc, err := bkt.GetRange(ctx, c.file.path, c.off, readSize)
				header := time.Since(start)
				if err != nil {
					b.Errorf("GetRange call %d: %v", i, err)
					continue
				}
				drainCh <- rc

				results[i] = benchResult{header: header}
			}
		}()
	}
	wg.Wait()
	close(drainCh)
	drainWg.Wait()

	b.StopTimer()

	headerSamples := make([]time.Duration, totalCalls)
	for i, r := range results {
		headerSamples[i] = r.header
	}

	reportDurationPercentiles(b, "hdr", headerSamples)
	b.ReportMetric(float64(ct.newConns.Load()), "new_tcp_conns")
}

func reportDurationPercentiles(b *testing.B, prefix string, samples []time.Duration) {
	b.Helper()
	if len(samples) == 0 {
		return
	}
	slices.Sort(samples)
	ms := func(p float64) float64 {
		return samples[int(float64(len(samples)-1)*p)].Seconds() * 1000
	}
	b.ReportMetric(ms(0.50), prefix+"_p50_ms")
	b.ReportMetric(ms(0.90), prefix+"_p90_ms")
	b.ReportMetric(ms(0.99), prefix+"_p99_ms")
	b.ReportMetric(ms(0.999), prefix+"_p999_ms")
	b.ReportMetric(samples[len(samples)-1].Seconds()*1000, prefix+"_max_ms")
}
