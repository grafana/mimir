// SPDX-License-Identifier: AGPL-3.0-only

// To run against a real GCS bucket, set GCS_BENCH_BUCKET
// and populate benchBlocks in a git-ignored Go file in this package.
//
//	GCS_BENCH_BUCKET=<your-gcs-bucket> \
//	  go test ./pkg/storage/bucket/gcs/ \
//	  -test.v -test.run='^$' -test.bench='^\QBenchmarkGetRangeGCS\E$' -test.benchtime=1x
//
// The bucket client is built through the same newBucketConfig path Mimir ships, so the
// measured configuration is the shipped configuration. Environment variables tweak the
// client and load shape, defaulting to the shipped Mimir defaults:
//
//	BENCH_HTTP2                    enable HTTP/2 (default: the -gcs.http2-enabled flag default)
//	BENCH_CONCURRENCY              concurrent GetRange goroutines (default 32)
//	BENCH_TOTAL_CALLS              total GetRange calls (default 10000, ~150s run time)
//	BENCH_MAX_CONNS_PER_HOST       cap on TCP connections to GCS, 0 = unlimited (default 0)
//	BENCH_MAX_IDLE_CONNS_PER_HOST  idle (keep-alive) connection pool size per host (default 100)
//	BENCH_HEDGE_AT                 hedge GETs slower than this duration, e.g. 750ms; 0 = disabled
//	BENCH_HEDGE_UP_TO              max total attempts per GET when hedging (default 2)
//
// Auth uses the standard GCP credential chain.
package gcs

import (
	"context"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/stretchr/testify/require"
	thanosgcs "github.com/thanos-io/objstore/providers/gcs"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/storage/bucket/common"
)

// indexFile describes one TSDB block index object in GCS.
type indexFile struct {
	path      string
	sizeBytes int64
}

// benchBlocks is populated by gcs_blocks_bench_test.go (not committed).
// If empty, the benchmark skips.
var benchBlocks []indexFile

const readSize = 1 << 20 // 1 MiB

// countingTransport counts new TCP connections via a wrapped DialContext.
type countingTransport struct {
	base     http.RoundTripper
	newConns atomic.Int64
}

func (c *countingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return c.base.RoundTrip(r)
}

// benchCall is a pre-computed (file, offset) pair for one GetRange call.
type benchCall struct {
	file indexFile
	off  int64
}

// benchResult holds the measurements from one GetRange call.
type benchResult struct {
	header time.Duration
}

// BenchmarkGetRangeGCS measures random 1 MiB GetRange header latency against real GCS
// index files. It makes BENCH_TOTAL_CALLS requests across BENCH_CONCURRENCY goroutines
// and reports:
//   - hdr latency:    time until GetRange returns (server response, any retry backoff)
//   - new_tcp_conns:  TCP connections opened during the run
//
// Skips unless GCS_BENCH_BUCKET is set and benchBlocks is populated.
func BenchmarkGetRangeGCS(b *testing.B) {
	bucket := os.Getenv("GCS_BENCH_BUCKET")
	if bucket == "" {
		b.Skip("set GCS_BENCH_BUCKET to run against a real GCS bucket")
	}
	if len(benchBlocks) == 0 {
		b.Skip("benchBlocks is empty — populate gcs_blocks_bench_test.go")
	}

	totalCalls := envInt(b, "BENCH_TOTAL_CALLS", 10_000)
	concurrency := envInt(b, "BENCH_CONCURRENCY", 32)

	ctx := context.Background()

	cfg := Config{}
	flagext.DefaultValues(&cfg)
	cfg.BucketName = bucket
	cfg.HTTP2Enabled = envBool(b, "BENCH_HTTP2", cfg.HTTP2Enabled)
	cfg.HTTP.MaxConnsPerHost = envInt(b, "BENCH_MAX_CONNS_PER_HOST", cfg.HTTP.MaxConnsPerHost)
	cfg.HTTP.MaxIdleConnsPerHost = envInt(b, "BENCH_MAX_IDLE_CONNS_PER_HOST", cfg.HTTP.MaxIdleConnsPerHost)
	cfg.HedgeRequestsAt = envDuration(b, "BENCH_HEDGE_AT", cfg.HedgeRequestsAt)
	cfg.HedgeRequestsUpTo = envInt(b, "BENCH_HEDGE_UP_TO", cfg.HedgeRequestsUpTo)

	bucketConfig, err := newBucketConfig(cfg)
	require.NoError(b, err)

	ct := &countingTransport{}
	bkt, err := thanosgcs.NewBucketWithConfig(ctx, log.NewNopLogger(), bucketConfig, "bench",
		func(rt http.RoundTripper) http.RoundTripper {
			// Wrap DialContext on the base transport to count new TCP connections.
			// This fires once per TCP handshake, not per HTTP request or stream.
			// HTTP/2 connections are dialed through the same DialContext, so the count
			// covers both protocols. The mutation is in place, so it also applies when
			// the base transport sits inside the hedging round tripper.
			base := rt
			if hrt, ok := base.(*common.HedgingRoundTripper); ok {
				base = hrt.Next
			}
			if t, ok := base.(*http.Transport); ok {
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

	// Pre-compute all calls with a fixed seed so results are reproducible.
	rng := rand.New(rand.NewSource(42))
	calls := make([]benchCall, totalCalls)
	for i := range calls {
		f := benchBlocks[rng.Intn(len(benchBlocks))]
		calls[i] = benchCall{f, rng.Int63n(f.sizeBytes - readSize)}
	}

	// drainCh receives response bodies from measurement goroutines immediately after
	// header time is stamped. Background drainers consume them so HTTP/2 streams are
	// released and the connection remains available for new streams.
	//
	// Buffer is bounded to concurrency so measurement goroutines block when all
	// drainers are busy, keeping simultaneous open HTTP/2 streams bounded to
	// ~2×concurrency. An unbounded buffer lets measurement goroutines race ahead,
	// accumulating thousands of open streams and creating artificial GCS load.
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

// envInt returns the integer value of the named environment variable, or def if unset.
func envInt(b *testing.B, name string, def int) int {
	b.Helper()
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		b.Fatalf("invalid %s value %q: %v", name, v, err)
	}
	return i
}

// envDuration returns the duration value of the named environment variable, or def if unset.
func envDuration(b *testing.B, name string, def time.Duration) time.Duration {
	b.Helper()
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		b.Fatalf("invalid %s value %q: %v", name, v, err)
	}
	return d
}

// envBool returns the boolean value of the named environment variable, or def if unset.
func envBool(b *testing.B, name string, def bool) bool {
	b.Helper()
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	bv, err := strconv.ParseBool(v)
	if err != nil {
		b.Fatalf("invalid %s value %q: %v", name, v, err)
	}
	return bv
}
