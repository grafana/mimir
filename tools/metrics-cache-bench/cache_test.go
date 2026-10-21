//go:build unix

// SPDX-License-Identifier: AGPL-3.0-only

package metricscachebench

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// This prototype accepts one fixed representation per instance so untrusted
// request headers cannot grow the cache. Production routing must negotiate first.
type responseCache struct {
	mu      sync.Mutex
	next    http.Handler
	now     func() time.Time
	expires time.Time
	header  http.Header
	body    []byte
}

func (c *responseCache) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c.mu.Lock()
	if c.body == nil || !c.now().Before(c.expires) {
		start := c.now()
		rr := httptest.NewRecorder()
		c.next.ServeHTTP(rr, r)
		if rr.Code != http.StatusOK {
			c.mu.Unlock()
			for k, v := range rr.Header() {
				w.Header()[k] = v
			}
			w.WriteHeader(rr.Code)
			_, _ = w.Write(rr.Body.Bytes())
			return
		}
		c.body = rr.Body.Bytes()
		c.header = rr.Header().Clone()
		c.expires = start.Add(15 * time.Second)
	}
	body, header := c.body, c.header
	c.mu.Unlock()
	for k, v := range header {
		w.Header()[k] = v
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

func registry(tenants int) *prometheus.Registry {
	reg := prometheus.NewRegistry()
	// Keep cardinality bounded while exercising per-tenant histogram collection.
	h := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "bench_request_bytes", Help: "Synthetic request sizes.", Buckets: []float64{math.Inf(1)}}, []string{"user", "handler"})
	reg.MustRegister(h)
	counters := make([]*prometheus.CounterVec, 15)
	for i := range counters {
		counters[i] = prometheus.NewCounterVec(prometheus.CounterOpts{Name: fmt.Sprintf("bench_tenant_events_%02d_total", i), Help: "Synthetic tenant events."}, []string{"user"})
		reg.MustRegister(counters[i])
	}
	for i := 0; i < tenants; i++ {
		user := fmt.Sprintf("synthetic-%08d", i)
		h.WithLabelValues(user, "push").Observe(float64(1024 + i%10000))
		for j, c := range counters {
			c.WithLabelValues(user).Add(float64((i + 1) * (j + 1)))
		}
	}
	return reg
}

func request(format, encoding string) *http.Request {
	r := httptest.NewRequest("GET", "/metrics", nil)
	switch format {
	case "text":
		r.Header.Set("Accept", "text/plain;version=0.0.4")
	case "openmetrics":
		r.Header.Set("Accept", "application/openmetrics-text;version=1.0.0")
	case "protobuf":
		r.Header.Set("Accept", "application/vnd.google.protobuf;proto=io.prometheus.client.MetricFamily;encoding=delimited")
	}
	r.Header.Set("Accept-Encoding", encoding)
	return r
}

type discardResponse struct{ header http.Header }

func (w *discardResponse) Header() http.Header         { return w.header }
func (w *discardResponse) WriteHeader(int)             {}
func (w *discardResponse) Write(p []byte) (int, error) { return len(p), nil }

func BenchmarkMetrics(b *testing.B) {
	for _, tenants := range []int{1000, 8000} {
		reg := registry(tenants)
		for _, format := range []string{"text", "openmetrics", "protobuf"} {
			for _, encoding := range []string{"identity", "gzip"} {
				r := request(format, encoding)
				handler := promhttp.HandlerFor(reg, promhttp.HandlerOpts{EnableOpenMetrics: true})
				for _, mode := range []string{"uncached", "cached_20_per_minute", "cached_hits"} {
					b.Run(fmt.Sprintf("tenants=%d/%s/%s/%s", tenants, format, encoding, mode), func(b *testing.B) {
						var h http.Handler = handler
						now := time.Unix(0, 0)
						if mode != "uncached" {
							c := &responseCache{next: handler, now: func() time.Time { return now }}
							c.ServeHTTP(&discardResponse{header: make(http.Header)}, r)
							h = c
						}
						w := &discardResponse{header: make(http.Header)}
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							if mode == "cached_20_per_minute" {
								now = now.Add(3 * time.Second)
							}
							h.ServeHTTP(w, r)
						}
					})
				}
			}
		}
	}
}

func TestCache(t *testing.T) {
	reg := registry(20)
	for _, format := range []string{"text", "openmetrics", "protobuf"} {
		for _, encoding := range []string{"identity", "gzip"} {
			t.Run(format+"/"+encoding, func(t *testing.T) {
				r := request(format, encoding)
				h := promhttp.HandlerFor(reg, promhttp.HandlerOpts{EnableOpenMetrics: true})
				var calls atomic.Int64
				now := time.Unix(0, 0)
				c := &responseCache{next: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); h.ServeHTTP(w, r) }), now: func() time.Time { return now }}
				first := httptest.NewRecorder()
				c.ServeHTTP(first, r)
				baseline := httptest.NewRecorder()
				h.ServeHTTP(baseline, r)
				if !bytes.Equal(first.Body.Bytes(), baseline.Body.Bytes()) || first.Header().Get("Content-Type") != baseline.Header().Get("Content-Type") || first.Header().Get("Content-Encoding") != baseline.Header().Get("Content-Encoding") {
					t.Fatal("response differs from uncached handler")
				}
				if encoding == "gzip" {
					z, err := gzip.NewReader(bytes.NewReader(first.Body.Bytes()))
					if err != nil {
						t.Fatal(err)
					}
					if _, err = io.Copy(io.Discard, z); err != nil {
						t.Fatal(err)
					}
					if err = z.Close(); err != nil {
						t.Fatal(err)
					}
				}
				now = now.Add(14 * time.Second)
				hit := httptest.NewRecorder()
				c.ServeHTTP(hit, r)
				if calls.Load() != 1 || !bytes.Equal(first.Body.Bytes(), hit.Body.Bytes()) {
					t.Fatal("cache miss before TTL")
				}
				now = now.Add(time.Second)
				var wg sync.WaitGroup
				for i := 0; i < 20; i++ {
					wg.Add(1)
					go func() { defer wg.Done(); c.ServeHTTP(httptest.NewRecorder(), r) }()
				}
				wg.Wait()
				if calls.Load() != 2 {
					t.Fatalf("want one refresh, got %d total gathers", calls.Load())
				}
			})
		}
	}
	t.Run("errors_not_cached", func(t *testing.T) {
		calls := 0
		c := &responseCache{next: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { calls++; http.Error(w, "failed", 500) }), now: time.Now}
		for i := 0; i < 2; i++ {
			w := httptest.NewRecorder()
			c.ServeHTTP(w, request("text", "identity"))
			if w.Code != 500 {
				t.Fatal(w.Code)
			}
		}
		if calls != 2 {
			t.Fatal("cached an error")
		}
	})
}

func TestFixture(t *testing.T) {
	r := registry(8000)
	h := promhttp.HandlerFor(r, promhttp.HandlerOpts{EnableOpenMetrics: true})
	for _, format := range []string{"text", "openmetrics", "protobuf"} {
		for _, encoding := range []string{"identity", "gzip"} {
			w := httptest.NewRecorder()
			h.ServeHTTP(w, request(format, encoding))
			if w.Code != 200 {
				t.Fatal(w.Code)
			}
			t.Logf("%s/%s: body=%d bytes, buffer-capacity=%d bytes, content-type=%s", format, encoding, w.Body.Len(), w.Body.Cap(), w.Header().Get("Content-Type"))
			if format == "text" && encoding == "identity" {
				samples := 0
				for _, line := range strings.Split(w.Body.String(), "\n") {
					if line != "" && !strings.HasPrefix(line, "#") {
						samples++
					}
				}
				if samples != 144000 {
					t.Fatalf("expected 144000 samples, got %d", samples)
				}
			}
		}
	}
}

// Loopback HTTP includes socket transfer and client reads so cache-hit timings
// do not reduce to writing a byte slice to a discard writer.
func BenchmarkMetricsHTTP(b *testing.B) {
	reg := registry(8000)
	for _, encoding := range []string{"identity", "gzip"} {
		for _, mode := range []string{"uncached", "cached_20_per_minute"} {
			b.Run(encoding+"/"+mode, func(b *testing.B) {
				var h http.Handler = promhttp.HandlerFor(reg, promhttp.HandlerOpts{EnableOpenMetrics: true})
				var tick atomic.Int64
				if mode != "uncached" {
					h = &responseCache{next: h, now: func() time.Time { return time.Unix(0, tick.Load()) }}
				}
				srv := httptest.NewServer(h)
				defer srv.Close()
				transport := &http.Transport{DisableCompression: true}
				defer transport.CloseIdleConnections()
				client := &http.Client{Transport: transport, Timeout: 30 * time.Second}
				scrape := func() {
					r, err := http.NewRequest("GET", srv.URL+"/metrics", nil)
					if err != nil {
						b.Fatal(err)
					}
					r.Header.Set("Accept", "text/plain;version=0.0.4")
					r.Header.Set("Accept-Encoding", encoding)
					resp, err := client.Do(r)
					if err != nil {
						b.Fatal(err)
					}
					if resp.StatusCode != 200 {
						b.Fatal(resp.Status)
					}
					_, err = io.Copy(io.Discard, resp.Body)
					closeErr := resp.Body.Close()
					if err != nil {
						b.Fatal(err)
					}
					if closeErr != nil {
						b.Fatal(closeErr)
					}
				}
				scrape()
				b.ReportAllocs()
				b.ResetTimer()
				var before, after syscall.Rusage
				if err := syscall.Getrusage(syscall.RUSAGE_SELF, &before); err != nil {
					b.Fatal(err)
				}
				for i := 0; i < b.N; i++ {
					tick.Add(int64(3 * time.Second))
					scrape()
				}
				if err := syscall.Getrusage(syscall.RUSAGE_SELF, &after); err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				cpuNanos := func(r syscall.Rusage) int64 {
					return (r.Utime.Sec+r.Stime.Sec)*1e9 + (int64(r.Utime.Usec)+int64(r.Stime.Usec))*1000
				}
				b.ReportMetric(float64(cpuNanos(after)-cpuNanos(before))/float64(b.N), "cpu-ns/op")
			})
		}
	}
}

func TestCacheFreshness(t *testing.T) {
	reg := prometheus.NewRegistry()
	gauge := prometheus.NewGauge(prometheus.GaugeOpts{Name: "bench_value", Help: "Synthetic changing value."})
	reg.MustRegister(gauge)
	now := time.Unix(0, 0)
	c := &responseCache{next: promhttp.HandlerFor(reg, promhttp.HandlerOpts{}), now: func() time.Time { return now }}
	read := func() string {
		w := httptest.NewRecorder()
		c.ServeHTTP(w, request("text", "identity"))
		return w.Body.String()
	}
	gauge.Set(1)
	first := read()
	gauge.Set(2)
	now = now.Add(14 * time.Second)
	if read() != first {
		t.Fatal("changed cached response within TTL")
	}
	now = now.Add(time.Second)
	if !strings.Contains(read(), "bench_value 2\n") {
		t.Fatal("did not refresh changed value at TTL")
	}
}
