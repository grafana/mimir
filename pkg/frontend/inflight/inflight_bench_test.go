// SPDX-License-Identifier: AGPL-3.0-only

package inflight

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func newBenchCollector() *MaxInflightCollector {
	return NewMaxInflightCollector(testType)
}

// BenchmarkAddRemove measures what the tracker adds to the query admission path. The
// parallel form is the interesting one: Add and Remove share one mutex across all tenants,
// so this is the contention a query-frontend would see.
func BenchmarkAddRemove(b *testing.B) {
	for _, tenants := range []int{1, 100} {
		b.Run(fmt.Sprintf("tenants=%d", tenants), func(b *testing.B) {
			c := newBenchCollector()
			ids := make([]string, tenants)
			for i := range ids {
				ids[i] = strconv.Itoa(i)
			}

			b.ReportAllocs()
			b.ResetTimer()

			var n int
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					n++
					c.Remove(c.Add(ids[n%tenants]))
				}
			})
		})
	}
}

// BenchmarkCollect measures the scrape-time scan. It runs while queries are in flight,
// because Collect walks every in-flight entry to fold in its current age, and it holds the
// same lock that query admission needs.
func BenchmarkCollect(b *testing.B) {
	for _, inflight := range []int{0, 100, 1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("inflight=%d", inflight), func(b *testing.B) {
			c := newBenchCollector()
			for i := range inflight {
				c.Add(strconv.Itoa(i % 100))
			}

			ch := make(chan prometheus.Metric, 1024)
			go func() {
				for range ch {
				}
			}()
			defer close(ch)

			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				c.Collect(ch)
			}
		})
	}
}
