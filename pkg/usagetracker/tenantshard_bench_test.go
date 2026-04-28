// SPDX-License-Identifier: AGPL-3.0-only

package usagetracker

import (
	"fmt"
	"math/rand"
	"testing"

	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/usagetracker/clock"
	"github.com/grafana/mimir/pkg/usagetracker/tenantshard"
)

func BenchmarkTenantShard(b *testing.B) {
	for _, totalSeries := range []int{1e6, 10e6, 100e6} {
		b.Run(fmt.Sprintf("totalSeries=%d", totalSeries), func(b *testing.B) {
			series := make([]uint64, totalSeries)
			for i := range series {
				series[i] = rand.Uint64() << 7
			}

			m := tenantshard.New(uint32(len(series)))
			benchmarkWithSeries(b, m, series)
		})
	}
}

type tenantShardMap interface {
	Put(key uint64, value clock.Minutes, series, limit *atomic.Uint64, track bool) (created, rejected bool)
}

func benchmarkWithSeries[T tenantShardMap](b *testing.B, m T, keys []uint64) {
	series := atomic.NewUint64(0)
	limit := atomic.NewUint64(uint64(2 * len(keys)))
	minutes := clock.Minutes(0)
	for _, s := range keys {
		_, _ = m.Put(s, minutes, series, limit, true)
		if s%32 == 0 {
			minutes++
			if minutes >= 120 {
				minutes = 0
			}
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, s := range keys {
			_, _ = m.Put(s, minutes, series, limit, true)
			if s%32 == 0 {
				minutes++
				if minutes >= 120 {
					minutes = 0
				}
			}
		}
	}
}
