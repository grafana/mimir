// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/tsdb/index/postingsstats_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package index

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPostingsStats(t *testing.T) {
	stats := &maxHeap{}
	max := 3000000
	heapLength := 10
	stats.init(heapLength)
	for i := 0; i < max; i++ {
		item := Stat{
			Name:  "Label-da",
			Count: uint64(i),
		}
		stats.push(item)
	}
	stats.push(Stat{Name: "Stuff", Count: 3000000})

	data := stats.get()
	require.Equal(t, 10, len(data))
	for i := 0; i < heapLength; i++ {
		require.Equal(t, uint64(max-i), data[i].Count)
	}
}

func TestPostingsStats2(t *testing.T) {
	stats := &maxHeap{}
	heapLength := 10

	stats.init(heapLength)
	stats.push(Stat{Name: "Stuff", Count: 10})
	stats.push(Stat{Name: "Stuff", Count: 11})
	stats.push(Stat{Name: "Stuff", Count: 1})
	stats.push(Stat{Name: "Stuff", Count: 6})

	data := stats.get()

	require.Equal(t, 4, len(data))
	require.Equal(t, uint64(11), data[0].Count)
}

func BenchmarkPostingStatsMaxHep(b *testing.B) {
	stats := &maxHeap{}
	max := 9000000
	heapLength := 10
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		stats.init(heapLength)
		for i := 0; i < max; i++ {
			item := Stat{
				Name:  "Label-da",
				Count: uint64(i),
			}
			stats.push(item)
		}
		stats.get()
	}
}
