package main

import (
	"fmt"
	"hash/fnv"
	"math"
	"math/rand"
	"sort"
)

func nautilusHash(metricName string, labelHash uint32, metricBits int) uint32 {
	h := fnv.New32a()
	h.Write([]byte(metricName))
	mh := h.Sum32()
	labelBits := uint(32 - metricBits)
	metricMask := uint32(0xFFFFFFFF) << labelBits
	labelMask := ^metricMask
	return (mh & metricMask) | (labelHash & labelMask)
}

func generateSyntheticData(cfg Config, rng *rand.Rand) ([]MetricInfo, []Series) {
	numMetrics := cfg.NumSyntheticMetrics
	numSeries := cfg.NumSyntheticSeries

	metrics := make([]MetricInfo, numMetrics)
	for i := range metrics {
		metrics[i] = MetricInfo{
			Name: fmt.Sprintf("metric_%05d", i),
		}
	}

	iw := zipfWeights(numMetrics, 1.2, rng)
	qw := zipfWeights(numMetrics, 1.2, rng)
	for i := range metrics {
		metrics[i].IngestionWeight = iw[i]
		metrics[i].QueryWeight = qw[i]
	}

	counts := distributeWithSkew(numSeries, numMetrics, cfg.SkewTopMetricFraction, rng)

	series := make([]Series, 0, numSeries)
	for mi, c := range counts {
		for j := 0; j < c; j++ {
			series = append(series, Series{
				MetricIndex: mi,
				Hash:        nautilusHash(metrics[mi].Name, rng.Uint32(), cfg.MetricHashBits),
			})
		}
	}
	sort.Slice(series, func(i, j int) bool { return series[i].Hash < series[j].Hash })

	return metrics, series
}

func distributeWithSkew(total, n int, topFraction float64, rng *rand.Rand) []int {
	counts := make([]int, n)
	if n == 0 || total == 0 {
		return counts
	}

	topCount := int(float64(total) * topFraction)
	counts[0] = topCount
	remaining := total - topCount

	if n == 1 || remaining <= 0 {
		counts[0] = total
		return counts
	}

	rest := n - 1
	if remaining < rest {
		for i := 0; i < remaining; i++ {
			counts[i+1] = 1
		}
		return counts
	}

	weights := make([]float64, rest)
	totalW := 0.0
	for i := range weights {
		weights[i] = 1.0 / math.Pow(float64(i+1), 1.5)
		totalW += weights[i]
	}

	allocated := 0
	for i, w := range weights {
		c := int(math.Round(float64(remaining) * w / totalW))
		if c < 1 {
			c = 1
		}
		counts[i+1] = c
		allocated += c
	}

	for allocated > remaining {
		idx := rng.Intn(rest) + 1
		if counts[idx] > 1 {
			counts[idx]--
			allocated--
		}
	}
	for allocated < remaining {
		idx := rng.Intn(rest) + 1
		counts[idx]++
		allocated++
	}

	return counts
}

func reshuffleWeights(metrics []MetricInfo, rng *rand.Rand) {
	n := len(metrics)
	iw := zipfWeights(n, 1.2, rng)
	qw := zipfWeights(n, 1.2, rng)
	for i := range metrics {
		metrics[i].IngestionWeight = iw[i]
		metrics[i].QueryWeight = qw[i]
	}
}

func zipfWeights(n int, exponent float64, rng *rand.Rand) []float64 {
	w := make([]float64, n)
	for i := range w {
		w[i] = 1.0 / math.Pow(float64(i+1), exponent)
	}
	rng.Shuffle(len(w), func(i, j int) { w[i], w[j] = w[j], w[i] })
	return w
}
