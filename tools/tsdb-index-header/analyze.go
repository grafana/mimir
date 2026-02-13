// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"log"
	"math"
	"sort"
)

// Data collection constants.
const (
	maxSamplesPerBucket  = 5
	maxDuplicateExamples = 10
	topNItems            = 20
)

// LabelStats holds statistics about label cardinality.
type LabelStats struct {
	TotalLabels      int
	TotalLabelValues int
	Labels           []LabelCardinality // sorted by cardinality descending
	CardinalityHist  map[string]int     // bucket -> count of labels
}

// LabelCardinality holds cardinality info for a single label.
type LabelCardinality struct {
	Name            string
	Cardinality     int
	TotalValueBytes int64 // total size of all label values in bytes
}

// SymbolStats holds statistics about the symbols table.
type SymbolStats struct {
	Count             int
	TotalLength       int64
	MinLength         int
	MaxLength         int
	LengthHistogram   map[string]int   // bucket -> count
	LengthSizes       map[string]int64 // bucket -> total bytes
	LongestSymbols    []string
	DuplicateCount    int      // number of duplicate symbols
	NotSortedCount    int      // number of symbols out of order (should be 0)
	DuplicateExamples []string // first few duplicate symbols for inspection
}

// LabelValueStats holds statistics about label value distribution.
type LabelValueStats struct {
	TotalValues     int
	TotalBytes      int64
	MinLength       int
	MaxLength       int
	LengthHistogram map[string]*LabelValueBucket
}

// LabelValueBucket holds statistics and samples for a length bucket.
type LabelValueBucket struct {
	Count      int
	TotalBytes int64
	Samples    []string
}

// MetricNameStats holds statistics about metric names that have a specific label.
type MetricNameStats struct {
	UniqueMetricNames int
	TopMetrics        []MetricCount // Top 10 by series count
}

// MetricCount holds the count of series for a metric name.
type MetricCount struct {
	Name        string
	SeriesCount int64
}

// analyzeSymbols iterates through all symbols and collects statistics.
func analyzeSymbols(ctx context.Context, analyzer IndexAnalyzer) *SymbolStats {
	iter, err := analyzer.SymbolsIterator(ctx)
	if err != nil {
		log.Fatalf("Failed to get symbols iterator: %v\n", err)
	}
	defer iter.Close()

	stats := &SymbolStats{
		MinLength:       math.MaxInt,
		LengthHistogram: make(map[string]int),
		LengthSizes:     make(map[string]int64),
	}

	// Track top N longest symbols using a sorted slice.
	type symbolWithLen struct {
		symbol string
		length int
	}
	var topLongest []symbolWithLen

	// Track previous symbol for duplicate/sort detection.
	var prevSymbol string
	isFirstSymbol := true

	for iter.Next() {
		sym := iter.At()

		// Check for duplicates and sort order (symbols should be sorted).
		if !isFirstSymbol {
			if sym == prevSymbol {
				stats.DuplicateCount++
				if len(stats.DuplicateExamples) < maxDuplicateExamples {
					stats.DuplicateExamples = append(stats.DuplicateExamples, sym)
				}
			} else if sym < prevSymbol {
				stats.NotSortedCount++
			}
		}
		prevSymbol = sym
		isFirstSymbol = false

		length := len(sym)
		stats.Count++
		stats.TotalLength += int64(length)

		if length < stats.MinLength {
			stats.MinLength = length
		}
		if length > stats.MaxLength {
			stats.MaxLength = length
		}

		// Update histogram.
		bucket := getLengthBucketName(length)
		stats.LengthHistogram[bucket]++
		stats.LengthSizes[bucket] += int64(length)

		// Track top N longest symbols.
		if len(topLongest) < topNItems || length > topLongest[len(topLongest)-1].length {
			topLongest = append(topLongest, symbolWithLen{symbol: sym, length: length})
			sort.Slice(topLongest, func(i, j int) bool {
				return topLongest[i].length > topLongest[j].length
			})
			if len(topLongest) > topNItems {
				topLongest = topLongest[:topNItems]
			}
		}
	}

	if err := iter.Err(); err != nil {
		log.Fatalf("Error iterating symbols: %v\n", err)
	}

	// Convert to string slice.
	for _, s := range topLongest {
		stats.LongestSymbols = append(stats.LongestSymbols, s.symbol)
	}

	if stats.Count == 0 {
		stats.MinLength = 0
	}

	return stats
}

// analyzeLabelCardinality analyzes label cardinality from the postings offset table.
func analyzeLabelCardinality(ctx context.Context, analyzer IndexAnalyzer) *LabelStats {
	labelNames, err := analyzer.LabelNames(ctx)
	if err != nil {
		log.Fatalf("Failed to get label names: %v\n", err)
	}

	stats := &LabelStats{
		Labels:          make([]LabelCardinality, 0, len(labelNames)),
		CardinalityHist: make(map[string]int),
	}

	for _, name := range labelNames {
		values, err := analyzer.LabelValues(ctx, name)
		if err != nil {
			log.Printf("Warning: failed to get values for label %q: %v\n", name, err)
			continue
		}

		stats.TotalLabels++
		cardinality := len(values)
		stats.TotalLabelValues += cardinality

		// Calculate total bytes of all label values.
		var totalBytes int64
		for _, v := range values {
			totalBytes += int64(len(v))
		}

		stats.Labels = append(stats.Labels, LabelCardinality{
			Name:            name,
			Cardinality:     cardinality,
			TotalValueBytes: totalBytes,
		})

		// Update histogram.
		bucket := getCardinalityBucketName(cardinality)
		stats.CardinalityHist[bucket]++
	}

	// Sort by cardinality descending.
	sort.Slice(stats.Labels, func(i, j int) bool {
		return stats.Labels[i].Cardinality > stats.Labels[j].Cardinality
	})

	return stats
}

// analyzeLabelValues analyzes the value distribution for a specific label.
// Returns nil if the label is not found, has no values, or on error.
func analyzeLabelValues(ctx context.Context, analyzer IndexAnalyzer, labelName string) *LabelValueStats {
	values, err := analyzer.LabelValues(ctx, labelName)
	if err != nil {
		log.Printf("Failed to get values for label %q: %v\n", labelName, err)
		return nil
	}

	if len(values) == 0 {
		return nil
	}

	stats := &LabelValueStats{
		TotalValues:     len(values),
		MinLength:       math.MaxInt,
		LengthHistogram: make(map[string]*LabelValueBucket),
	}

	for _, v := range values {
		length := len(v)
		stats.TotalBytes += int64(length)

		if length < stats.MinLength {
			stats.MinLength = length
		}
		if length > stats.MaxLength {
			stats.MaxLength = length
		}

		// Update histogram.
		bucket := getLengthBucketName(length)
		if stats.LengthHistogram[bucket] == nil {
			stats.LengthHistogram[bucket] = &LabelValueBucket{}
		}
		b := stats.LengthHistogram[bucket]
		b.Count++
		b.TotalBytes += int64(length)

		// Collect samples.
		if len(b.Samples) < maxSamplesPerBucket {
			b.Samples = append(b.Samples, v)
		}
	}

	return stats
}

// analyzeMetricNamesForLabel analyzes which metric names have the given label.
// Returns nil if the analyzer doesn't support series iteration (e.g., index-headers).
func analyzeMetricNamesForLabel(ctx context.Context, analyzer IndexAnalyzer, labelName string) *MetricNameStats {
	iter := analyzer.SeriesWithLabel(ctx, labelName)
	if iter == nil {
		return nil // Not supported (e.g., index-header format)
	}

	metricCounts := make(map[string]int64)
	for iter.Next() {
		name := iter.Labels().Get("__name__")
		if name != "" {
			metricCounts[name]++
		}
	}

	if err := iter.Err(); err != nil {
		log.Printf("Warning: error iterating series for label %q: %v\n", labelName, err)
	}

	// Convert to slice and sort by count descending.
	metrics := make([]MetricCount, 0, len(metricCounts))
	for name, count := range metricCounts {
		metrics = append(metrics, MetricCount{Name: name, SeriesCount: count})
	}
	sort.Slice(metrics, func(i, j int) bool {
		return metrics[i].SeriesCount > metrics[j].SeriesCount
	})

	// Take top 10.
	const topMetricNames = 10
	if len(metrics) > topMetricNames {
		metrics = metrics[:topMetricNames]
	}

	return &MetricNameStats{
		UniqueMetricNames: len(metricCounts),
		TopMetrics:        metrics,
	}
}
