// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"log"
	"math"
	"sort"

	"github.com/grafana/mimir/pkg/storage/indexheader"
)

// Data collection constants.
const (
	maxSamplesPerBucket  = 5
	maxDuplicateExamples = 10
	topNItems            = 20
)

// TOCInfo holds table of contents information.
type TOCInfo struct {
	IndexHeaderSize         int64
	IndexHeaderVersion      int
	TSDBIndexVersion        int
	SymbolsSize             uint64
	PostingsOffsetTableSize uint64
}

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

// analyzeTOC analyzes the table of contents of the index-header.
func analyzeTOC(ctx context.Context, reader *indexheader.StreamBinaryReader, indexHeaderSize int64) *TOCInfo {
	toc := reader.TOC()
	indexVersion, _ := reader.IndexVersion(ctx)
	return &TOCInfo{
		IndexHeaderSize:         indexHeaderSize,
		IndexHeaderVersion:      reader.IndexHeaderVersion(),
		TSDBIndexVersion:        indexVersion,
		SymbolsSize:             toc.PostingsOffsetTable - toc.Symbols,
		PostingsOffsetTableSize: uint64(indexHeaderSize) - indexheader.BinaryTOCLen - toc.PostingsOffsetTable,
	}
}

// analyzeSymbols iterates through all symbols and collects statistics.
func analyzeSymbols(ctx context.Context, reader *indexheader.StreamBinaryReader) *SymbolStats {
	symbolsReader, err := reader.SymbolsReader(ctx)
	if err != nil {
		log.Fatalf("Failed to get symbols reader: %v\n", err)
	}
	defer symbolsReader.Close()

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

	for i := uint32(0); ; i++ {
		sym, err := symbolsReader.Read(i)
		if err != nil {
			// End of symbols.
			break
		}

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
func analyzeLabelCardinality(ctx context.Context, reader *indexheader.StreamBinaryReader) *LabelStats {
	labelNames, err := reader.LabelNames(ctx)
	if err != nil {
		log.Fatalf("Failed to get label names: %v\n", err)
	}

	stats := &LabelStats{
		TotalLabels:     len(labelNames),
		Labels:          make([]LabelCardinality, 0, len(labelNames)),
		CardinalityHist: make(map[string]int),
	}

	for _, name := range labelNames {
		values, err := reader.LabelValuesOffsets(ctx, name, "", nil)
		if err != nil {
			log.Printf("Warning: failed to get values for label %q: %v\n", name, err)
			continue
		}

		cardinality := len(values)
		stats.TotalLabelValues += cardinality

		// Calculate total bytes of all label values.
		var totalBytes int64
		for _, v := range values {
			totalBytes += int64(len(v.LabelValue))
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
func analyzeLabelValues(ctx context.Context, reader *indexheader.StreamBinaryReader, labelName string) *LabelValueStats {
	values, err := reader.LabelValuesOffsets(ctx, labelName, "", nil)
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
		length := len(v.LabelValue)
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
			b.Samples = append(b.Samples, v.LabelValue)
		}
	}

	return stats
}
