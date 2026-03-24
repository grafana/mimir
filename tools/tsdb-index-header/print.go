// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"fmt"
	"io"

	"github.com/grafana/mimir/pkg/storage/indexheader"
)

// Display constants for printing.
const (
	displayTruncateLen = 80
)

// printIndexInfo prints information about the index.
func printIndexInfo(_ context.Context, w io.Writer, info *IndexInfo, _ IndexAnalyzer) {
	if info.IsIndexHeader {
		fmt.Fprintf(w, "Index type:                Index-Header\n")
		fmt.Fprintf(w, "Index-header size:         %s\n", formatBytes(info.Size))
		fmt.Fprintln(w, "Index-header version:     ", info.IndexHeaderVersion)
		fmt.Fprintln(w, "TSDB index version:       ", info.IndexVersion)
		fmt.Fprintln(w, "Header size:              ", indexheader.HeaderLen)
		fmt.Fprintf(w, "Symbols size:              %s\n", formatBytes(int64(info.SymbolsSize)))
		fmt.Fprintf(w, "Postings offset table:     %s\n", formatBytes(int64(info.PostingsTableSize)))
		fmt.Fprintln(w, "TOC + CRC32:              ", indexheader.BinaryTOCLen)
	} else {
		fmt.Fprintf(w, "Index type:                Full Index\n")
		fmt.Fprintf(w, "Index size:                %s\n", formatBytes(info.Size))
		fmt.Fprintln(w, "TSDB index version:       ", info.IndexVersion)
	}
}

// printSymbolStats prints the collected symbol statistics.
func printSymbolStats(_ context.Context, w io.Writer, stats *SymbolStats) {
	fmt.Fprintf(w, "Symbol count:              %d\n", stats.Count)
	if stats.Count > 0 {
		avgLength := float64(stats.TotalLength) / float64(stats.Count)
		fmt.Fprintf(w, "Total symbols length:      %s\n", formatBytes(stats.TotalLength))
		fmt.Fprintf(w, "Average symbol length:     %.2f bytes\n", avgLength)
		fmt.Fprintf(w, "Min symbol length:         %d bytes\n", stats.MinLength)
		fmt.Fprintf(w, "Max symbol length:         %d bytes\n", stats.MaxLength)
	}

	// Print sort and duplicate info.
	if stats.NotSortedCount == 0 {
		fmt.Fprintln(w, "Symbols sorted:            yes")
	} else {
		fmt.Fprintf(w, "Symbols sorted:            NO (%d out of order)\n", stats.NotSortedCount)
	}
	fmt.Fprintf(w, "Duplicate symbols:         %d\n", stats.DuplicateCount)
	if len(stats.DuplicateExamples) > 0 {
		fmt.Fprintln(w, "  Examples:")
		for _, sym := range stats.DuplicateExamples {
			fmt.Fprintf(w, "    %q\n", truncateString(sym, displayTruncateLen))
		}
	}

	fmt.Fprintln(w)
	fmt.Fprintln(w, "Length distribution:")
	for _, bucket := range lengthBucketNames() {
		count := stats.LengthHistogram[bucket]
		if count > 0 {
			size := stats.LengthSizes[bucket]
			pct := float64(count) / float64(stats.Count) * 100
			var sizePct float64
			if stats.TotalLength > 0 {
				sizePct = float64(size) / float64(stats.TotalLength) * 100
			}
			fmt.Fprintf(w, "  %8s: %10d symbols (%5.2f%%)  %12d bytes (%5.2f%%, %.2f MB)\n", bucket, count, pct, size, sizePct, bytesToMB(size))
		}
	}

	if len(stats.LongestSymbols) > 0 {
		fmt.Fprintln(w)
		fmt.Fprintf(w, "Top %d longest symbols:\n", topNItems)
		for i, sym := range stats.LongestSymbols {
			fmt.Fprintf(w, "  %2d. [%5d bytes] %s\n", i+1, len(sym), truncateString(sym, 100))
		}
	}
}

// printLabelStats prints the collected label cardinality statistics.
func printLabelStats(_ context.Context, w io.Writer, stats *LabelStats) {
	fmt.Fprintf(w, "Total label names:         %d\n", stats.TotalLabels)
	fmt.Fprintf(w, "Total label-value pairs:   %d\n", stats.TotalLabelValues)

	fmt.Fprintln(w)
	fmt.Fprintln(w, "Cardinality distribution:")
	for _, bucket := range cardinalityBucketNames() {
		count := stats.CardinalityHist[bucket]
		if count > 0 {
			pct := float64(count) / float64(stats.TotalLabels) * 100
			fmt.Fprintf(w, "  %9s: %5d labels (%5.2f%%)\n", bucket, count, pct)
		}
	}

	if len(stats.Labels) > 0 {
		fmt.Fprintln(w)
		fmt.Fprintf(w, "Top %d highest cardinality labels:\n", topNItems)
		limit := topNItems
		if len(stats.Labels) < limit {
			limit = len(stats.Labels)
		}
		for i := 0; i < limit; i++ {
			label := stats.Labels[i]
			fmt.Fprintf(w, "  %2d. [%8d values, %10d bytes (%.2f MB)] %s\n", i+1, label.Cardinality, label.TotalValueBytes, bytesToMB(label.TotalValueBytes), label.Name)
		}
	}
}

// printLabelValueStats prints the collected label value statistics.
func printLabelValueStats(_ context.Context, w io.Writer, stats *LabelValueStats) {
	fmt.Fprintf(w, "Total values:              %d\n", stats.TotalValues)
	fmt.Fprintf(w, "Total bytes:               %s\n", formatBytes(stats.TotalBytes))
	if stats.TotalValues > 0 {
		avgLength := float64(stats.TotalBytes) / float64(stats.TotalValues)
		fmt.Fprintf(w, "Average value length:      %.2f bytes\n", avgLength)
		fmt.Fprintf(w, "Min value length:          %d bytes\n", stats.MinLength)
		fmt.Fprintf(w, "Max value length:          %d bytes\n", stats.MaxLength)
	}

	fmt.Fprintln(w)
	fmt.Fprintln(w, "Length distribution:")
	for _, bucket := range lengthBucketNames() {
		b := stats.LengthHistogram[bucket]
		if b != nil && b.Count > 0 {
			pct := float64(b.Count) / float64(stats.TotalValues) * 100
			var sizePct float64
			if stats.TotalBytes > 0 {
				sizePct = float64(b.TotalBytes) / float64(stats.TotalBytes) * 100
			}
			fmt.Fprintf(w, "  %8s: %10d values (%5.2f%%)  %12d bytes (%5.2f%%, %.2f MB)\n",
				bucket, b.Count, pct, b.TotalBytes, sizePct, bytesToMB(b.TotalBytes))

			// Print samples.
			fmt.Fprintln(w, "    Samples:")
			for i, sample := range b.Samples {
				fmt.Fprintf(w, "      %d. [%d bytes] %q\n", i+1, len(sample), truncateString(sample, displayTruncateLen))
			}
		}
	}
}

// printMetricNameStats prints statistics about metric names that have a specific label.
func printMetricNameStats(_ context.Context, w io.Writer, stats *MetricNameStats) {
	fmt.Fprintf(w, "\nMetric names with this label: %d unique\n", stats.UniqueMetricNames)
	if len(stats.TopMetrics) > 0 {
		fmt.Fprintln(w, "Top 10 metric names by series count:")
		for i, m := range stats.TopMetrics {
			fmt.Fprintf(w, "  %2d. [%8d series] %s\n", i+1, m.SeriesCount, m.Name)
		}
	}
}
