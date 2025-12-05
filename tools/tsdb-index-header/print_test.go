// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPrintTOCInfo(t *testing.T) {
	info := &TOCInfo{
		IndexHeaderSize:         1048576, // 1 MB
		IndexHeaderVersion:      1,
		TSDBIndexVersion:        2,
		SymbolsSize:             524288, // 0.5 MB
		PostingsOffsetTableSize: 262144, // 0.25 MB
	}

	var buf bytes.Buffer
	printTOCInfo(context.Background(), &buf, info)

	expected := `
Index-header size:         1048576 (1.00 MB)
Index-header version:      1
TSDB index version:        2
Header size:               14
Symbols size:              524288 (0.50 MB)
Postings offset table:     262144 (0.25 MB)
TOC + CRC32:               20
`
	assert.Equal(t, strings.TrimPrefix(expected, "\n"), buf.String())
}

func TestPrintSymbolStats(t *testing.T) {
	stats := &SymbolStats{
		Count:           100,
		TotalLength:     5000,
		MinLength:       10,
		MaxLength:       200,
		LengthHistogram: map[string]int{"0-16": 50, "17-32": 30, "33-64": 20},
		LengthSizes:     map[string]int64{"0-16": 500, "17-32": 750, "33-64": 1000},
		LongestSymbols:  []string{"this_is_a_long_symbol", "another_long_one"},
		DuplicateCount:  0,
		NotSortedCount:  0,
	}

	var buf bytes.Buffer
	printSymbolStats(context.Background(), &buf, stats)

	expected := `
Symbol count:              100
Total symbols length:      5000 (0.00 MB)
Average symbol length:     50.00 bytes
Min symbol length:         10 bytes
Max symbol length:         200 bytes
Symbols sorted:            yes
Duplicate symbols:         0

Length distribution:
      0-16:         50 symbols (50.00%)           500 bytes (10.00%, 0.00 MB)
     17-32:         30 symbols (30.00%)           750 bytes (15.00%, 0.00 MB)
     33-64:         20 symbols (20.00%)          1000 bytes (20.00%, 0.00 MB)

Top 20 longest symbols:
   1. [   21 bytes] this_is_a_long_symbol
   2. [   16 bytes] another_long_one
`
	assert.Equal(t, strings.TrimPrefix(expected, "\n"), buf.String())
}

func TestPrintLabelStats(t *testing.T) {
	stats := &LabelStats{
		TotalLabels:      50,
		TotalLabelValues: 10000,
		Labels: []LabelCardinality{
			{Name: "instance", Cardinality: 5000, TotalValueBytes: 100000},
			{Name: "pod", Cardinality: 3000, TotalValueBytes: 60000},
			{Name: "container", Cardinality: 2000, TotalValueBytes: 40000},
		},
		CardinalityHist: map[string]int{"1K-10K": 3, "101-1K": 20, "11-100": 27},
	}

	var buf bytes.Buffer
	printLabelStats(context.Background(), &buf, stats)

	expected := `
Total label names:         50
Total label-value pairs:   10000

Cardinality distribution:
     11-100:    27 labels (54.00%)
     101-1K:    20 labels (40.00%)
     1K-10K:     3 labels ( 6.00%)

Top 20 highest cardinality labels:
   1. [    5000 values,     100000 bytes (0.10 MB)] instance
   2. [    3000 values,      60000 bytes (0.06 MB)] pod
   3. [    2000 values,      40000 bytes (0.04 MB)] container
`
	assert.Equal(t, strings.TrimPrefix(expected, "\n"), buf.String())
}

func TestPrintLabelValueStats(t *testing.T) {
	stats := &LabelValueStats{
		TotalValues: 100,
		TotalBytes:  1000,
		MinLength:   5,
		MaxLength:   15,
		LengthHistogram: map[string]*LabelValueBucket{
			"0-16": {Count: 100, TotalBytes: 1000, Samples: []string{"foo", "bar", "baz"}},
		},
	}

	var buf bytes.Buffer
	printLabelValueStats(context.Background(), &buf, stats)

	expected := `
Total values:              100
Total bytes:               1000 (0.00 MB)
Average value length:      10.00 bytes
Min value length:          5 bytes
Max value length:          15 bytes

Length distribution:
      0-16:        100 values (100.00%)          1000 bytes (100.00%, 0.00 MB)
    Samples:
      1. [3 bytes] "foo"
      2. [3 bytes] "bar"
      3. [3 bytes] "baz"
`
	assert.Equal(t, strings.TrimPrefix(expected, "\n"), buf.String())
}
