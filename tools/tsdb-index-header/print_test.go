// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPrintIndexInfo_IndexHeader(t *testing.T) {
	info := &IndexInfo{
		Path:               "/path/to/index-header",
		Size:               1048576, // 1 MB
		IsIndexHeader:      true,
		IndexVersion:       2,
		IndexHeaderVersion: 1,
		SymbolsSize:        524288,  // 0.5 MB
		PostingsTableSize:  262144,  // 0.25 MB
	}

	var buf bytes.Buffer
	printIndexInfo(context.Background(), &buf, info, nil)

	expected := `
Index type:                Index-Header
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

func TestPrintIndexInfo_FullIndex(t *testing.T) {
	info := &IndexInfo{
		Path:          "/path/to/index",
		Size:          10485760, // 10 MB
		IsIndexHeader: false,
		IndexVersion:  2,
	}

	var buf bytes.Buffer
	printIndexInfo(context.Background(), &buf, info, nil)

	expected := `
Index type:                Full Index
Index size:                10485760 (10.00 MB)
TSDB index version:        2
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

func TestPrintMetricNameStats(t *testing.T) {
	stats := &MetricNameStats{
		UniqueMetricNames: 156,
		TopMetrics: []MetricCount{
			{Name: "container_cpu_usage_seconds_total", SeriesCount: 45231},
			{Name: "container_memory_working_set_bytes", SeriesCount: 38442},
			{Name: "kube_pod_container_status_running", SeriesCount: 31205},
		},
	}

	var buf bytes.Buffer
	printMetricNameStats(context.Background(), &buf, stats)

	// Output starts with newline to separate from previous section.
	expected := `
Metric names with this label: 156 unique
Top 10 metric names by series count:
   1. [   45231 series] container_cpu_usage_seconds_total
   2. [   38442 series] container_memory_working_set_bytes
   3. [   31205 series] kube_pod_container_status_running
`
	assert.Equal(t, expected, buf.String())
}

func TestPrintMetricNameStats_Empty(t *testing.T) {
	stats := &MetricNameStats{
		UniqueMetricNames: 0,
		TopMetrics:        nil,
	}

	var buf bytes.Buffer
	printMetricNameStats(context.Background(), &buf, stats)

	// Output starts with newline to separate from previous section.
	expected := `
Metric names with this label: 0 unique
`
	assert.Equal(t, expected, buf.String())
}
