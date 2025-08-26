// SPDX-License-Identifier: AGPL-3.0-only

package parquet

import (
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus-community/parquet-common/search"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockSortingColumn creates a mock SortingColumn for testing
func mockSortingColumn(labelName string) parquet.SortingColumn {
	return parquet.Ascending(schema.LabelToColumn(labelName))
}

func TestGroupBySortingColumns(t *testing.T) {
	tests := []struct {
		name           string
		sortingColumns []parquet.SortingColumn
		labels         []labels.Labels
		rowRanges      []search.RowRange
		expectedGroups [][]labels.Labels
		expectedRanges [][]search.RowRange
		expectError    bool
	}{
		{
			name:           "empty sorting columns",
			sortingColumns: []parquet.SortingColumn{},
			labels: []labels.Labels{
				labels.FromStrings("__name__", "metric1", "job", "app1"),
				labels.FromStrings("__name__", "metric2", "job", "app2"),
			},
			rowRanges: []search.RowRange{{From: 0, Count: 2}},
			expectedGroups: [][]labels.Labels{{
				labels.FromStrings("__name__", "metric1", "job", "app1"),
				labels.FromStrings("__name__", "metric2", "job", "app2"),
			}},
			expectedRanges: [][]search.RowRange{{{From: 0, Count: 2}}},
		},
		{
			name:           "empty labels",
			sortingColumns: []parquet.SortingColumn{mockSortingColumn("job")},
			labels:         []labels.Labels{},
			rowRanges:      []search.RowRange{},
			expectedGroups: nil,
			expectedRanges: nil,
		},
		{
			name:           "single sorting column - no grouping needed",
			sortingColumns: []parquet.SortingColumn{mockSortingColumn("job")},
			labels: []labels.Labels{
				labels.FromStrings("__name__", "metric1", "job", "app1"),
				labels.FromStrings("__name__", "metric2", "job", "app1"),
				labels.FromStrings("__name__", "metric3", "job", "app1"),
			},
			rowRanges: []search.RowRange{{From: 0, Count: 3}},
			expectedGroups: [][]labels.Labels{{
				labels.FromStrings("__name__", "metric1", "job", "app1"),
				labels.FromStrings("__name__", "metric2", "job", "app1"),
				labels.FromStrings("__name__", "metric3", "job", "app1"),
			}},
			expectedRanges: [][]search.RowRange{{{From: 0, Count: 3}}},
		},
		{
			name:           "single sorting column - simple grouping",
			sortingColumns: []parquet.SortingColumn{mockSortingColumn("job")},
			labels: []labels.Labels{
				labels.FromStrings("__name__", "metric1", "job", "app1"),
				labels.FromStrings("__name__", "metric2", "job", "app1"),
				labels.FromStrings("__name__", "metric3", "job", "app2"),
				labels.FromStrings("__name__", "metric4", "job", "app2"),
			},
			rowRanges: []search.RowRange{{From: 0, Count: 4}},
			expectedGroups: [][]labels.Labels{
				{
					labels.FromStrings("__name__", "metric1", "job", "app1"),
					labels.FromStrings("__name__", "metric2", "job", "app1"),
				},
				{
					labels.FromStrings("__name__", "metric3", "job", "app2"),
					labels.FromStrings("__name__", "metric4", "job", "app2"),
				},
			},
			expectedRanges: [][]search.RowRange{
				{{From: 0, Count: 2}},
				{{From: 2, Count: 2}},
			},
		},
		{
			name:           "multiple sorting columns",
			sortingColumns: []parquet.SortingColumn{mockSortingColumn("job"), mockSortingColumn("instance")},
			labels: []labels.Labels{
				labels.FromStrings("__name__", "metric1", "job", "app1", "instance", "host1"),
				labels.FromStrings("__name__", "metric2", "job", "app1", "instance", "host1"),
				labels.FromStrings("__name__", "metric3", "job", "app1", "instance", "host2"),
				labels.FromStrings("__name__", "metric4", "job", "app2", "instance", "host1"),
			},
			rowRanges: []search.RowRange{{From: 0, Count: 4}},
			expectedGroups: [][]labels.Labels{
				{
					labels.FromStrings("__name__", "metric1", "job", "app1", "instance", "host1"),
					labels.FromStrings("__name__", "metric2", "job", "app1", "instance", "host1"),
				},
				{
					labels.FromStrings("__name__", "metric3", "job", "app1", "instance", "host2"),
				},
				{
					labels.FromStrings("__name__", "metric4", "job", "app2", "instance", "host1"),
				},
			},
			expectedRanges: [][]search.RowRange{
				{{From: 0, Count: 2}},
				{{From: 2, Count: 1}},
				{{From: 3, Count: 1}},
			},
		},
		{
			name:           "multiple row ranges - same groups",
			sortingColumns: []parquet.SortingColumn{mockSortingColumn("job")},
			labels: []labels.Labels{
				labels.FromStrings("__name__", "metric1", "job", "app1"),
				labels.FromStrings("__name__", "metric2", "job", "app1"),
				labels.FromStrings("__name__", "metric3", "job", "app1"),
				labels.FromStrings("__name__", "metric4", "job", "app1"),
			},
			rowRanges: []search.RowRange{{From: 0, Count: 2}, {From: 10, Count: 2}},
			expectedGroups: [][]labels.Labels{
				{
					labels.FromStrings("__name__", "metric1", "job", "app1"),
					labels.FromStrings("__name__", "metric2", "job", "app1"),
					labels.FromStrings("__name__", "metric3", "job", "app1"),
					labels.FromStrings("__name__", "metric4", "job", "app1"),
				},
			},
			expectedRanges: [][]search.RowRange{
				{{From: 0, Count: 2}, {From: 10, Count: 2}},
			},
		},
		{
			name:           "multiple row ranges - different groups",
			sortingColumns: []parquet.SortingColumn{mockSortingColumn("job")},
			labels: []labels.Labels{
				labels.FromStrings("__name__", "metric1", "job", "app1"),
				labels.FromStrings("__name__", "metric2", "job", "app1"),
				labels.FromStrings("__name__", "metric3", "job", "app2"),
				labels.FromStrings("__name__", "metric4", "job", "app3"),
			},
			rowRanges: []search.RowRange{{From: 0, Count: 2}, {From: 10, Count: 2}},
			expectedGroups: [][]labels.Labels{
				{
					labels.FromStrings("__name__", "metric1", "job", "app1"),
					labels.FromStrings("__name__", "metric2", "job", "app1"),
				},
				{
					labels.FromStrings("__name__", "metric3", "job", "app2"),
				},
				{
					labels.FromStrings("__name__", "metric4", "job", "app3"),
				},
			},
			expectedRanges: [][]search.RowRange{
				{{From: 0, Count: 2}},
				{{From: 10, Count: 1}},
				{{From: 11, Count: 1}},
			},
		},
		{
			name:           "range splitting within single RowRange",
			sortingColumns: []parquet.SortingColumn{mockSortingColumn("job")},
			labels: []labels.Labels{
				labels.FromStrings("__name__", "metric1", "job", "app1"),
				labels.FromStrings("__name__", "metric2", "job", "app2"),
				labels.FromStrings("__name__", "metric3", "job", "app2"),
				labels.FromStrings("__name__", "metric4", "job", "app1"),
			},
			rowRanges: []search.RowRange{{From: 100, Count: 4}},
			expectedGroups: [][]labels.Labels{
				{
					labels.FromStrings("__name__", "metric1", "job", "app1"),
					labels.FromStrings("__name__", "metric4", "job", "app1"),
				},
				{
					labels.FromStrings("__name__", "metric2", "job", "app2"),
					labels.FromStrings("__name__", "metric3", "job", "app2"),
				},
			},
			expectedRanges: [][]search.RowRange{
				{{From: 100, Count: 1}, {From: 103, Count: 1}},
				{{From: 101, Count: 2}},
			},
		},
		{
			name:           "complex alternating groups",
			sortingColumns: []parquet.SortingColumn{mockSortingColumn("job")},
			labels: []labels.Labels{
				labels.FromStrings("__name__", "metric1", "job", "app1"),
				labels.FromStrings("__name__", "metric2", "job", "app2"),
				labels.FromStrings("__name__", "metric3", "job", "app1"),
				labels.FromStrings("__name__", "metric4", "job", "app2"),
				labels.FromStrings("__name__", "metric5", "job", "app1"),
			},
			rowRanges: []search.RowRange{{From: 50, Count: 5}},
			expectedGroups: [][]labels.Labels{
				{
					labels.FromStrings("__name__", "metric1", "job", "app1"),
					labels.FromStrings("__name__", "metric3", "job", "app1"),
					labels.FromStrings("__name__", "metric5", "job", "app1"),
				},
				{
					labels.FromStrings("__name__", "metric2", "job", "app2"),
					labels.FromStrings("__name__", "metric4", "job", "app2"),
				},
			},
			expectedRanges: [][]search.RowRange{
				{{From: 50, Count: 1}, {From: 52, Count: 1}, {From: 54, Count: 1}},
				{{From: 51, Count: 1}, {From: 53, Count: 1}},
			},
		},
		{
			name:           "missing label in sorting columns",
			sortingColumns: []parquet.SortingColumn{mockSortingColumn("missing_label")},
			labels: []labels.Labels{
				labels.FromStrings("__name__", "metric1", "job", "app1"),
				labels.FromStrings("__name__", "metric2", "job", "app2"),
			},
			rowRanges: []search.RowRange{{From: 0, Count: 2}},
			expectedGroups: [][]labels.Labels{{
				labels.FromStrings("__name__", "metric1", "job", "app1"),
				labels.FromStrings("__name__", "metric2", "job", "app2"),
			}},
			expectedRanges: [][]search.RowRange{{{From: 0, Count: 2}}},
		},
		{
			name:           "single label single range",
			sortingColumns: []parquet.SortingColumn{mockSortingColumn("job")},
			labels: []labels.Labels{
				labels.FromStrings("__name__", "metric1", "job", "app1"),
			},
			rowRanges: []search.RowRange{{From: 42, Count: 1}},
			expectedGroups: [][]labels.Labels{{
				labels.FromStrings("__name__", "metric1", "job", "app1"),
			}},
			expectedRanges: [][]search.RowRange{{{From: 42, Count: 1}}},
		},
		{
			name:           "label count mismatch - too few labels",
			sortingColumns: []parquet.SortingColumn{mockSortingColumn("job")},
			labels: []labels.Labels{
				labels.FromStrings("__name__", "metric1", "job", "app1"),
			},
			rowRanges:   []search.RowRange{{From: 0, Count: 2}},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			labelsGroups, rrGroups, err := groupBySortingColumns(tt.sortingColumns, tt.labels, tt.rowRanges)

			if tt.expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, len(tt.expectedGroups), len(labelsGroups), "Number of label groups should match")
			assert.Equal(t, len(tt.expectedRanges), len(rrGroups), "Number of row range groups should match")
			assert.Equal(t, len(labelsGroups), len(rrGroups), "Number of label groups should equal row range groups")

			// Check each group
			for i, expectedLabelGroup := range tt.expectedGroups {
				require.Less(t, i, len(labelsGroups), "Group index should be valid")
				assert.Equal(t, expectedLabelGroup, labelsGroups[i], "Label group %d should match", i)
			}

			for i, expectedRangeGroup := range tt.expectedRanges {
				require.Less(t, i, len(rrGroups), "Range group index should be valid")
				assert.Equal(t, expectedRangeGroup, rrGroups[i], "Row range group %d should match", i)
			}

			// Verify that the total number of labels in all groups equals the original
			totalLabelsInGroups := 0
			for _, group := range labelsGroups {
				totalLabelsInGroups += len(group)
			}
			assert.Equal(t, len(tt.labels), totalLabelsInGroups, "Total labels in groups should equal original labels")

			// Verify that the total count of rows in all ranges equals the original
			totalRowsInOriginal := int64(0)
			for _, rr := range tt.rowRanges {
				totalRowsInOriginal += rr.Count
			}
			totalRowsInGroups := int64(0)
			for _, group := range rrGroups {
				for _, rr := range group {
					totalRowsInGroups += rr.Count
				}
			}
			assert.Equal(t, totalRowsInOriginal, totalRowsInGroups, "Total rows in groups should equal original rows")
		})
	}
}

func TestGroupBySortingColumns_InvalidColumnName(t *testing.T) {
	// Create a mock SortingColumn with invalid column name that doesn't extract to a label
	invalidColumn := parquet.Ascending("invalid_column_path")

	lbls := []labels.Labels{
		labels.FromStrings("__name__", "metric1", "job", "app1"),
	}
	rowRanges := []search.RowRange{{From: 0, Count: 1}}

	_, _, err := groupBySortingColumns([]parquet.SortingColumn{invalidColumn}, lbls, rowRanges)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot extract label from sorting column")
}

// Benchmark tests to verify performance characteristics
func BenchmarkGroupBySortingColumns(b *testing.B) {
	// Create test data
	sortingColumns := []parquet.SortingColumn{mockSortingColumn("job"), mockSortingColumn("instance")}

	// Generate labels with different group patterns
	lbls := make([]labels.Labels, 1000)
	for i := 0; i < 1000; i++ {
		jobValue := "app" + string(rune('1'+(i%5)))        // 5 different job values
		instanceValue := "host" + string(rune('1'+(i%10))) // 10 different instance values
		lbls[i] = labels.FromStrings("__name__", "metric", "job", jobValue, "instance", instanceValue)
	}

	rowRanges := []search.RowRange{{From: 0, Count: 1000}}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := groupBySortingColumns(sortingColumns, lbls, rowRanges)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGroupBySortingColumns_ManySmallRanges(b *testing.B) {
	sortingColumns := []parquet.SortingColumn{mockSortingColumn("job")}

	// Create many small ranges
	lbls := make([]labels.Labels, 100)
	rowRanges := make([]search.RowRange, 100)
	for i := 0; i < 100; i++ {
		jobValue := "app" + string(rune('1'+(i%3))) // 3 different job values
		lbls[i] = labels.FromStrings("__name__", "metric", "job", jobValue)
		rowRanges[i] = search.RowRange{From: int64(i * 10), Count: 1}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := groupBySortingColumns(sortingColumns, lbls, rowRanges)
		if err != nil {
			b.Fatal(err)
		}
	}
}
