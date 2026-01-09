// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSplitTimeRangeUTC(t *testing.T) {
	tests := []struct {
		name     string
		start    time.Time
		end      time.Time
		expected []struct{ Start, End time.Time }
	}{
		{
			name:  "5 minute range uses 30 second chunks",
			start: time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC),
			end:   time.Date(2024, 1, 1, 10, 5, 0, 0, time.UTC),
			expected: []struct{ Start, End time.Time }{
				{time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC), time.Date(2024, 1, 1, 10, 0, 30, 0, time.UTC)},
				{time.Date(2024, 1, 1, 10, 0, 30, 0, time.UTC), time.Date(2024, 1, 1, 10, 1, 0, 0, time.UTC)},
				{time.Date(2024, 1, 1, 10, 1, 0, 0, time.UTC), time.Date(2024, 1, 1, 10, 1, 30, 0, time.UTC)},
				{time.Date(2024, 1, 1, 10, 1, 30, 0, time.UTC), time.Date(2024, 1, 1, 10, 2, 0, 0, time.UTC)},
				{time.Date(2024, 1, 1, 10, 2, 0, 0, time.UTC), time.Date(2024, 1, 1, 10, 2, 30, 0, time.UTC)},
				{time.Date(2024, 1, 1, 10, 2, 30, 0, time.UTC), time.Date(2024, 1, 1, 10, 3, 0, 0, time.UTC)},
				{time.Date(2024, 1, 1, 10, 3, 0, 0, time.UTC), time.Date(2024, 1, 1, 10, 3, 30, 0, time.UTC)},
				{time.Date(2024, 1, 1, 10, 3, 30, 0, time.UTC), time.Date(2024, 1, 1, 10, 4, 0, 0, time.UTC)},
				{time.Date(2024, 1, 1, 10, 4, 0, 0, time.UTC), time.Date(2024, 1, 1, 10, 4, 30, 0, time.UTC)},
				{time.Date(2024, 1, 1, 10, 4, 30, 0, time.UTC), time.Date(2024, 1, 1, 10, 5, 0, 0, time.UTC)},
			},
		},
		{
			name:  "30 minute range uses 5 minute chunks",
			start: time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC),
			end:   time.Date(2024, 1, 1, 10, 30, 0, 0, time.UTC),
			expected: []struct{ Start, End time.Time }{
				{time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC), time.Date(2024, 1, 1, 10, 5, 0, 0, time.UTC)},
				{time.Date(2024, 1, 1, 10, 5, 0, 0, time.UTC), time.Date(2024, 1, 1, 10, 10, 0, 0, time.UTC)},
				{time.Date(2024, 1, 1, 10, 10, 0, 0, time.UTC), time.Date(2024, 1, 1, 10, 15, 0, 0, time.UTC)},
				{time.Date(2024, 1, 1, 10, 15, 0, 0, time.UTC), time.Date(2024, 1, 1, 10, 20, 0, 0, time.UTC)},
				{time.Date(2024, 1, 1, 10, 20, 0, 0, time.UTC), time.Date(2024, 1, 1, 10, 25, 0, 0, time.UTC)},
				{time.Date(2024, 1, 1, 10, 25, 0, 0, time.UTC), time.Date(2024, 1, 1, 10, 30, 0, 0, time.UTC)},
			},
		},
		{
			name:  "60 minute range uses 5 minute chunks",
			start: time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC),
			end:   time.Date(2024, 1, 1, 11, 0, 0, 0, time.UTC),
			expected: []struct{ Start, End time.Time }{
				{time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC), time.Date(2024, 1, 1, 10, 5, 0, 0, time.UTC)},
				{time.Date(2024, 1, 1, 10, 5, 0, 0, time.UTC), time.Date(2024, 1, 1, 10, 10, 0, 0, time.UTC)},
				{time.Date(2024, 1, 1, 10, 10, 0, 0, time.UTC), time.Date(2024, 1, 1, 10, 15, 0, 0, time.UTC)},
				{time.Date(2024, 1, 1, 10, 15, 0, 0, time.UTC), time.Date(2024, 1, 1, 10, 20, 0, 0, time.UTC)},
				{time.Date(2024, 1, 1, 10, 20, 0, 0, time.UTC), time.Date(2024, 1, 1, 10, 25, 0, 0, time.UTC)},
				{time.Date(2024, 1, 1, 10, 25, 0, 0, time.UTC), time.Date(2024, 1, 1, 10, 30, 0, 0, time.UTC)},
				{time.Date(2024, 1, 1, 10, 30, 0, 0, time.UTC), time.Date(2024, 1, 1, 10, 35, 0, 0, time.UTC)},
				{time.Date(2024, 1, 1, 10, 35, 0, 0, time.UTC), time.Date(2024, 1, 1, 10, 40, 0, 0, time.UTC)},
				{time.Date(2024, 1, 1, 10, 40, 0, 0, time.UTC), time.Date(2024, 1, 1, 10, 45, 0, 0, time.UTC)},
				{time.Date(2024, 1, 1, 10, 45, 0, 0, time.UTC), time.Date(2024, 1, 1, 10, 50, 0, 0, time.UTC)},
				{time.Date(2024, 1, 1, 10, 50, 0, 0, time.UTC), time.Date(2024, 1, 1, 10, 55, 0, 0, time.UTC)},
				{time.Date(2024, 1, 1, 10, 55, 0, 0, time.UTC), time.Date(2024, 1, 1, 11, 0, 0, 0, time.UTC)},
			},
		},
		{
			name:  "2 hour range uses 30 minute chunks",
			start: time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC),
			end:   time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
			expected: []struct{ Start, End time.Time }{
				{time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC), time.Date(2024, 1, 1, 10, 30, 0, 0, time.UTC)},
				{time.Date(2024, 1, 1, 10, 30, 0, 0, time.UTC), time.Date(2024, 1, 1, 11, 0, 0, 0, time.UTC)},
				{time.Date(2024, 1, 1, 11, 0, 0, 0, time.UTC), time.Date(2024, 1, 1, 11, 30, 0, 0, time.UTC)},
				{time.Date(2024, 1, 1, 11, 30, 0, 0, time.UTC), time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)},
			},
		},
		{
			name:  "unaligned start with 30m chunks",
			start: time.Date(2024, 1, 1, 10, 15, 0, 0, time.UTC),
			end:   time.Date(2024, 1, 1, 12, 30, 0, 0, time.UTC),
			expected: []struct{ Start, End time.Time }{
				{time.Date(2024, 1, 1, 10, 15, 0, 0, time.UTC), time.Date(2024, 1, 1, 10, 30, 0, 0, time.UTC)},
				{time.Date(2024, 1, 1, 10, 30, 0, 0, time.UTC), time.Date(2024, 1, 1, 11, 0, 0, 0, time.UTC)},
				{time.Date(2024, 1, 1, 11, 0, 0, 0, time.UTC), time.Date(2024, 1, 1, 11, 30, 0, 0, time.UTC)},
				{time.Date(2024, 1, 1, 11, 30, 0, 0, time.UTC), time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)},
				{time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC), time.Date(2024, 1, 1, 12, 30, 0, 0, time.UTC)},
			},
		},
		{
			name:  "unaligned start with 5m chunks",
			start: time.Date(2024, 1, 1, 10, 2, 0, 0, time.UTC),
			end:   time.Date(2024, 1, 1, 10, 17, 0, 0, time.UTC),
			expected: []struct{ Start, End time.Time }{
				{time.Date(2024, 1, 1, 10, 2, 0, 0, time.UTC), time.Date(2024, 1, 1, 10, 5, 0, 0, time.UTC)},
				{time.Date(2024, 1, 1, 10, 5, 0, 0, time.UTC), time.Date(2024, 1, 1, 10, 10, 0, 0, time.UTC)},
				{time.Date(2024, 1, 1, 10, 10, 0, 0, time.UTC), time.Date(2024, 1, 1, 10, 15, 0, 0, time.UTC)},
				{time.Date(2024, 1, 1, 10, 15, 0, 0, time.UTC), time.Date(2024, 1, 1, 10, 17, 0, 0, time.UTC)},
			},
		},
		{
			name:  "unaligned start with 30s chunks",
			start: time.Date(2024, 1, 1, 10, 0, 10, 0, time.UTC),
			end:   time.Date(2024, 1, 1, 10, 1, 40, 0, time.UTC),
			expected: []struct{ Start, End time.Time }{
				{time.Date(2024, 1, 1, 10, 0, 10, 0, time.UTC), time.Date(2024, 1, 1, 10, 0, 30, 0, time.UTC)},
				{time.Date(2024, 1, 1, 10, 0, 30, 0, time.UTC), time.Date(2024, 1, 1, 10, 1, 0, 0, time.UTC)},
				{time.Date(2024, 1, 1, 10, 1, 0, 0, time.UTC), time.Date(2024, 1, 1, 10, 1, 30, 0, time.UTC)},
				{time.Date(2024, 1, 1, 10, 1, 30, 0, time.UTC), time.Date(2024, 1, 1, 10, 1, 40, 0, time.UTC)},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := splitTimeRangeUTC(tt.start, tt.end)
			assert.Equal(t, tt.expected, result)
		})
	}
}
