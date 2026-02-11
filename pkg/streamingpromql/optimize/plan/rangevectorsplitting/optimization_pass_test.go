// SPDX-License-Identifier: AGPL-3.0-only

package rangevectorsplitting

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/planning"
)

func TestComputeSplitRanges(t *testing.T) {
	hourInMs := int64(time.Hour / time.Millisecond)
	minuteInMs := int64(time.Minute / time.Millisecond)

	tests := []struct {
		name           string
		startTs        int64
		endTs          int64
		splitInterval  time.Duration
		expectedRanges []SplitRange
	}{
		{
			name:          "5h range at 6h with 2h splits",
			startTs:       1 * hourInMs,
			endTs:         6 * hourInMs,
			splitInterval: 2 * time.Hour,
			expectedRanges: []SplitRange{
				{Start: 1 * hourInMs, End: 2*hourInMs - 1, Cacheable: false},  // Head: (1h, 2h-1ms]
				{Start: 2*hourInMs - 1, End: 4*hourInMs - 1, Cacheable: true}, // Block: (2h-1ms, 4h-1ms]
				{Start: 4*hourInMs - 1, End: 6*hourInMs - 1, Cacheable: true}, // Block: (4h-1ms, 6h-1ms]
				{Start: 6*hourInMs - 1, End: 6 * hourInMs, Cacheable: false},  // Tail: (6h-1ms, 6h]
			},
		},
		{
			name:          "5h range at 6h10m with 2h splits",
			startTs:       1*hourInMs + 10*minuteInMs,
			endTs:         6*hourInMs + 10*minuteInMs,
			splitInterval: 2 * time.Hour,
			expectedRanges: []SplitRange{
				{Start: 1*hourInMs + 10*minuteInMs, End: 2*hourInMs - 1, Cacheable: false}, // Head: (1h10m, 2h-1ms]
				{Start: 2*hourInMs - 1, End: 4*hourInMs - 1, Cacheable: true},              // Block: (2h-1ms, 4h-1ms]
				{Start: 4*hourInMs - 1, End: 6*hourInMs - 1, Cacheable: true},              // Block: (4h-1ms, 6h-1ms]
				{Start: 6*hourInMs - 1, End: 6*hourInMs + 10*minuteInMs, Cacheable: false}, // Tail: (6h-1ms, 6h10m]
			},
		},
		{
			name:          "5h range at 7h with 2h splits",
			startTs:       2 * hourInMs,
			endTs:         7 * hourInMs,
			splitInterval: 2 * time.Hour,
			expectedRanges: []SplitRange{
				{Start: 2 * hourInMs, End: 4*hourInMs - 1, Cacheable: false},  // Head: (2h, 4h-1ms]
				{Start: 4*hourInMs - 1, End: 6*hourInMs - 1, Cacheable: true}, // Block: (4h-1ms, 6h-1ms]
				{Start: 6*hourInMs - 1, End: 7 * hourInMs, Cacheable: false},  // Tail: (6h-1ms, 7h]
			},
		},
		{
			name:          "5h range at 8h20m with 2h splits",
			startTs:       3*hourInMs + 20*minuteInMs,
			endTs:         8*hourInMs + 20*minuteInMs,
			splitInterval: 2 * time.Hour,
			expectedRanges: []SplitRange{
				{Start: 3*hourInMs + 20*minuteInMs, End: 4*hourInMs - 1, Cacheable: false}, // Head: (3h20m, 4h-1ms]
				{Start: 4*hourInMs - 1, End: 6*hourInMs - 1, Cacheable: true},              // Block: (4h-1ms, 6h-1ms]
				{Start: 6*hourInMs - 1, End: 8*hourInMs - 1, Cacheable: true},              // Block: (6h-1ms, 8h-1ms]
				{Start: 8*hourInMs - 1, End: 8*hourInMs + 20*minuteInMs, Cacheable: false}, // Tail: (8h-1ms, 8h20m]
			},
		},
		{
			name:          "exact aligned boundaries - 4h range at 6h with 2h splits",
			startTs:       2 * hourInMs,
			endTs:         6 * hourInMs,
			splitInterval: 2 * time.Hour,
			expectedRanges: []SplitRange{
				{Start: 2 * hourInMs, End: 4*hourInMs - 1, Cacheable: false},  // Head: (2h, 4h-1ms]
				{Start: 4*hourInMs - 1, End: 6*hourInMs - 1, Cacheable: true}, // Block: (4h-1ms, 6h-1ms]
				{Start: 6*hourInMs - 1, End: 6 * hourInMs, Cacheable: false},  // Tail: (6h-1ms, 6h]
			},
		},
		{
			name:          "aligned start - 4h range at 4h with 2h splits",
			startTs:       2*hourInMs - 1,
			endTs:         6*hourInMs - 1,
			splitInterval: 2 * time.Hour,
			expectedRanges: []SplitRange{
				{Start: 2*hourInMs - 1, End: 4*hourInMs - 1, Cacheable: true}, // Block: (2h-1ms, 4h-1ms]
				{Start: 4*hourInMs - 1, End: 6*hourInMs - 1, Cacheable: true}, // Block: (4h-1ms, 6h-1ms]
			},
		},
		{
			name:          "single aligned block with head and tail",
			startTs:       1*hourInMs + 30*minuteInMs,
			endTs:         4*hourInMs + 30*minuteInMs,
			splitInterval: 2 * time.Hour,
			expectedRanges: []SplitRange{
				{Start: 1*hourInMs + 30*minuteInMs, End: 2*hourInMs - 1, Cacheable: false}, // Head: (1h30m, 2h-1ms]
				{Start: 2*hourInMs - 1, End: 4*hourInMs - 1, Cacheable: true},              // Block: (2h-1ms, 4h-1ms]
				{Start: 4*hourInMs - 1, End: 4*hourInMs + 30*minuteInMs, Cacheable: false}, // Tail: (4h-1ms, 4h30m]
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := computeSplitRanges(tt.startTs, tt.endTs, tt.splitInterval, 0) // No OOO window
			require.Equal(t, tt.expectedRanges, actual)
		})
	}
}

func TestComputeSplitRangesWithOOOWindow(t *testing.T) {
	hourInMs := int64(time.Hour / time.Millisecond)
	minuteInMs := int64(time.Minute / time.Millisecond)

	// Fixed "now" time for consistent testing: 10 hours
	now := 10 * hourInMs

	tests := []struct {
		name           string
		startTs        int64
		endTs          int64
		splitInterval  time.Duration
		oooThreshold   int64 // 0 means no OOO window
		expectedRanges []SplitRange
	}{
		{
			name:          "no OOO window (threshold = 0)",
			startTs:       1 * hourInMs,
			endTs:         6 * hourInMs,
			splitInterval: 2 * time.Hour,
			oooThreshold:  0,
			expectedRanges: []SplitRange{
				{Start: 1 * hourInMs, End: 2*hourInMs - 1, Cacheable: false},  // Head
				{Start: 2*hourInMs - 1, End: 4*hourInMs - 1, Cacheable: true}, // Block
				{Start: 4*hourInMs - 1, End: 6*hourInMs - 1, Cacheable: true}, // Block
				{Start: 6*hourInMs - 1, End: 6 * hourInMs, Cacheable: false},  // Tail
			},
		},
		{
			name:          "OOO window in middle - last cacheable block extends into OOO",
			startTs:       1 * hourInMs,
			endTs:         6 * hourInMs,
			splitInterval: 2 * time.Hour,
			oooThreshold:  5 * hourInMs,
			expectedRanges: []SplitRange{
				{Start: 1 * hourInMs, End: 2*hourInMs - 1, Cacheable: false},
				{Start: 2*hourInMs - 1, End: 4*hourInMs - 1, Cacheable: true},
				{Start: 4*hourInMs - 1, End: 6 * hourInMs, Cacheable: false},
			},
		},
		{
			name:          "OOO window covers all - entire query in OOO",
			startTs:       1 * hourInMs,
			endTs:         6 * hourInMs,
			splitInterval: 2 * time.Hour,
			oooThreshold:  1*hourInMs + 30*minuteInMs,
			expectedRanges: []SplitRange{
				{Start: 1 * hourInMs, End: 6 * hourInMs, Cacheable: false},
			},
		},
		{
			name:          "OOO window before start - entire query in OOO",
			startTs:       1 * hourInMs,
			endTs:         6 * hourInMs,
			splitInterval: 2 * time.Hour,
			oooThreshold:  30 * minuteInMs,
			expectedRanges: []SplitRange{
				{Start: 1 * hourInMs, End: 6 * hourInMs, Cacheable: false},
			},
		},
		{
			name:          "OOO window exactly at block boundary",
			startTs:       1 * hourInMs,
			endTs:         6 * hourInMs,
			splitInterval: 2 * time.Hour,
			oooThreshold:  4*hourInMs - 1,
			expectedRanges: []SplitRange{
				{Start: 1 * hourInMs, End: 2*hourInMs - 1, Cacheable: false},
				{Start: 2*hourInMs - 1, End: 4*hourInMs - 1, Cacheable: true},
				{Start: 4*hourInMs - 1, End: 6 * hourInMs, Cacheable: false},
			},
		},
		{
			name:          "multiple cacheable blocks before OOO",
			startTs:       0,
			endTs:         9 * hourInMs,
			splitInterval: 2 * time.Hour,
			oooThreshold:  now - 1*hourInMs, // 9h threshold
			expectedRanges: []SplitRange{
				{Start: 0, End: 2*hourInMs - 1, Cacheable: false},             // Head
				{Start: 2*hourInMs - 1, End: 4*hourInMs - 1, Cacheable: true}, // Block
				{Start: 4*hourInMs - 1, End: 6*hourInMs - 1, Cacheable: true}, // Block
				{Start: 6*hourInMs - 1, End: 8*hourInMs - 1, Cacheable: true}, // Block
				{Start: 8*hourInMs - 1, End: 9 * hourInMs, Cacheable: false},  // Merged: last block + tail in OOO
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := computeSplitRanges(tt.startTs, tt.endTs, tt.splitInterval, tt.oooThreshold)
			require.Equal(t, tt.expectedRanges, actual, "split ranges mismatch")
		})
	}
}

func TestComputeAlignedStart(t *testing.T) {
	hourInMs := int64(time.Hour / time.Millisecond)
	minuteInMs := int64(time.Minute / time.Millisecond)

	tests := []struct {
		name          string
		startTs       int64
		splitInterval time.Duration
		expected      int64
	}{
		{
			name:          "1h with 2h interval",
			startTs:       1 * hourInMs,
			splitInterval: 2 * time.Hour,
			expected:      2*hourInMs - 1, // 2h - 1ms
		},
		{
			name:          "2h with 2h interval",
			startTs:       2 * hourInMs,
			splitInterval: 2 * time.Hour,
			expected:      4*hourInMs - 1, // 4h - 1ms
		},
		{
			name:          "2h-1ms with 2h interval (already aligned)",
			startTs:       2*hourInMs - 1,
			splitInterval: 2 * time.Hour,
			expected:      2*hourInMs - 1, // 2h - 1ms (no change)
		},
		{
			name:          "1h10m with 2h interval",
			startTs:       1*hourInMs + 10*minuteInMs,
			splitInterval: 2 * time.Hour,
			expected:      2*hourInMs - 1, // 2h - 1ms
		},
		{
			name:          "3h20m with 2h interval",
			startTs:       3*hourInMs + 20*minuteInMs,
			splitInterval: 2 * time.Hour,
			expected:      4*hourInMs - 1, // 4h - 1ms
		},
		{
			name:          "30m with 2h interval",
			startTs:       30 * minuteInMs,
			splitInterval: 2 * time.Hour,
			expected:      2*hourInMs - 1, // 2h - 1ms
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := computeBlockAlignedStart(tt.startTs, tt.splitInterval)
			require.Equal(t, tt.expected, actual)
		})
	}
}

func TestCalculateInnerTimeRange(t *testing.T) {
	hourInMs := int64(time.Hour / time.Millisecond)
	minuteInMs := int64(time.Minute / time.Millisecond)

	tests := []struct {
		name          string
		evalTime      int64
		timeParams    planning.RangeParams
		expectedStart int64
		expectedEnd   int64
	}{
		{
			name:     "basic 5h range at 6h",
			evalTime: 6 * hourInMs,
			timeParams: planning.RangeParams{
				IsSet: true,
				Range: 5 * time.Hour,
			},
			expectedStart: 1 * hourInMs,
			expectedEnd:   6 * hourInMs,
		},
		{
			name:     "5h range with 1h offset at 8h",
			evalTime: 8 * hourInMs,
			timeParams: planning.RangeParams{
				IsSet:  true,
				Offset: 1 * time.Hour,
				Range:  5 * time.Hour,
			},
			expectedStart: 2 * hourInMs,
			expectedEnd:   7 * hourInMs,
		},
		{
			name:     "5h range with @ 7h evaluated at 8h",
			evalTime: 8 * hourInMs,
			timeParams: planning.RangeParams{
				IsSet:        true,
				HasTimestamp: true,
				Timestamp:    time.UnixMilli(7 * hourInMs),
				Range:        5 * time.Hour,
			},
			expectedStart: 2 * hourInMs,
			expectedEnd:   7 * hourInMs,
		},
		{
			name:     "3h range with 31m offset at 4h30m",
			evalTime: 4*hourInMs + 30*minuteInMs,
			timeParams: planning.RangeParams{
				IsSet:  true,
				Offset: 31 * time.Minute,
				Range:  3 * time.Hour,
			},
			expectedStart: 59 * minuteInMs,
			expectedEnd:   3*hourInMs + 59*minuteInMs,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			startTs, endTs := calculateInnerTimeRange(tt.evalTime, tt.timeParams)
			require.Equal(t, tt.expectedStart, startTs, "start timestamp mismatch")
			require.Equal(t, tt.expectedEnd, endTs, "end timestamp mismatch")
		})
	}
}
