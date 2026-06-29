// SPDX-License-Identifier: AGPL-3.0-only

package schedulerpb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJobSpec_Ranges(t *testing.T) {
	tests := map[string]struct {
		spec     JobSpec
		expected map[int32]OffsetRange
	}{
		"returns the explicit offset ranges when set": {
			spec: JobSpec{OffsetRanges: map[int32]OffsetRange{
				0: {StartOffset: 5, EndOffset: 9},
				1: {StartOffset: 10, EndOffset: 15},
			}},
			expected: map[int32]OffsetRange{
				0: {StartOffset: 5, EndOffset: 9},
				1: {StartOffset: 10, EndOffset: 15},
			},
		},
		"derives a single cluster-zero range from start/end offsets": {
			spec:     JobSpec{StartOffset: 42, EndOffset: 100},
			expected: map[int32]OffsetRange{0: {StartOffset: 42, EndOffset: 100}},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.expected, tc.spec.Ranges())
		})
	}
}

func TestJobSpec_Validate(t *testing.T) {
	const numCompartments = 4

	tests := map[string]struct {
		spec                JobSpec
		compartmentsEnabled bool
		expectedErr         string
	}{
		"disabled: accepts a single start/end range": {
			spec:                JobSpec{StartOffset: 1, EndOffset: 5},
			compartmentsEnabled: false,
		},
		"disabled: rejects offset ranges combined with start/end offsets": {
			spec: JobSpec{
				StartOffset:  1,
				EndOffset:    5,
				OffsetRanges: map[int32]OffsetRange{0: {StartOffset: 1, EndOffset: 5}},
			},
			compartmentsEnabled: false,
			expectedErr:         "offset_ranges should not be set",
		},
		"enabled: accepts offset ranges with valid cluster IDs": {
			spec: JobSpec{OffsetRanges: map[int32]OffsetRange{
				0: {StartOffset: 1, EndOffset: 5},
				3: {StartOffset: 2, EndOffset: 8},
			}},
			compartmentsEnabled: true,
		},
		"enabled: rejects start/end offsets": {
			spec: JobSpec{
				StartOffset:  1,
				EndOffset:    5,
				OffsetRanges: map[int32]OffsetRange{0: {StartOffset: 1, EndOffset: 5}},
			},
			compartmentsEnabled: true,
			expectedErr:         "start/end_offsets should not be set",
		},
		"enabled: rejects a cluster ID outside the valid range": {
			spec:                JobSpec{OffsetRanges: map[int32]OffsetRange{numCompartments: {StartOffset: 1, EndOffset: 5}}},
			compartmentsEnabled: true,
			expectedErr:         "invalid compartment id",
		},
		"enabled: rejects a negative cluster ID": {
			spec:                JobSpec{OffsetRanges: map[int32]OffsetRange{-1: {StartOffset: 1, EndOffset: 5}}},
			compartmentsEnabled: true,
			expectedErr:         "invalid compartment id",
		},
		"enabled: rejects empty range": {
			spec:                JobSpec{OffsetRanges: map[int32]OffsetRange{}},
			compartmentsEnabled: true,
			expectedErr:         "offset_ranges should not be empty when compartments is enabled",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := tc.spec.Validate(tc.compartmentsEnabled, numCompartments)
			if tc.expectedErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.expectedErr)
		})
	}
}
