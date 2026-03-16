// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

import (
	"errors"
	"testing"

	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAnnotationErrorsRoundTrip(t *testing.T) {
	tests := map[string]struct {
		input []error
	}{
		"nil input": {
			input: nil,
		},
		"empty input": {
			input: []error{},
		},
		"generic error": {
			input: []error{errors.New("something went wrong")},
		},
		"possibleNonCounterErr with count": {
			input: []error{
				annotations.NewPossibleNonCounterInfo("metric_counter", posrange.PositionRange{Start: 0, End: 10}, 42),
			},
		},
		"histogramQuantileForcedMonotonicityErr with all fields": {
			input: []error{
				annotations.NewHistogramQuantileForcedMonotonicityInfo(
					"http_duration_bucket",
					posrange.PositionRange{Start: 5, End: 20},
					1700000000000, // ts
					0.5,           // minBucket
					10.0,          // maxBucket
					0.01,          // maxDiff
				),
			},
		},
		"multiple mixed annotation types": {
			input: []error{
				errors.New("plain warning"),
				annotations.NewPossibleNonCounterInfo("requests_total", posrange.PositionRange{Start: 0, End: 5}, 100),
				annotations.NewHistogramQuantileForcedMonotonicityInfo(
					"latency_bucket",
					posrange.PositionRange{Start: 10, End: 30},
					1700000000000,
					1.0,
					100.0,
					0.05,
				),
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			// Forward: errors → proto
			protoAnnotations := ErrorsToAnnotationErrors(tc.input)

			// Reverse: proto → errors
			roundTripped := AnnotationErrorsToErrors(protoAnnotations)

			if tc.input == nil || len(tc.input) == 0 {
				require.Empty(t, roundTripped)
				return
			}

			require.Len(t, roundTripped, len(tc.input))

			for i, original := range tc.input {
				rt := roundTripped[i]

				// Extract the canonical data from both to compare—this is what
				// the serialization layer sees.
				originalData := annotations.ExtractAnnotationData(original)
				rtData := annotations.ExtractAnnotationData(rt)

				assert.Equal(t, originalData.Type, rtData.Type, "annotation %d: Type mismatch", i)
				assert.Equal(t, originalData.Message, rtData.Message, "annotation %d: Message mismatch", i)
				assert.Equal(t, originalData.Fields, rtData.Fields, "annotation %d: Fields mismatch", i)
			}

			strs := AnnotationErrorsToStrings(protoAnnotations)
			annErrs := StringsToAnnotationErrors(strs)

			require.Equal(t, protoAnnotations, annErrs)
		})
	}
}

// TestAnnotationErrorsRoundTripMerge verifies that after a round-trip through
// proto, typed errors still participate correctly in Annotations.Merge().
func TestAnnotationErrorsRoundTripMerge(t *testing.T) {
	tests := map[string]struct {
		// Two errors with the same metric name that should merge into one.
		a, b     error
		wantType annotations.AnnotationType
	}{
		"possibleNonCounterErr": {
			a:        annotations.NewPossibleNonCounterInfo("my_metric", posrange.PositionRange{}, 10),
			b:        annotations.NewPossibleNonCounterInfo("my_metric", posrange.PositionRange{}, 20),
			wantType: annotations.AnnotationTypePossibleNonCounter,
		},
		"histogramQuantileForcedMonotonicityErr": {
			a:        annotations.NewHistogramQuantileForcedMonotonicityInfo("my_bucket", posrange.PositionRange{}, 1000, 1.0, 10.0, 0.01),
			b:        annotations.NewHistogramQuantileForcedMonotonicityInfo("my_bucket", posrange.PositionRange{}, 2000, 0.5, 20.0, 0.05),
			wantType: annotations.AnnotationTypeHistogramQuantileForcedMonotonicity,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			// Round-trip both errors through proto.
			rtA := AnnotationErrorsToErrors(ErrorsToAnnotationErrors([]error{tc.a}))[0]
			rtB := AnnotationErrorsToErrors(ErrorsToAnnotationErrors([]error{tc.b}))[0]

			// Pre-merge: both should independently have the correct type.
			dataA := annotations.ExtractAnnotationData(rtA)
			dataB := annotations.ExtractAnnotationData(rtB)
			require.Equal(t, tc.wantType, dataA.Type)
			require.Equal(t, tc.wantType, dataB.Type)

			// They must have the same Error() key for Annotations to merge them.
			require.Equal(t, rtA.Error(), rtB.Error(), "Error() strings must match for merge")

			// Add both to an Annotations set—this triggers Merge().
			var ann annotations.Annotations
			ann.Add(rtA)
			ann.Add(rtB)

			require.Len(t, ann, 1, "expected the two annotations to merge into a single entry")

			// Verify the merged entry has accumulated fields.
			for _, merged := range ann {
				mergedData := annotations.ExtractAnnotationData(merged)
				assert.Equal(t, tc.wantType, mergedData.Type)

				// Count should reflect both inputs were merged.
				// The exact value depends on the Merge() implementation, but it
				// must be strictly greater than either individual count.
				mergedCount := mergedData.Fields["count"]
				assert.Greater(t, mergedCount, dataA.Fields["count"],
					"merged count should exceed A's count")
				assert.Greater(t, mergedCount, dataB.Fields["count"],
					"merged count should exceed B's count")
			}
		})
	}
}

func TestStringsToAnnotationErrorsRoundTrip(t *testing.T) {
	input := []string{
		"PromQL warning: some warning message",
		`PromQL info: metric might not be a counter, name does not end in _total/_sum/_count/_bucket: "my_metric"`,
		"another annotation",
	}

	protoAnnotations := StringsToAnnotationErrors(input)
	roundTripped := AnnotationErrorsToStrings(protoAnnotations)

	require.Equal(t, input, roundTripped)

	// Verify all are generic type.
	for i, ae := range protoAnnotations {
		assert.Equal(t, ANNOTATION_GENERIC, ae.Type, "annotation %d: expected ANNOTATION_GENERIC", i)
		assert.Zero(t, ae.Count, "annotation %d: Count should be zero for generic", i)
	}
}
