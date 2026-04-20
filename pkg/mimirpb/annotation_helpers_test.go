// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

import (
	"errors"
	"strings"
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

			if len(tc.input) == 0 {
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

			require.Len(t, annErrs, len(protoAnnotations))
			for j := range protoAnnotations {
				assert.Equal(t, protoAnnotations[j], annErrs[j], "annotation %d: string round-trip mismatch", j)
			}
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

// TestTypedAnnotationStringsMerge verifies that final-form annotation strings
// with different sample counts are parsed into typed errors that merge properly
// in an Annotations set, producing a single entry with accumulated count.
// This is a regression test for a bug where annotations were wrapped as generic
// errors (losing their typed merge semantics), causing annotations for the same
// metric to appear as duplicates with slightly different counts.
func TestTypedAnnotationStringsMerge(t *testing.T) {
	tests := map[string]struct {
		// Two final-form annotation strings for the same metric but different stats.
		s1, s2    string
		wantType  annotations.AnnotationType
		wantCount float64
	}{
		"histogramQuantileForcedMonotonicityErr": {
			s1:        `PromQL info: input to histogram_quantile needed to be fixed for monotonicity (see https://prometheus.io/docs/prometheus/latest/querying/functions/#histogram_quantile) for metric name "bucket", from buckets 0.5 to 10, with a max diff of 0.01, over 5 samples from 2023-11-14T22:13:20Z to 2023-11-14T22:13:21Z`,
			s2:        `PromQL info: input to histogram_quantile needed to be fixed for monotonicity (see https://prometheus.io/docs/prometheus/latest/querying/functions/#histogram_quantile) for metric name "bucket", from buckets 1 to 20, with a max diff of 0.05, over 3 samples from 2023-11-14T22:13:22Z to 2023-11-14T22:13:23Z`,
			wantType:  annotations.AnnotationTypeHistogramQuantileForcedMonotonicity,
			wantCount: 7, // displayCount-1 stored: (5-1)+(3-1)+1 = 7
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			// Parse each final-form string into a typed error independently,
			// simulating two evaluation results from split-by-time.
			errs1 := StringsToAnnotationErrs([]string{tc.s1})
			errs2 := StringsToAnnotationErrs([]string{tc.s2})
			require.Len(t, errs1, 1)
			require.Len(t, errs2, 1)

			// Add both to an Annotations set — this triggers Merge().
			var ann annotations.Annotations
			ann.Add(errs1[0])
			ann.Add(errs2[0])

			// The two annotations must merge into a single entry.
			// When annotations are incorrectly wrapped as generic errors,
			// their Error() strings differ (because they include counts)
			// and they appear as two separate entries.
			require.Len(t, ann, 1,
				"expected the two annotations for the same metric to merge into a single entry")

			for _, merged := range ann {
				data := annotations.ExtractAnnotationData(merged)
				assert.Equal(t, tc.wantType, data.Type)
				assert.Equal(t, tc.wantCount, data.Fields["count"],
					"merged count should reflect both inputs")
			}
		})
	}
}

func TestStringsToAnnotationErrorsRoundTrip(t *testing.T) {
	tests := map[string]struct {
		input         string
		wantHistogram bool
		isFinal       bool
	}{
		"generic warning": {
			input:         "PromQL warning: some warning message",
			wantHistogram: false,
		},
		"generic plain string": {
			input:         "another annotation",
			wantHistogram: false,
		},
		"non-final HistogramQuantileForcedMonotonicity": {
			input:         `PromQL info: input to histogram_quantile needed to be fixed for monotonicity (see https://prometheus.io/docs/prometheus/latest/querying/functions/#histogram_quantile) for metric name "http_duration_bucket"`,
			wantHistogram: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			protoAnnotations := StringsToAnnotationErrors([]string{tc.input})
			require.Len(t, protoAnnotations, 1)
			assert.Equal(t, tc.wantHistogram, protoAnnotations[0].GetHistogramQuantile() != nil)

			if tc.isFinal {
				roundTripped := AnnotationErrorsToStrings(protoAnnotations)
				require.Len(t, roundTripped, 1)
				// Position label is now preserved through the string round-trip.
				assert.Equal(t, tc.input, roundTripped[0])
			}
		})
	}
}

// TestAnnotationFromDataNoSpuriousPosition verifies that factory-created annotations
// with no position label do not produce a bogus position like "1:1" when the error
// is later extracted and rendered with a query string.
func TestAnnotationFromDataNoSpuriousPosition(t *testing.T) {
	// Simulate what happens when the querier's protobuf codec encodes an annotation
	// with no position: the AnnotationError is sent over the wire and the QFE
	// decodes and round-trips through AnnotationErrorsToErrors → ErrorsToAnnotationErrors.
	types := []struct {
		name  string
		input AnnotationError
	}{
		{
			name: "histogramQuantileForcedMonotonicity with zero position",
			input: AnnotationError{
				Message: `PromQL info: input to histogram_quantile needed to be fixed for monotonicity (see https://prometheus.io/docs/prometheus/latest/querying/functions/#histogram_quantile) for metric name "bucket"`,
				Data: &AnnotationError_HistogramQuantile{
					HistogramQuantile: &AnnotationHistogramQuantileForcedMonotonicityData{
						Count:     10,
						MinTs:     1700000000000,
						MaxTs:     1700000001000,
						MinBucket: 0.5,
						MaxBucket: 10.0,
						MaxDiff:   0.01,
					},
				},
			},
		},
	}

	for _, tc := range types {
		t.Run(tc.name, func(t *testing.T) {
			// Round-trip: proto → error → proto
			errs := AnnotationErrorsToErrors([]AnnotationError{tc.input})
			require.Len(t, errs, 1)

			result := ErrorsToAnnotationErrors(errs)
			require.Len(t, result, 1)

			assert.Empty(t, result[0].PositionLabel, "PositionLabel should be empty")

			// The reconstructed error's position must not produce "1:1" when a query is available.
			data := annotations.ExtractAnnotationData(errs[0])
			assert.Empty(t, data.PositionLabel, "position label should be empty")
		})
	}
}

// TestPositionLabelPreservedThroughTypedParsing verifies that when annotation
// strings contain a position suffix (e.g. "(1:10)"), parsing them into typed
// errors and back to proto preserves the position label. This is the exact
// pipeline used in the remote execution path where the querier sends annotation
// strings and the QFE parses, processes, and re-serializes them.
func TestPositionLabelPreservedThroughTypedParsing(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "histogramQuantile with position",
			input: `PromQL info: input to histogram_quantile needed to be fixed for monotonicity (see https://prometheus.io/docs/prometheus/latest/querying/functions/#histogram_quantile) for metric name "bucket", from buckets 0.5 to 10, with a max diff of 0.01, over 42 samples from 2023-11-14T22:13:20Z to 2023-11-14T22:13:21Z (1:10)`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Step 1: Parse string → typed error (this is what StringsToAnnotationErrs does)
			errs := StringsToAnnotationErrs([]string{tc.input})
			require.Len(t, errs, 1)

			// Step 2: Convert error → proto (this is what ErrorsToAnnotationErrors does in the QFE)
			aes := ErrorsToAnnotationErrors(errs)
			require.Len(t, aes, 1)

			// Step 3: Check that position label survived from the string parser
			// through the error type to the proto representation.
			if strings.Contains(tc.input, "(1:10)") {
				assert.Equal(t, "1:10", aes[0].PositionLabel,
					"position label should be preserved from original string through typed error round-trip")
			} else {
				assert.Empty(t, aes[0].PositionLabel,
					"position label should be empty when input has no position suffix")
			}

			// Step 4: Convert back to string (this is what AnnotationErrorsToStrings does for the JSON response)
			strs := AnnotationErrorsToStrings(aes)
			require.Len(t, strs, 1)
			if strings.Contains(tc.input, "(1:10)") {
				assert.True(t, strings.HasSuffix(strs[0], "(1:10)"),
					"final string should contain position suffix, got: %s", strs[0])
			}
		})
	}
}
