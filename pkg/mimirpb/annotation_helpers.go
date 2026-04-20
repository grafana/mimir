// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

import (
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/prometheus/prometheus/util/annotations"
)

// annotationStringParser parses a final-form annotation string back into an
// AnnotationError. Each implementation handles a specific AnnotationErrorType
// using a type-specific regex.
type annotationStringParser interface {
	Parse(s string) (AnnotationError, bool)
}

// annotationParsers is the ordered list of parsers tried by StringsToAnnotationErrors.
// More specific patterns (histogram) must come before less specific ones (possibleNonCounter)
// because both contain "over N samples" but the histogram suffix is longer.
var annotationParsers = []annotationStringParser{
	histogramQuantileStringParser{},
	possibleNonCounterStringParser{},
}

// --- possibleNonCounterStringParser ---

type possibleNonCounterStringParser struct{}

// possibleNonCounterRe matches the final form produced by possibleNonCounterErr.Error():
//
//	"<message>, over <count> samples"
var possibleNonCounterRe = regexp.MustCompile(`^(.+), over (\d+) samples$`)

func (possibleNonCounterStringParser) Parse(s string) (AnnotationError, bool) {
	m := possibleNonCounterRe.FindStringSubmatch(s)
	if m == nil {
		return AnnotationError{}, false
	}
	count, _ := strconv.Atoi(m[2])
	return AnnotationError{
		Type:    ANNOTATION_POSSIBLE_NON_COUNTER,
		Message: m[1],
		Count:   int32(count),
	}, true
}

// --- histogramQuantileStringParser ---

type histogramQuantileStringParser struct{}

// histogramQuantileRe matches the final form produced by
// histogramQuantileForcedMonotonicityErr.Error():
//
//	"<message>, from buckets <min> to <max>, with a max diff of <diff>, over <count> samples from <start> to <end>"
var histogramQuantileRe = regexp.MustCompile(
	`^(.+), from buckets (\S+) to (\S+), with a max diff of (\S+), over (\d+) samples from (\S+) to (\S+)$`,
)

func (histogramQuantileStringParser) Parse(s string) (AnnotationError, bool) {
	m := histogramQuantileRe.FindStringSubmatch(s)
	if m == nil {
		return AnnotationError{}, false
	}
	minBucket, _ := strconv.ParseFloat(m[2], 64)
	maxBucket, _ := strconv.ParseFloat(m[3], 64)
	maxDiff, _ := strconv.ParseFloat(m[4], 64)
	displayCount, _ := strconv.Atoi(m[5])
	startTime, _ := time.Parse(time.RFC3339, m[6])
	endTime, _ := time.Parse(time.RFC3339, m[7])

	return AnnotationError{
		Type:      ANNOTATION_HISTOGRAM_QUANTILE_FORCED_MONOTONICITY,
		Message:   m[1],
		Count:     int32(displayCount - 1), // upstream Error() displays count+1
		MinTs:     startTime.Unix() * 1000,
		MaxTs:     endTime.Unix() * 1000,
		MinBucket: minBucket,
		MaxBucket: maxBucket,
		MaxDiff:   maxDiff,
	}, true
}

// StringsToAnnotationErrors converts annotation strings (in final form) back to
// typed AnnotationError values. It tries each registered annotationStringParser
// in order and falls back to ANNOTATION_GENERIC if none match.
func StringsToAnnotationErrors(ss []string) []AnnotationError {
	if len(ss) == 0 {
		return nil
	}
	result := make([]AnnotationError, len(ss))
	for i, s := range ss {
		result[i] = parseAnnotationString(s)
	}
	return result
}

func parseAnnotationString(s string) AnnotationError {
	for _, p := range annotationParsers {
		if ae, ok := p.Parse(s); ok {
			return ae
		}
	}
	return AnnotationError{
		Type:    ANNOTATION_GENERIC,
		Message: s,
	}
}

// AnnotationErrorsToStrings converts AnnotationError values to their final-form
// string representations. For typed annotations, the string encodes all
// annotation data (count, timestamps, buckets, etc.) in the format matching the
// upstream Error() output with SetFinal() applied.
func AnnotationErrorsToStrings(aes []AnnotationError) []string {
	if len(aes) == 0 {
		return nil
	}
	result := make([]string, len(aes))
	for i, ae := range aes {
		result[i] = formatAnnotationError(ae)
	}
	return result
}

func formatAnnotationError(ae AnnotationError) string {
	switch ae.Type {
	case ANNOTATION_POSSIBLE_NON_COUNTER:
		return fmt.Sprintf("%s, over %d samples", ae.Message, ae.Count)
	case ANNOTATION_HISTOGRAM_QUANTILE_FORCED_MONOTONICITY:
		// Mirror the upstream skip when all configurable fields are zero.
		if ae.MinTs == 0 && ae.MaxTs == 0 && ae.MinBucket == 0 && ae.MaxBucket == 0 && ae.MaxDiff == 0 {
			return ae.Message
		}
		startTime := time.Unix(ae.MinTs/1000, 0).UTC().Format(time.RFC3339)
		endTime := time.Unix(ae.MaxTs/1000, 0).UTC().Format(time.RFC3339)
		return fmt.Sprintf("%s, from buckets %g to %g, with a max diff of %.2g, over %d samples from %s to %s",
			ae.Message, ae.MinBucket, ae.MaxBucket, ae.MaxDiff, ae.Count+1, startTime, endTime)
	default:
		return ae.Message
	}
}

// ErrorsToAnnotationErrors converts typed annotation errors to their protobuf representation.
func ErrorsToAnnotationErrors(errs []error) []AnnotationError {
	if len(errs) == 0 {
		return nil
	}
	result := make([]AnnotationError, len(errs))
	for i, err := range errs {
		d := annotations.ExtractAnnotationData(err)
		ae := AnnotationError{
			Type:    AnnotationErrorType(d.Type),
			Message: d.Message,
		}
		// Map opaque Fields to the concrete proto fields.
		if len(d.Fields) > 0 {
			ae.Count = int32(d.Fields["count"])
			ae.MinTs = int64(d.Fields["min_ts"])
			ae.MaxTs = int64(d.Fields["max_ts"])
			ae.MinBucket = d.Fields["min_bucket"]
			ae.MaxBucket = d.Fields["max_bucket"]
			ae.MaxDiff = d.Fields["max_diff"]
		}
		result[i] = ae
	}
	return result
}

// AnnotationErrorsToErrors converts protobuf annotation errors back to typed Go errors.
func AnnotationErrorsToErrors(aes []AnnotationError) []error {
	if len(aes) == 0 {
		return nil
	}
	result := make([]error, len(aes))
	for i, ae := range aes {
		result[i] = annotations.AnnotationFromData(annotations.AnnotationData{
			Type:    annotations.AnnotationType(ae.Type),
			Message: ae.Message,
			Fields: map[string]float64{
				"count":      float64(ae.Count),
				"min_ts":     float64(ae.MinTs),
				"max_ts":     float64(ae.MaxTs),
				"min_bucket": ae.MinBucket,
				"max_bucket": ae.MaxBucket,
				"max_diff":   ae.MaxDiff,
			},
		})
	}
	return result
}
