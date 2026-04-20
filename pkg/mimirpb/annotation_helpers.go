// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

import (
	"github.com/prometheus/prometheus/util/annotations"
)

// StringsToAnnotationErrors converts plain string annotations to generic AnnotationError values.
// Use this when encoding a QueryResponse from code that only has string annotations
// (e.g. the querier API handler that receives already-stringified annotations).
func StringsToAnnotationErrors(ss []string) []AnnotationError {
	if len(ss) == 0 {
		return nil
	}
	result := make([]AnnotationError, len(ss))
	for i, s := range ss {
		result[i] = AnnotationError{
			Type:    ANNOTATION_GENERIC,
			Message: s,
		}
	}
	return result
}

// AnnotationErrorsToStrings extracts the Message field from each AnnotationError.
// Use this when decoding a QueryResponse into a representation that uses string annotations
// (e.g. PrometheusResponse in the query-frontend middleware).
func AnnotationErrorsToStrings(aes []AnnotationError) []string {
	if len(aes) == 0 {
		return nil
	}
	result := make([]string, len(aes))
	for i, ae := range aes {
		result[i] = ae.Message
	}
	return result
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
