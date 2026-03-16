// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

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
