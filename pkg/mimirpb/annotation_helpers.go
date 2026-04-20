// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/prometheus/util/annotations"
)

// annotationStringParser parses a final-form annotation string back into the
// original prometheus annotation error type. Each implementation handles a
// specific annotation type using a type-specific regex.
type annotationStringParser interface {
	Parse(s string) (error, bool)
}

// annotationParsers is the ordered list of parsers tried when parsing annotation strings.
// More specific patterns (histogram) must come before less specific ones (possibleNonCounter)
// because both contain "over N samples" but the histogram suffix is longer.
var annotationParsers = []annotationStringParser{
	histogramQuantileStringParser{},
	possibleNonCounterStringParser{},
}

// --- possibleNonCounterStringParser ---

type possibleNonCounterStringParser struct{}

// possibleNonCounterFinalRe matches the final form produced by possibleNonCounterErr.Error()
// when SetFinal() has been called, with an optional positional suffix:
//
//	"<message>, over <count> samples"
//	"<message>, over <count> samples (<position>)"
var possibleNonCounterFinalRe = regexp.MustCompile(`^(.+), over (\d+) samples(?: \((\d+:\d+)\))?$`)

// possibleNonCounterRe matches the non-final form — the raw Err.Error() of a
// possibleNonCounterErr, which is the PossibleNonCounterInfo sentinel followed
// by the metric name.
var possibleNonCounterRe = regexp.MustCompile(
	`^(` + regexp.QuoteMeta(annotations.PossibleNonCounterInfo.Error()) + ` .+)$`,
)

func (possibleNonCounterStringParser) Parse(s string) (error, bool) {
	if m := possibleNonCounterFinalRe.FindStringSubmatch(s); m != nil {
		count, _ := strconv.Atoi(m[2])
		data := annotations.AnnotationData{
			Type:          annotations.AnnotationTypePossibleNonCounter,
			Message:       m[1],
			Fields:        map[string]float64{"count": float64(count)},
			PositionLabel: m[3], // captured position suffix e.g. "1:10", empty if absent
		}
		return annotations.AnnotationFromData(data), true
	}
	if possibleNonCounterRe.MatchString(s) {
		return annotations.AnnotationFromData(annotations.AnnotationData{
			Type:    annotations.AnnotationTypePossibleNonCounter,
			Message: s,
		}), true
	}
	return nil, false
}

// --- histogramQuantileStringParser ---

type histogramQuantileStringParser struct{}

// histogramQuantileFinalRe matches the final form produced by
// histogramQuantileForcedMonotonicityErr.Error() when SetFinal() has been called,
// with an optional positional suffix:
//
//	"<message>, from buckets <min> to <max>, with a max diff of <diff>, over <count> samples from <start> to <end>"
//	"<message>, from buckets <min> to <max>, with a max diff of <diff>, over <count> samples from <start> to <end> (<position>)"
var histogramQuantileFinalRe = regexp.MustCompile(
	`^(.+), from buckets (\S+) to (\S+), with a max diff of (\S+), over (\d+) samples from (\S+) to (\S+)(?: \((\d+:\d+)\))?$`,
)

// histogramQuantileRe matches the non-final form — the raw Err.Error() of a
// histogramQuantileForcedMonotonicityErr, which is the
// HistogramQuantileForcedMonotonicityInfo sentinel optionally followed by a
// metric name suffix.
var histogramQuantileRe = regexp.MustCompile(
	`^(` + regexp.QuoteMeta(annotations.HistogramQuantileForcedMonotonicityInfo.Error()) + `.*)$`,
)

func (histogramQuantileStringParser) Parse(s string) (error, bool) {
	if m := histogramQuantileFinalRe.FindStringSubmatch(s); m != nil {
		minBucket, _ := strconv.ParseFloat(m[2], 64)
		maxBucket, _ := strconv.ParseFloat(m[3], 64)
		maxDiff, _ := strconv.ParseFloat(m[4], 64)
		displayCount, _ := strconv.Atoi(m[5])
		startTime, _ := time.Parse(time.RFC3339, m[6])
		endTime, _ := time.Parse(time.RFC3339, m[7])

		return annotations.AnnotationFromData(annotations.AnnotationData{
			Type:    annotations.AnnotationTypeHistogramQuantileForcedMonotonicity,
			Message: m[1],
			Fields: map[string]float64{
				"count":      float64(displayCount - 1), // upstream Error() displays count+1
				"min_ts":     float64(startTime.Unix() * 1000),
				"max_ts":     float64(endTime.Unix() * 1000),
				"min_bucket": minBucket,
				"max_bucket": maxBucket,
				"max_diff":   maxDiff,
			},
			PositionLabel: m[8], // captured position suffix e.g. "1:10", empty if absent
		}), true
	}
	if histogramQuantileRe.MatchString(s) {
		return annotations.AnnotationFromData(annotations.AnnotationData{
			Type:    annotations.AnnotationTypeHistogramQuantileForcedMonotonicity,
			Message: s,
		}), true
	}
	return nil, false
}

// StringsToAnnotationErrors converts annotation strings (in final form) back to
// typed AnnotationError values. It parses each string into the original
// prometheus annotation error type, then converts those errors to AnnotationError.
func StringsToAnnotationErrors(ss []string) []AnnotationError {
	return ErrorsToAnnotationErrors(StringsToAnnotationErrs(ss))
}

// StringsToAnnotationErrs parses final-form annotation strings back into the
// original prometheus annotation error types. It tries each registered
// annotationStringParser in order and falls back to a generic error if none match.
func StringsToAnnotationErrs(ss []string) []error {
	if len(ss) == 0 {
		return nil
	}
	result := make([]error, len(ss))
	for i, s := range ss {
		result[i] = parseAnnotationString(s)
	}
	return result
}

func parseAnnotationString(s string) error {
	for _, p := range annotationParsers {
		if err, ok := p.Parse(s); ok {
			return err
		}
	}
	return errors.New(s)
}

// setFinaler is satisfied by the unexported annoError interface in the
// prometheus annotations package. It lets us call SetFinal() on reconstructed
// errors so that their Error() output includes the full detail suffix.
type setFinaler interface {
	SetFinal()
}

// AnnotationErrorsToStrings converts AnnotationError values to their final-form
// string representations by reconstructing the original prometheus error types
// and calling Error() on them after SetFinal(). If the AnnotationError has a
// pre-computed PositionLabel (e.g. "1:25"), it is appended as a suffix.
func AnnotationErrorsToStrings(aes []AnnotationError) []string {
	if len(aes) == 0 {
		return nil
	}
	errs := AnnotationErrorsToErrors(aes)
	result := make([]string, len(errs))
	for i, err := range errs {
		if sf, ok := err.(setFinaler); ok {
			sf.SetFinal()
		}
		s := err.Error()
		// Append position label if the error string doesn't already contain it
		// (typed errors with Query set include it via Error(); errors reconstructed
		// from cache don't have Query but carry the pre-computed label).
		if aes[i].PositionLabel != "" && !strings.HasSuffix(s, "("+aes[i].PositionLabel+")") {
			s = fmt.Sprintf("%s (%s)", s, aes[i].PositionLabel)
		}
		result[i] = s
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
			Type:          AnnotationErrorType(d.Type),
			Message:       d.Message,
			PositionLabel: d.PositionLabel,
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
			PositionLabel: ae.PositionLabel,
		})
	}
	return result
}
