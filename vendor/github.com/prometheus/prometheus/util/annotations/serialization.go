// Copyright The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package annotations

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/prometheus/prometheus/promql/parser/posrange"
)

// AnnotationType identifies the concrete type of an annotation error for serialization.
type AnnotationType int

const (
	AnnotationTypeGeneric                             AnnotationType = 0
	AnnotationTypePossibleNonCounter                  AnnotationType = 1
	AnnotationTypeHistogramQuantileForcedMonotonicity AnnotationType = 2
)

// mergeableAnnotation is implemented by annotation types that carry mergeable
// state beyond their message string. Adding a new mergeable annotation type
// only requires:
//  1. Implementing this interface on the concrete type.
//  2. Registering a factory in init() below.
//
// No changes to AnnotationData or the Extract/From functions are needed.
type mergeableAnnotation interface {
	annotationType() AnnotationType
	rawMessage() string              // Err.Error() — the merge key, without position/count decoration.
	mergeFields() map[string]float64 // Type-specific merge state as an opaque key-value map.
	applyMergeFields(map[string]float64)
}

// AnnotationData is a portable, type-agnostic representation of a typed annotation error,
// suitable for serialization to/from protobuf or other wire formats.
// Fields holds the type-specific merge state; its key names are defined by each
// concrete type's mergeFields/applyMergeFields implementation.
type AnnotationData struct {
	Type    AnnotationType
	Message string
	Fields  map[string]float64
	// PositionStart and PositionEnd are the character offsets into the query
	// string that triggered this annotation.
	PositionStart int
	PositionEnd   int
	// PositionLabel is the pre-computed "line:col" string for the position,
	// e.g. "1:25". Computed from the query string and PositionStart at
	// extraction time so it survives serialization without storing the full
	// query. Empty if position info is unavailable.
	PositionLabel string
}

// annotationFactory creates a zero-value instance of a mergeable annotation
// with the given raw message. The caller will apply merge state via applyMergeFields.
type annotationFactory func(msg string) mergeableAnnotation

var annotationFactories = map[AnnotationType]annotationFactory{}

func init() {
	annotationFactories[AnnotationTypePossibleNonCounter] = func(msg string) mergeableAnnotation {
		// Reconstruct the error chain matching NewPossibleNonCounterInfo:
		// Err: fmt.Errorf("%w %q", PossibleNonCounterInfo, metricName)
		prefix := PossibleNonCounterInfo.Error() + " "
		if quoted, ok := strings.CutPrefix(msg, prefix); ok {
			if metricName, err := strconv.Unquote(quoted); err == nil {
				return &possibleNonCounterErr{Err: fmt.Errorf("%w %q", PossibleNonCounterInfo, metricName)}
			}
		}
		return &possibleNonCounterErr{Err: fmt.Errorf("%s", msg)}
	}
	annotationFactories[AnnotationTypeHistogramQuantileForcedMonotonicity] = func(msg string) mergeableAnnotation {
		// Reconstruct the error chain matching NewHistogramQuantileForcedMonotonicityInfo:
		// Err: maybeAddMetricName(HistogramQuantileForcedMonotonicityInfo, metricName)
		sentinel := HistogramQuantileForcedMonotonicityInfo.Error()
		if suffix, ok := strings.CutPrefix(msg, sentinel+" for metric name "); ok {
			if metricName, err := strconv.Unquote(suffix); err == nil {
				return &histogramQuantileForcedMonotonicityErr{Err: maybeAddMetricName(HistogramQuantileForcedMonotonicityInfo, metricName)}
			}
		}
		if msg == sentinel {
			return &histogramQuantileForcedMonotonicityErr{Err: HistogramQuantileForcedMonotonicityInfo}
		}
		return &histogramQuantileForcedMonotonicityErr{Err: fmt.Errorf("%s", msg)}
	}
}

// ExtractAnnotationData extracts a portable representation from a typed annotation error.
// For unrecognized error types it returns a generic annotation with the error message.
func ExtractAnnotationData(err error) AnnotationData {
	var d AnnotationData

	// Extract position from any annoError implementation.
	var anErr annoError
	if errors.As(err, &anErr) {
		pos := anErr.GetPosition()
		if pos.Start >= 0 {
			d.PositionStart = int(pos.Start)
			d.PositionEnd = int(pos.End)
			if q := anErr.GetQuery(); q != "" {
				d.PositionLabel = pos.StartPosInput(q, 0)
			}
		}
		// Fall back to a stored position label (e.g. "1:10") that was
		// preserved from a previous serialization round-trip. This handles
		// the case where the original byte offset was lost during string
		// parsing but the pre-computed label was kept.
		if d.PositionLabel == "" {
			type posLabelGetter interface{ GetPositionLabel() string }
			if plg, ok := anErr.(posLabelGetter); ok {
				d.PositionLabel = plg.GetPositionLabel()
			}
		}
	}

	var m mergeableAnnotation
	if errors.As(err, &m) {
		d.Type = m.annotationType()
		d.Message = m.rawMessage()
		d.Fields = m.mergeFields()
		return d
	}
	d.Type = AnnotationTypeGeneric
	d.Message = err.Error()
	return d
}

// AnnotationFromData reconstructs a typed annotation error from its portable representation.
// The reconstructed error supports Merge() for proper annotation combining.
func AnnotationFromData(d AnnotationData) error {
	// Only set a real position when the data actually carries one.
	// PositionStart=0 is ambiguous (could be "first character" or "unset" in
	// proto3), so we require PositionEnd > 0 as evidence of a real position.
	// Using Start=-1 prevents StartPosInput from producing a misleading "1:1"
	// when the position was never set.
	pos := posrange.PositionRange{Start: -1, End: -1}
	if d.PositionEnd > 0 || d.PositionStart > 0 {
		pos = posrange.PositionRange{
			Start: posrange.Pos(d.PositionStart),
			End:   posrange.Pos(d.PositionEnd),
		}
	}

	if f, ok := annotationFactories[d.Type]; ok {
		a := f(d.Message)
		if len(d.Fields) > 0 {
			a.applyMergeFields(d.Fields)
		}
		// Always set position, even when pos is {-1, -1}. This overrides the
		// Go zero value {0, 0} that factories produce, preventing
		// ExtractAnnotationData from mistaking an unset position for "offset 0"
		// (which would render as "1:1").
		a.(annoError).SetPosition(pos)
		// Preserve a pre-computed position label (e.g. "1:10") so it survives
		// serialization round-trips even when we don't have the query string
		// to recompute it from a byte offset.
		if d.PositionLabel != "" {
			type posLabelSetter interface{ SetPositionLabel(string) }
			if pls, ok := a.(posLabelSetter); ok {
				pls.SetPositionLabel(d.PositionLabel)
			}
		}
		return a.(error)
	}
	// Generic or unknown: wrap as annoErr so it still implements annoError.
	return &annoErr{Err: fmt.Errorf("%s", d.Message), PositionRange: pos, positionLabel: d.PositionLabel}
}
