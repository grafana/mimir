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
}

// annotationFactory creates a zero-value instance of a mergeable annotation
// with the given raw message. The caller will apply merge state via applyMergeFields.
type annotationFactory func(msg string) mergeableAnnotation

var annotationFactories = map[AnnotationType]annotationFactory{}

func init() {
	annotationFactories[AnnotationTypePossibleNonCounter] = func(msg string) mergeableAnnotation {
		return &possibleNonCounterErr{Err: fmt.Errorf("%s", msg)}
	}
	annotationFactories[AnnotationTypeHistogramQuantileForcedMonotonicity] = func(msg string) mergeableAnnotation {
		return &histogramQuantileForcedMonotonicityErr{Err: fmt.Errorf("%s", msg)}
	}
}

// ExtractAnnotationData extracts a portable representation from a typed annotation error.
// For unrecognized error types it returns a generic annotation with the error message.
func ExtractAnnotationData(err error) AnnotationData {
	var m mergeableAnnotation
	if errors.As(err, &m) {
		return AnnotationData{
			Type:    m.annotationType(),
			Message: m.rawMessage(),
			Fields:  m.mergeFields(),
		}
	}
	return AnnotationData{
		Type:    AnnotationTypeGeneric,
		Message: err.Error(),
	}
}

// AnnotationFromData reconstructs a typed annotation error from its portable representation.
// The reconstructed error supports Merge() for proper annotation combining.
func AnnotationFromData(d AnnotationData) error {
	if f, ok := annotationFactories[d.Type]; ok {
		a := f(d.Message)
		if len(d.Fields) > 0 {
			a.applyMergeFields(d.Fields)
		}
		return a.(error)
	}
	// Generic or unknown: wrap as annoErr so it still implements annoError.
	return &annoErr{Err: fmt.Errorf("%s", d.Message)}
}
