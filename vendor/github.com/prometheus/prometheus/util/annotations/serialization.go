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
	AnnotationTypeGeneric                                AnnotationType = 0
	AnnotationTypePossibleNonCounter                     AnnotationType = 1
	AnnotationTypeHistogramQuantileForcedMonotonicity    AnnotationType = 2
)

// AnnotationData is a portable representation of a typed annotation error,
// suitable for serialization to/from protobuf or other wire formats.
type AnnotationData struct {
	Type      AnnotationType
	// Message is the Err.Error() string — the deduplication/merge key.
	Message   string
	// Count is used by possibleNonCounterErr and histogramQuantileForcedMonotonicityErr.
	Count     int
	// Fields below are used only by histogramQuantileForcedMonotonicityErr.
	MinTs     int64
	MaxTs     int64
	MinBucket float64
	MaxBucket float64
	MaxDiff   float64
}

// ExtractAnnotationData extracts the portable data from a typed annotation error.
// For unrecognized error types, it returns a generic annotation with the error message.
func ExtractAnnotationData(err error) AnnotationData {
	var pnc *possibleNonCounterErr
	if errors.As(err, &pnc) {
		return AnnotationData{
			Type:    AnnotationTypePossibleNonCounter,
			Message: pnc.Err.Error(),
			Count:   pnc.count,
		}
	}

	var hq *histogramQuantileForcedMonotonicityErr
	if errors.As(err, &hq) {
		return AnnotationData{
			Type:      AnnotationTypeHistogramQuantileForcedMonotonicity,
			Message:   hq.Err.Error(),
			Count:     hq.count,
			MinTs:     hq.minTs,
			MaxTs:     hq.maxTs,
			MinBucket: hq.minBucket,
			MaxBucket: hq.maxBucket,
			MaxDiff:   hq.maxDiff,
		}
	}

	// Generic annoErr or plain error — just preserve the message.
	return AnnotationData{
		Type:    AnnotationTypeGeneric,
		Message: err.Error(),
	}
}

// AnnotationFromData reconstructs a typed annotation error from its portable representation.
// The reconstructed error supports Merge() for proper annotation combining.
func AnnotationFromData(d AnnotationData) error {
	switch d.Type {
	case AnnotationTypePossibleNonCounter:
		return &possibleNonCounterErr{
			Err:   fmt.Errorf("%s", d.Message),
			count: d.Count,
		}
	case AnnotationTypeHistogramQuantileForcedMonotonicity:
		return &histogramQuantileForcedMonotonicityErr{
			Err:       fmt.Errorf("%s", d.Message),
			count:     d.Count,
			minTs:     d.MinTs,
			maxTs:     d.MaxTs,
			minBucket: d.MinBucket,
			maxBucket: d.MaxBucket,
			maxDiff:   d.MaxDiff,
		}
	default:
		// Generic: wrap as annoErr so it still implements annoError interface.
		return &annoErr{
			Err: fmt.Errorf("%s", d.Message),
		}
	}
}
