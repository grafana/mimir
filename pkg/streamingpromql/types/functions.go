// SPDX-License-Identifier: AGPL-3.0-only

package types

import (
	"github.com/prometheus/prometheus/promql/parser/posrange"
)

// EmitAnnotationFunc is a function that emits the annotation created by generator.
type EmitAnnotationFunc func(generator AnnotationGenerator)

// AnnotationGenerator is a function that returns an annotation for the given metric name and expression position.
type AnnotationGenerator func(metricName string, expressionPosition posrange.PositionRange) error
