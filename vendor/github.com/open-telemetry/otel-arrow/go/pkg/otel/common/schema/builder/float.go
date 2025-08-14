/*
 * Copyright The OpenTelemetry Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package builder

import (
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema/update"
)

// Float64Builder is a wrapper around the arrow array builder for float64.
type Float64Builder struct {
	builder       *array.Float64Builder
	transformNode *schema.TransformNode
	updateRequest *update.SchemaUpdateRequest
}

// Append adds a value to the underlying builder or updates the transform node
// if the builder is nil.
func (b *Float64Builder) Append(value float64) {
	if b.builder != nil {
		b.builder.Append(value)
		return
	}

	if b.updateRequest != nil {
		// If the builder is nil, then the transform node is not optional.
		b.transformNode.RemoveOptional()
		b.updateRequest.Inc(&update.NewFieldEvent{FieldName: b.transformNode.Path()})
		b.updateRequest = nil // No need to report this again.
	}
}

// AppendNonZero adds a value to the underlying builder or updates the transform node
// if the builder is nil.
// Note: 0 values are not appended to the builder.
func (b *Float64Builder) AppendNonZero(value float64) {
	if b.builder != nil {
		if value == 0 {
			b.builder.AppendNull()
			return
		}

		b.builder.Append(value)
		return
	}

	if value != 0 && b.updateRequest != nil {
		// If the builder is nil, then the transform node is not optional.
		b.transformNode.RemoveOptional()
		b.updateRequest.Inc(&update.NewFieldEvent{FieldName: b.transformNode.Path()})
		b.updateRequest = nil // No need to report this again.
	}
}

// AppendNull adds a null value to the underlying builder. If the builder is
// nil we do nothing as we have no information about the presence of this field
// in the data.
func (b *Float64Builder) AppendNull() {
	if b.builder != nil {
		b.builder.AppendNull()
		return
	}
}
