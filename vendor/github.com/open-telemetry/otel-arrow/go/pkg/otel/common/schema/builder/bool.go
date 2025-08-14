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

// MapBuilder is a wrapper around the arrow MapBuilder.
type BooleanBuilder struct {
	builder       *array.BooleanBuilder
	transformNode *schema.TransformNode
	updateRequest *update.SchemaUpdateRequest
}

// Append appends a value to the underlying builder and updates the
// transform node if the builder is nil.
func (b *BooleanBuilder) Append(value bool) {
	if b.builder != nil {
		b.builder.Append(value)
		return
	}

	if b.updateRequest != nil {
		// If the builder is nil and value is true (default value being false),
		// then the transform node is not optional.
		b.transformNode.RemoveOptional()
		b.updateRequest.Inc(&update.NewFieldEvent{FieldName: b.transformNode.Path()})
		b.updateRequest = nil // No need to report this again.
	}
}

// AppendNonFalse appends a value to the underlying builder and updates the
// transform node if the builder is nil.
// Note: false values are not appended to the builder.
func (b *BooleanBuilder) AppendNonFalse(value bool) {
	if b.builder != nil {
		if !value {
			b.builder.AppendNull()
			return
		}

		b.builder.Append(value)
		return
	}

	if value && b.updateRequest != nil {
		// If the builder is nil and value is true (default value being false),
		// then the transform node is not optional.
		b.transformNode.RemoveOptional()
		b.updateRequest.Inc(&update.NewFieldEvent{FieldName: b.transformNode.Path()})
		b.updateRequest = nil // No need to report this again.
	}
}

// AppendNull appends a null value to the underlying builder. If the builder is
// nil we do nothing as we have no information about the presence of this field
// in the data.
func (b *BooleanBuilder) AppendNull() {
	if b.builder != nil {
		b.builder.AppendNull()
		return
	}
}
