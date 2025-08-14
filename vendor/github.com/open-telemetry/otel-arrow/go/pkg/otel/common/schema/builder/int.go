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

// Int32Builder is a wrapper around the arrow Int32Builder.
type Int32Builder struct {
	builder       array.Builder
	transformNode *schema.TransformNode
	updateRequest *update.SchemaUpdateRequest
}

// Append appends a value to the underlying builder and updates the
// transform node if the builder is nil.
func (b *Int32Builder) Append(value int32) {
	if b.builder != nil {
		switch builder := b.builder.(type) {
		case *array.Int32Builder:
			builder.Append(value)
		case *array.Int32DictionaryBuilder:
			if err := builder.Append(value); err != nil {
				// Should never happen.
				panic(err)
			}
		default:
			// Should never happen.
			panic("unknown builder type")
		}

		return
	}

	if b.updateRequest != nil {
		// If the builder is nil, then the transform node is not optional.
		b.transformNode.RemoveOptional()
		b.updateRequest.Inc(&update.NewFieldEvent{FieldName: b.transformNode.Path()})
		b.updateRequest = nil // No need to report this again.
	}
}

// AppendNonZero appends a value to the underlying builder and updates the
// transform node if the builder is nil.
// Note: 0 values are not appended to the builder.
func (b *Int32Builder) AppendNonZero(value int32) {
	if b.builder != nil {
		if value == 0 {
			b.builder.AppendNull()
			return
		}

		switch builder := b.builder.(type) {
		case *array.Int32Builder:
			builder.Append(value)
		case *array.Int32DictionaryBuilder:
			if err := builder.Append(value); err != nil {
				// Should never happen.
				panic(err)
			}
		default:
			// Should never happen.
			panic("unknown builder type")
		}

		return
	}

	if value != 0 && b.updateRequest != nil {
		// If the builder is nil, then the transform node is not optional.
		b.transformNode.RemoveOptional()
		b.updateRequest.Inc(&update.NewFieldEvent{FieldName: b.transformNode.Path()})
		b.updateRequest = nil // No need to report this again.
	}
}

// AppendNull appends a null value to the underlying builder. If the builder is
// nil we do nothing as we have no information about the presence of this field
// in the data.
func (b *Int32Builder) AppendNull() {
	if b.builder != nil {
		b.builder.AppendNull()
		return
	}
}

// Int64Builder is a wrapper around the arrow Int64Builder.
type Int64Builder struct {
	builder       array.Builder
	transformNode *schema.TransformNode
	updateRequest *update.SchemaUpdateRequest
}

// Append appends a value to the underlying builder and updates the
// transform node if the builder is nil.
func (b *Int64Builder) Append(value int64) {
	if b.builder != nil {
		switch builder := b.builder.(type) {
		case *array.Int64Builder:
			builder.Append(value)
		case *array.Int64DictionaryBuilder:
			if err := builder.Append(value); err != nil {
				// Should never happen.
				panic(err)
			}
		default:
			// Should never happen.
			panic("unknown builder type")
		}

		return
	}

	if b.updateRequest != nil {
		// If the builder is nil, then the transform node is not optional.
		b.transformNode.RemoveOptional()
		b.updateRequest.Inc(&update.NewFieldEvent{FieldName: b.transformNode.Path()})
		b.updateRequest = nil // No need to report this again.
	}
}

// AppendNonZero appends a value to the underlying builder and updates the
// transform node if the builder is nil.
// Note: 0 value are not appended.
func (b *Int64Builder) AppendNonZero(value int64) {
	if b.builder != nil {
		if value == 0 {
			b.builder.AppendNull()
			return
		}

		switch builder := b.builder.(type) {
		case *array.Int64Builder:
			builder.Append(value)
		case *array.Int64DictionaryBuilder:
			if err := builder.Append(value); err != nil {
				// Should never happen.
				panic(err)
			}
		default:
			// Should never happen.
			panic("unknown builder type")
		}

		return
	}

	if value != 0 && b.updateRequest != nil {
		// If the builder is nil, then the transform node is not optional.
		b.transformNode.RemoveOptional()
		b.updateRequest.Inc(&update.NewFieldEvent{FieldName: b.transformNode.Path()})
		b.updateRequest = nil // No need to report this again.
	}
}

// AppendNull appends a null value to the underlying builder. If the builder is
// nil we do nothing as we have no information about the presence of this field
// in the data.
func (b *Int64Builder) AppendNull() {
	if b.builder != nil {
		b.builder.AppendNull()
		return
	}
}
