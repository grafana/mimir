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
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema/update"
)

type SparseUnionBuilder struct {
	protoDataType *arrow.SparseUnionType
	builder       *array.SparseUnionBuilder
	transformNode *schema.TransformNode
	updateRequest *update.SchemaUpdateRequest
}

func NewSparseUnionBuilder(
	protoDataType *arrow.SparseUnionType,
	builder *array.SparseUnionBuilder,
	transformNode *schema.TransformNode,
	updateRequest *update.SchemaUpdateRequest,
) *SparseUnionBuilder {
	return &SparseUnionBuilder{
		protoDataType: protoDataType,
		builder:       builder,
		transformNode: transformNode,
		updateRequest: updateRequest,
	}
}

func (sub *SparseUnionBuilder) NewSparseUnionArray() *array.SparseUnion {
	if sub.builder != nil {
		return sub.builder.NewSparseUnionArray()
	}
	return nil
}

func (sub *SparseUnionBuilder) Release() {
	if sub.builder != nil {
		sub.builder.Release()
	}
}

func (sub *SparseUnionBuilder) protoDataTypeAndTransformNode(childCode arrow.UnionTypeCode) (arrow.DataType, *schema.TransformNode) {
	for i, code := range sub.protoDataType.TypeCodes() {
		if code == childCode {
			return sub.protoDataType.Fields()[i].Type, sub.transformNode.Children[i]
		}
	}

	panic(fmt.Sprintf("child code %d not found in the proto schema", childCode))
}

func (sub *SparseUnionBuilder) getBuilder(childCode arrow.UnionTypeCode) array.Builder {
	if sub.builder == nil {
		return nil
	}
	structType := sub.builder.Type().(*arrow.SparseUnionType)
	for i, code := range structType.TypeCodes() {
		if code == childCode {
			return sub.builder.Child(i)
		}
	}
	return nil
}

// Uint8Builder returns a builder for the given child code.
func (sub *SparseUnionBuilder) Uint8Builder(code arrow.UnionTypeCode) *Uint8Builder {
	builder := sub.getBuilder(code)
	_, transformNode := sub.protoDataTypeAndTransformNode(code)

	if builder != nil {
		return &Uint8Builder{builder: builder, transformNode: transformNode, updateRequest: sub.updateRequest}
	} else {
		return &Uint8Builder{builder: nil, transformNode: transformNode, updateRequest: sub.updateRequest}
	}
}

// Uint32Builder returns a builder for the given child code.
func (sub *SparseUnionBuilder) Uint32Builder(code arrow.UnionTypeCode) *Uint32Builder {
	builder := sub.getBuilder(code)
	_, transformNode := sub.protoDataTypeAndTransformNode(code)

	if builder != nil {
		return &Uint32Builder{builder: builder, transformNode: transformNode, updateRequest: sub.updateRequest}
	} else {
		return &Uint32Builder{builder: nil, transformNode: transformNode, updateRequest: sub.updateRequest}
	}
}

// Uint64Builder returns a builder for the given child code.
func (sub *SparseUnionBuilder) Uint64Builder(code arrow.UnionTypeCode) *Uint64Builder {
	builder := sub.getBuilder(code)
	_, transformNode := sub.protoDataTypeAndTransformNode(code)

	if builder != nil {
		return &Uint64Builder{builder: builder, transformNode: transformNode, updateRequest: sub.updateRequest}
	} else {
		return &Uint64Builder{builder: nil, transformNode: transformNode, updateRequest: sub.updateRequest}
	}
}

// Int32Builder returns a builder for the given child code.
func (sub *SparseUnionBuilder) Int32Builder(code arrow.UnionTypeCode) *Int32Builder {
	builder := sub.getBuilder(code)
	_, transformNode := sub.protoDataTypeAndTransformNode(code)

	if builder != nil {
		return &Int32Builder{builder: builder, transformNode: transformNode, updateRequest: sub.updateRequest}
	} else {
		return &Int32Builder{builder: nil, transformNode: transformNode, updateRequest: sub.updateRequest}
	}
}

// Int64Builder returns a builder for the given child code.
func (sub *SparseUnionBuilder) Int64Builder(code arrow.UnionTypeCode) *Int64Builder {
	builder := sub.getBuilder(code)
	_, transformNode := sub.protoDataTypeAndTransformNode(code)

	if builder != nil {
		return &Int64Builder{builder: builder, transformNode: transformNode, updateRequest: sub.updateRequest}
	} else {
		return &Int64Builder{builder: nil, transformNode: transformNode, updateRequest: sub.updateRequest}
	}
}

// Float64Builder returns a builder for the given child code.
func (sub *SparseUnionBuilder) Float64Builder(code arrow.UnionTypeCode) *Float64Builder {
	builder := sub.getBuilder(code)
	_, transformNode := sub.protoDataTypeAndTransformNode(code)

	if builder != nil {
		return &Float64Builder{builder: builder.(*array.Float64Builder), transformNode: transformNode, updateRequest: sub.updateRequest}
	} else {
		return &Float64Builder{builder: nil, transformNode: transformNode, updateRequest: sub.updateRequest}
	}
}

// StringBuilder returns a builder for the given child code.
func (sub *SparseUnionBuilder) StringBuilder(code arrow.UnionTypeCode) *StringBuilder {
	builder := sub.getBuilder(code)
	_, transformNode := sub.protoDataTypeAndTransformNode(code)

	if builder != nil {
		return NewStringBuilder(builder, transformNode, sub.updateRequest)
	} else {
		return NewStringBuilder(nil, transformNode, sub.updateRequest)
	}
}

// BooleanBuilder returns a builder for the given child code.
func (sub *SparseUnionBuilder) BooleanBuilder(code arrow.UnionTypeCode) *BooleanBuilder {
	builder := sub.getBuilder(code)
	_, transformNode := sub.protoDataTypeAndTransformNode(code)

	if builder != nil {
		return &BooleanBuilder{builder: builder.(*array.BooleanBuilder), transformNode: transformNode, updateRequest: sub.updateRequest}
	} else {
		return &BooleanBuilder{builder: nil, transformNode: transformNode, updateRequest: sub.updateRequest}
	}
}

// BinaryBuilder returns a builder for the given child code.
func (sub *SparseUnionBuilder) BinaryBuilder(code arrow.UnionTypeCode) *BinaryBuilder {
	builder := sub.getBuilder(code)
	_, transformNode := sub.protoDataTypeAndTransformNode(code)

	if builder != nil {
		return &BinaryBuilder{builder: builder, transformNode: transformNode, updateRequest: sub.updateRequest}
	} else {
		return &BinaryBuilder{builder: nil, transformNode: transformNode, updateRequest: sub.updateRequest}
	}
}

// StructBuilder returns a builder for the given child code.
func (sub *SparseUnionBuilder) StructBuilder(code arrow.UnionTypeCode) *StructBuilder {
	builder := sub.getBuilder(code)
	structDT, transformNode := sub.protoDataTypeAndTransformNode(code)

	if builder != nil {
		return &StructBuilder{protoDataType: structDT.(*arrow.StructType), builder: builder.(*array.StructBuilder), transformNode: transformNode, updateRequest: sub.updateRequest}
	} else {
		return &StructBuilder{protoDataType: structDT.(*arrow.StructType), builder: nil, transformNode: transformNode, updateRequest: sub.updateRequest}
	}
}

func (sub *SparseUnionBuilder) AppendNull() {
	if sub.builder != nil {
		sub.builder.AppendNull()
	}
}

func (sub *SparseUnionBuilder) Append(code int8) {
	if sub.builder != nil {
		sub.builder.Append(code)
		return
	}

	if sub.updateRequest != nil {
		// If the builder is nil, then the transform node is not optional.
		sub.transformNode.RemoveOptional()
		sub.updateRequest.Inc(&update.NewFieldEvent{FieldName: sub.transformNode.Path()})
		sub.updateRequest = nil // No need to report this again.
	}
}
