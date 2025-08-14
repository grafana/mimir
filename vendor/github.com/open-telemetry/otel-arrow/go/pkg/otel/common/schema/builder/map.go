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
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema/update"
)

// MapBuilder is a wrapper around the arrow MapBuilder.
type MapBuilder struct {
	protoDataType *arrow.MapType
	builder       *array.MapBuilder
	transformNode *schema.TransformNode
	updateRequest *update.SchemaUpdateRequest
}

func (b *MapBuilder) Append(mapLen int, mapAppender func() error) error {
	if mapLen == 0 {
		b.AppendNull()
		return nil
	}
	b.Reserve(mapLen)
	return mapAppender()
}

// Reserve reserves space for the given number of entries in the map.
func (b *MapBuilder) Reserve(numEntries int) {
	if b.builder != nil {
		b.builder.Append(numEntries > 0)
		b.builder.Reserve(numEntries)
		return
	}

	if numEntries > 0 && b.updateRequest != nil {
		// If the builder is nil, then the transform node is not optional.
		b.transformNode.RemoveOptional()
		b.updateRequest.Inc(&update.NewFieldEvent{FieldName: b.transformNode.Path()})
		b.updateRequest = nil // No need to report this again.
	}
}

// AppendNull appends a null value to the underlying builder. If the builder is
// nil we do nothing as we have no information about the presence of this field
// in the data.
func (b *MapBuilder) AppendNull() {
	if b.builder != nil {
		b.builder.AppendNull()
		return
	}
}

func (b *MapBuilder) NewMapArray() *array.Map {
	if b.builder != nil {
		return b.builder.NewMapArray()
	}

	return nil
}

func (b *MapBuilder) Release() {
	if b.builder != nil {
		b.builder.Release()
	}
}

func (b *MapBuilder) KeyStringBuilder() *StringBuilder {
	var keyBuilder array.Builder
	if b.builder != nil {
		keyBuilder = b.builder.KeyBuilder()
	}

	return NewStringBuilder(
		keyBuilder,
		b.transformNode.Children[0],
		b.updateRequest,
	)
}

func (b *MapBuilder) KeyBinaryBuilder() *BinaryBuilder {
	var keyBuilder array.Builder
	if b.builder != nil {
		keyBuilder = b.builder.KeyBuilder()
	}

	return &BinaryBuilder{
		builder:       keyBuilder,
		transformNode: b.transformNode.Children[0],
		updateRequest: b.updateRequest,
	}
}

func (b *MapBuilder) KeyFixedSizeBinaryBuilder() *FixedSizeBinaryBuilder {
	var keyBuilder array.Builder
	if b.builder != nil {
		keyBuilder = b.builder.KeyBuilder()
	}

	return &FixedSizeBinaryBuilder{
		builder:       keyBuilder,
		transformNode: b.transformNode.Children[0],
		updateRequest: b.updateRequest,
	}
}

func (b *MapBuilder) KeyBooleanBuilder() *BooleanBuilder {
	var keyBuilder *array.BooleanBuilder
	if b.builder != nil {
		keyBuilder = b.builder.KeyBuilder().(*array.BooleanBuilder)
	}

	return &BooleanBuilder{
		builder:       keyBuilder,
		transformNode: b.transformNode.Children[0],
		updateRequest: b.updateRequest,
	}
}

func (b *MapBuilder) KeyFloat64Builder() *Float64Builder {
	var keyBuilder *array.Float64Builder
	if b.builder != nil {
		keyBuilder = b.builder.KeyBuilder().(*array.Float64Builder)
	}

	return &Float64Builder{
		builder:       keyBuilder,
		transformNode: b.transformNode.Children[0],
		updateRequest: b.updateRequest,
	}
}

func (b *MapBuilder) KeyUint32Builder() *Uint32Builder {
	var keyBuilder array.Builder
	if b.builder != nil {
		keyBuilder = b.builder.KeyBuilder()
	}

	return &Uint32Builder{
		builder:       keyBuilder,
		transformNode: b.transformNode.Children[0],
		updateRequest: b.updateRequest,
	}
}

func (b *MapBuilder) KeyUint64Builder() *Uint64Builder {
	var keyBuilder array.Builder
	if b.builder != nil {
		keyBuilder = b.builder.KeyBuilder()
	}

	return &Uint64Builder{
		builder:       keyBuilder,
		transformNode: b.transformNode.Children[0],
		updateRequest: b.updateRequest,
	}
}

func (b *MapBuilder) KeyInt64Builder() *Int64Builder {
	var keyBuilder *array.Int64Builder
	if b.builder != nil {
		keyBuilder = b.builder.KeyBuilder().(*array.Int64Builder)
	}

	return &Int64Builder{
		builder:       keyBuilder,
		transformNode: b.transformNode.Children[0],
		updateRequest: b.updateRequest,
	}
}

func (b *MapBuilder) KeyInt32Builder() *Int32Builder {
	var keyBuilder *array.Int32Builder
	if b.builder != nil {
		keyBuilder = b.builder.KeyBuilder().(*array.Int32Builder)
	}

	return &Int32Builder{
		builder:       keyBuilder,
		transformNode: b.transformNode.Children[0],
		updateRequest: b.updateRequest,
	}
}

func (b *MapBuilder) KeyListBuilder() *ListBuilder {
	var keyBuilder *array.ListBuilder
	if b.builder != nil {
		keyBuilder = b.builder.KeyBuilder().(*array.ListBuilder)
	}

	return &ListBuilder{
		builder:       keyBuilder,
		transformNode: b.transformNode.Children[0],
		updateRequest: b.updateRequest,
	}
}

func (b *MapBuilder) KeyMapBuilder() *MapBuilder {
	var keyBuilder *array.MapBuilder
	if b.builder != nil {
		keyBuilder = b.builder.KeyBuilder().(*array.MapBuilder)
	}

	return &MapBuilder{
		protoDataType: b.protoDataType.KeyType().(*arrow.MapType),
		builder:       keyBuilder,
		transformNode: b.transformNode.Children[0],
		updateRequest: b.updateRequest,
	}
}

func (b *MapBuilder) KeyStructBuilder() *StructBuilder {
	var keyBuilder *array.StructBuilder
	if b.builder != nil {
		keyBuilder = b.builder.KeyBuilder().(*array.StructBuilder)
	}

	return &StructBuilder{
		builder:       keyBuilder,
		transformNode: b.transformNode.Children[0],
		updateRequest: b.updateRequest,
	}
}

func (b *MapBuilder) KeyTimestampBuilder() *TimestampBuilder {
	var keyBuilder *array.TimestampBuilder
	if b.builder != nil {
		keyBuilder = b.builder.KeyBuilder().(*array.TimestampBuilder)
	}

	return &TimestampBuilder{
		builder:       keyBuilder,
		transformNode: b.transformNode.Children[0],
		updateRequest: b.updateRequest,
	}
}

func (b *MapBuilder) KeySparseUnionBuilder() *SparseUnionBuilder {
	var keyBuilder *array.SparseUnionBuilder
	if b.builder != nil {
		keyBuilder = b.builder.KeyBuilder().(*array.SparseUnionBuilder)
	}

	return &SparseUnionBuilder{
		protoDataType: b.protoDataType.KeyType().(*arrow.SparseUnionType),
		builder:       keyBuilder,
		transformNode: b.transformNode.Children[0],
		updateRequest: b.updateRequest,
	}
}

func (b *MapBuilder) ItemStringBuilder() *StringBuilder {
	var valueBuilder *array.StringBuilder
	if b.builder != nil {
		valueBuilder = b.builder.ItemBuilder().(*array.StringBuilder)
	}

	return NewStringBuilder(
		valueBuilder,
		b.transformNode.Children[1],
		b.updateRequest,
	)
}

func (b *MapBuilder) ItemBinaryBuilder() *BinaryBuilder {
	var valueBuilder *array.BinaryBuilder
	if b.builder != nil {
		valueBuilder = b.builder.ItemBuilder().(*array.BinaryBuilder)
	}

	return &BinaryBuilder{
		builder:       valueBuilder,
		transformNode: b.transformNode.Children[1],
		updateRequest: b.updateRequest,
	}
}

func (b *MapBuilder) ItemFixedSizeBinaryBuilder() *FixedSizeBinaryBuilder {
	var valueBuilder *array.FixedSizeBinaryBuilder
	if b.builder != nil {
		valueBuilder = b.builder.ItemBuilder().(*array.FixedSizeBinaryBuilder)
	}

	return &FixedSizeBinaryBuilder{
		builder:       valueBuilder,
		transformNode: b.transformNode.Children[1],
		updateRequest: b.updateRequest,
	}
}

func (b *MapBuilder) ItemBooleanBuilder() *BooleanBuilder {
	var valueBuilder *array.BooleanBuilder
	if b.builder != nil {
		valueBuilder = b.builder.ItemBuilder().(*array.BooleanBuilder)
	}

	return &BooleanBuilder{
		builder:       valueBuilder,
		transformNode: b.transformNode.Children[1],
		updateRequest: b.updateRequest,
	}
}

func (b *MapBuilder) ItemFloat64Builder() *Float64Builder {
	var valueBuilder *array.Float64Builder
	if b.builder != nil {
		valueBuilder = b.builder.ItemBuilder().(*array.Float64Builder)
	}

	return &Float64Builder{
		builder:       valueBuilder,
		transformNode: b.transformNode.Children[1],
		updateRequest: b.updateRequest,
	}
}

func (b *MapBuilder) ItemUint64Builder() *Uint64Builder {
	var valueBuilder *array.Uint64Builder
	if b.builder != nil {
		valueBuilder = b.builder.ItemBuilder().(*array.Uint64Builder)
	}

	return &Uint64Builder{
		builder:       valueBuilder,
		transformNode: b.transformNode.Children[1],
		updateRequest: b.updateRequest,
	}
}

func (b *MapBuilder) ItemUint32Builder() *Uint32Builder {
	var valueBuilder *array.Uint32Builder
	if b.builder != nil {
		valueBuilder = b.builder.ItemBuilder().(*array.Uint32Builder)
	}

	return &Uint32Builder{
		builder:       valueBuilder,
		transformNode: b.transformNode.Children[1],
		updateRequest: b.updateRequest,
	}
}

func (b *MapBuilder) ItemUint8Builder() *Uint8Builder {
	var valueBuilder *array.Uint8Builder
	if b.builder != nil {
		valueBuilder = b.builder.ItemBuilder().(*array.Uint8Builder)
	}

	return &Uint8Builder{
		builder:       valueBuilder,
		transformNode: b.transformNode.Children[1],
		updateRequest: b.updateRequest,
	}
}

func (b *MapBuilder) ItemInt64Builder() *Int64Builder {
	var valueBuilder *array.Int64Builder
	if b.builder != nil {
		valueBuilder = b.builder.ItemBuilder().(*array.Int64Builder)
	}

	return &Int64Builder{
		builder:       valueBuilder,
		transformNode: b.transformNode.Children[1],
		updateRequest: b.updateRequest,
	}
}

func (b *MapBuilder) ItemInt32Builder() *Int32Builder {
	var valueBuilder *array.Int32Builder
	if b.builder != nil {
		valueBuilder = b.builder.ItemBuilder().(*array.Int32Builder)
	}

	return &Int32Builder{
		builder:       valueBuilder,
		transformNode: b.transformNode.Children[1],
		updateRequest: b.updateRequest,
	}
}

func (b *MapBuilder) ItemTimestampBuilder() *TimestampBuilder {
	var valueBuilder *array.TimestampBuilder
	if b.builder != nil {
		valueBuilder = b.builder.ItemBuilder().(*array.TimestampBuilder)
	}

	return &TimestampBuilder{
		builder:       valueBuilder,
		transformNode: b.transformNode.Children[1],
		updateRequest: b.updateRequest,
	}
}

func (b *MapBuilder) ItemStructBuilder() *StructBuilder {
	var valueBuilder *array.StructBuilder
	if b.builder != nil {
		valueBuilder = b.builder.ItemBuilder().(*array.StructBuilder)
	}

	return &StructBuilder{
		builder:       valueBuilder,
		transformNode: b.transformNode.Children[1],
		updateRequest: b.updateRequest,
	}
}

func (b *MapBuilder) ItemListBuilder() *ListBuilder {
	var valueBuilder *array.ListBuilder
	if b.builder != nil {
		valueBuilder = b.builder.ItemBuilder().(*array.ListBuilder)
	}

	return &ListBuilder{
		builder:       valueBuilder,
		transformNode: b.transformNode.Children[1],
		updateRequest: b.updateRequest,
	}
}

func (b *MapBuilder) ItemMapBuilder() *MapBuilder {
	var valueBuilder *array.MapBuilder
	if b.builder != nil {
		valueBuilder = b.builder.ItemBuilder().(*array.MapBuilder)
	}

	return &MapBuilder{
		protoDataType: b.protoDataType.ItemType().(*arrow.MapType),
		builder:       valueBuilder,
		transformNode: b.transformNode.Children[1],
		updateRequest: b.updateRequest,
	}
}

func (b *MapBuilder) ItemSparseUnionBuilder() *SparseUnionBuilder {
	var valueBuilder *array.SparseUnionBuilder
	if b.builder != nil {
		valueBuilder = b.builder.ItemBuilder().(*array.SparseUnionBuilder)
	}

	return &SparseUnionBuilder{
		protoDataType: b.protoDataType.ItemType().(*arrow.SparseUnionType),
		builder:       valueBuilder,
		transformNode: b.transformNode.Children[1],
		updateRequest: b.updateRequest,
	}
}
