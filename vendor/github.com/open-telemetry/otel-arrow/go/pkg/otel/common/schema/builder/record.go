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
	"github.com/apache/arrow-go/v18/arrow/memory"

	carrow "github.com/open-telemetry/otel-arrow/go/pkg/arrow"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema"
	builder "github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema/config"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema/events"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema/transform"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema/update"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/observer"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/stats"
	"github.com/open-telemetry/otel-arrow/go/pkg/werror"
)

// RecordBuilderExt is a wrapper/decorator around array.RecordBuilder that
// allows you to request schema updates while building an Arrow record.
//
// This capability allows to automatically handle optional fields and dictionary
// fields that can directly mutate to their base type if the dictionary index
// overflows.
type RecordBuilderExt struct {
	// The allocator used to allocate memory for the underlying array builders.
	allocator memory.Allocator

	// The record builder that is being wrapped.
	recordBuilder *array.RecordBuilder

	// The prototype schema initially defined to build the AdaptiveSchema.
	protoSchema *arrow.Schema

	// The transform tree that is used to build the final schema from the
	// prototype schema after applying the transformation tree.
	transformTree *schema.TransformNode

	// Map dictId to transform node
	dictTransformNodes map[string]*transform.DictionaryField

	// The pending schema update requests.
	updateRequest *update.SchemaUpdateRequest

	// The current schema ID.
	schemaID string

	events *events.Events

	// stats is a set of counters that are incremented when certain events occur.
	stats *stats.ProducerStats

	// Label is a string that is used to identify the source of the data.
	// [optional].
	label string

	// Metadata to be added to the schema.
	metadata map[string]string

	// The observer that is notified when certain events occur.
	observer observer.ProducerObserver
}

// NewRecordBuilderExt creates a new RecordBuilderExt from the given allocator
// and a prototype schema.
func NewRecordBuilderExt(
	allocator memory.Allocator,
	protoSchema *arrow.Schema,
	dictConfig *builder.Dictionary,
	stats *stats.ProducerStats,
	observer observer.ProducerObserver,
) *RecordBuilderExt {
	schemaUpdateRequest := update.NewSchemaUpdateRequest()
	evts := &events.Events{
		DictionariesWithOverflow:     make(map[string]bool),
		DictionariesIndexTypeChanged: make(map[string]string),
	}
	transformTree, dictTransformNodes := schema.NewTransformTreeFrom(protoSchema, dictConfig, schemaUpdateRequest, evts)
	s := schema.NewSchemaFrom(protoSchema, transformTree, nil)
	schemaID := carrow.SchemaToID(s)
	recordBuilder := array.NewRecordBuilder(allocator, s)

	return &RecordBuilderExt{
		allocator:          allocator,
		recordBuilder:      recordBuilder,
		protoSchema:        protoSchema,
		transformTree:      transformTree,
		dictTransformNodes: dictTransformNodes,
		updateRequest:      schemaUpdateRequest,
		schemaID:           schemaID,
		events:             evts,
		stats:              stats,
		metadata:           make(map[string]string),
		observer:           observer,
	}
}

func (rb *RecordBuilderExt) Label() string {
	return rb.label
}

func (rb *RecordBuilderExt) SetLabel(label string) {
	rb.label = label
}

func (rb *RecordBuilderExt) Events() *events.Events {
	return rb.events
}

func (rb *RecordBuilderExt) SchemaID() string {
	return rb.schemaID
}

func (rb *RecordBuilderExt) Schema() *arrow.Schema {
	return rb.recordBuilder.Schema()
}

func (rb *RecordBuilderExt) AddMetadata(key, value string) {
	prevValue, ok := rb.metadata[key]
	if !ok || prevValue != value {
		rb.metadata[key] = value
		// If the metadata has changed, then the schema must be updated.
		rb.updateRequest.Inc(&update.MetadataEvent{MetadataKey: key})
	}
}

func (rb *RecordBuilderExt) Reserve(size int) {
	rb.recordBuilder.Reserve(size)
}

func (rb *RecordBuilderExt) Release() {
	rb.recordBuilder.Release()
}

// RecordBuilder returns the underlying array.RecordBuilder.
func (rb *RecordBuilderExt) RecordBuilder() *array.RecordBuilder {
	return rb.recordBuilder
}

// NewRecord returns a new record from the underlying array.RecordBuilder or
// ErrSchemaNotUpToDate if the schema is not up-to-date.
func (rb *RecordBuilderExt) NewRecord() (arrow.Record, error) {
	// If one of the tree transformation has been removed, or updated, then
	// the schema must be updated.
	if !rb.IsSchemaUpToDate() {
		rb.UpdateSchema()
		return nil, werror.Wrap(schema.ErrSchemaNotUpToDate)
	}

	record := rb.recordBuilder.NewRecord()

	// Detect dictionary overflow
	fields := rb.recordBuilder.Schema().Fields()
	columns := record.Columns()
	for fieldIdx := range fields {
		rb.detectDictionaryOverflow(&fields[fieldIdx], columns[fieldIdx])
	}

	// If dictionary overflow is detected, update the schema
	if !rb.IsSchemaUpToDate() {
		record.Release()
		rb.UpdateSchema()
		return nil, werror.Wrap(schema.ErrSchemaNotUpToDate)
	} else {
		return record, nil
	}
}

func (rb *RecordBuilderExt) detectDictionaryOverflow(field *arrow.Field, column arrow.Array) {
	switch dt := field.Type.(type) {
	case *arrow.StructType:
		fields := dt.Fields()
		structColumn := column.(*array.Struct)

		for i := 0; i < len(fields); i++ {
			subField := &fields[i]
			subColumn := structColumn.Field(i)
			rb.detectDictionaryOverflow(subField, subColumn)
		}
	case *arrow.ListType:
		elemField := dt.ElemField()
		listValues := column.(*array.List).ListValues()
		rb.detectDictionaryOverflow(&elemField, listValues)
	case arrow.UnionType:
		fields := dt.Fields()
		unionColumn := column.(array.Union)

		for i := 0; i < len(fields); i++ {
			rb.detectDictionaryOverflow(&fields[i], unionColumn.Field(i))
		}
	case *arrow.MapType:
		mapColumn := column.(*array.Map)
		keyField := dt.KeyField()
		keyColumn := mapColumn.Keys()
		valueField := dt.ValueField()
		valueColumn := mapColumn.ListValues()

		rb.detectDictionaryOverflow(&keyField, keyColumn)
		rb.detectDictionaryOverflow(&valueField, valueColumn)
	default:
		dictIdIdx := field.Metadata.FindKey(transform.DictIdKey)
		if dictIdIdx != -1 {
			dictId := field.Metadata.Values()[dictIdIdx]
			if dictTransform, ok := rb.dictTransformNodes[dictId]; ok {
				switch dictColumn := column.(type) {
				case *array.Dictionary:
					dictTransform.AddTotal(dictColumn.Len())
					dictTransform.SetCardinality(uint64(dictColumn.Dictionary().Len()), &rb.stats.RecordBuilderStats)
				}
			} else {
				panic(fmt.Sprintf("Dictionary transform not found for field %s", field.Name))
			}
		}
	}
}

func (rb *RecordBuilderExt) IsSchemaUpToDate() bool {
	return rb.updateRequest.Count() == 0
}

func (rb *RecordBuilderExt) SchemaUpdateRequestReset() {
	rb.updateRequest.Reset()
}

func (rb *RecordBuilderExt) builder(name string) array.Builder {
	// Retrieve the builder for the field.
	s := rb.recordBuilder.Schema()
	fieldIndices := s.FieldIndices(name)

	if len(fieldIndices) == 1 {
		return rb.recordBuilder.Field(fieldIndices[0])
	}
	return nil
}

// UpdateSchema updates the schema based on the pending schema update requests
// the initial prototype schema.
func (rb *RecordBuilderExt) UpdateSchema() {
	if rb.observer != nil {
		rb.updateRequest.Notify(rb.label, rb.observer)
	}
	if rb.stats.SchemaUpdates {
		println("=====================================================")
		fmt.Printf("Updating schema for %q\n", rb.label)
		println("From =====>")
		rb.ShowSchema()
	}

	rb.transformTree.RevertCounters()
	oldSchema := rb.Schema()
	newSchema := schema.NewSchemaFrom(rb.protoSchema, rb.transformTree, rb.metadata)

	// Build a new record builder with the updated schema
	// and transfer the dictionaries from the old record builder
	// to the new one.
	newRecBuilder := array.NewRecordBuilder(rb.allocator, newSchema)

	// Note: dictionaries are no longer copied between schema changes as it can
	// be very expensive when the number of reusable dictionary entries is not
	// an important percentage of the existing dictionary.
	// ToDo Find a way to identify when it makes sense to use dictionary migration between schema changes.
	//if err := rb.copyDictValuesTo(rb.recordBuilder, newRecBuilder); err != nil {
	//	panic(err)
	//}

	rb.recordBuilder.Release()
	rb.recordBuilder = newRecBuilder
	rb.schemaID = carrow.SchemaToID(newSchema)

	rb.updateRequest.Reset()
	rb.stats.RecordBuilderStats.SchemaUpdatesPerformed++

	if rb.stats.SchemaUpdates {
		println("To =====>")
		rb.ShowSchema()
	}
	if rb.observer != nil {
		rb.observer.OnSchemaUpdate(rb.label, oldSchema, newSchema)
	}
}

// CopyDictValuesTo recursively copy the dictionary values from the source
// record builder to the destination record builder.
func (rb *RecordBuilderExt) copyDictValuesTo(srcRecBuilder *array.RecordBuilder, destRecBuilder *array.RecordBuilder) error {
	srcSchema := srcRecBuilder.Schema()
	destSchema := destRecBuilder.Schema()

	for srcFieldIdx, srcField := range srcSchema.Fields() {
		srcBuilder := srcRecBuilder.Field(srcFieldIdx)
		destFieldIndices := destSchema.FieldIndices(srcField.Name)
		if len(destFieldIndices) == 1 {
			destBuilder := destRecBuilder.Field(destFieldIndices[0])
			if err := rb.copyFieldDictValuesTo(srcBuilder, destBuilder); err != nil {
				return werror.Wrap(err)
			}
		}
	}
	return nil
}

// Recursively copy the dictionary values from the source array builder to the
// destination array builder.
func (rb *RecordBuilderExt) copyFieldDictValuesTo(srcBuilder array.Builder, destBuilder array.Builder) (err error) {
	srcDT := srcBuilder.Type()
	destDT := destBuilder.Type()

	if srcDT.ID() == arrow.DICTIONARY && destDT.ID() != arrow.DICTIONARY {
		// The dictionary has been promoted to a string/binary field.
		return nil
	}

	if srcDT.ID() != destDT.ID() {
		panic("The source and destination record builders must have the same schema (except for dictionary indices)")
	}

	switch sBuilder := srcBuilder.(type) {
	case *array.StructBuilder:
		dBuilder, ok := destBuilder.(*array.StructBuilder)
		if !ok {
			return nil
		}
		for i := 0; i < sBuilder.NumField(); i++ {
			srcSubField := srcDT.(*arrow.StructType).Fields()[i]
			destSubFieldIdx, found := destDT.(*arrow.StructType).FieldIdx(srcSubField.Name)
			if !found {
				continue
			}
			if err = rb.copyFieldDictValuesTo(sBuilder.FieldBuilder(i), dBuilder.FieldBuilder(destSubFieldIdx)); err != nil {
				return
			}
		}
	case *array.ListBuilder:
		if err = rb.copyFieldDictValuesTo(sBuilder.ValueBuilder(), destBuilder.(*array.ListBuilder).ValueBuilder()); err != nil {
			return
		}
	case array.UnionBuilder:
		srcTypeCodes := srcDT.(arrow.UnionType).TypeCodes()
		destUnionDT := destDT.(arrow.UnionType)
		destTypeCodes := destUnionDT.TypeCodes()
		destChildIDs := destUnionDT.ChildIDs()
		for srcChildID := 0; srcChildID < len(srcTypeCodes); srcChildID++ {
			srcTypeCode := srcTypeCodes[srcChildID]
			destChildID := -1
			for i, destTypeCode := range destTypeCodes {
				if srcTypeCode == destTypeCode {
					destChildID = destChildIDs[i]
					break
				}
			}
			if destChildID == -1 {
				continue
			}
			if err = rb.copyFieldDictValuesTo(sBuilder.Child(srcChildID), destBuilder.(array.UnionBuilder).Child(destChildID)); err != nil {
				return
			}
		}
	case *array.MapBuilder:
		if err = rb.copyFieldDictValuesTo(sBuilder.KeyBuilder(), destBuilder.(*array.MapBuilder).KeyBuilder()); err != nil {
			return werror.Wrap(err)
		}
		if err = rb.copyFieldDictValuesTo(sBuilder.ItemBuilder(), destBuilder.(*array.MapBuilder).ItemBuilder()); err != nil {
			return werror.Wrap(err)
		}
	case array.DictionaryBuilder:
		srcDictArr := sBuilder.NewDictionaryArray()
		defer srcDictArr.Release()
		srcDict := srcDictArr.Dictionary()
		defer srcDict.Release()
		switch dict := srcDict.(type) {
		case *array.String:
			err = destBuilder.(*array.BinaryDictionaryBuilder).InsertStringDictValues(dict)
		case *array.Binary:
			err = destBuilder.(*array.BinaryDictionaryBuilder).InsertDictValues(dict)
		case *array.FixedSizeBinary:
			err = destBuilder.(*array.FixedSizeBinaryDictionaryBuilder).InsertDictValues(dict)
		case *array.Int32:
			err = destBuilder.(*array.Int32DictionaryBuilder).InsertDictValues(dict)
		case *array.Int64:
			err = destBuilder.(*array.Int64DictionaryBuilder).InsertDictValues(dict)
		case *array.Uint32:
			err = destBuilder.(*array.Uint32DictionaryBuilder).InsertDictValues(dict)
		case *array.Duration:
			err = destBuilder.(*array.DurationDictionaryBuilder).InsertDictValues(dict)
		default:
			panic("copyFieldDictValuesTo: unsupported dictionary type " + dict.DataType().Name())
		}
	}
	return nil
}

func (rb *RecordBuilderExt) protoDataTypeAndTransformNode(name string) (arrow.DataType, *schema.TransformNode) {
	// Retrieve the transform node for the field.
	protoFieldIndices := rb.protoSchema.FieldIndices(name)

	if len(protoFieldIndices) == 0 {
		// The field doesn't exist in the proto schema so we panic because this
		// is a programming error.
		panic(fmt.Sprintf("field %q not found in the proto schema", name))
	}

	if len(protoFieldIndices) > 1 {
		// The field is ambiguous in the proto schema so we panic because this
		// is a programming error.
		panic(fmt.Sprintf("field %q is ambiguous in the proto schema", name))
	}

	return rb.protoSchema.Field(protoFieldIndices[0]).Type, rb.transformTree.Children[protoFieldIndices[0]]
}

// TimestampBuilder returns a TimestampBuilder wrapper for the field with the
// given name. If the underlying builder doesn't exist, an empty wrapper is
// returned, so that the feeding process can continue without panicking. This
// is useful to handle optional fields.
func (rb *RecordBuilderExt) TimestampBuilder(name string) *TimestampBuilder {
	_, transformNode := rb.protoDataTypeAndTransformNode(name)
	b := rb.builder(name)

	if b != nil {
		return &TimestampBuilder{builder: b.(*array.TimestampBuilder), transformNode: transformNode, updateRequest: rb.updateRequest}
	} else {
		return &TimestampBuilder{builder: nil, transformNode: transformNode, updateRequest: rb.updateRequest}
	}
}

// DurationBuilder returns a DurationBuilder wrapper for the field with the
// given name. If the underlying builder doesn't exist, an empty wrapper is
// returned, so that the feeding process can continue without panicking. This
// is useful to handle optional fields.
func (rb *RecordBuilderExt) DurationBuilder(name string) *DurationBuilder {
	_, transformNode := rb.protoDataTypeAndTransformNode(name)
	b := rb.builder(name)

	if b != nil {
		return &DurationBuilder{builder: b, transformNode: transformNode, updateRequest: rb.updateRequest}
	} else {
		return &DurationBuilder{builder: nil, transformNode: transformNode, updateRequest: rb.updateRequest}
	}
}

// FixedSizeBinaryBuilder returns a FixedSizeBinaryBuilder wrapper for the
// field with the given name. If the underlying builder doesn't exist, an empty
// wrapper is returned, so that the feeding process can continue without
// panicking. This is useful to handle optional fields.
func (rb *RecordBuilderExt) FixedSizeBinaryBuilder(name string) *FixedSizeBinaryBuilder {
	_, transformNode := rb.protoDataTypeAndTransformNode(name)
	b := rb.builder(name)

	if b != nil {
		return &FixedSizeBinaryBuilder{builder: b, transformNode: transformNode, updateRequest: rb.updateRequest}
	} else {
		return &FixedSizeBinaryBuilder{builder: nil, transformNode: transformNode, updateRequest: rb.updateRequest}
	}
}

// MapBuilder returns a MapBuilder wrapper for the field with the given name.
// If the underlying builder doesn't exist, an empty wrapper is returned, so
// that the feeding process can continue without panicking. This is useful to
// handle optional fields.
func (rb *RecordBuilderExt) MapBuilder(name string) *MapBuilder {
	dt, transformNode := rb.protoDataTypeAndTransformNode(name)
	b := rb.builder(name)

	if b != nil {
		return &MapBuilder{
			protoDataType: dt.(*arrow.MapType),
			builder:       b.(*array.MapBuilder),
			transformNode: transformNode,
			updateRequest: rb.updateRequest,
		}
	} else {
		return &MapBuilder{
			protoDataType: dt.(*arrow.MapType),
			builder:       nil,
			transformNode: transformNode,
			updateRequest: rb.updateRequest,
		}
	}
}

// StringBuilder returns a StringBuilder wrapper for the field with the given
// name. If the underlying builder doesn't exist, an empty wrapper is returned,
// so that the feeding process can continue without panicking. This is useful
// to handle optional fields.
func (rb *RecordBuilderExt) StringBuilder(name string) *StringBuilder {
	_, transformNode := rb.protoDataTypeAndTransformNode(name)
	b := rb.builder(name)

	if b != nil {
		return NewStringBuilder(b, transformNode, rb.updateRequest)
	} else {
		return NewStringBuilder(nil, transformNode, rb.updateRequest)
	}
}

// BooleanBuilder returns a BooleanBuilder wrapper for the field with the given
// name. If the underlying builder doesn't exist, an empty wrapper is returned,
// so that the feeding process can continue without panicking. This is useful
// to handle optional fields.
func (rb *RecordBuilderExt) BooleanBuilder(name string) *BooleanBuilder {
	_, transformNode := rb.protoDataTypeAndTransformNode(name)
	b := rb.builder(name)

	if b != nil {
		return &BooleanBuilder{builder: b.(*array.BooleanBuilder), transformNode: transformNode, updateRequest: rb.updateRequest}
	} else {
		return &BooleanBuilder{builder: nil, transformNode: transformNode, updateRequest: rb.updateRequest}
	}
}

// BinaryBuilder returns a BinaryBuilder wrapper for the field with the given
// name. If the underlying builder doesn't exist, an empty wrapper is returned,
// so that the feeding process can continue without panicking. This is useful
// to handle optional fields.
func (rb *RecordBuilderExt) BinaryBuilder(name string) *BinaryBuilder {
	_, transformNode := rb.protoDataTypeAndTransformNode(name)
	b := rb.builder(name)

	if b != nil {
		return &BinaryBuilder{builder: b, transformNode: transformNode, updateRequest: rb.updateRequest}
	} else {
		return &BinaryBuilder{builder: nil, transformNode: transformNode, updateRequest: rb.updateRequest}
	}
}

// Uint8Builder returns a Uint8Builder wrapper for the field with the given
// name. If the underlying builder doesn't exist, an empty wrapper is returned,
// so that the feeding process can continue without panicking. This is useful
// to handle optional fields.
func (rb *RecordBuilderExt) Uint8Builder(name string) *Uint8Builder {
	_, transformNode := rb.protoDataTypeAndTransformNode(name)
	b := rb.builder(name)

	if b != nil {
		return &Uint8Builder{builder: b, transformNode: transformNode, updateRequest: rb.updateRequest}
	} else {
		return &Uint8Builder{builder: nil, transformNode: transformNode, updateRequest: rb.updateRequest}
	}
}

// Uint16Builder returns a Uint16Builder wrapper for the field with the given
// name. If the underlying builder doesn't exist, an empty wrapper is returned,
// so that the feeding process can continue without panicking. This is useful
// to handle optional fields.
func (rb *RecordBuilderExt) Uint16Builder(name string) *Uint16Builder {
	_, transformNode := rb.protoDataTypeAndTransformNode(name)
	b := rb.builder(name)

	if b != nil {
		return &Uint16Builder{builder: b, transformNode: transformNode, updateRequest: rb.updateRequest}
	} else {
		return &Uint16Builder{builder: nil, transformNode: transformNode, updateRequest: rb.updateRequest}
	}
}

// Uint32Builder returns a Uint32Builder wrapper for the field with the given
// name. If the underlying builder doesn't exist, an empty wrapper is returned,
// so that the feeding process can continue without panicking. This is useful
// to handle optional fields.
func (rb *RecordBuilderExt) Uint32Builder(name string) *Uint32Builder {
	_, transformNode := rb.protoDataTypeAndTransformNode(name)
	b := rb.builder(name)

	if b != nil {
		return &Uint32Builder{builder: b, transformNode: transformNode, updateRequest: rb.updateRequest}
	} else {
		return &Uint32Builder{builder: nil, transformNode: transformNode, updateRequest: rb.updateRequest}
	}
}

// Uint64Builder returns a Uint64Builder wrapper for the field with the given
// name. If the underlying builder doesn't exist, an empty wrapper is returned,
// so that the feeding process can continue without panicking. This is useful
// to handle optional fields.
func (rb *RecordBuilderExt) Uint64Builder(name string) *Uint64Builder {
	_, transformNode := rb.protoDataTypeAndTransformNode(name)
	b := rb.builder(name)

	if b != nil {
		return &Uint64Builder{builder: b, transformNode: transformNode, updateRequest: rb.updateRequest}
	} else {
		return &Uint64Builder{builder: nil, transformNode: transformNode, updateRequest: rb.updateRequest}
	}
}

// Uint16DeltaBuilder returns a Uint16DeltaBuilder wrapper for the field with the given
// name. If the underlying builder doesn't exist, an empty wrapper is returned,
// so that the feeding process can continue without panicking. This is useful
// to handle optional fields.
func (rb *RecordBuilderExt) Uint16DeltaBuilder(name string) *Uint16DeltaBuilder {
	_, transformNode := rb.protoDataTypeAndTransformNode(name)
	b := rb.builder(name)

	if b != nil {
		return NewUint16DeltaBuilder(b, transformNode, rb.updateRequest)
	} else {
		return NewUint16DeltaBuilder(nil, transformNode, rb.updateRequest)
	}
}

// Uint32DeltaBuilder returns a Uint32DeltaBuilder wrapper for the field with the given
// name. If the underlying builder doesn't exist, an empty wrapper is returned,
// so that the feeding process can continue without panicking. This is useful
// to handle optional fields.
func (rb *RecordBuilderExt) Uint32DeltaBuilder(name string) *Uint32DeltaBuilder {
	_, transformNode := rb.protoDataTypeAndTransformNode(name)
	b := rb.builder(name)

	if b != nil {
		return NewUint32DeltaBuilder(b, transformNode, rb.updateRequest)
	} else {
		return NewUint32DeltaBuilder(nil, transformNode, rb.updateRequest)
	}
}

// Int32Builder returns a Int32Builder wrapper for the field with the given
// name. If the underlying builder doesn't exist, an empty wrapper is returned,
// so that the feeding process can continue without panicking. This is useful
// to handle optional fields.
func (rb *RecordBuilderExt) Int32Builder(name string) *Int32Builder {
	_, transformNode := rb.protoDataTypeAndTransformNode(name)
	b := rb.builder(name)

	if b != nil {
		return &Int32Builder{builder: b, transformNode: transformNode, updateRequest: rb.updateRequest}
	} else {
		return &Int32Builder{builder: nil, transformNode: transformNode, updateRequest: rb.updateRequest}
	}
}

// Int64Builder returns a Int64Builder wrapper for the field with the given
// name. If the underlying builder doesn't exist, an empty wrapper is returned,
// so that the feeding process can continue without panicking. This is useful
// to handle optional fields.
func (rb *RecordBuilderExt) Int64Builder(name string) *Int64Builder {
	_, transformNode := rb.protoDataTypeAndTransformNode(name)
	b := rb.builder(name)

	if b != nil {
		return &Int64Builder{builder: b, transformNode: transformNode, updateRequest: rb.updateRequest}
	} else {
		return &Int64Builder{builder: nil, transformNode: transformNode, updateRequest: rb.updateRequest}
	}
}

// Float64Builder returns a Float64Builder wrapper for the field with the given
// name. If the underlying builder doesn't exist, an empty wrapper is returned,
// so that the feeding process can continue without panicking. This is useful
// to handle optional fields.
func (rb *RecordBuilderExt) Float64Builder(name string) *Float64Builder {
	_, transformNode := rb.protoDataTypeAndTransformNode(name)
	b := rb.builder(name)

	if b != nil {
		return &Float64Builder{builder: b.(*array.Float64Builder), transformNode: transformNode, updateRequest: rb.updateRequest}
	} else {
		return &Float64Builder{builder: nil, transformNode: transformNode, updateRequest: rb.updateRequest}
	}
}

// StructBuilder returns a StructBuilder wrapper for the field with the given
// name. If the underlying builder doesn't exist, an empty wrapper is returned,
// so that the feeding process can continue without panicking. This is useful
// to handle optional fields.
func (rb *RecordBuilderExt) StructBuilder(name string) *StructBuilder {
	protoDataType, transformNode := rb.protoDataTypeAndTransformNode(name)
	b := rb.builder(name)

	if b != nil {
		return &StructBuilder{protoDataType: protoDataType.(*arrow.StructType), builder: b.(*array.StructBuilder), transformNode: transformNode, updateRequest: rb.updateRequest}
	} else {
		return &StructBuilder{protoDataType: protoDataType.(*arrow.StructType), builder: nil, transformNode: transformNode, updateRequest: rb.updateRequest}
	}
}

// ListBuilder returns a ListBuilder wrapper for the field with the given
// name. If the underlying builder doesn't exist, an empty wrapper is returned,
// so that the feeding process can continue without panicking. This is useful
// to handle optional fields.
func (rb *RecordBuilderExt) ListBuilder(name string) *ListBuilder {
	protoDataType, transformNode := rb.protoDataTypeAndTransformNode(name)
	b := rb.builder(name)

	if b != nil {
		return &ListBuilder{protoDataType: protoDataType.(*arrow.ListType), builder: b.(*array.ListBuilder), transformNode: transformNode, updateRequest: rb.updateRequest}
	} else {
		return &ListBuilder{protoDataType: protoDataType.(*arrow.ListType), builder: nil, transformNode: transformNode, updateRequest: rb.updateRequest}
	}
}

// SparseUnionBuilder returns a SparseUnionBuilder wrapper for the field with
// the given name. If the underlying builder doesn't exist, an empty wrapper is
// returned, so that the feeding process can continue without panicking. This
// is useful to handle optional fields.
func (rb *RecordBuilderExt) SparseUnionBuilder(name string) *SparseUnionBuilder {
	protoDataType, transformNode := rb.protoDataTypeAndTransformNode(name)
	b := rb.builder(name)

	if b != nil {
		return &SparseUnionBuilder{protoDataType: protoDataType.(*arrow.SparseUnionType), builder: b.(*array.SparseUnionBuilder), transformNode: transformNode, updateRequest: rb.updateRequest}
	} else {
		return &SparseUnionBuilder{protoDataType: protoDataType.(*arrow.SparseUnionType), builder: nil, transformNode: transformNode, updateRequest: rb.updateRequest}
	}
}

func (rb *RecordBuilderExt) ShowSchema() {
	rb.VisitRecordBuilder(rb.recordBuilder, "")
}

func (rb *RecordBuilderExt) VisitRecordBuilder(recBuilder *array.RecordBuilder, prefix string) {
	s := recBuilder.Schema()
	println(prefix + "Schema {")
	for i, b := range recBuilder.Fields() {
		field := s.Field(i)
		rb.VisitField(b, &field, prefix+"  ")
	}
	println(prefix + "}")
}

func (rb *RecordBuilderExt) VisitField(builder array.Builder, field *arrow.Field, prefix string) {
	fmt.Printf("%s%s: ", prefix, field.Name)
	rb.VisitDataType(builder, field.Type, dictionaryID(field), prefix)
	fmt.Println()
}

func (rb *RecordBuilderExt) VisitDataType(builder array.Builder, dt arrow.DataType, dictID string, prefix string) {
	switch t := dt.(type) {
	case *arrow.BooleanType:
		fmt.Printf("Bool")
	case *arrow.Int8Type:
		fmt.Printf("Int8")
	case *arrow.Int16Type:
		fmt.Printf("Int16")
	case *arrow.Int32Type:
		fmt.Printf("Int32")
	case *arrow.Int64Type:
		fmt.Printf("Int64")
	case *arrow.Uint8Type:
		fmt.Printf("Uint8")
	case *arrow.Uint16Type:
		fmt.Printf("Uint16")
	case *arrow.Uint32Type:
		fmt.Printf("Uint32")
	case *arrow.Uint64Type:
		fmt.Printf("Uint64")
	case *arrow.Float32Type:
		fmt.Printf("Float32")
	case *arrow.Float64Type:
		fmt.Printf("Float64")
	case *arrow.StringType:
		fmt.Printf("String")
	case *arrow.BinaryType:
		fmt.Printf("Binary")
	case *arrow.TimestampType:
		fmt.Printf("Timestamp")
	case *arrow.DurationType:
		fmt.Printf("Duration")
	case *arrow.StructType:
		structBuilder := builder.(*array.StructBuilder)
		fmt.Printf("Struct {\n")
		for i, field := range t.Fields() {
			rb.VisitField(structBuilder.FieldBuilder(i), &field, prefix+"  ")
		}
		fmt.Printf("%s}", prefix)
	case *arrow.ListType:
		listBuilder := builder.(*array.ListBuilder)
		fmt.Printf("[")
		elemField := t.ElemField()
		rb.VisitDataType(listBuilder.ValueBuilder(), elemField.Type, "", prefix)
		fmt.Printf("]")
	case *arrow.DictionaryType:
		dictionaryBuilder := builder.(array.DictionaryBuilder)
		var cumulativeTotal uint64
		var card uint64
		if len(dictID) > 0 {
			if dictTransform, ok := rb.dictTransformNodes[dictID]; ok {
				cumulativeTotal = dictTransform.CumulativeTotal()
				card = dictTransform.Cardinality()
			} else {
				panic(fmt.Sprintf("Dictionary transform not found dictID: %s", dictID))
			}
		}
		fmt.Printf("Dictionary<key:")
		rb.VisitDataType(dictionaryBuilder, t.IndexType, "", prefix)
		fmt.Printf("{card:%d, total: %d},value:", card, cumulativeTotal)
		rb.VisitDataType(dictionaryBuilder, t.ValueType, "", prefix)
		fmt.Printf(">")
	case *arrow.DenseUnionType:
		unionBuilder := builder.(*array.DenseUnionBuilder)
		fmt.Printf("DenseUnion {\n")
		for i, field := range t.Fields() {
			rb.VisitField(unionBuilder.Child(i), &field, prefix+"  ")
		}
		fmt.Printf("%s}", prefix)
	case *arrow.SparseUnionType:
		unionBuilder := builder.(*array.SparseUnionBuilder)
		fmt.Printf("SparseUnion {\n")
		for i, field := range t.Fields() {
			rb.VisitField(unionBuilder.Child(i), &field, prefix+"  ")
		}
		fmt.Printf("%s}", prefix)
	case *arrow.MapType:
		mapBuilder := builder.(*array.MapBuilder)
		fmt.Printf("Map<")
		keyField := t.KeyField()
		rb.VisitDataType(mapBuilder.KeyBuilder(), t.KeyType(), dictionaryID(&keyField), prefix)
		fmt.Printf(",")
		itemField := t.ItemField()
		rb.VisitDataType(mapBuilder.ItemBuilder(), t.ItemType(), dictionaryID(&itemField), prefix)
		fmt.Printf(">")
	case *arrow.FixedSizeBinaryType:
		fmt.Printf("FixedSizeBinary<%d>", t.ByteWidth)
	default:
		panic("unsupported data type " + dt.String())
	}
}

func dictionaryID(field *arrow.Field) string {
	dictIdIdx := field.Metadata.FindKey(transform.DictIdKey)
	if dictIdIdx != -1 {
		return field.Metadata.Values()[dictIdIdx]
	}
	return ""
}
