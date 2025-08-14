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

package arrow

// Attributes record builder for 32-bit Parent IDs.

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/open-telemetry/otel-arrow/go/pkg/config"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema/builder"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/constants"
	"github.com/open-telemetry/otel-arrow/go/pkg/werror"
)

var (
	// AttrsSchema32 is the Arrow schema used to represent attribute records
	// with 16-bit Parent IDs.
	// This schema doesn't use the Arrow union type to make Parquet conversion
	// more direct.
	AttrsSchema32 = arrow.NewSchema([]arrow.Field{
		{Name: constants.ParentID, Type: arrow.PrimitiveTypes.Uint32, Metadata: schema.Metadata(schema.Dictionary8)},
		{Name: constants.AttributeKey, Type: arrow.BinaryTypes.String, Metadata: schema.Metadata(schema.Dictionary8)},
		{Name: constants.AttributeType, Type: arrow.PrimitiveTypes.Uint8},
		{Name: constants.AttributeStr, Type: arrow.BinaryTypes.String, Metadata: schema.Metadata(schema.Dictionary16), Nullable: true},
		{Name: constants.AttributeInt, Type: arrow.PrimitiveTypes.Int64, Metadata: schema.Metadata(schema.Dictionary16), Nullable: true},
		{Name: constants.AttributeDouble, Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: constants.AttributeBool, Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: constants.AttributeBytes, Type: arrow.BinaryTypes.Binary, Metadata: schema.Metadata(schema.Dictionary16), Nullable: true},
		{Name: constants.AttributeSer, Type: arrow.BinaryTypes.Binary, Metadata: schema.Metadata(schema.Dictionary16), Nullable: true},
	}, nil)

	// DeltaEncodedAttrsSchema32 is the Arrow schema used to represent attribute records
	// with 16-bit Parent IDs that are delta encoded.
	// This schema doesn't use the Arrow union type to make Parquet conversion
	// more direct.
	DeltaEncodedAttrsSchema32 = arrow.NewSchema([]arrow.Field{
		{Name: constants.ParentID, Type: arrow.PrimitiveTypes.Uint32, Metadata: schema.Metadata(schema.Dictionary8, schema.DeltaEncoding)},
		{Name: constants.AttributeKey, Type: arrow.BinaryTypes.String, Metadata: schema.Metadata(schema.Dictionary8)},
		{Name: constants.AttributeType, Type: arrow.PrimitiveTypes.Uint8},
		{Name: constants.AttributeStr, Type: arrow.BinaryTypes.String, Metadata: schema.Metadata(schema.Dictionary16), Nullable: true},
		{Name: constants.AttributeInt, Type: arrow.PrimitiveTypes.Int64, Metadata: schema.Metadata(schema.Dictionary16), Nullable: true},
		{Name: constants.AttributeDouble, Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: constants.AttributeBool, Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: constants.AttributeBytes, Type: arrow.BinaryTypes.Binary, Metadata: schema.Metadata(schema.Dictionary16), Nullable: true},
		{Name: constants.AttributeSer, Type: arrow.BinaryTypes.Binary, Metadata: schema.Metadata(schema.Dictionary16), Nullable: true},
	}, nil)
)

type (
	Attrs32Builder struct {
		released bool

		builder *builder.RecordBuilderExt // Record builder

		pib   *builder.Uint32Builder
		keyb  *builder.StringBuilder
		typeb *builder.Uint8Builder
		strb  *builder.StringBuilder
		i64b  *builder.Int64Builder
		f64b  *builder.Float64Builder
		boolb *builder.BooleanBuilder
		binb  *builder.BinaryBuilder
		serb  *builder.BinaryBuilder

		accumulator *Attributes32Accumulator
		payloadType *PayloadType
	}

	Attrs32ByNothing              struct{}
	Attrs32ByTypeParentIdKeyValue struct {
		prevParentID uint32
		prevValue    *pcommon.Value
	}
	Attrs32ByTypeKeyParentIdValue struct {
		prevParentID uint32
		prevKey      string
		prevValue    *pcommon.Value
	}
	Attrs32ByTypeKeyValueParentId struct {
		prevParentID uint32
		prevKey      string
		prevValue    *pcommon.Value
	}
	Attrs32ByKeyValueParentId struct {
		prevParentID uint32
		prevKey      string
		prevValue    *pcommon.Value
	}
)

func NewAttrs32Builder(rBuilder *builder.RecordBuilderExt, payloadType *PayloadType, sorter Attrs32Sorter) *Attrs32Builder {
	b := &Attrs32Builder{
		released:    false,
		builder:     rBuilder,
		accumulator: NewAttributes32Accumulator(sorter),
		payloadType: payloadType,
	}
	b.init()
	return b
}

func NewAttrs32BuilderWithEncoding(rBuilder *builder.RecordBuilderExt, payloadType *PayloadType, conf *Attrs32Config) *Attrs32Builder {
	b := &Attrs32Builder{
		released:    false,
		builder:     rBuilder,
		accumulator: NewAttributes32Accumulator(conf.Sorter),
		payloadType: payloadType,
	}

	b.init()
	return b
}

func (b *Attrs32Builder) init() {
	b.pib = b.builder.Uint32Builder(constants.ParentID)
	b.keyb = b.builder.StringBuilder(constants.AttributeKey)
	b.typeb = b.builder.Uint8Builder(constants.AttributeType)
	b.strb = b.builder.StringBuilder(constants.AttributeStr)
	b.i64b = b.builder.Int64Builder(constants.AttributeInt)
	b.f64b = b.builder.Float64Builder(constants.AttributeDouble)
	b.boolb = b.builder.BooleanBuilder(constants.AttributeBool)
	b.binb = b.builder.BinaryBuilder(constants.AttributeBytes)
	b.serb = b.builder.BinaryBuilder(constants.AttributeSer)
}

func (b *Attrs32Builder) Accumulator() *Attributes32Accumulator {
	return b.accumulator
}

func (b *Attrs32Builder) TryBuild() (record arrow.Record, err error) {
	if b.released {
		return nil, werror.Wrap(ErrBuilderAlreadyReleased)
	}

	sortingColumns := b.accumulator.Sort()
	b.builder.AddMetadata(constants.SortingColumns, strings.Join(sortingColumns, ","))

	b.builder.Reserve(len(b.accumulator.attrs))

	for _, attr := range b.accumulator.attrs {
		b.pib.Append(b.accumulator.sorter.Encode(attr.ParentID, attr.Key, attr.Value))
		b.keyb.Append(attr.Key)

		switch attr.Value.Type() {
		case pcommon.ValueTypeStr:
			b.typeb.Append(uint8(pcommon.ValueTypeStr))
			b.strb.Append(attr.Value.Str())
			b.i64b.AppendNull()
			b.f64b.AppendNull()
			b.boolb.AppendNull()
			b.binb.AppendNull()
			b.serb.AppendNull()
		case pcommon.ValueTypeInt:
			b.typeb.Append(uint8(pcommon.ValueTypeInt))
			b.i64b.Append(attr.Value.Int())
			b.strb.AppendNull()
			b.f64b.AppendNull()
			b.boolb.AppendNull()
			b.binb.AppendNull()
			b.serb.AppendNull()
		case pcommon.ValueTypeDouble:
			b.typeb.Append(uint8(pcommon.ValueTypeDouble))
			b.f64b.Append(attr.Value.Double())
			b.strb.AppendNull()
			b.i64b.AppendNull()
			b.boolb.AppendNull()
			b.binb.AppendNull()
			b.serb.AppendNull()
		case pcommon.ValueTypeBool:
			b.typeb.Append(uint8(pcommon.ValueTypeBool))
			b.boolb.Append(attr.Value.Bool())
			b.strb.AppendNull()
			b.i64b.AppendNull()
			b.f64b.AppendNull()
			b.binb.AppendNull()
			b.serb.AppendNull()
		case pcommon.ValueTypeBytes:
			b.typeb.Append(uint8(pcommon.ValueTypeBytes))
			b.binb.Append(attr.Value.Bytes().AsRaw())
			b.strb.AppendNull()
			b.i64b.AppendNull()
			b.f64b.AppendNull()
			b.boolb.AppendNull()
			b.serb.AppendNull()
		case pcommon.ValueTypeSlice:
			cborData, err := common.Serialize(attr.Value)
			if err != nil {
				break
			}
			b.typeb.Append(uint8(pcommon.ValueTypeSlice))
			b.serb.Append(cborData)
			b.strb.AppendNull()
			b.i64b.AppendNull()
			b.f64b.AppendNull()
			b.boolb.AppendNull()
			b.binb.AppendNull()
		case pcommon.ValueTypeMap:
			cborData, err := common.Serialize(attr.Value)
			if err != nil {
				break
			}
			b.typeb.Append(uint8(pcommon.ValueTypeMap))
			b.serb.Append(cborData)
			b.strb.AppendNull()
			b.i64b.AppendNull()
			b.f64b.AppendNull()
			b.boolb.AppendNull()
			b.binb.AppendNull()
		}
	}

	record, err = b.builder.NewRecord()
	if err != nil {
		b.init()
	}

	return
}

func (b *Attrs32Builder) IsEmpty() bool {
	return b.accumulator.IsEmpty()
}

func (b *Attrs32Builder) Build() (arrow.Record, error) {
	schemaNotUpToDateCount := 0

	var record arrow.Record
	var err error

	// Loop until the record is built successfully.
	// Intermediaries steps may be required to update the schema.
	for {
		record, err = b.TryBuild()
		if err != nil {
			if record != nil {
				record.Release()
			}

			switch {
			case errors.Is(err, schema.ErrSchemaNotUpToDate):
				schemaNotUpToDateCount++
				if schemaNotUpToDateCount > 5 {
					panic("Too many consecutive schema updates. This shouldn't happen.")
				}
			default:
				return nil, werror.Wrap(err)
			}
		} else {
			break
		}
	}

	return record, werror.Wrap(err)
}

func (b *Attrs32Builder) SchemaID() string {
	return b.builder.SchemaID()
}

func (b *Attrs32Builder) Schema() *arrow.Schema {
	return b.builder.Schema()
}

func (b *Attrs32Builder) PayloadType() *PayloadType {
	return b.payloadType
}

func (b *Attrs32Builder) Reset() {
	b.accumulator.Reset()
}

// Release releases the memory allocated by the builder.
func (b *Attrs32Builder) Release() {
	if !b.released {
		b.builder.Release()
		b.released = true
	}
}

func (b *Attrs32Builder) ShowSchema() {
	b.builder.ShowSchema()
}

// Attrs32FindOrderByFunc returns the sorter for the given order by
func Attrs32FindOrderByFunc(orderBy config.OrderAttrs32By) Attrs32Sorter {
	switch orderBy {
	case config.OrderAttrs32ByNothing:
		return &Attrs32ByNothing{}
	case config.OrderAttrs32ByTypeParentIdKeyValue:
		return &Attrs32ByTypeParentIdKeyValue{}
	case config.OrderAttrs32ByTypeKeyParentIdValue:
		return &Attrs32ByTypeKeyParentIdValue{}
	case config.OrderAttrs32ByTypeKeyValueParentId:
		return &Attrs32ByTypeKeyValueParentId{}
	case config.OrderAttrs32ByKeyValueParentId:
		return &Attrs32ByKeyValueParentId{}
	default:
		panic(fmt.Sprintf("unknown OrderAttrs32By variant: %d", orderBy))
	}
}

// No sorting
// ==========

func UnsortedAttrs32() *Attrs32ByNothing {
	return &Attrs32ByNothing{}
}

func (s *Attrs32ByNothing) Sort(_ []Attr32) []string {
	// Do nothing
	return []string{}
}

func (s *Attrs32ByNothing) Encode(parentID uint32, _ string, _ *pcommon.Value) uint32 {
	return parentID
}

func (s *Attrs32ByNothing) Reset() {}

// Sorts the attributes by value type, parentID, key, and value
// ============================================================

func SortAttrs32ByTypeParentIdKeyValue() *Attrs32ByTypeParentIdKeyValue {
	return &Attrs32ByTypeParentIdKeyValue{}
}

func (s *Attrs32ByTypeParentIdKeyValue) Sort(attrs []Attr32) []string {
	sort.Slice(attrs, func(i, j int) bool {
		attrsI := attrs[i]
		attrsJ := attrs[j]
		if attrsI.Value.Type() == attrsJ.Value.Type() {
			if attrsI.ParentID == attrsJ.ParentID {
				if attrsI.Key == attrsJ.Key {
					return IsLess(attrsI.Value, attrsJ.Value)
				} else {
					return attrsI.Key < attrsJ.Key
				}
			} else {
				return attrsI.ParentID < attrsJ.ParentID
			}
		} else {
			return attrsI.Value.Type() < attrsJ.Value.Type()
		}
	})
	return []string{constants.AttributeType, constants.ParentID, constants.AttributeKey, constants.Value}
}

func (s *Attrs32ByTypeParentIdKeyValue) Encode(parentID uint32, _ string, value *pcommon.Value) uint32 {
	if s.prevValue == nil {
		s.prevValue = value
		s.prevParentID = parentID
		return parentID
	}

	if s.prevValue.Type() == value.Type() {
		delta := parentID - s.prevParentID
		s.prevParentID = parentID
		return delta
	} else {
		s.prevValue = value
		s.prevParentID = parentID
		return parentID
	}
}

func (s *Attrs32ByTypeParentIdKeyValue) Reset() {
	s.prevParentID = 0
	s.prevValue = nil
}

// Sorts the attributes by key, parentID, and value
// ================================================

func SortAttrs32ByTypeKeyParentIdValue() *Attrs32ByTypeKeyParentIdValue {
	return &Attrs32ByTypeKeyParentIdValue{}
}

func (s *Attrs32ByTypeKeyParentIdValue) Sort(attrs []Attr32) []string {
	sort.Slice(attrs, func(i, j int) bool {
		attrsI := attrs[i]
		attrsJ := attrs[j]
		if attrsI.Value.Type() == attrsJ.Value.Type() {
			if attrsI.Key == attrsJ.Key {
				if attrsI.ParentID == attrsJ.ParentID {
					return IsLess(attrsI.Value, attrsJ.Value)
				} else {
					return attrsI.ParentID < attrsJ.ParentID
				}
			} else {
				return attrsI.Key < attrsJ.Key
			}
		} else {
			return attrsI.Value.Type() < attrsJ.Value.Type()
		}
	})
	return []string{constants.AttributeType, constants.AttributeKey, constants.ParentID, constants.Value}
}

func (s *Attrs32ByTypeKeyParentIdValue) Encode(parentID uint32, key string, value *pcommon.Value) uint32 {
	if s.prevValue == nil {
		s.prevKey = key
		s.prevValue = value
		s.prevParentID = parentID
		return parentID
	}

	if s.IsSameGroup(key, value) {
		delta := parentID - s.prevParentID
		s.prevParentID = parentID
		return delta
	} else {
		s.prevKey = key
		s.prevValue = value
		s.prevParentID = parentID
		return parentID
	}
}

func (s *Attrs32ByTypeKeyParentIdValue) Reset() {
	s.prevParentID = 0
	s.prevKey = ""
	s.prevValue = nil
}

func (s *Attrs32ByTypeKeyParentIdValue) IsSameGroup(key string, value *pcommon.Value) bool {
	if s.prevValue == nil {
		return false
	}

	if s.prevValue.Type() == value.Type() && s.prevKey == key {
		return true
	}
	return false
}

// Sorts the attributes by type, key, value, and parentID
// ======================================================

func SortAttrs32ByTypeKeyValueParentId() *Attrs32ByTypeKeyValueParentId {
	return &Attrs32ByTypeKeyValueParentId{}
}

func (s *Attrs32ByTypeKeyValueParentId) Sort(attrs []Attr32) []string {
	sort.Slice(attrs, func(i, j int) bool {
		attrsI := attrs[i]
		attrsJ := attrs[j]
		if attrsI.Value.Type() == attrsJ.Value.Type() {
			if attrsI.Key == attrsJ.Key {
				cmp := Compare(attrsI.Value, attrsJ.Value)
				if cmp == 0 {
					return attrsI.ParentID < attrsJ.ParentID
				} else {
					return cmp < 0
				}
			} else {
				return attrsI.Key < attrsJ.Key
			}
		} else {
			return attrsI.Value.Type() < attrsJ.Value.Type()
		}
	})
	return []string{constants.AttributeType, constants.AttributeKey, constants.Value, constants.ParentID}
}

func (s *Attrs32ByTypeKeyValueParentId) Encode(parentID uint32, key string, value *pcommon.Value) uint32 {
	if s.prevValue == nil {
		s.prevKey = key
		s.prevValue = value
		s.prevParentID = parentID
		return parentID
	}

	if s.IsSameGroup(key, value) {
		delta := parentID - s.prevParentID
		s.prevParentID = parentID
		return delta
	} else {
		s.prevKey = key
		s.prevValue = value
		s.prevParentID = parentID
		return parentID
	}
}

func (s *Attrs32ByTypeKeyValueParentId) Reset() {
	s.prevParentID = 0
	s.prevKey = ""
	s.prevValue = nil
}

func (s *Attrs32ByTypeKeyValueParentId) IsSameGroup(key string, value *pcommon.Value) bool {
	if s.prevValue == nil {
		return false
	}

	if s.prevKey == key {
		return Equal(s.prevValue, value)
	} else {
		return false
	}
}

// Sorts the attributes by key, value, and parentID
// ======================================================

func SortAttrs32ByKeyValueParentId() *Attrs32ByKeyValueParentId {
	return &Attrs32ByKeyValueParentId{}
}

func (s *Attrs32ByKeyValueParentId) Sort(attrs []Attr32) []string {
	sort.Slice(attrs, func(i, j int) bool {
		attrsI := attrs[i]
		attrsJ := attrs[j]
		if attrsI.Key == attrsJ.Key {
			cmp := Compare(attrsI.Value, attrsJ.Value)
			if cmp == 0 {
				return attrsI.ParentID < attrsJ.ParentID
			} else {
				return cmp < 0
			}
		} else {
			return attrsI.Key < attrsJ.Key
		}
	})
	return []string{constants.AttributeKey, constants.Value, constants.ParentID}
}

func (s *Attrs32ByKeyValueParentId) Encode(parentID uint32, key string, value *pcommon.Value) uint32 {
	if s.prevValue == nil {
		s.prevKey = key
		s.prevValue = value
		s.prevParentID = parentID
		return parentID
	}

	if s.IsSameGroup(key, value) {
		delta := parentID - s.prevParentID
		s.prevParentID = parentID
		return delta
	} else {
		s.prevKey = key
		s.prevValue = value
		s.prevParentID = parentID
		return parentID
	}
}

func (s *Attrs32ByKeyValueParentId) Reset() {
	s.prevParentID = 0
	s.prevKey = ""
	s.prevValue = nil
}

func (s *Attrs32ByKeyValueParentId) IsSameGroup(key string, value *pcommon.Value) bool {
	if s.prevValue == nil {
		return false
	}

	if s.prevKey == key {
		return Equal(s.prevValue, value)
	} else {
		return false
	}
}
