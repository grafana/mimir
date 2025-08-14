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

import (
	"bytes"
	"sort"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/open-telemetry/otel-arrow/go/pkg/otel/constants"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/observer"
)

const (
	MetadataType = "type"

	BoolType       = "bool"
	IntType        = "i64"
	IntDictType    = "i64_dict"
	DoubleType     = "f64"
	StringType     = "str"
	StringDictType = "str_dict"
	BytesType      = "bytes"
	BytesDictType  = "bytes_dict"
	CborType       = "cbor"
	CborDictType   = "cbor_dict"
)

type (
	DynAttrsBuilder struct {
		mem         memory.Allocator
		payloadType *PayloadType

		newColumn         bool
		schemaID          string
		schemaUpdateCount int

		parentIDColumn *ParentIDColumn

		colIdx  map[string]int
		columns []AttrColumn

		builder *array.RecordBuilder
	}

	ParentIDColumn struct {
		colName string
		values  []uint32
		builder *array.Uint32Builder
	}

	AttrColumn interface {
		ColName() string
		ColType() arrow.DataType
		ColMetadata() arrow.Metadata
		Append(v pcommon.Value)
		AppendNull()
		Len() int
		SetBuilder(builder array.Builder)
		Build() error
		Compare(i, j int) int
		Reset()
	}

	BoolAttrColumn struct {
		colName  string
		metadata arrow.Metadata
		values   []*bool
		builder  *array.BooleanBuilder
	}

	IntAttrColumn struct {
		colName  string
		metadata arrow.Metadata
		values   []*int64
		builder  array.Builder
	}

	DoubleAttrColumn struct {
		colName  string
		metadata arrow.Metadata
		values   []*float64
		builder  *array.Float64Builder
	}

	StringAttrColumn struct {
		colName  string
		metadata arrow.Metadata
		values   []*string
		builder  array.Builder
	}

	BinaryAttrColumn struct {
		colName  string
		metadata arrow.Metadata
		values   [][]byte
		builder  array.Builder
	}

	CborAttrColumn struct {
		colName  string
		metadata arrow.Metadata
		values   [][]byte
		builder  array.Builder
	}
)

func NewDynAttrsBuilder(payloadType *PayloadType, mem memory.Allocator) *DynAttrsBuilder {
	return &DynAttrsBuilder{
		mem:         mem,
		newColumn:   true,
		payloadType: payloadType,
		parentIDColumn: &ParentIDColumn{
			colName: constants.ParentID,
			values:  make([]uint32, 0),
		},
		colIdx: make(map[string]int),
	}
}

func (b *DynAttrsBuilder) Append(parentID uint32, attrs pcommon.Map) error {
	if attrs.Len() == 0 {
		return nil
	}

	currRow := len(b.parentIDColumn.values)

	// Append all the attributes to their respective columns
	addedCount := 0
	attrs.Range(func(k string, v pcommon.Value) bool {
		if v.Type() == pcommon.ValueTypeEmpty {
			return true
		}
		name, metadata := colName(k, v)
		colIdx, ok := b.colIdx[name]
		if !ok {
			colIdx = len(b.columns)
			b.colIdx[name] = colIdx
			b.columns = append(b.columns, createColumn(name, metadata, v, currRow))
			b.newColumn = true
		}
		col := b.columns[colIdx]
		col.Append(v)
		addedCount++
		return true
	})
	if addedCount == 0 {
		return nil
	}

	b.parentIDColumn.values = append(b.parentIDColumn.values, parentID)

	// Append nils to columns that don't have a value for this row
	for _, col := range b.columns {
		if col.Len() < len(b.parentIDColumn.values) {
			col.AppendNull()
			addedCount++
		}
	}

	return nil
}

func (b *DynAttrsBuilder) SchemaUpdateCount() int {
	return b.schemaUpdateCount
}

func (b *DynAttrsBuilder) IsEmpty() bool {
	return len(b.parentIDColumn.values) == 0
}

func (b *DynAttrsBuilder) Build(observer observer.ProducerObserver) (arrow.Record, error) {
	if b.newColumn {
		b.sortColumns()
		b.createBuilder()
		b.updateSchemaID()
		b.newColumn = false
		b.schemaUpdateCount++
	}

	b.parentIDColumn.Build()

	for _, col := range b.columns {
		err := col.Build()
		if err != nil {
			return nil, err
		}
	}

	record := b.builder.NewRecord()

	b.Reset()

	return record, nil
}

func (b *DynAttrsBuilder) SchemaID() string {
	return b.schemaID
}

func (b *DynAttrsBuilder) Schema() *arrow.Schema {
	return b.builder.Schema()
}

func (b *DynAttrsBuilder) PayloadType() *PayloadType {
	return b.payloadType
}

func (b *DynAttrsBuilder) Reset() {
	b.parentIDColumn.Reset()
	for _, col := range b.columns {
		col.Reset()
	}
}

// Release releases the memory allocated by the builder.
func (b *DynAttrsBuilder) Release() {
	if b.builder != nil {
		b.builder.Release()
		b.builder = nil
	}
}

func (b *DynAttrsBuilder) sortColumns() {
	sort.Slice(b.columns, func(i, j int) bool {
		return b.columns[i].ColName() < b.columns[j].ColName()
	})

	for i, col := range b.columns {
		b.colIdx[col.ColName()] = i
	}
}

func (b *DynAttrsBuilder) Compare(rowI, rowJ, colIdx int) int {
	col := b.columns[colIdx]
	return col.Compare(rowI, rowJ)
}

func (b *DynAttrsBuilder) createBuilder() {
	if b.builder != nil {
		b.builder.Release()
	}

	fields := make([]arrow.Field, len(b.columns)+1)
	fields[0] = arrow.Field{Name: constants.ParentID, Type: arrow.PrimitiveTypes.Uint32}

	for i, col := range b.columns {
		fields[i+1] = arrow.Field{
			Name:     col.ColName(),
			Type:     col.ColType(),
			Metadata: col.ColMetadata(),
		}
	}

	b.builder = array.NewRecordBuilder(b.mem, arrow.NewSchema(fields, nil))

	b.parentIDColumn.builder = b.builder.Field(0).(*array.Uint32Builder)
	for i, builder := range b.builder.Fields()[1:] {
		b.columns[i].SetBuilder(builder)
	}
}

func (b *DynAttrsBuilder) updateSchemaID() {
	var buf bytes.Buffer
	buf.WriteString("struct{")
	buf.WriteString(constants.ParentID)
	buf.WriteString(":u32")
	for _, col := range b.columns {
		buf.WriteString(",")
		buf.WriteString(col.ColName())
		buf.WriteString(":")
		buf.WriteString(col.ColType().String())
	}
	buf.WriteString("}")
	b.schemaID = buf.String()
}

func colName(k string, v pcommon.Value) (string, arrow.Metadata) {
	switch v.Type() {
	case pcommon.ValueTypeBool:
		return k + "_" + BoolType, arrow.NewMetadata([]string{MetadataType}, []string{BoolType})
	case pcommon.ValueTypeInt:
		return k + "_" + IntType, arrow.NewMetadata([]string{MetadataType}, []string{IntDictType})
	case pcommon.ValueTypeDouble:
		return k + "_" + DoubleType, arrow.NewMetadata([]string{MetadataType}, []string{DoubleType})
	case pcommon.ValueTypeStr:
		return k + "_" + StringType, arrow.NewMetadata([]string{MetadataType}, []string{StringDictType})
	case pcommon.ValueTypeBytes:
		return k + "_" + BytesType, arrow.NewMetadata([]string{MetadataType}, []string{BytesDictType})
	case pcommon.ValueTypeMap:
		return k + "_" + CborType, arrow.NewMetadata([]string{MetadataType}, []string{CborDictType})
	case pcommon.ValueTypeSlice:
		return k + "_" + CborType, arrow.NewMetadata([]string{MetadataType}, []string{CborDictType})
	default:
		panic("unknown value type")
	}
}

func createColumn(k string, metadata arrow.Metadata, v pcommon.Value, initLen int) AttrColumn {
	switch v.Type() {
	case pcommon.ValueTypeBool:
		return &BoolAttrColumn{
			colName:  k,
			metadata: metadata,
			values:   make([]*bool, initLen),
		}
	case pcommon.ValueTypeInt:
		return &IntAttrColumn{
			colName:  k,
			metadata: metadata,
			values:   make([]*int64, initLen),
		}
	case pcommon.ValueTypeDouble:
		return &DoubleAttrColumn{
			colName:  k,
			metadata: metadata,
			values:   make([]*float64, initLen),
		}
	case pcommon.ValueTypeStr:
		return &StringAttrColumn{
			colName:  k,
			metadata: metadata,
			values:   make([]*string, initLen),
		}
	case pcommon.ValueTypeBytes:
		return &BinaryAttrColumn{
			colName:  k,
			metadata: metadata,
			values:   make([][]byte, initLen),
		}
	case pcommon.ValueTypeMap:
		return &CborAttrColumn{
			colName:  k,
			metadata: metadata,
			values:   make([][]byte, initLen),
		}
	case pcommon.ValueTypeSlice:
		return &CborAttrColumn{
			colName:  k,
			metadata: metadata,
			values:   make([][]byte, initLen),
		}
	default:
		panic("unknown value type")
	}
}

func (c *ParentIDColumn) Build() {
	prevParentID := uint32(0)
	for _, parentID := range c.values {
		delta := parentID - prevParentID
		c.builder.Append(delta)
		prevParentID = parentID
	}
}

func (c *ParentIDColumn) Reset() {
	c.values = c.values[:0]
}

func (c *BoolAttrColumn) ColName() string {
	return c.colName
}

func (c *BoolAttrColumn) ColMetadata() arrow.Metadata {
	return c.metadata
}

func (c *BoolAttrColumn) ColType() arrow.DataType {
	return arrow.FixedWidthTypes.Boolean
}

func (c *BoolAttrColumn) Append(v pcommon.Value) {
	val := v.Bool()
	c.values = append(c.values, &val)
}

func (c *BoolAttrColumn) Len() int {
	return len(c.values)
}

func (c *BoolAttrColumn) AppendNull() {
	c.values = append(c.values, nil)
}

func (c *BoolAttrColumn) SetBuilder(builder array.Builder) {
	c.builder = builder.(*array.BooleanBuilder)
}

func (c *BoolAttrColumn) Build() error {
	for _, value := range c.values {
		if value == nil {
			c.builder.AppendNull()
		} else {
			c.builder.Append(*value)
		}
	}
	return nil
}

func (c *BoolAttrColumn) Reset() {
	c.values = c.values[:0]
}

func (c *BoolAttrColumn) Compare(i, j int) int {
	if c.values[i] == nil {
		if c.values[j] == nil {
			return 0
		}
		return -1
	}
	if c.values[j] == nil {
		return 1
	}
	if *c.values[i] == *c.values[j] {
		return 0
	}
	if *c.values[i] {
		return 1
	}
	return -1
}

func (c *IntAttrColumn) ColName() string {
	return c.colName
}

func (c *IntAttrColumn) ColMetadata() arrow.Metadata {
	return c.metadata
}

func (c *IntAttrColumn) ColType() arrow.DataType {
	dt := arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Uint16,
		ValueType: arrow.PrimitiveTypes.Int64,
	}
	return &dt
}

func (c *IntAttrColumn) Append(v pcommon.Value) {
	val := v.Int()
	c.values = append(c.values, &val)
}

func (c *IntAttrColumn) Len() int {
	return len(c.values)
}

func (c *IntAttrColumn) AppendNull() {
	c.values = append(c.values, nil)
}

func (c *IntAttrColumn) SetBuilder(builder array.Builder) {
	c.builder = builder
}

func (c *IntAttrColumn) Build() error {
	switch b := c.builder.(type) {
	case *array.Int64Builder:
		for _, value := range c.values {
			if value == nil {
				b.AppendNull()
			} else {
				b.Append(*value)
			}
		}
	case *array.Int64DictionaryBuilder:
		for _, value := range c.values {
			if value == nil {
				b.AppendNull()
			} else {
				err := b.Append(*value)
				if err != nil {
					return err
				}
			}
		}
	default:
		panic("invalid int64 builder type")
	}

	return nil
}

func (c *IntAttrColumn) Reset() {
	c.values = c.values[:0]
}

func (c *IntAttrColumn) Compare(i, j int) int {
	if c.values[i] == nil {
		if c.values[j] == nil {
			return 0
		}
		return -1
	}
	if c.values[j] == nil {
		return 1
	}
	return int(*c.values[i] - *c.values[j])
}

func (c *DoubleAttrColumn) ColName() string {
	return c.colName
}

func (c *DoubleAttrColumn) ColMetadata() arrow.Metadata {
	return c.metadata
}

func (c *DoubleAttrColumn) ColType() arrow.DataType {
	return arrow.PrimitiveTypes.Float64
}

func (c *DoubleAttrColumn) Append(v pcommon.Value) {
	val := v.Double()
	c.values = append(c.values, &val)
}

func (c *DoubleAttrColumn) Len() int {
	return len(c.values)
}

func (c *DoubleAttrColumn) AppendNull() {
	c.values = append(c.values, nil)
}

func (c *DoubleAttrColumn) SetBuilder(builder array.Builder) {
	c.builder = builder.(*array.Float64Builder)
}

func (c *DoubleAttrColumn) Build() error {
	for _, value := range c.values {
		if value == nil {
			c.builder.AppendNull()
		} else {
			c.builder.Append(*value)
		}
	}
	return nil
}

func (c *DoubleAttrColumn) Reset() {
	c.values = c.values[:0]
}

func (c *DoubleAttrColumn) Compare(i, j int) int {
	if c.values[i] == nil {
		if c.values[j] == nil {
			return 0
		}
		return -1
	}
	if c.values[j] == nil {
		return 1
	}
	if *c.values[i] == *c.values[j] {
		return 0
	}
	if *c.values[i] > *c.values[j] {
		return 1
	}
	return -1
}

func (c *StringAttrColumn) ColName() string {
	return c.colName
}

func (c *StringAttrColumn) ColMetadata() arrow.Metadata {
	return c.metadata
}

func (c *StringAttrColumn) ColType() arrow.DataType {
	dt := arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Uint16,
		ValueType: arrow.BinaryTypes.String,
	}
	return &dt
}

func (c *StringAttrColumn) Append(v pcommon.Value) {
	val := v.Str()
	c.values = append(c.values, &val)
}

func (c *StringAttrColumn) Len() int {
	return len(c.values)
}

func (c *StringAttrColumn) AppendNull() {
	c.values = append(c.values, nil)
}

func (c *StringAttrColumn) SetBuilder(builder array.Builder) {
	c.builder = builder
}

func (c *StringAttrColumn) Build() error {
	switch b := c.builder.(type) {
	case *array.StringBuilder:
		for _, v := range c.values {
			if v == nil {
				b.AppendNull()
			} else {
				b.Append(*v)
			}
		}
	case *array.BinaryDictionaryBuilder:
		for _, v := range c.values {
			if v == nil {
				b.AppendNull()
			} else {
				err := b.AppendString(*v)
				if err != nil {
					return err
				}
			}
		}
	default:
		panic("invalid string builder type")
	}
	return nil
}

func (c *StringAttrColumn) Reset() {
	c.values = c.values[:0]
}

func (c *StringAttrColumn) Compare(i, j int) int {
	if c.values[i] == nil {
		if c.values[j] == nil {
			return 0
		}
		return -1
	}
	if c.values[j] == nil {
		return 1
	}
	return strings.Compare(*c.values[i], *c.values[j])
}

func (c *BinaryAttrColumn) ColName() string {
	return c.colName
}

func (c *BinaryAttrColumn) ColMetadata() arrow.Metadata {
	return c.metadata
}

func (c *BinaryAttrColumn) ColType() arrow.DataType {
	dt := arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Uint16,
		ValueType: arrow.BinaryTypes.Binary,
	}
	return &dt
}

func (c *BinaryAttrColumn) Append(v pcommon.Value) {
	val := v.Bytes().AsRaw()
	c.values = append(c.values, val)
}

func (c *BinaryAttrColumn) Len() int {
	return len(c.values)
}

func (c *BinaryAttrColumn) AppendNull() {
	c.values = append(c.values, nil)
}

func (c *BinaryAttrColumn) SetBuilder(builder array.Builder) {
	c.builder = builder
}

func (c *BinaryAttrColumn) Build() error {
	switch b := c.builder.(type) {
	case *array.BinaryBuilder:
		for _, val := range c.values {
			if val == nil {
				b.AppendNull()
			} else {
				b.Append(val)
			}
		}
	case *array.BinaryDictionaryBuilder:
		for _, val := range c.values {
			if val == nil {
				b.AppendNull()
			} else {
				err := b.Append(val)
				if err != nil {
					return err
				}
			}
		}
	default:
		panic("invalid binary builder type")
	}
	return nil
}

func (c *BinaryAttrColumn) Reset() {
	c.values = c.values[:0]
}

func (c *BinaryAttrColumn) Compare(i, j int) int {
	if c.values[i] == nil {
		if c.values[j] == nil {
			return 0
		}
		return -1
	}
	if c.values[j] == nil {
		return 1
	}
	return bytes.Compare(c.values[i], c.values[j])
}

func (c *CborAttrColumn) ColName() string {
	return c.colName
}

func (c *CborAttrColumn) ColMetadata() arrow.Metadata {
	return c.metadata
}

func (c *CborAttrColumn) ColType() arrow.DataType {
	dt := arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Uint16,
		ValueType: arrow.BinaryTypes.Binary,
	}
	return &dt
}

func (c *CborAttrColumn) Append(v pcommon.Value) {
	panic("implement me")
}

func (c *CborAttrColumn) Len() int {
	return len(c.values)
}

func (c *CborAttrColumn) AppendNull() {
	c.values = append(c.values, nil)
}

func (c *CborAttrColumn) SetBuilder(builder array.Builder) {
	c.builder = builder.(*array.BinaryBuilder)
}

func (c *CborAttrColumn) Build() error {
	switch b := c.builder.(type) {
	case *array.BinaryBuilder:
		for _, value := range c.values {
			if value == nil {
				b.AppendNull()
			} else {
				b.Append(value)
			}
		}
	case *array.BinaryDictionaryBuilder:
		for _, value := range c.values {
			if value == nil {
				b.AppendNull()
			} else {
				err := b.Append(value)
				if err != nil {
					return err
				}
			}
		}
	default:
		panic("invalid cbor builder type")
	}
	return nil
}

func (c *CborAttrColumn) Reset() {
	c.values = c.values[:0]
}

func (c *CborAttrColumn) Compare(i, j int) int {
	if c.values[i] == nil {
		if c.values[j] == nil {
			return 0
		}
		return -1
	}
	if c.values[j] == nil {
		return 1
	}
	return bytes.Compare(c.values[i], c.values[j])
}
