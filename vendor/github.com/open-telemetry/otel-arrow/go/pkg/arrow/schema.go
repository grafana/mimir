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
	"fmt"
	"sort"

	"github.com/apache/arrow-go/v18/arrow"
)

// Constants used to create schema id signature.

const BoolSig = "Bol"
const U8Sig = "U8"
const U16Sig = "U16"
const U32Sig = "U32"
const U64Sig = "U64"
const I8Sig = "I8"
const I16Sig = "I16"
const I32Sig = "I32"
const I64Sig = "I64"
const F32Sig = "F32"
const F64Sig = "F64"
const BinarySig = "Bin"
const FixedSizeBinarySig = "FSB"
const StringSig = "Str"
const Timestamp = "Tns" // Timestamp in nanoseconds.
const Duration = "Dur"  // Duration in nanoseconds.
const DictionarySig = "Dic"
const DenseUnionSig = "DU"
const SparseUnionSig = "SU"
const MapSig = "Map"

// SortableField is a wrapper around arrow.Field that implements sort.Interface.
type SortableField struct {
	name  *string
	field *arrow.Field
}

type Fields []SortableField

func (d Fields) Less(i, j int) bool {
	return *d[i].name < *d[j].name
}
func (d Fields) Len() int      { return len(d) }
func (d Fields) Swap(i, j int) { d[i], d[j] = d[j], d[i] }

// SchemaToID creates a unique id for a schema.
// Fields are sorted by name before creating the id (done at each nested level).
func SchemaToID(schema *arrow.Schema) string {
	schemaID := ""
	fields := sortedFields(schema.Fields())

	for i := range fields {
		field := &fields[i]
		if i != 0 {
			schemaID += ","
		}
		schemaID += FieldToID(field.field)
	}

	return schemaID
}

func sortedFields(fields []arrow.Field) []SortableField {
	sortedField := make([]SortableField, len(fields))
	for i := 0; i < len(fields); i++ {
		sortedField[i] = SortableField{
			name:  &fields[i].Name,
			field: &fields[i],
		}
	}
	sort.Sort(Fields(sortedField))

	return sortedField
}

// FieldToID creates a unique id for a field.
func FieldToID(field *arrow.Field) string {
	return field.Name + ":" + DataTypeToID(field.Type)
}

// DataTypeToID creates a unique id for a data type.
func DataTypeToID(dt arrow.DataType) string {
	id := ""
	switch t := dt.(type) {
	case *arrow.BooleanType:
		id += BoolSig
	case *arrow.Int8Type:
		id += I8Sig
	case *arrow.Int16Type:
		id += I16Sig
	case *arrow.Int32Type:
		id += I32Sig
	case *arrow.Int64Type:
		id += I64Sig
	case *arrow.Uint8Type:
		id += U8Sig
	case *arrow.Uint16Type:
		id += U16Sig
	case *arrow.Uint32Type:
		id += U32Sig
	case *arrow.Uint64Type:
		id += U64Sig
	case *arrow.Float32Type:
		id += F32Sig
	case *arrow.Float64Type:
		id += F64Sig
	case *arrow.StringType:
		id += StringSig
	case *arrow.BinaryType:
		id += BinarySig
	case *arrow.TimestampType:
		id += Timestamp
	case *arrow.DurationType:
		id += Duration
	case *arrow.StructType:
		id += "{"
		fields := sortedFields(t.Fields())

		for i := range fields {
			if i > 0 {
				id += ","
			}
			id += FieldToID(fields[i].field)
		}
		id += "}"
	case *arrow.ListType:
		id += "["

		elemField := t.ElemField()

		id += DataTypeToID(elemField.Type)
		id += "]"
	case *arrow.DictionaryType:
		id += DictionarySig + "<"
		id += DataTypeToID(t.IndexType)
		id += ","
		id += DataTypeToID(t.ValueType)
		id += ">"
	case *arrow.DenseUnionType:
		id += DenseUnionSig + "{"
		fields := sortedFields(t.Fields())

		for i := range fields {
			if i > 0 {
				id += ","
			}
			id += FieldToID(fields[i].field)
		}

		id += "}"
	case *arrow.SparseUnionType:
		id += SparseUnionSig + "{"
		fields := sortedFields(t.Fields())

		for i := range fields {
			if i > 0 {
				id += ","
			}
			id += FieldToID(fields[i].field)
		}

		id += "}"
	case *arrow.MapType:
		id += MapSig + "<"
		id += DataTypeToID(t.KeyType())
		id += ","
		id += DataTypeToID(t.ItemType())
		id += ">"
	case *arrow.FixedSizeBinaryType:
		id += fmt.Sprintf("%s<%d>", FixedSizeBinarySig, t.ByteWidth)
	default:
		panic("unsupported data type " + dt.String())
	}

	return id
}

func ShowSchema(schema *arrow.Schema, schemaName string, prefix string) {
	println(prefix + "Schema " + schemaName + " {")
	for _, f := range schema.Fields() {
		ShowField(&f, prefix+"  ")
	}
	println(prefix + "}")
}

func ShowField(field *arrow.Field, prefix string) {
	fmt.Printf("%s%s: ", prefix, field.Name)
	ShowDataType(field.Type, prefix)
	fmt.Println()
}

func ShowDataType(dt arrow.DataType, prefix string) {
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
		fmt.Printf("Struct {\n")
		for _, field := range t.Fields() {
			ShowField(&field, prefix+"  ")
		}
		fmt.Printf("%s}", prefix)
	case *arrow.ListType:
		fmt.Printf("[")
		elemField := t.ElemField()
		ShowDataType(elemField.Type, prefix)
		fmt.Printf("]")
	case *arrow.DictionaryType:
		fmt.Printf("Dictionary<key:")
		ShowDataType(t.IndexType, prefix)
		fmt.Printf(",value:")
		ShowDataType(t.ValueType, prefix)
		fmt.Printf(">")
	case *arrow.DenseUnionType:
		fmt.Printf("DenseUnion {\n")
		for _, field := range t.Fields() {
			ShowField(&field, prefix+"  ")
		}
		fmt.Printf("%s}", prefix)
	case *arrow.SparseUnionType:
		fmt.Printf("SparseUnion {\n")
		for _, field := range t.Fields() {
			ShowField(&field, prefix+"  ")
		}
		fmt.Printf("%s}", prefix)
	case *arrow.MapType:
		fmt.Printf("Map<")
		ShowDataType(t.KeyType(), prefix)
		fmt.Printf(",")
		ShowDataType(t.ItemType(), prefix)
		fmt.Printf(">")
	case *arrow.FixedSizeBinaryType:
		fmt.Printf("FixedSizeBinary<%d>", t.ByteWidth)
	default:
		panic("unsupported data type " + dt.String())
	}
}
