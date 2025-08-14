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

// Utility functions to extract ids, and values from Struct data type or from
// Arrow arrays.

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/open-telemetry/otel-arrow/go/pkg/werror"
)

// U16FromStruct returns the uint16 value for a specific row in an Arrow struct
// or 0 if the field doesn't exist.
func U16FromStruct(structArr *array.Struct, row int, fieldID int) (uint16, error) {
	if fieldID == AbsentFieldID {
		return 0, nil
	}
	return U16FromArray(structArr.Field(fieldID), row)
}

// NullableU16FromStruct returns a reference to an uint16 value for a specific
// row in an Arrow struct or nil if the field doesn't exist.
func NullableU16FromStruct(structArr *array.Struct, row int, fieldID int) (*uint16, error) {
	if fieldID == AbsentFieldID {
		return nil, nil
	}
	if structArr.IsNull(row) {
		return nil, nil
	}
	val, err := U16FromArray(structArr.Field(fieldID), row)
	if err != nil {
		return nil, err
	}
	return &val, nil
}

// U8FromStruct returns the uint8 value for a specific row in an Arrow struct
// or 0 if the field doesn't exist.
func U8FromStruct(structArr *array.Struct, row int, fieldID int) (uint8, error) {
	if fieldID == AbsentFieldID {
		return 0, nil
	}
	return U8FromArray(structArr.Field(fieldID), row)
}

// U32FromStruct returns the uint32 value for a specific row in an Arrow struct
// or 0 if the field doesn't exist.
func U32FromStruct(structArr *array.Struct, row int, fieldID int) (uint32, error) {
	if fieldID == AbsentFieldID {
		return 0, nil
	}
	return U32FromArray(structArr.Field(fieldID), row)
}

// ListOfStructsFieldIDFromStruct returns the field id of a list of structs
// field from an Arrow struct or AbsentFieldID if the field is not found.
//
// An error is returned if the field is not a list of structs.
func ListOfStructsFieldIDFromStruct(dt *arrow.StructType, fieldName string) (int, *arrow.StructType, error) {
	if dt == nil {
		return AbsentFieldID, nil, nil
	}

	id, ok := dt.FieldIdx(fieldName)
	if !ok {
		return AbsentFieldID, nil, nil
	}

	if lt, ok := dt.Field(id).Type.(*arrow.ListType); ok {
		st, ok := lt.ElemField().Type.(*arrow.StructType)
		if !ok {
			return 0, nil, werror.WrapWithContext(ErrNotListOfStructsType, map[string]interface{}{"fieldName": fieldName})
		}
		return id, st, nil
	} else {
		return 0, nil, werror.WrapWithContext(ErrNotListType, map[string]interface{}{"fieldName": fieldName})
	}
}

// FieldIDFromStruct returns the field id of a named field from an Arrow struct
// or AbsentFieldID for an unknown field.
func FieldIDFromStruct(dt *arrow.StructType, fieldName string) (int, *arrow.DataType) {
	if dt == nil {
		return AbsentFieldID, nil
	}

	id, found := dt.FieldIdx(fieldName)
	if !found {
		return AbsentFieldID, nil
	}
	field := dt.Field(id)
	return id, &field.Type
}

// StructFieldIDFromStruct returns the field id of a struct field from an Arrow
// struct or AbsentFieldID for an unknown field.
//
// An error is returned if the field is not a struct.
func StructFieldIDFromStruct(dt *arrow.StructType, fieldName string) (int, *arrow.StructType, error) {
	if dt == nil {
		return AbsentFieldID, nil, nil
	}

	id, found := dt.FieldIdx(fieldName)
	if !found {
		return AbsentFieldID, nil, nil
	}
	if st, ok := dt.Field(id).Type.(*arrow.StructType); ok {
		return id, st, nil
	} else {
		return 0, nil, werror.WrapWithContext(ErrNotStructType, map[string]interface{}{"fieldName": fieldName})
	}
}

// StringFromStruct returns the string value for a specific row in an Arrow struct.
func StringFromStruct(arr arrow.Array, row int, id int) (string, error) {
	if id == AbsentFieldID {
		return "", nil
	}

	structArr, ok := arr.(*array.Struct)
	if !ok {
		return "", werror.WrapWithContext(ErrNotArrayStruct, map[string]interface{}{"row": row, "id": id})
	}
	if structArr != nil {
		return StringFromArray(structArr.Field(id), row)
	} else {
		return "", werror.WrapWithContext(ErrNotArrayStruct, map[string]interface{}{"row": row, "id": id})
	}
}

// I32FromStruct returns the int32 value for a specific field+row in an Arrow
// Array struct.
func I32FromStruct(arr arrow.Array, row int, id int) (int32, error) {
	if id == AbsentFieldID {
		return 0, nil
	}
	structArr, ok := arr.(*array.Struct)
	if !ok {
		return 0, werror.WrapWithContext(ErrNotArrayStruct, map[string]interface{}{"row": row, "id": id})
	}
	if structArr != nil {
		return I32FromArray(structArr.Field(id), row)
	} else {
		return 0, werror.WrapWithContext(ErrNotArrayStruct, map[string]interface{}{
			"row": row,
			"id":  id,
		})
	}
}

// I64FromStruct returns the int64 value for a specific field+row in an Arrow
// Array struct.
func I64FromStruct(arr arrow.Array, row int, id int) (int64, error) {
	if id == AbsentFieldID {
		return 0, nil
	}
	structArr, ok := arr.(*array.Struct)
	if !ok {
		return 0, werror.WrapWithContext(ErrNotArrayStruct, map[string]interface{}{"row": row, "id": id})
	}
	if structArr != nil {
		return I64FromArray(structArr.Field(id), row)
	} else {
		return 0, werror.WrapWithContext(ErrNotArrayStruct, map[string]interface{}{
			"row": row,
			"id":  id,
		})
	}
}

// F64FromStruct returns the float64 value for a specific field+row in an Arrow
// Array struct.
func F64FromStruct(arr arrow.Array, row int, id int) (float64, error) {
	if id == AbsentFieldID {
		return 0, nil
	}
	structArr, ok := arr.(*array.Struct)
	if !ok {
		return 0, werror.WrapWithContext(ErrNotArrayStruct, map[string]interface{}{"row": row, "id": id})
	}
	if structArr != nil {
		return F64FromArray(structArr.Field(id), row)
	} else {
		return 0, werror.WrapWithContext(ErrNotArrayStruct, map[string]interface{}{
			"row": row,
			"id":  id,
		})
	}
}

// BoolFromStruct returns the bool value for a specific field+row in an Arrow
// Array struct.
func BoolFromStruct(arr arrow.Array, row int, id int) (bool, error) {
	if id == AbsentFieldID {
		return false, nil
	}
	structArr, ok := arr.(*array.Struct)
	if !ok {
		return false, werror.WrapWithContext(ErrNotArrayStruct, map[string]interface{}{"row": row, "id": id})
	}
	if structArr != nil {
		return BoolFromArray(structArr.Field(id), row)
	} else {
		return false, werror.WrapWithContext(ErrNotArrayStruct, map[string]interface{}{
			"row": row,
			"id":  id,
		})
	}
}

// BinaryFromStruct returns the []byte value for a specific field+row in an Arrow
// Array struct.
func BinaryFromStruct(arr arrow.Array, row int, id int) ([]byte, error) {
	if id == AbsentFieldID {
		return nil, nil
	}
	structArr, ok := arr.(*array.Struct)
	if !ok {
		return nil, werror.WrapWithContext(ErrNotArrayStruct, map[string]interface{}{"row": row, "id": id})
	}
	if structArr != nil {
		return BinaryFromArray(structArr.Field(id), row)
	} else {
		return nil, werror.WrapWithContext(ErrNotArrayStruct, map[string]interface{}{
			"row": row,
			"id":  id,
		})
	}
}

// OptionalFieldIDFromStruct returns the field id of a named field from an Arrow struct or AbsentFieldID if the field is unknown.
func OptionalFieldIDFromStruct(dt *arrow.StructType, fieldName string) (id int) {
	if dt == nil {
		id = AbsentFieldID
		return
	}

	id, found := dt.FieldIdx(fieldName)
	if !found {
		id = AbsentFieldID
	}
	return
}

// ListOfStructsFromStruct return a ListOfStructs from a struct field.
func ListOfStructsFromStruct(parent *array.Struct, fieldID int, row int) (*ListOfStructs, error) {
	if fieldID == AbsentFieldID {
		return nil, nil
	}

	arr := parent.Field(fieldID)
	if listArr, ok := arr.(*array.List); ok {
		if listArr.IsNull(row) {
			return nil, nil
		}

		switch structArr := listArr.ListValues().(type) {
		case *array.Struct:
			dt, ok := structArr.DataType().(*arrow.StructType)
			if !ok {
				return nil, werror.WrapWithContext(ErrNotStructType, map[string]interface{}{"fieldID": fieldID, "row": row})
			}
			start := int(listArr.Offsets()[row])
			end := int(listArr.Offsets()[row+1])

			return &ListOfStructs{
				dt:    dt,
				arr:   structArr,
				start: start,
				end:   end,
			}, nil
		default:
			return nil, werror.WrapWithContext(ErrNotArrayStruct, map[string]interface{}{"fieldID": fieldID, "row": row})
		}
	} else {
		return nil, werror.WrapWithContext(ErrNotArrayList, map[string]interface{}{"fieldID": fieldID, "row": row})
	}
}
