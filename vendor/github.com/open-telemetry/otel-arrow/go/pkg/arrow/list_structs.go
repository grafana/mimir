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

// Wrapper around an Arrow list of structs used to expose utility functions.

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/open-telemetry/otel-arrow/go/pkg/werror"
)

// ListOfStructs is a wrapper around an Arrow list of structs used to expose utility functions.
type ListOfStructs struct {
	dt    *arrow.StructType
	arr   *array.Struct
	start int
	end   int
}

// ListOfStructsFromRecord returns the struct type and an array of structs for a given field id.
func ListOfStructsFromRecord(record arrow.Record, fieldID int, row int) (*ListOfStructs, error) {
	if fieldID == AbsentFieldID {
		return nil, nil
	}
	arr := record.Column(fieldID)
	switch listArr := arr.(type) {
	case *array.List:
		if listArr.IsNull(row) {
			return nil, nil
		}

		switch structArr := listArr.ListValues().(type) {
		case *array.Struct:
			dt, ok := structArr.DataType().(*arrow.StructType)
			if !ok {
				return nil, werror.WrapWithContext(ErrNotArrayListOfStructs, map[string]interface{}{"fieldID": fieldID, "row": row})
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
			return nil, werror.WrapWithContext(ErrNotArrayListOfStructs, map[string]interface{}{"fieldID": fieldID, "row": row})
		}
	default:
		return nil, werror.WrapWithContext(ErrNotArrayList, map[string]interface{}{"fieldID": fieldID, "row": row})
	}
}

func ListOfStructsFromArray(arr arrow.Array, row int) (*ListOfStructs, error) {
	switch listArr := arr.(type) {
	case *array.List:
		if listArr.IsNull(row) {
			return nil, nil
		}

		switch structArr := listArr.ListValues().(type) {
		case *array.Struct:
			dt, ok := structArr.DataType().(*arrow.StructType)
			if !ok {
				return nil, werror.WrapWithContext(ErrNotArrayListOfStructs, map[string]interface{}{"row": row})
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
			return nil, werror.WrapWithContext(ErrNotArrayListOfStructs, map[string]interface{}{"row": row})
		}
	default:
		return nil, werror.WrapWithContext(ErrNotArrayList, map[string]interface{}{"row": row})
	}
}

// Start returns the start index of the list of structs.
func (los *ListOfStructs) Start() int {
	return los.start
}

// End returns the end index of the list of structs.
func (los *ListOfStructs) End() int {
	return los.end
}

// FieldIdx returns the field id of a named field.
// The boolean return value indicates whether the field was found.
func (los *ListOfStructs) FieldIdx(name string) (int, bool) {
	return los.dt.FieldIdx(name)
}

// Field returns the field array of a named field.
// The boolean return value indicates whether the field was found.
func (los *ListOfStructs) Field(name string) (arrow.Array, bool) {
	id, ok := los.dt.FieldIdx(name)
	if !ok {
		return nil, false
	}
	return los.arr.Field(id), true
}

// FieldByID returns the field array of a field id.
func (los *ListOfStructs) FieldByID(id int) arrow.Array {
	return los.arr.Field(id)
}

// StringFieldByID returns the string value of a field id for a specific row
// or empty string if the field doesn't exist.
func (los *ListOfStructs) StringFieldByID(fieldID int, row int) (string, error) {
	if fieldID == AbsentFieldID {
		return "", nil
	}
	column := los.arr.Field(fieldID)
	return StringFromArray(column, row)
}

// U16FieldByID returns the uint16 value of a field id for a specific row or 0
// if the field doesn't exist.
func (los *ListOfStructs) U16FieldByID(fieldID int, row int) (uint16, error) {
	if fieldID == AbsentFieldID {
		return 0, nil
	}
	column := los.arr.Field(fieldID)
	return U16FromArray(column, row)
}

// NullableU16FieldByID returns the uint16 value of a field id for a specific row or nil
// if the field doesn't exist.
func (los *ListOfStructs) NullableU16FieldByID(fieldID int, row int) (*uint16, error) {
	if fieldID == AbsentFieldID {
		return nil, nil
	}
	column := los.arr.Field(fieldID)
	if column.IsNull(row) {
		return nil, nil
	}
	val, err := U16FromArray(column, row)
	if err != nil {
		return nil, err
	}
	return &val, nil
}

// U32FieldByID returns the uint32 value of a field id for a specific row or 0
// if the field doesn't exist.
func (los *ListOfStructs) U32FieldByID(fieldID int, row int) (uint32, error) {
	if fieldID == AbsentFieldID {
		return 0, nil
	}
	column := los.arr.Field(fieldID)
	return U32FromArray(column, row)
}

// U64FieldByID returns the uint64 value of a field id for a specific row or 0
// if the field doesn't exist.
func (los *ListOfStructs) U64FieldByID(fieldID int, row int) (uint64, error) {
	if fieldID == AbsentFieldID {
		return 0, nil
	}
	column := los.arr.Field(fieldID)
	return U64FromArray(column, row)
}

// TimestampFieldByID returns the timestamp value of a field id for a specific
// row or a zero timestamp if the field doesn't exist.
func (los *ListOfStructs) TimestampFieldByID(fieldID int, row int) (arrow.Timestamp, error) {
	if fieldID == AbsentFieldID {
		return arrow.Timestamp(0), nil
	}
	column := los.arr.Field(fieldID)
	return TimestampFromArray(column, row)
}

// DurationFieldByID returns the duration value of a field id for a specific
// row or a 0 if the field doesn't exist.
func (los *ListOfStructs) DurationFieldByID(fieldID int, row int) (arrow.Duration, error) {
	if fieldID == AbsentFieldID {
		return arrow.Duration(0), nil
	}
	column := los.arr.Field(fieldID)
	return DurationFromArray(column, row)
}

// OptionalTimestampFieldByID returns the timestamp value of a field id for a
// specific row or nil if the field is null.
func (los *ListOfStructs) OptionalTimestampFieldByID(fieldID int, row int) *pcommon.Timestamp {
	if fieldID == AbsentFieldID {
		return nil
	}
	column := los.arr.Field(fieldID)
	if column.IsNull(row) {
		return nil
	}
	ts, err := TimestampFromArray(column, row)
	if err != nil {
		return nil
	}

	timestamp := pcommon.Timestamp(ts)
	return &timestamp
}

// I32FieldByID returns the int32 value of a field id for a specific row.
func (los *ListOfStructs) I32FieldByID(fieldID int, row int) (int32, error) {
	if fieldID == AbsentFieldID {
		return 0, nil
	}
	column := los.arr.Field(fieldID)
	return I32FromArray(column, row)
}

// I64FieldByID returns the int64 value of a field id for a specific row.
func (los *ListOfStructs) I64FieldByID(fieldID int, row int) (int64, error) {
	if fieldID == AbsentFieldID {
		return 0, nil
	}
	column := los.arr.Field(fieldID)
	return I64FromArray(column, row)
}

// F64FieldByID returns the float64 value of a field id for a specific row.
func (los *ListOfStructs) F64FieldByID(fieldID int, row int) (float64, error) {
	if fieldID == AbsentFieldID {
		return 0.0, nil
	}
	column := los.arr.Field(fieldID)
	return F64FromArray(column, row)
}

// F64OrNilFieldByID returns the float64 value of a field id for a specific row or nil if the field is null.
func (los *ListOfStructs) F64OrNilFieldByID(fieldID int, row int) (*float64, error) {
	if fieldID == AbsentFieldID {
		return nil, nil
	}
	column := los.arr.Field(fieldID)
	return F64OrNilFromArray(column, row)
}

// BoolFieldByID returns the bool value of a field id for a specific row.
func (los *ListOfStructs) BoolFieldByID(fieldID int, row int) (bool, error) {
	if fieldID == AbsentFieldID {
		return false, nil
	}
	column := los.arr.Field(fieldID)
	return BoolFromArray(column, row)
}

// BinaryFieldByID returns the binary value of a field id for a specific row.
func (los *ListOfStructs) BinaryFieldByID(fieldID int, row int) ([]byte, error) {
	if fieldID == AbsentFieldID {
		return nil, nil
	}
	column := los.arr.Field(fieldID)
	return BinaryFromArray(column, row)
}

// FixedSizeBinaryFieldByID returns the fixed size binary value of a field id for a specific row.
func (los *ListOfStructs) FixedSizeBinaryFieldByID(fieldID int, row int) ([]byte, error) {
	if fieldID == AbsentFieldID {
		return nil, nil
	}
	column := los.arr.Field(fieldID)
	return FixedSizeBinaryFromArray(column, row)
}

// StringFieldByName returns the string value of a named field for a specific row.
func (los *ListOfStructs) StringFieldByName(name string, row int) (string, error) {
	fieldID, found := los.dt.FieldIdx(name)
	if !found {
		return "", nil
	}
	column := los.arr.Field(fieldID)
	return StringFromArray(column, row)
}

// U32FieldByName returns the uint32 value of a named field for a specific row.
func (los *ListOfStructs) U32FieldByName(name string, row int) (uint32, error) {
	fieldID, found := los.dt.FieldIdx(name)
	if !found {
		return 0, nil
	}
	column := los.arr.Field(fieldID)
	return U32FromArray(column, row)
}

// U64FieldByName returns the uint64 value of a named field for a specific row.
func (los *ListOfStructs) U64FieldByName(name string, row int) (uint64, error) {
	fieldID, found := los.dt.FieldIdx(name)
	if !found {
		return 0, nil
	}
	column := los.arr.Field(fieldID)
	return U64FromArray(column, row)
}

// I32FieldByName returns the int32 value of a named field for a specific row.
func (los *ListOfStructs) I32FieldByName(name string, row int) (int32, error) {
	fieldID, found := los.dt.FieldIdx(name)
	if !found {
		return 0, nil
	}
	column := los.arr.Field(fieldID)
	return I32FromArray(column, row)
}

// I64FieldByName returns the int64 value of a named field for a specific row.
func (los *ListOfStructs) I64FieldByName(name string, row int) (int64, error) {
	fieldID, found := los.dt.FieldIdx(name)
	if !found {
		return 0, nil
	}
	column := los.arr.Field(fieldID)
	return I64FromArray(column, row)
}

// F64FieldByName returns the float64 value of a named field for a specific row.
func (los *ListOfStructs) F64FieldByName(name string, row int) (float64, error) {
	fieldID, found := los.dt.FieldIdx(name)
	if !found {
		return 0.0, nil
	}
	column := los.arr.Field(fieldID)
	return F64FromArray(column, row)
}

// BoolFieldByName returns the bool value of a named field for a specific row.
func (los *ListOfStructs) BoolFieldByName(name string, row int) (bool, error) {
	fieldID, found := los.dt.FieldIdx(name)
	if !found {
		return false, nil
	}
	column := los.arr.Field(fieldID)
	return BoolFromArray(column, row)
}

// BinaryFieldByName returns the binary value of a named field for a specific row.
func (los *ListOfStructs) BinaryFieldByName(name string, row int) ([]byte, error) {
	fieldID, found := los.dt.FieldIdx(name)
	if !found {
		return nil, nil
	}
	column := los.arr.Field(fieldID)
	return BinaryFromArray(column, row)
}

// FixedSizeBinaryFieldByName returns the fixed size binary value of a named field for a specific row.
func (los *ListOfStructs) FixedSizeBinaryFieldByName(name string, row int) ([]byte, error) {
	fieldID, found := los.dt.FieldIdx(name)
	if !found {
		return nil, nil
	}
	column := los.arr.Field(fieldID)
	return FixedSizeBinaryFromArray(column, row)
}

// StructArray returns the underlying arrow array for a named field for a specific row.
func (los *ListOfStructs) StructArray(name string, row int) (*arrow.StructType, *array.Struct, error) {
	fieldID, found := los.dt.FieldIdx(name)
	if !found {
		return nil, nil, nil
	}
	column := los.arr.Field(fieldID)

	switch structArr := column.(type) {
	case *array.Struct:
		if structArr.IsNull(row) {
			return nil, nil, nil
		}
		return structArr.DataType().(*arrow.StructType), structArr, nil
	default:
		return nil, nil, werror.WrapWithContext(ErrNotArrayStruct, map[string]interface{}{"fieldName": name, "row": row})
	}
}

// StructByID returns the underlying arrow struct stype and arrow array for a field id for a specific row.
func (los *ListOfStructs) StructByID(fieldID int, row int) (*arrow.StructType, *array.Struct, error) {
	if fieldID == AbsentFieldID {
		return nil, nil, nil
	}
	column := los.arr.Field(fieldID)
	switch structArr := column.(type) {
	case *array.Struct:
		if structArr.IsNull(row) {
			return nil, nil, nil
		}
		return structArr.DataType().(*arrow.StructType), structArr, nil
	default:
		return nil, nil, werror.WrapWithContext(ErrNotArrayStruct, map[string]interface{}{"fieldID": fieldID, "row": row})
	}
}

// IsNull returns true if the row is null.
func (los *ListOfStructs) IsNull(row int) bool {
	return los.arr.IsNull(row)
}

// ListValuesById return the list array for a field id for a specific row.
func (los *ListOfStructs) ListValuesById(row int, fieldID int) (arr arrow.Array, start int, end int, err error) {
	if fieldID == AbsentFieldID {
		return nil, 0, 0, nil
	}
	column := los.arr.Field(fieldID)
	switch listArr := column.(type) {
	case *array.List:
		if listArr.IsNull(row) {
			return nil, 0, 0, nil
		}
		start = int(listArr.Offsets()[row])
		end = int(listArr.Offsets()[row+1])
		arr = listArr.ListValues()
	default:
		err = werror.WrapWithContext(ErrNotArrayList, map[string]interface{}{"fieldID": fieldID, "row": row})
	}
	return
}

// ListOfStructsById returns the list of structs for a field id for a specific row.
func (los *ListOfStructs) ListOfStructsById(row int, fieldID int) (*ListOfStructs, error) {
	if fieldID == AbsentFieldID {
		return nil, nil
	}
	column := los.arr.Field(fieldID)
	switch listArr := column.(type) {
	case *array.List:
		if listArr.IsNull(row) {
			return nil, nil
		}

		switch structArr := listArr.ListValues().(type) {
		case *array.Struct:
			dt, ok := structArr.DataType().(*arrow.StructType)
			if !ok {
				return nil, werror.WrapWithContext(ErrNotArrayStruct, map[string]interface{}{"fieldID": fieldID, "row": row})
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
			return nil, werror.WrapWithContext(ErrNotArrayListOfStructs, map[string]interface{}{"fieldID": fieldID, "row": row})
		}
	default:
		return nil, werror.WrapWithContext(ErrNotArrayList, map[string]interface{}{"fieldID": fieldID, "row": row})
	}
}

// DataType returns the underlying arrow struct type.
func (los *ListOfStructs) DataType() *arrow.StructType {
	return los.dt
}

// Array returns the underlying arrow array.
func (los *ListOfStructs) Array() *array.Struct {
	return los.arr
}
