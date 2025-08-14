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

// Package providing functions to print Arrow records to stdout for
// debugging and analysis.

import (
	"fmt"
	"math"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/open-telemetry/otel-arrow/go/pkg/otel/constants"
	"github.com/open-telemetry/otel-arrow/go/pkg/werror"
)

const (
	StrCode    int8 = 0
	I64Code    int8 = 1
	F64Code    int8 = 2
	BoolCode   int8 = 3
	BinaryCode int8 = 4
	CborCode   int8 = 5
)

// TextColumn memorizes the contents of a column for printing.
// MaxLen is used to determine the column width.
type TextColumn struct {
	Name   string
	MaxLen int
	Values []string
}

// PrintRecord prints the contents of an Arrow record to stdout.
func PrintRecord(name string, record arrow.Record, maxRows int) {
	PrintRecordWithProgression(name, record, maxRows, 0, 0)
}

// PrintRecordWithProgression prints the contents of an Arrow record to stdout.
func PrintRecordWithProgression(name string, record arrow.Record, maxRows, countPrints, maxPrints int) {
	progression := ""
	if maxPrints > 0 {
		progression = fmt.Sprintf(", progression %d/%d", countPrints, maxPrints)
	}

	sortingColumns := ""
	sc, ok := record.Schema().Metadata().GetValue(constants.SortingColumns)
	if ok {
		sortingColumns = fmt.Sprintf(", sort by %q", sc)
	}

	if record.NumRows() > int64(maxRows) {
		fmt.Printf("Dump record %q: %d rows/%d (total)%s%s\n", name, maxRows, record.NumRows(), progression, sortingColumns)
	} else {
		fmt.Printf("Dump record %q: %d rows%s%s\n", name, record.NumRows(), progression, sortingColumns)
	}

	schema := record.Schema()
	colNames := schemaColNames(schema)

	columns := make([]TextColumn, len(colNames))
	for i := 0; i < len(colNames); i++ {
		columns[i].Name = colNames[i]
		columns[i].MaxLen = len(colNames[i])
	}

	maxColSize := 60
	rows := int(math.Min(float64(maxRows), float64(record.NumRows())))
	for row := 0; row < rows; row++ {
		values := recordColValues(record, row)
		for i, value := range values {
			if len(value) > maxColSize {
				value = value[:maxColSize] + "..."
			}
			columns[i].Values = append(columns[i].Values, value)
			if columns[i].MaxLen < len(value) {
				columns[i].MaxLen = len(value)
			}
		}
	}

	for i := 0; i < len(columns); i++ {
		print(strings.Repeat("-", columns[i].MaxLen), "-+")
	}
	println()

	for i := 0; i < len(columns); i++ {
		fmt.Printf(fmt.Sprintf("%%%ds", columns[i].MaxLen), columns[i].Name)
		print(" |")
	}
	println()

	for i := 0; i < len(columns); i++ {
		print(strings.Repeat("-", columns[i].MaxLen), "-+")
	}
	println()

	for row := 0; row < rows; row++ {
		for i := 0; i < len(columns); i++ {
			fmt.Printf(fmt.Sprintf("%%%ds", columns[i].MaxLen), columns[i].Values[row])
			print(" |")
		}
		println()
	}
}

func schemaColNames(schema *arrow.Schema) []string {
	var names []string
	for _, field := range schema.Fields() {
		childNames := fieldColNames("", &field)
		names = append(names, childNames...)
	}
	return names
}

func fieldColNames(path string, field *arrow.Field) []string {
	path = path + field.Name

	st, isStruct := field.Type.(*arrow.StructType)
	if isStruct {
		var names []string
		path = path + "."
		for _, structField := range st.Fields() {
			childNames := fieldColNames(path, &structField)
			names = append(names, childNames...)
		}
		return names
	}

	return []string{path}
}

func recordColValues(record arrow.Record, row int) []string {
	var values []string

	for col := 0; col < int(record.NumCols()); col++ {
		arr := record.Column(col)
		values = append(values, arrayColValues(arr, row)...)
	}
	return values
}

func arrayColValues(arr arrow.Array, row int) []string {
	if arr.IsNull(row) {
		return []string{"NULL"}
	}

	switch c := arr.(type) {
	case *array.Boolean:
		return []string{fmt.Sprintf("%v", c.Value(row))}
	// uints
	case *array.Uint8:
		return []string{fmt.Sprintf("%v", c.Value(row))}
	case *array.Uint16:
		return []string{fmt.Sprintf("%v", c.Value(row))}
	case *array.Uint32:
		return []string{fmt.Sprintf("%v", c.Value(row))}
	case *array.Uint64:
		return []string{fmt.Sprintf("%v", c.Value(row))}
	// ints
	case *array.Int8:
		return []string{fmt.Sprintf("%v", c.Value(row))}
	case *array.Int16:
		return []string{fmt.Sprintf("%v", c.Value(row))}
	case *array.Int32:
		return []string{fmt.Sprintf("%v", c.Value(row))}
	case *array.Int64:
		return []string{fmt.Sprintf("%v", c.Value(row))}
	// floats
	case *array.Float32:
		return []string{fmt.Sprintf("%v", c.Value(row))}
	case *array.Float64:
		return []string{fmt.Sprintf("%v", c.Value(row))}

	case *array.String:
		str := c.Value(row)

		return []string{fmt.Sprintf("%v", escapeNonPrintable(str))}
	case *array.Binary:
		bin := c.Value(row)
		return []string{fmt.Sprintf("%v", bin)}
	case *array.FixedSizeBinary:
		bin := c.Value(row)
		return []string{fmt.Sprintf("%v", bin)}
	case *array.Timestamp:
		return []string{fmt.Sprintf("%v", c.Value(row))}
	case *array.Duration:
		return []string{fmt.Sprintf("%v", c.Value(row))}
	case *array.Dictionary:
		switch arr := c.Dictionary().(type) {
		case *array.Int32:
			return []string{fmt.Sprintf("%d", arr.Value(c.GetValueIndex(row)))}
		case *array.Int64:
			return []string{fmt.Sprintf("%d", arr.Value(c.GetValueIndex(row)))}
		case *array.Uint32:
			return []string{fmt.Sprintf("%d", arr.Value(c.GetValueIndex(row)))}
		case *array.String:
			str := arr.Value(c.GetValueIndex(row))
			return []string{fmt.Sprintf("%v", escapeNonPrintable(str))}
		case *array.Binary:
			bin := arr.Value(c.GetValueIndex(row))
			return []string{fmt.Sprintf("%v", bin)}
		case *array.Duration:
			return []string{fmt.Sprintf("%v", arr.Value(c.GetValueIndex(row)))}
		case *array.FixedSizeBinary:
			bin := arr.Value(c.GetValueIndex(row))
			return []string{fmt.Sprintf("%v", bin)}
		default:
			panic(fmt.Sprintf("unsupported dictionary type %T", arr))
		}
	case *array.Struct:
		var values []string
		for i := 0; i < c.NumField(); i++ {
			values = append(values, arrayColValues(c.Field(i), row)...)
		}
		return values
	case *array.SparseUnion:
		return []string{sparseUnionValue(c, row)}
	case *array.List:
		return []string{"List not supported"}
	default:
		panic(fmt.Sprintf("unsupported array type %T", arr))
	}
	return []string{}
}

// escapeNonPrintable replaces non-printable characters with escape sequences.
func escapeNonPrintable(str string) string {
	var sb strings.Builder
	for _, r := range str {
		if r < 32 || r > 126 {
			sb.WriteString(fmt.Sprintf("\\x%02x", r))
		} else {
			sb.WriteRune(r)
		}
	}
	return sb.String()
}

func sparseUnionValue(union *array.SparseUnion, row int) string {
	tcode := union.TypeCode(row)
	fieldID := union.ChildID(row)

	switch tcode {
	case StrCode:
		strArr := union.Field(fieldID)
		if strArr.IsNull(row) {
			return ""
		}

		switch arr := strArr.(type) {
		case *array.String:
			return arr.Value(row)
		case *array.Dictionary:
			return arr.Dictionary().(*array.String).Value(arr.GetValueIndex(row))
		default:
			panic(fmt.Sprintf("unsupported array type %T", arr))
		}
	case I64Code:
		i64Arr := union.Field(fieldID)
		val, err := i64FromArray(i64Arr, row)
		if err != nil {
			panic(err)
		}
		return fmt.Sprintf("%d", val)
	case F64Code:
		f64Arr := union.Field(fieldID)
		val := f64Arr.(*array.Float64).Value(row)
		return fmt.Sprintf("%f", val)
	case BoolCode:
		boolArr := union.Field(fieldID)
		val := boolArr.(*array.Boolean).Value(row)
		return fmt.Sprintf("%t", val)
	case BinaryCode:
		binArr := union.Field(fieldID)
		val, err := binaryFromArray(binArr, row)
		if err != nil {
			panic(err)
		}
		return fmt.Sprintf("%x", val)
	case CborCode:
		panic("cbor not supported")
	default:
		panic(fmt.Sprintf("unsupported type code %d", tcode))
	}
}

func i64FromArray(arr arrow.Array, row int) (int64, error) {
	if arr == nil {
		return 0, nil
	} else {
		switch arr := arr.(type) {
		case *array.Int64:
			if arr.IsNull(row) {
				return 0, nil
			} else {
				return arr.Value(row), nil
			}
		case *array.Dictionary:
			i64Arr := arr.Dictionary().(*array.Int64)
			if arr.IsNull(row) {
				return 0, nil
			} else {
				return i64Arr.Value(arr.GetValueIndex(row)), nil
			}
		default:
			return 0, werror.WrapWithMsg(ErrInvalidArrayType, "not an int64 array")
		}
	}
}

func binaryFromArray(arr arrow.Array, row int) ([]byte, error) {
	if arr == nil {
		return nil, nil
	} else {
		if arr.IsNull(row) {
			return nil, nil
		}

		switch arr := arr.(type) {
		case *array.Binary:
			return arr.Value(row), nil
		case *array.Dictionary:
			return arr.Dictionary().(*array.Binary).Value(arr.GetValueIndex(row)), nil
		default:
			return nil, werror.WrapWithMsg(ErrInvalidArrayType, "not a binary array")
		}
	}
}
