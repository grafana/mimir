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

import "errors"

var (
	// ErrInvalidArrayType is returned when an array is not of the expected type.
	ErrInvalidArrayType = errors.New("invalid arrow array type")

	// ErrNotStructType is returned when an array is not of type Struct.
	ErrNotStructType = errors.New("not arrow.StructType")
	// ErrNotListOfStructsType is returned when an array is not of type List of
	// structs.
	ErrNotListOfStructsType = errors.New("not arrow.ListType of arrow.StructType")
	// ErrNotListType is returned when an array is not of type list.
	ErrNotListType = errors.New("not an arrow.ListType")

	// ErrNotArrayStruct is returned when an array is not an array.Struct.
	ErrNotArrayStruct = errors.New("not an arrow array.Struct")
	// ErrNotArrayList is returned when an array is not an array.List.
	ErrNotArrayList = errors.New("not an arrow array.List")
	// ErrNotArrayListOfStructs is returned when an array is not an array.List
	// of array.Struct.
	ErrNotArrayListOfStructs = errors.New("not an Arrow array.List of array.Struct")

	// ErrDuplicateFieldName is returned when a field name is duplicated in the
	// same struct.
	ErrDuplicateFieldName = errors.New("duplicate field name")

	// ErrMissingFieldName is returned when a field name is missing in a struct.
	ErrMissingFieldName = errors.New("missing field name")
)
