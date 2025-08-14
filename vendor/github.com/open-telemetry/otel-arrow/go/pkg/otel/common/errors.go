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

package common

import (
	"errors"
)

var (
	ErrInvalidKeyMap         = errors.New("invalid key map")
	ErrUnsupportedCborType   = errors.New("unsupported cbor type")
	ErrInvalidTypeConversion = errors.New("invalid type conversion")

	ErrInvalidSpanIDLength  = errors.New("invalid span id length")
	ErrInvalidTraceIDLength = errors.New("invalid trace id length")

	ErrNotArraySparseUnion = errors.New("not an arrow array.SparseUnion")
	ErrNotArrayMap         = errors.New("not an arrow array.Map")
)
