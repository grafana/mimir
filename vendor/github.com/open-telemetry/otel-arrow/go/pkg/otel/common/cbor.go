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
	"bytes"
	"math"

	"github.com/fxamacker/cbor/v2"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/open-telemetry/otel-arrow/go/pkg/werror"
)

// The CBOR representation is used to serialize complex pcommon.Value types (e.g. maps and slices) in the attributes
// and body fields of OTEL entities (i.e. metrics, logs, and traces). The goal is to minimize the complexity of the
// Arrow Schema by defining a limited number of variants in the sparse union used to represent the attributes and body
// fields. This design decision is also motivated by the fact that the memory footprint of sparse unions is directly
// proportional to the number of variants in the union. The CBOR serialization for complex values limits the number of
// variants in the sparse union to a fixed number of variant types independently of the complexity of the value.
//
// CBOR (Concise Binary Object Representation) is a data format whose design goals include the possibility of extremely
// small code size, fairly small message size, and extensibility without the need for version negotiation. CBOR is
// defined in RFC 8949.

// Serialize serializes the given pcommon.value into a CBOR byte array.
func Serialize(v *pcommon.Value) ([]byte, error) {
	var buf bytes.Buffer

	em, err := cbor.EncOptions{Sort: cbor.SortCanonical}.EncMode()
	if err != nil {
		return nil, werror.Wrap(err)
	}

	if err = encode(em.NewEncoder(&buf), v); err != nil {
		return nil, werror.Wrap(err)
	}

	return buf.Bytes(), nil
}

// Deserialize deserializes the given CBOR byte array into a pcommon.value.
func Deserialize(cborData []byte, target pcommon.Value) error {
	dec := cbor.NewDecoder(bytes.NewReader(cborData))

	var v interface{}
	if err := dec.Decode(&v); err != nil {
		return werror.Wrap(err)
	}

	if err := decode(v, target); err != nil {
		return werror.Wrap(err)
	}
	return nil
}

func encode(enc *cbor.Encoder, v *pcommon.Value) (err error) {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		err = enc.Encode(v.Str())
	case pcommon.ValueTypeInt:
		err = enc.Encode(v.Int())
	case pcommon.ValueTypeDouble:
		err = enc.Encode(v.Double())
	case pcommon.ValueTypeBool:
		err = enc.Encode(v.Bool())
	case pcommon.ValueTypeMap:
		err = enc.StartIndefiniteMap()
		if err != nil {
			return
		}
		v.Map().Range(func(k string, v pcommon.Value) bool {
			if err := enc.Encode(k); err != nil {
				return false
			}
			if err := encode(enc, &v); err != nil {
				return false
			}
			return true
		})
		err = enc.EndIndefinite()
		if err != nil {
			return
		}
	case pcommon.ValueTypeSlice:
		err = enc.StartIndefiniteArray()
		if err != nil {
			return
		}

		slice := v.Slice()
		for i := 0; i < slice.Len(); i++ {
			v := slice.At(i)
			if err := encode(enc, &v); err != nil {
				return werror.Wrap(err)
			}
		}

		err = enc.EndIndefinite()
		if err != nil {
			return
		}
	case pcommon.ValueTypeBytes:
		// Note: Unfortunately, an empty byte array is encoded as a nil value by pcommon.Value. So the conversion
		// from pcommon.Value to CBOR is not reversible.
		err = enc.Encode(v.Bytes().AsRaw())
	case pcommon.ValueTypeEmpty:
		err = enc.Encode(nil)
	}

	err = werror.Wrap(err)
	return
}

func decode(inVal interface{}, outVal pcommon.Value) error {
	switch typedV := inVal.(type) {
	case string:
		outVal.SetStr(typedV)
	case int:
		outVal.SetInt(int64(typedV))
	case int64:
		outVal.SetInt(typedV)
	case uint64:
		if typedV > math.MaxInt64 {
			return werror.Wrap(ErrInvalidTypeConversion)
		}
		outVal.SetInt(int64(typedV))
	case float64:
		outVal.SetDouble(typedV)
	case bool:
		outVal.SetBool(typedV)
	case map[interface{}]interface{}:
		mapV := outVal.SetEmptyMap()

		for k, v := range typedV {
			if kStr, ok := k.(string); ok {
				if err := decode(v, mapV.PutEmpty(kStr)); err != nil {
					return werror.Wrap(err)
				}
			} else {
				return werror.WrapWithContext(ErrInvalidKeyMap, map[string]interface{}{"key": k, "value": v})
			}
		}
	case []interface{}:
		slice := outVal.SetEmptySlice()
		slice.EnsureCapacity(len(typedV))
		for _, v := range typedV {
			if err := decode(v, slice.AppendEmpty()); err != nil {
				return werror.Wrap(err)
			}
		}
	case []byte:
		binary := outVal.SetEmptyBytes()
		binary.Append(typedV...)
	case nil:
		// nothing to do
	default:
		return werror.WrapWithContext(ErrUnsupportedCborType, map[string]interface{}{"type": inVal})
	}
	return nil
}
