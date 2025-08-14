// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package otlp

import (
	"github.com/apache/arrow-go/v18/arrow/array"
	"go.opentelemetry.io/collector/pdata/pcommon"

	arrowutils "github.com/open-telemetry/otel-arrow/go/pkg/arrow"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common"
	commonarrow "github.com/open-telemetry/otel-arrow/go/pkg/otel/common/arrow"
	"github.com/open-telemetry/otel-arrow/go/pkg/werror"
)

func UpdateValueFrom(v pcommon.Value, vArr *array.SparseUnion, row int) error {
	tcode := vArr.TypeCode(row)
	fieldID := vArr.ChildID(row)

	switch tcode {
	case commonarrow.StrCode:
		strArr := vArr.Field(fieldID)
		if strArr == nil {
			return werror.WrapWithContext(ErrInvalidFieldId, map[string]interface{}{"tcode": tcode, "fieldID": fieldID, "row": row})
		}
		val, err := arrowutils.StringFromArray(strArr, row)
		if err != nil {
			return werror.Wrap(err)
		}
		v.SetStr(val)
	case commonarrow.I64Code:
		i64Arr := vArr.Field(fieldID)
		if i64Arr == nil {
			return werror.WrapWithContext(ErrInvalidFieldId, map[string]interface{}{"tcode": tcode, "fieldID": fieldID, "row": row})
		}
		val, err := arrowutils.I64FromArray(i64Arr, row)
		if err != nil {
			return werror.Wrap(err)
		}
		v.SetInt(val)
	case commonarrow.F64Code:
		f64Arr := vArr.Field(fieldID)
		if f64Arr == nil {
			return werror.WrapWithContext(ErrInvalidFieldId, map[string]interface{}{"tcode": tcode, "fieldID": fieldID, "row": row})
		}
		val := f64Arr.(*array.Float64).Value(row)
		v.SetDouble(val)
	case commonarrow.BoolCode:
		boolArr := vArr.Field(fieldID)
		if boolArr == nil {
			return werror.WrapWithContext(ErrInvalidFieldId, map[string]interface{}{"tcode": tcode, "fieldID": fieldID, "row": row})
		}
		val := boolArr.(*array.Boolean).Value(row)
		v.SetBool(val)
	case commonarrow.BinaryCode:
		binArr := vArr.Field(fieldID)
		if binArr == nil {
			return werror.WrapWithContext(ErrInvalidFieldId, map[string]interface{}{"tcode": tcode, "fieldID": fieldID, "row": row})
		}
		val, err := arrowutils.BinaryFromArray(binArr, row)
		if err != nil {
			return werror.Wrap(err)
		}
		v.SetEmptyBytes().FromRaw(val)
	case commonarrow.CborCode:
		cborArr := vArr.Field(fieldID)
		if cborArr == nil {
			return werror.WrapWithContext(ErrInvalidFieldId, map[string]interface{}{"tcode": tcode, "fieldID": fieldID, "row": row})
		}
		val, err := arrowutils.BinaryFromArray(cborArr, row)
		if err != nil {
			return werror.Wrap(err)
		}
		if err = common.Deserialize(val, v); err != nil {
			return werror.Wrap(err)
		}
	default:
		return werror.WrapWithContext(ErrInvalidTypeCode, map[string]interface{}{"tcode": tcode, "fieldID": fieldID, "row": row})
	}

	return nil
}
