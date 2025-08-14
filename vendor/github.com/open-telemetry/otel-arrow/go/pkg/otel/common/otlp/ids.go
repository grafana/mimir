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

package otlp

import (
	"encoding/hex"
	"sort"
	"strconv"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common"
)

func ResourceID(r pcommon.Resource, schemaUrl string) string {
	var b strings.Builder
	AttributesId(r.Attributes(), &b)
	b.WriteString("|")
	b.WriteString(strconv.FormatUint(uint64(r.DroppedAttributesCount()), 10))
	b.WriteString("|")
	b.WriteString(schemaUrl)
	return b.String()
}

func AttributesId(attrs pcommon.Map, b *strings.Builder) {
	tmp := make(common.AttributeEntries, 0, attrs.Len())
	attrs.Range(func(k string, v pcommon.Value) bool {
		tmp = append(tmp, common.AttributeEntry{
			Key:   k,
			Value: v,
		})
		return true
	})
	sort.Stable(tmp)

	b.WriteString("{")
	for i, e := range tmp {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(e.Key)
		b.WriteString(":")
		ValueID(e.Value, b)
	}
	b.WriteString("}")
}

func ValueID(v pcommon.Value, b *strings.Builder) {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		b.WriteString(v.Str())
	case pcommon.ValueTypeInt:
		b.WriteString(strconv.FormatInt(v.Int(), 10))
	case pcommon.ValueTypeDouble:
		b.WriteString(strconv.FormatFloat(v.Double(), 'E', -1, 64))
	case pcommon.ValueTypeBool:
		b.WriteString(strconv.FormatBool(v.Bool()))
	case pcommon.ValueTypeMap:
		AttributesId(v.Map(), b)
	case pcommon.ValueTypeBytes:
		b.WriteString(hex.EncodeToString(v.Bytes().AsRaw()))
	case pcommon.ValueTypeSlice:
		values := v.Slice()
		b.WriteString("[")
		for i := 0; i < values.Len(); i++ {
			if i > 0 {
				b.WriteString(",")
			}
			ValueID(values.At(i), b)
		}
		b.WriteString("]")
	case pcommon.ValueTypeEmpty:
		return
	default:
		// includes pcommon.ValueTypeEmpty
		panic("unsupported value type")
	}
}

func ScopeID(is pcommon.InstrumentationScope, schemaUrl string) string {
	var b strings.Builder
	b.WriteString("name:")
	b.WriteString(is.Name())
	b.WriteString("|version:")
	b.WriteString(is.Version())
	b.WriteString("|")
	AttributesId(is.Attributes(), &b)
	b.WriteString("|")
	b.WriteString(strconv.FormatUint(uint64(is.DroppedAttributesCount()), 10))
	b.WriteString("|")
	b.WriteString(schemaUrl)
	return b.String()
}
