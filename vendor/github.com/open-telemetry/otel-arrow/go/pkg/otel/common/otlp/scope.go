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
	"github.com/apache/arrow-go/v18/arrow"
	"go.opentelemetry.io/collector/pdata/pcommon"

	arrowutils "github.com/open-telemetry/otel-arrow/go/pkg/arrow"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/constants"
	"github.com/open-telemetry/otel-arrow/go/pkg/werror"
)

type ScopeIds struct {
	Scope                  int
	Name                   int
	Version                int
	ID                     int
	DroppedAttributesCount int
}

func NewScopeIdsFromSchema(schema *arrow.Schema) (*ScopeIds, error) {
	scopeID, scopeDT, err := arrowutils.StructFieldIDFromSchema(schema, constants.Scope)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	nameID, _ := arrowutils.FieldIDFromStruct(scopeDT, constants.Name)
	versionID, _ := arrowutils.FieldIDFromStruct(scopeDT, constants.Version)
	droppedAttributesCountID, _ := arrowutils.FieldIDFromStruct(scopeDT, constants.DroppedAttributesCount)
	ID, _ := arrowutils.FieldIDFromStruct(scopeDT, constants.ID)
	return &ScopeIds{
		Scope:                  scopeID,
		Name:                   nameID,
		Version:                versionID,
		DroppedAttributesCount: droppedAttributesCountID,
		ID:                     ID,
	}, nil
}

func UpdateScopeFromRecord(
	s pcommon.InstrumentationScope,
	record arrow.Record,
	row int,
	ids *ScopeIds,
	attrsStore *AttributesStore[uint16],
) error {
	scopeArray, err := arrowutils.StructFromRecord(record, ids.Scope, row)
	if err != nil {
		return werror.WrapWithContext(err, map[string]interface{}{"row": row})
	}
	name, err := arrowutils.StringFromStruct(scopeArray, row, ids.Name)
	if err != nil {
		return werror.WrapWithContext(err, map[string]interface{}{"row": row})
	}
	version, err := arrowutils.StringFromStruct(scopeArray, row, ids.Version)
	if err != nil {
		return werror.WrapWithContext(err, map[string]interface{}{"row": row})
	}
	droppedAttributesCount, err := arrowutils.U32FromStruct(scopeArray, row, ids.DroppedAttributesCount)
	if err != nil {
		return werror.WrapWithContext(err, map[string]interface{}{"row": row})
	}

	ID, err := arrowutils.NullableU16FromStruct(scopeArray, row, ids.ID)
	if err != nil {
		return werror.WrapWithContext(err, map[string]interface{}{"row": row})
	}
	if ID != nil {
		attrs := attrsStore.AttributesByDeltaID(*ID)
		if attrs != nil {
			attrs.CopyTo(s.Attributes())
		}
	}
	s.SetName(name)
	s.SetVersion(version)
	s.SetDroppedAttributesCount(droppedAttributesCount)
	return nil
}

func ScopeIDFromRecord(record arrow.Record, row int, IDs *ScopeIds) (uint16, error) {
	scopeStruct, err := arrowutils.StructFromRecord(record, IDs.Scope, row)
	if err != nil {
		return 0, err
	}
	return arrowutils.U16FromStruct(scopeStruct, row, IDs.ID)

}
