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
	"github.com/apache/arrow-go/v18/arrow"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"golang.org/x/exp/constraints"

	arrowutils "github.com/open-telemetry/otel-arrow/go/pkg/arrow"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common"
	carrow "github.com/open-telemetry/otel-arrow/go/pkg/otel/common/arrow"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/constants"
	"github.com/open-telemetry/otel-arrow/go/pkg/werror"
)

type (
	// AttributeIDs is a struct containing the Arrow field IDs of the
	// attributes.
	AttributeIDs struct {
		ParentID int
		Key      int
		Type     int
		Str      int
		Int      int
		Double   int
		Bool     int
		Bytes    int
		Ser      int
	}

	// AttributesStore is a store for attributes.
	// The attributes are stored in a map by ID. This ID represents the
	// identifier of the main entity (span, event, link, etc.) to which the
	// attributes are attached. So the maximum number of attributes per entity
	// is not limited.
	AttributesStore[u constraints.Unsigned] struct {
		lastID         u
		attributesByID map[u]*pcommon.Map
	}

	AttrsParentIDDecoder[u constraints.Unsigned] struct {
		prevParentID u
		prevKey      string
		prevValue    *pcommon.Value
	}
)

// NewAttributesStore creates a new AttributesStore.
func NewAttributesStore[u constraints.Unsigned]() *AttributesStore[u] {
	return &AttributesStore[u]{
		attributesByID: make(map[u]*pcommon.Map),
	}
}

// AttributesByDeltaID returns the attributes for the given Delta ID.
func (s *AttributesStore[u]) AttributesByDeltaID(ID u) *pcommon.Map {
	s.lastID += ID
	if m, ok := s.attributesByID[s.lastID]; ok {
		return m
	}
	return nil
}

// AttributesByID returns the attributes for the given ID.
func (s *AttributesStore[u]) AttributesByID(ID u) *pcommon.Map {
	if m, ok := s.attributesByID[ID]; ok {
		return m
	}
	return nil
}

// AttributesStoreFrom creates an Attribute16Store from an arrow.Record.
// Note: This function doesn't release the record passed as argument. This is
// the responsibility of the caller
func AttributesStoreFrom[unsigned constraints.Unsigned](record arrow.Record, store *AttributesStore[unsigned]) error {
	attrIDS, err := SchemaToAttributeIDs(record.Schema())
	if err != nil {
		return werror.Wrap(err)
	}

	attrsCount := int(record.NumRows())

	parentIDDecoder := NewAttrsParentIDDecoder[unsigned]()

	// Read all key/value tuples from the record and reconstruct the attributes
	// map by ID.
	for i := 0; i < attrsCount; i++ {
		key, err := arrowutils.StringFromRecord(record, attrIDS.Key, i)
		if err != nil {
			return werror.Wrap(err)
		}

		vType, err := arrowutils.U8FromRecord(record, attrIDS.Type, i)
		if err != nil {
			return werror.Wrap(err)
		}
		value := pcommon.NewValueEmpty()
		switch pcommon.ValueType(vType) {
		case pcommon.ValueTypeStr:
			v, err := arrowutils.StringFromRecord(record, attrIDS.Str, i)
			if err != nil {
				return werror.Wrap(err)
			}
			value.SetStr(v)
		case pcommon.ValueTypeInt:
			v, err := arrowutils.I64FromRecord(record, attrIDS.Int, i)
			if err != nil {
				return werror.Wrap(err)
			}
			value.SetInt(v)
		case pcommon.ValueTypeDouble:
			v, err := arrowutils.F64FromRecord(record, attrIDS.Double, i)
			if err != nil {
				return werror.Wrap(err)
			}
			value.SetDouble(v)
		case pcommon.ValueTypeBool:
			v, err := arrowutils.BoolFromRecord(record, attrIDS.Bool, i)
			if err != nil {
				return werror.Wrap(err)
			}
			value.SetBool(v)
		case pcommon.ValueTypeBytes:
			v, err := arrowutils.BinaryFromRecord(record, attrIDS.Bytes, i)
			if err != nil {
				return werror.Wrap(err)
			}
			value.SetEmptyBytes().FromRaw(v)
		case pcommon.ValueTypeSlice:
			v, err := arrowutils.BinaryFromRecord(record, attrIDS.Ser, i)
			if err != nil {
				return werror.Wrap(err)
			}
			if err = common.Deserialize(v, value); err != nil {
				return werror.Wrap(err)
			}
		case pcommon.ValueTypeMap:
			v, err := arrowutils.BinaryFromRecord(record, attrIDS.Ser, i)
			if err != nil {
				return werror.Wrap(err)
			}
			if err = common.Deserialize(v, value); err != nil {
				return werror.Wrap(err)
			}
		default:
			// silently ignore unknown types to avoid DOS attacks
		}

		deltaOrParentID, err := arrowutils.UnsignedFromRecord[unsigned](record, attrIDS.ParentID, i)
		if err != nil {
			return werror.Wrap(err)
		}

		parentID := parentIDDecoder.Decode(deltaOrParentID, key, &value)

		m, ok := store.attributesByID[parentID]
		if !ok {
			newMap := pcommon.NewMap()
			m = &newMap
			store.attributesByID[parentID] = m
		}
		value.CopyTo(m.PutEmpty(key))
	}

	return nil
}

// SchemaToAttributeIDs pre-computes the field IDs for the attributes record.
func SchemaToAttributeIDs(schema *arrow.Schema) (*AttributeIDs, error) {
	parentID, err := arrowutils.MandatoryFieldIDFromSchema(schema, constants.ParentID)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	key, err := arrowutils.MandatoryFieldIDFromSchema(schema, constants.AttributeKey)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	vType, err := arrowutils.MandatoryFieldIDFromSchema(schema, constants.AttributeType)
	if err != nil {
		return nil, werror.Wrap(err)
	}
	vStr, err := arrowutils.FieldIDFromSchema(schema, constants.AttributeStr)
	if err != nil {
		return nil, werror.Wrap(err)
	}
	vInt, err := arrowutils.FieldIDFromSchema(schema, constants.AttributeInt)
	if err != nil {
		return nil, werror.Wrap(err)
	}
	vDouble, err := arrowutils.FieldIDFromSchema(schema, constants.AttributeDouble)
	if err != nil {
		return nil, werror.Wrap(err)
	}
	vBool, err := arrowutils.FieldIDFromSchema(schema, constants.AttributeBool)
	if err != nil {
		return nil, werror.Wrap(err)
	}
	vBytes, err := arrowutils.FieldIDFromSchema(schema, constants.AttributeBytes)
	if err != nil {
		return nil, werror.Wrap(err)
	}
	vSer, err := arrowutils.FieldIDFromSchema(schema, constants.AttributeSer)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	return &AttributeIDs{
		ParentID: parentID,
		Key:      key,
		Type:     vType,
		Str:      vStr,
		Int:      vInt,
		Double:   vDouble,
		Bool:     vBool,
		Bytes:    vBytes,
		Ser:      vSer,
	}, nil
}

// AttrsParentIdDecoder implements parent_id decoding for attribute
// sets.  The parent_id in this case is the entity which refers to the
// set of attributes (e.g., a resource, a scope, a metric) contained
// in a RecordBatch.
//
// Phase 1 note: there were several experimental encoding schemes
// tested.  Two schemes named "ParentIdDeltaEncoding",
// "ParentIdNoEncoding" have been removed.
func NewAttrsParentIDDecoder[u constraints.Unsigned]() *AttrsParentIDDecoder[u] {
	return &AttrsParentIDDecoder[u]{}
}

func (d *AttrsParentIDDecoder[unsigned]) Decode(deltaOrParentID unsigned, key string, value *pcommon.Value) unsigned {
	if d.prevKey == key && carrow.Equal(d.prevValue, value) {
		parentID := d.prevParentID + deltaOrParentID
		d.prevParentID = parentID
		return parentID
	} else {
		d.prevKey = key
		d.prevValue = value
		d.prevParentID = deltaOrParentID
		return deltaOrParentID
	}
}
