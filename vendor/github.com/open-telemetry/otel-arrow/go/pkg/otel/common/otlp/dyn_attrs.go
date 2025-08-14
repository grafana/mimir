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
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"go.opentelemetry.io/collector/pdata/pcommon"

	arrowutils "github.com/open-telemetry/otel-arrow/go/pkg/arrow"
	carrow "github.com/open-telemetry/otel-arrow/go/pkg/otel/common/arrow"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/constants"
	"github.com/open-telemetry/otel-arrow/go/pkg/werror"
)

type (
	DynAttrsStore struct {
		parentID   *ParentIDGetter
		feeders    []AttributeFeeder
		attributes map[uint32]pcommon.Map
	}

	ParentIDGetter struct {
		fieldID  int
		parentID uint32
	}

	AttributeFeeder interface {
		Update(record arrow.Record, row int, attrs pcommon.Map) (err error)
	}

	BoolAttributeFeeder struct {
		fieldID  int
		attrName string
	}

	IntAttributeFeeder struct {
		fieldID  int
		attrName string
	}

	DoubleAttributeFeeder struct {
		fieldID  int
		attrName string
	}

	StringAttributeFeeder struct {
		fieldID  int
		attrName string
	}

	BinaryAttributeFeeder struct {
		fieldID  int
		attrName string
	}

	CborAttributeFeeder struct {
		fieldID  int
		attrName string
	}
)

var feederCreatorByType = map[string]func(fieldID int, attrName string) AttributeFeeder{
	carrow.BoolType: func(fieldID int, attrName string) AttributeFeeder {
		return &BoolAttributeFeeder{
			fieldID:  fieldID,
			attrName: attrName,
		}
	},
	carrow.IntType: func(fieldID int, attrName string) AttributeFeeder {
		return &IntAttributeFeeder{
			fieldID:  fieldID,
			attrName: attrName,
		}
	},
	carrow.IntDictType: func(fieldID int, attrName string) AttributeFeeder {
		return &IntAttributeFeeder{
			fieldID:  fieldID,
			attrName: attrName,
		}
	},
	carrow.DoubleType: func(fieldID int, attrName string) AttributeFeeder {
		return &DoubleAttributeFeeder{
			fieldID:  fieldID,
			attrName: attrName,
		}
	},
	carrow.StringType: func(fieldID int, attrName string) AttributeFeeder {
		return &StringAttributeFeeder{
			fieldID:  fieldID,
			attrName: attrName,
		}
	},
	carrow.StringDictType: func(fieldID int, attrName string) AttributeFeeder {
		return &StringAttributeFeeder{
			fieldID:  fieldID,
			attrName: attrName,
		}
	},
	carrow.BytesType: func(fieldID int, attrName string) AttributeFeeder {
		return &BinaryAttributeFeeder{
			fieldID:  fieldID,
			attrName: attrName,
		}
	},
	carrow.BytesDictType: func(fieldID int, attrName string) AttributeFeeder {
		return &BinaryAttributeFeeder{
			fieldID:  fieldID,
			attrName: attrName,
		}
	},
	carrow.CborType: func(fieldID int, attrName string) AttributeFeeder {
		return &CborAttributeFeeder{
			fieldID:  fieldID,
			attrName: attrName,
		}
	},
	carrow.CborDictType: func(fieldID int, attrName string) AttributeFeeder {
		return &CborAttributeFeeder{
			fieldID:  fieldID,
			attrName: attrName,
		}
	},
}

func DynAttrsStoreFromRecord(record arrow.Record) (*DynAttrsStore, error) {
	defer record.Release()

	store, err := CreateDynAttrsStoreFrom(record)
	if err != nil {
		return nil, err
	}

	rowCount := int(record.NumRows())

	for row := 0; row < rowCount; row++ {
		parentID, err := store.parentID.Get(record, row)
		if err != nil {
			return nil, err
		}
		attrs := pcommon.NewMap()
		for _, feeder := range store.feeders {
			if err := feeder.Update(record, row, attrs); err != nil {
				return nil, err
			}
		}

		store.attributes[parentID] = attrs
	}
	return store, nil

}

func (s *DynAttrsStore) Attributes(parentID uint32) (pcommon.Map, bool) {
	attrs, found := s.attributes[parentID]
	return attrs, found
}

func attrName(fieldName string, mType string) (string, error) {
	switch mType {
	case carrow.BoolType:
		if !strings.HasSuffix(fieldName, "_"+mType) {
			return "", werror.WrapWithContext(ErrInvalidAttrName, map[string]interface{}{"fieldName": fieldName})
		}
		return strings.TrimSuffix(fieldName, "_"+mType), nil
	case carrow.IntType:
		if !strings.HasSuffix(fieldName, "_"+mType) {
			return "", werror.WrapWithContext(ErrInvalidAttrName, map[string]interface{}{"fieldName": fieldName})
		}
		return strings.TrimSuffix(fieldName, "_"+mType), nil
	case carrow.IntDictType:
		if !strings.HasSuffix(fieldName, "_"+carrow.IntType) {
			return "", werror.WrapWithContext(ErrInvalidAttrName, map[string]interface{}{"fieldName": fieldName})
		}
		return strings.TrimSuffix(fieldName, "_"+carrow.IntType), nil
	case carrow.DoubleType:
		if !strings.HasSuffix(fieldName, "_"+mType) {
			return "", werror.WrapWithContext(ErrInvalidAttrName, map[string]interface{}{"fieldName": fieldName})
		}
		return strings.TrimSuffix(fieldName, "_"+mType), nil
	case carrow.StringType:
		if !strings.HasSuffix(fieldName, "_"+mType) {
			return "", werror.WrapWithContext(ErrInvalidAttrName, map[string]interface{}{"fieldName": fieldName})
		}
		return strings.TrimSuffix(fieldName, "_"+mType), nil
	case carrow.StringDictType:
		if !strings.HasSuffix(fieldName, "_"+carrow.StringType) {
			return "", werror.WrapWithContext(ErrInvalidAttrName, map[string]interface{}{"fieldName": fieldName})
		}
		return strings.TrimSuffix(fieldName, "_"+carrow.StringType), nil
	case carrow.BytesType:
		if !strings.HasSuffix(fieldName, "_"+mType) {
			return "", werror.WrapWithContext(ErrInvalidAttrName, map[string]interface{}{"fieldName": fieldName})
		}
		return strings.TrimSuffix(fieldName, "_"+mType), nil
	case carrow.BytesDictType:
		if !strings.HasSuffix(fieldName, "_"+carrow.BytesType) {
			return "", werror.WrapWithContext(ErrInvalidAttrName, map[string]interface{}{"fieldName": fieldName})
		}
		return strings.TrimSuffix(fieldName, "_"+carrow.BytesType), nil
	case carrow.CborType:
		if !strings.HasSuffix(fieldName, "_"+mType) {
			return "", werror.WrapWithContext(ErrInvalidAttrName, map[string]interface{}{"fieldName": fieldName})
		}
		return strings.TrimSuffix(fieldName, "_"+mType), nil
	case carrow.CborDictType:
		if !strings.HasSuffix(fieldName, "_"+carrow.CborType) {
			return "", werror.WrapWithContext(ErrInvalidAttrName, map[string]interface{}{"fieldName": fieldName})
		}
		return strings.TrimSuffix(fieldName, "_"+carrow.CborType), nil
	default:
		return "", werror.WrapWithContext(ErrInvalidAttrName, map[string]interface{}{"fieldName": fieldName, "mType": mType})
	}
}

func CreateDynAttrsStoreFrom(record arrow.Record) (*DynAttrsStore, error) {
	schema := record.Schema()
	handler := &DynAttrsStore{
		feeders:    make([]AttributeFeeder, 0),
		attributes: make(map[uint32]pcommon.Map),
	}
	for fieldID, field := range schema.Fields() {
		if field.Name == constants.ParentID {
			handler.parentID = &ParentIDGetter{
				fieldID: fieldID,
			}
		} else {
			mtype, found := field.Metadata.GetValue(carrow.MetadataType)
			if !found {
				return nil, werror.WrapWithContext(ErrMissingTypeMetadata, map[string]interface{}{"fieldName": field.Name})
			}
			feederCreator, found := feederCreatorByType[mtype]
			if !found {
				panic(fmt.Sprintf("unsupported feeder type: %s", mtype))
			}
			name, err := attrName(field.Name, mtype)
			if err != nil {
				return nil, err
			}
			feeder := feederCreator(fieldID, name)
			handler.feeders = append(handler.feeders, feeder)
		}
	}

	if handler.parentID == nil {
		return nil, werror.Wrap(ErrParentIDMissing)
	}

	return handler, nil
}

func (g *ParentIDGetter) Get(record arrow.Record, row int) (parentID uint32, err error) {
	delta, err := arrowutils.U32FromRecord(record, g.fieldID, row)
	if err == nil {
		g.parentID = g.parentID + delta
		return g.parentID, nil
	}
	return
}

func (f *BoolAttributeFeeder) Update(record arrow.Record, row int, attrs pcommon.Map) (err error) {
	if record.Column(f.fieldID).IsNull(row) {
		return
	}
	v, err := arrowutils.BoolFromRecord(record, f.fieldID, row)
	if err == nil {
		attrs.PutBool(f.attrName, v)
	}
	return
}

func (f *IntAttributeFeeder) Update(record arrow.Record, row int, attrs pcommon.Map) (err error) {
	if record.Column(f.fieldID).IsNull(row) {
		return
	}
	v, err := arrowutils.I64FromRecord(record, f.fieldID, row)
	if err == nil {
		attrs.PutInt(f.attrName, v)
	}
	return err
}

func (f *DoubleAttributeFeeder) Update(record arrow.Record, row int, attrs pcommon.Map) (err error) {
	if record.Column(f.fieldID).IsNull(row) {
		return
	}
	v, err := arrowutils.F64FromRecord(record, f.fieldID, row)
	if err == nil {
		attrs.PutDouble(f.attrName, v)
	}
	return err
}

func (f *StringAttributeFeeder) Update(record arrow.Record, row int, attrs pcommon.Map) (err error) {
	if record.Column(f.fieldID).IsNull(row) {
		return
	}
	v, err := arrowutils.StringFromRecord(record, f.fieldID, row)
	if err == nil {
		attrs.PutStr(f.attrName, v)
	}
	return err
}

func (f *BinaryAttributeFeeder) Update(record arrow.Record, row int, attrs pcommon.Map) (err error) {
	if record.Column(f.fieldID).IsNull(row) {
		return
	}
	v, err := arrowutils.BinaryFromRecord(record, f.fieldID, row)
	if err == nil {
		attrs.PutEmptyBytes(f.attrName).Append(v...)
	}
	return err
}

func (f *CborAttributeFeeder) Update(record arrow.Record, row int, attrs pcommon.Map) (err error) {
	if record.Column(f.fieldID).IsNull(row) {
		return
	}
	panic("implement me -> CborAttributeFeeder.Update")
}
