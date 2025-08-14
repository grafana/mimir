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
	"go.opentelemetry.io/collector/pdata/plog"

	arrowutils "github.com/open-telemetry/otel-arrow/go/pkg/arrow"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/otlp"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/constants"
	"github.com/open-telemetry/otel-arrow/go/pkg/werror"
)

const None = -1

type LogRecordIDs struct {
	ID                   int // Numerical ID of the current span
	Resource             *otlp.ResourceIds
	Scope                *otlp.ScopeIds
	SchemaUrl            int
	TimeUnixNano         int
	ObservedTimeUnixNano int
	TraceID              int
	SpanID               int
	SeverityNumber       int
	SeverityText         int

	Body       int
	BodyType   int
	BodyStr    int
	BodyInt    int
	BodyDouble int
	BodyBool   int
	BodyBytes  int
	BodySer    int

	DropAttributesCount int
	Flags               int
}

// LogsFrom creates a [plog.Logs] from the given Arrow Record.
//
// Important Note: This function doesn't take ownership of the record, so the
// record must be released by the caller.
func LogsFrom(record arrow.Record, relatedData *RelatedData) (plog.Logs, error) {
	logs := plog.NewLogs()

	if relatedData == nil {
		return logs, werror.Wrap(otlp.ErrMissingRelatedData)
	}

	logRecordIDs, err := SchemaToIDs(record.Schema())
	if err != nil {
		return logs, werror.Wrap(err)
	}

	var resLogs plog.ResourceLogs
	var scopeLogsSlice plog.ScopeLogsSlice
	var logRecordSlice plog.LogRecordSlice

	resLogsSlice := logs.ResourceLogs()
	rows := int(record.NumRows())

	prevResID := None
	prevScopeID := None

	var resID uint16
	var scopeID uint16

	for row := 0; row < rows; row++ {
		// Process resource logs, resource, schema url (resource)
		resDeltaID, err := otlp.ResourceIDFromRecord(record, row, logRecordIDs.Resource)
		resID += resDeltaID
		if err != nil {
			return logs, werror.Wrap(err)
		}
		if prevResID != int(resID) {
			prevResID = int(resID)
			resLogs = resLogsSlice.AppendEmpty()
			scopeLogsSlice = resLogs.ScopeLogs()
			prevScopeID = None
			schemaUrl, err := otlp.UpdateResourceFromRecord(resLogs.Resource(), record, row, logRecordIDs.Resource, relatedData.ResAttrMapStore)
			if err != nil {
				return logs, werror.Wrap(err)
			}
			resLogs.SetSchemaUrl(schemaUrl)
		}

		// Process scope logs, scope, schema url (scope)
		scopeDeltaID, err := otlp.ScopeIDFromRecord(record, row, logRecordIDs.Scope)
		scopeID += scopeDeltaID
		if err != nil {
			return logs, werror.Wrap(err)
		}
		if prevScopeID != int(scopeID) {
			prevScopeID = int(scopeID)
			scopeLogs := scopeLogsSlice.AppendEmpty()
			logRecordSlice = scopeLogs.LogRecords()
			if err = otlp.UpdateScopeFromRecord(scopeLogs.Scope(), record, row, logRecordIDs.Scope, relatedData.ScopeAttrMapStore); err != nil {
				return logs, werror.Wrap(err)
			}

			schemaUrl, err := arrowutils.StringFromRecord(record, logRecordIDs.SchemaUrl, row)
			if err != nil {
				return logs, werror.Wrap(err)
			}
			scopeLogs.SetSchemaUrl(schemaUrl)
		}

		// Process log record fields
		logRecord := logRecordSlice.AppendEmpty()
		deltaID, err := arrowutils.NullableU16FromRecord(record, logRecordIDs.ID, row)
		if err != nil {
			return logs, werror.Wrap(err)
		}

		timeUnixNano, err := arrowutils.TimestampFromRecord(record, logRecordIDs.TimeUnixNano, row)
		if err != nil {
			return logs, werror.WrapWithContext(err, map[string]interface{}{"row": row})
		}
		observedTimeUnixNano, err := arrowutils.TimestampFromRecord(record, logRecordIDs.ObservedTimeUnixNano, row)
		if err != nil {
			return logs, werror.WrapWithContext(err, map[string]interface{}{"row": row})
		}

		traceID, err := arrowutils.FixedSizeBinaryFromRecord(record, logRecordIDs.TraceID, row)
		if err != nil {
			return logs, werror.WrapWithContext(err, map[string]interface{}{"row": row})
		}
		if len(traceID) != 16 {
			return logs, werror.WrapWithContext(common.ErrInvalidTraceIDLength, map[string]interface{}{"row": row, "traceID": traceID})
		}
		spanID, err := arrowutils.FixedSizeBinaryFromRecord(record, logRecordIDs.SpanID, row)
		if err != nil {
			return logs, werror.WrapWithContext(err, map[string]interface{}{"row": row})
		}
		if len(spanID) != 8 {
			return logs, werror.WrapWithContext(common.ErrInvalidSpanIDLength, map[string]interface{}{"row": row, "spanID": spanID})
		}

		severityNumber, err := arrowutils.I32FromRecord(record, logRecordIDs.SeverityNumber, row)
		if err != nil {
			return logs, werror.WrapWithContext(err, map[string]interface{}{"row": row})
		}
		severityText, err := arrowutils.StringFromRecord(record, logRecordIDs.SeverityText, row)
		if err != nil {
			return logs, werror.WrapWithContext(err, map[string]interface{}{"row": row})
		}

		// Read the body value based on the body type
		bodyStruct, err := arrowutils.StructFromRecord(record, logRecordIDs.Body, row)
		if err != nil {
			return logs, werror.WrapWithContext(err, map[string]interface{}{"row": row})
		}

		if bodyStruct != nil {
			// If there is a body struct, read the body type and value
			bodyType, err := arrowutils.U8FromStruct(bodyStruct, row, logRecordIDs.BodyType)
			if err != nil {
				return logs, werror.Wrap(err)
			}
			body := logRecord.Body()
			switch pcommon.ValueType(bodyType) {
			case pcommon.ValueTypeStr:
				v, err := arrowutils.StringFromStruct(bodyStruct, row, logRecordIDs.BodyStr)
				if err != nil {
					return logs, werror.Wrap(err)
				}
				body.SetStr(v)
			case pcommon.ValueTypeInt:
				v, err := arrowutils.I64FromStruct(bodyStruct, row, logRecordIDs.BodyInt)
				if err != nil {
					return logs, werror.Wrap(err)
				}
				body.SetInt(v)
			case pcommon.ValueTypeDouble:
				v, err := arrowutils.F64FromStruct(bodyStruct, row, logRecordIDs.BodyDouble)
				if err != nil {
					return logs, werror.Wrap(err)
				}
				body.SetDouble(v)
			case pcommon.ValueTypeBool:
				v, err := arrowutils.BoolFromStruct(bodyStruct, row, logRecordIDs.BodyBool)
				if err != nil {
					return logs, werror.Wrap(err)
				}
				body.SetBool(v)
			case pcommon.ValueTypeBytes:
				v, err := arrowutils.BinaryFromStruct(bodyStruct, row, logRecordIDs.BodyBytes)
				if err != nil {
					return logs, werror.Wrap(err)
				}
				body.SetEmptyBytes().FromRaw(v)
			case pcommon.ValueTypeSlice:
				v, err := arrowutils.BinaryFromStruct(bodyStruct, row, logRecordIDs.BodySer)
				if err != nil {
					return logs, werror.Wrap(err)
				}
				if err = common.Deserialize(v, body); err != nil {
					return logs, werror.Wrap(err)
				}
			case pcommon.ValueTypeMap:
				v, err := arrowutils.BinaryFromStruct(bodyStruct, row, logRecordIDs.BodySer)
				if err != nil {
					return logs, werror.Wrap(err)
				}
				if err = common.Deserialize(v, body); err != nil {
					return logs, werror.Wrap(err)
				}
			default:
				// silently ignore unknown types to avoid DOS attacks
			}
		}

		logRecordAttrs := logRecord.Attributes()

		if deltaID != nil {
			ID := relatedData.LogRecordIDFromDelta(*deltaID)
			attrs := relatedData.LogRecordAttrMapStore.AttributesByID(ID)
			if attrs != nil {
				attrs.CopyTo(logRecordAttrs)
			}
		}

		droppedAttributesCount, err := arrowutils.U32FromRecord(record, logRecordIDs.DropAttributesCount, row)
		if err != nil {
			return logs, werror.WrapWithContext(err, map[string]interface{}{"row": row})
		}

		flags, err := arrowutils.U32FromRecord(record, logRecordIDs.Flags, row)
		if err != nil {
			return logs, werror.WrapWithContext(err, map[string]interface{}{"row": row})
		}

		var tid pcommon.TraceID
		var sid pcommon.SpanID
		copy(tid[:], traceID)
		copy(sid[:], spanID)

		logRecord.SetTimestamp(pcommon.Timestamp(timeUnixNano))
		logRecord.SetObservedTimestamp(pcommon.Timestamp(observedTimeUnixNano))
		logRecord.SetTraceID(tid)
		logRecord.SetSpanID(sid)
		logRecord.SetSeverityNumber(plog.SeverityNumber(severityNumber))
		logRecord.SetSeverityText(severityText)
		logRecord.SetDroppedAttributesCount(droppedAttributesCount)
		logRecord.SetFlags(plog.LogRecordFlags(flags))
	}

	return logs, nil
}

func SchemaToIDs(schema *arrow.Schema) (*LogRecordIDs, error) {
	ID, _ := arrowutils.FieldIDFromSchema(schema, constants.ID)
	resourceIDs, err := otlp.NewResourceIdsFromSchema(schema)
	if err != nil {
		return nil, werror.Wrap(err)
	}
	scopeIDs, err := otlp.NewScopeIdsFromSchema(schema)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	schemaUrlID, _ := arrowutils.FieldIDFromSchema(schema, constants.SchemaUrl)
	timeUnixNano, _ := arrowutils.FieldIDFromSchema(schema, constants.TimeUnixNano)
	observedTimeUnixNano, _ := arrowutils.FieldIDFromSchema(schema, constants.ObservedTimeUnixNano)
	traceID, _ := arrowutils.FieldIDFromSchema(schema, constants.TraceId)
	spanID, _ := arrowutils.FieldIDFromSchema(schema, constants.SpanId)
	severityNumber, _ := arrowutils.FieldIDFromSchema(schema, constants.SeverityNumber)
	severityText, _ := arrowutils.FieldIDFromSchema(schema, constants.SeverityText)

	body, bodyDT, err := arrowutils.StructFieldIDFromSchema(schema, constants.Body)
	if err != nil {
		return nil, werror.Wrap(err)
	}
	bType, _ := arrowutils.FieldIDFromStruct(bodyDT, constants.BodyType)
	if err != nil {
		return nil, werror.Wrap(err)
	}
	bStr, _ := arrowutils.FieldIDFromStruct(bodyDT, constants.BodyStr)
	if err != nil {
		return nil, werror.Wrap(err)
	}
	bInt, _ := arrowutils.FieldIDFromStruct(bodyDT, constants.BodyInt)
	if err != nil {
		return nil, werror.Wrap(err)
	}
	bDouble, _ := arrowutils.FieldIDFromStruct(bodyDT, constants.BodyDouble)
	if err != nil {
		return nil, werror.Wrap(err)
	}
	bBool, _ := arrowutils.FieldIDFromStruct(bodyDT, constants.BodyBool)
	if err != nil {
		return nil, werror.Wrap(err)
	}
	bBytes, _ := arrowutils.FieldIDFromStruct(bodyDT, constants.BodyBytes)
	if err != nil {
		return nil, werror.Wrap(err)
	}
	bSer, _ := arrowutils.FieldIDFromStruct(bodyDT, constants.BodySer)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	droppedAttributesCount, _ := arrowutils.FieldIDFromSchema(schema, constants.DroppedAttributesCount)
	flags, _ := arrowutils.FieldIDFromSchema(schema, constants.Flags)

	return &LogRecordIDs{
		ID:                   ID,
		Resource:             resourceIDs,
		Scope:                scopeIDs,
		SchemaUrl:            schemaUrlID,
		TimeUnixNano:         timeUnixNano,
		ObservedTimeUnixNano: observedTimeUnixNano,
		TraceID:              traceID,
		SpanID:               spanID,
		SeverityNumber:       severityNumber,
		SeverityText:         severityText,

		Body:       body,
		BodyType:   bType,
		BodyStr:    bStr,
		BodyInt:    bInt,
		BodyDouble: bDouble,
		BodyBool:   bBool,
		BodyBytes:  bBytes,
		BodySer:    bSer,

		DropAttributesCount: droppedAttributesCount,
		Flags:               flags,
	}, nil
}
