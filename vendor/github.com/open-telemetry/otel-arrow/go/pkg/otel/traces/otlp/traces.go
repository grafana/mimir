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
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	arrowutils "github.com/open-telemetry/otel-arrow/go/pkg/arrow"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/otlp"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/constants"
	"github.com/open-telemetry/otel-arrow/go/pkg/werror"
)

const None = -1

type (
	// SpanIDs contains the field IDs for the span schema.
	SpanIDs struct {
		ID                   int // Numerical ID of the current span
		Resource             *otlp.ResourceIds
		Scope                *otlp.ScopeIds
		SchemaUrl            int
		StartTimeUnixNano    int
		DurationTimeUnixNano int
		TraceID              int
		SpanID               int
		TraceState           int
		ParentSpanID         int
		Name                 int
		Kind                 int
		DropAttributesCount  int
		DropEventsCount      int
		DropLinksCount       int
		Status               *StatusIDs
	}

	// StatusIDs contains the field IDs for the status Arrow struct.
	StatusIDs struct {
		Status  int
		Code    int
		Message int
	}
)

// TracesFrom creates a [ptrace.Traces] from the given Arrow Record.
//
// Important Note: This function doesn't take ownership of the record, so the
// record must be released by the caller.
func TracesFrom(record arrow.Record, relatedData *RelatedData) (ptrace.Traces, error) {
	traces := ptrace.NewTraces()

	if relatedData == nil {
		return traces, werror.Wrap(otlp.ErrMissingRelatedData)
	}

	traceIDs, err := SchemaToIds(record.Schema())
	if err != nil {
		return traces, err
	}

	var resSpans ptrace.ResourceSpans
	var scopeSpansSlice ptrace.ScopeSpansSlice
	var spanSlice ptrace.SpanSlice

	resSpansSlice := traces.ResourceSpans()
	rows := int(record.NumRows())

	prevResID := None
	prevScopeID := None

	var resID uint16
	var scopeID uint16

	for row := 0; row < rows; row++ {
		// Process resource spans, resource, schema url (resource)
		resDeltaID, err := otlp.ResourceIDFromRecord(record, row, traceIDs.Resource)
		resID += resDeltaID
		if err != nil {
			return traces, werror.Wrap(err)
		}

		if prevResID != int(resID) {
			prevResID = int(resID)
			resSpans = resSpansSlice.AppendEmpty()
			scopeSpansSlice = resSpans.ScopeSpans()
			prevScopeID = None

			schemaUrl, err := otlp.UpdateResourceFromRecord(resSpans.Resource(), record, row, traceIDs.Resource, relatedData.ResAttrMapStore)
			if err != nil {
				return traces, werror.Wrap(err)
			}
			resSpans.SetSchemaUrl(schemaUrl)
		}

		// Process scope spans, scope, schema url (scope)
		scopeDeltaID, err := otlp.ScopeIDFromRecord(record, row, traceIDs.Scope)
		scopeID += scopeDeltaID
		if err != nil {
			return traces, werror.Wrap(err)
		}
		if prevScopeID != int(scopeID) {
			prevScopeID = int(scopeID)
			scopeSpans := scopeSpansSlice.AppendEmpty()
			spanSlice = scopeSpans.Spans()
			if err = otlp.UpdateScopeFromRecord(scopeSpans.Scope(), record, row, traceIDs.Scope, relatedData.ScopeAttrMapStore); err != nil {
				return traces, werror.Wrap(err)
			}

			schemaUrl, err := arrowutils.StringFromRecord(record, traceIDs.SchemaUrl, row)
			if err != nil {
				return traces, werror.Wrap(err)
			}
			scopeSpans.SetSchemaUrl(schemaUrl)
		}

		// Process span fields
		span := spanSlice.AppendEmpty()
		deltaID, err := arrowutils.NullableU16FromRecord(record, traceIDs.ID, row)
		if err != nil {
			return traces, werror.Wrap(err)
		}

		traceID, err := arrowutils.FixedSizeBinaryFromRecord(record, traceIDs.TraceID, row)
		if err != nil {
			return traces, werror.Wrap(err)
		}
		if len(traceID) != 16 {
			return traces, werror.WrapWithContext(common.ErrInvalidTraceIDLength, map[string]interface{}{"traceID": traceID})
		}
		spanID, err := arrowutils.FixedSizeBinaryFromRecord(record, traceIDs.SpanID, row)
		if err != nil {
			return traces, werror.Wrap(err)
		}
		if len(spanID) != 8 {
			return traces, werror.WrapWithContext(common.ErrInvalidSpanIDLength, map[string]interface{}{"spanID": spanID})
		}
		traceState, err := arrowutils.StringFromRecord(record, traceIDs.TraceState, row)
		if err != nil {
			return traces, werror.Wrap(err)
		}
		parentSpanID, err := arrowutils.FixedSizeBinaryFromRecord(record, traceIDs.ParentSpanID, row)
		if err != nil {
			return traces, werror.Wrap(err)
		}
		if parentSpanID != nil && len(parentSpanID) != 8 {
			return traces, werror.WrapWithContext(common.ErrInvalidSpanIDLength, map[string]interface{}{"parentSpanID": parentSpanID})
		}
		if parentSpanID == nil {
			// parentSpanID can be null
			parentSpanID = []byte{}
		}
		name, err := arrowutils.StringFromRecord(record, traceIDs.Name, row)
		if err != nil {
			return traces, werror.Wrap(err)
		}
		kind, err := arrowutils.I32FromRecord(record, traceIDs.Kind, row)
		if err != nil {
			return traces, werror.Wrap(err)
		}
		startTimeUnixNano, err := arrowutils.TimestampFromRecord(record, traceIDs.StartTimeUnixNano, row)
		if err != nil {
			return traces, werror.Wrap(err)
		}
		durationNano, err := arrowutils.DurationFromRecord(record, traceIDs.DurationTimeUnixNano, row)
		if err != nil {
			return traces, werror.Wrap(err)
		}
		endTimeUnixNano := startTimeUnixNano.ToTime(arrow.Nanosecond).Add(time.Duration(durationNano))
		droppedAttributesCount, err := arrowutils.U32FromRecord(record, traceIDs.DropAttributesCount, row)
		if err != nil {
			return traces, werror.Wrap(err)
		}
		droppedEventsCount, err := arrowutils.U32FromRecord(record, traceIDs.DropEventsCount, row)
		if err != nil {
			return traces, werror.Wrap(err)
		}
		droppedLinksCount, err := arrowutils.U32FromRecord(record, traceIDs.DropLinksCount, row)
		if err != nil {
			return traces, werror.Wrap(err)
		}
		statusArr, err := arrowutils.StructFromRecord(record, traceIDs.Status.Status, row)
		if err != nil {
			return traces, werror.Wrap(err)
		}
		if statusArr != nil {
			// Status exists
			message, err := arrowutils.StringFromStruct(statusArr, row, traceIDs.Status.Message)
			if err != nil {
				return traces, werror.Wrap(err)
			}
			span.Status().SetMessage(message)

			code, err := arrowutils.I32FromStruct(statusArr, row, traceIDs.Status.Code)
			if err != nil {
				return traces, werror.Wrap(err)
			}
			span.Status().SetCode(ptrace.StatusCode(code))
		}

		if deltaID != nil {
			ID := relatedData.SpanIDFromDelta(*deltaID)

			spanAttrs := span.Attributes()
			attrs := relatedData.SpanAttrMapStore.AttributesByID(ID)
			if attrs != nil {
				attrs.CopyTo(spanAttrs)
			}

			events := relatedData.SpanEventsStore.EventsByID(ID)
			eventSlice := span.Events()
			for _, event := range events {
				event.MoveTo(eventSlice.AppendEmpty())
			}

			links := relatedData.SpanLinksStore.LinksByID(ID)
			linkSlice := span.Links()
			for _, link := range links {
				link.MoveTo(linkSlice.AppendEmpty())
			}
		}

		var tid pcommon.TraceID
		var sid pcommon.SpanID
		var psid pcommon.SpanID

		copy(tid[:], traceID)
		copy(sid[:], spanID)
		copy(psid[:], parentSpanID)

		span.SetTraceID(tid)
		span.SetSpanID(sid)
		span.TraceState().FromRaw(traceState)
		span.SetParentSpanID(psid)
		span.SetName(name)
		span.SetKind(ptrace.SpanKind(kind))
		span.SetStartTimestamp(pcommon.Timestamp(startTimeUnixNano))
		span.SetEndTimestamp(pcommon.Timestamp(endTimeUnixNano.UnixNano()))
		span.SetDroppedAttributesCount(droppedAttributesCount)
		span.SetDroppedEventsCount(droppedEventsCount)
		span.SetDroppedLinksCount(droppedLinksCount)
	}
	return traces, err
}

func SchemaToIds(schema *arrow.Schema) (*SpanIDs, error) {
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
	startTimeUnixNano, _ := arrowutils.FieldIDFromSchema(schema, constants.StartTimeUnixNano)
	durationTimeUnixNano, _ := arrowutils.FieldIDFromSchema(schema, constants.DurationTimeUnixNano)
	traceId, _ := arrowutils.FieldIDFromSchema(schema, constants.TraceId)
	spanId, _ := arrowutils.FieldIDFromSchema(schema, constants.SpanId)
	traceState, _ := arrowutils.FieldIDFromSchema(schema, constants.TraceState)
	parentSpanId, _ := arrowutils.FieldIDFromSchema(schema, constants.ParentSpanId)
	name, _ := arrowutils.FieldIDFromSchema(schema, constants.Name)
	kind, _ := arrowutils.FieldIDFromSchema(schema, constants.KIND)
	droppedAttributesCount, _ := arrowutils.FieldIDFromSchema(schema, constants.DroppedAttributesCount)
	droppedEventsCount, _ := arrowutils.FieldIDFromSchema(schema, constants.DroppedEventsCount)
	droppedLinksCount, _ := arrowutils.FieldIDFromSchema(schema, constants.DroppedLinksCount)

	status, err := NewStatusIdsFromSchema(schema)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	return &SpanIDs{
		ID:                   ID,
		Resource:             resourceIDs,
		Scope:                scopeIDs,
		SchemaUrl:            schemaUrlID,
		StartTimeUnixNano:    startTimeUnixNano,
		DurationTimeUnixNano: durationTimeUnixNano,
		TraceID:              traceId,
		SpanID:               spanId,
		TraceState:           traceState,
		ParentSpanID:         parentSpanId,
		Name:                 name,
		Kind:                 kind,
		DropAttributesCount:  droppedAttributesCount,
		DropEventsCount:      droppedEventsCount,
		DropLinksCount:       droppedLinksCount,
		Status:               status,
	}, nil
}

func NewStatusIdsFromSchema(schema *arrow.Schema) (*StatusIDs, error) {
	statusId, statusDT, err := arrowutils.StructFieldIDFromSchema(schema, constants.Status)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	code, _ := arrowutils.FieldIDFromStruct(statusDT, constants.StatusCode)
	message, _ := arrowutils.FieldIDFromStruct(statusDT, constants.StatusMessage)

	return &StatusIDs{
		Status:  statusId,
		Code:    code,
		Message: message,
	}, nil
}
