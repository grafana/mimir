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

package record_message

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	v1 "github.com/open-telemetry/otel-arrow/go/api/experimental/arrow/v1"
)

type PayloadType = v1.ArrowPayloadType

// RecordMessage wraps an Arrow Record with a set of metadata used to identify the batch, schema ID, and few other
// properties.
type RecordMessage struct {
	batchId     int64
	schemaID    string
	payloadType PayloadType
	record      arrow.Record
}

// NewRecordMessage creates a record message.
func NewRecordMessage(batchId int64, payloadType PayloadType, record arrow.Record) *RecordMessage {
	return &RecordMessage{
		batchId:     batchId,
		payloadType: payloadType,
		record:      record,
	}
}

// NewMetricsMessage creates a reference to a new RecordMessage from a given Arrow Record representing a collection of
// metrics.
func NewMetricsMessage(schemaID string, record arrow.Record) *RecordMessage {
	return &RecordMessage{
		schemaID:    schemaID,
		payloadType: v1.ArrowPayloadType_UNIVARIATE_METRICS,
		record:      record,
	}
}

// NewLogsMessage creates a reference to a new RecordMessage from a given Arrow Record representing a collection of
// logs.
func NewLogsMessage(schemaID string, record arrow.Record) *RecordMessage {
	record.Schema()
	return &RecordMessage{
		schemaID:    schemaID,
		payloadType: v1.ArrowPayloadType_LOGS,
		record:      record,
	}
}

// NewTraceMessage creates a reference to a new RecordMessage from a given Arrow Record representing a collection of
// traces.
func NewTraceMessage(schemaID string, record arrow.Record) *RecordMessage {
	return &RecordMessage{
		schemaID:    schemaID,
		payloadType: v1.ArrowPayloadType_SPANS,
		record:      record,
	}
}

func NewRelatedDataMessage(schemaID string, record arrow.Record, payloadType PayloadType) *RecordMessage {
	return &RecordMessage{
		schemaID:    schemaID,
		payloadType: payloadType,
		record:      record,
	}
}

func (rm *RecordMessage) BatchId() int64 {
	return rm.batchId
}

func (rm *RecordMessage) SchemaID() string {
	return rm.schemaID
}

// Record returns the Arrow Record associated with this RecordMessage.
func (rm *RecordMessage) Record() arrow.Record {
	return rm.record
}

func (rm *RecordMessage) SetRecord(record arrow.Record) {
	rm.record = record
}

// PayloadType returns the type of payload contained in this RecordMessage.
func (rm *RecordMessage) PayloadType() PayloadType {
	return rm.payloadType
}

func (rm *RecordMessage) SetPayloadType(payloadType PayloadType) {
	rm.payloadType = payloadType
}

func (rm *RecordMessage) ShowStats() {
	schema := rm.record.Schema()
	columns := rm.record.Columns()

	for i, field := range schema.Fields() {
		ShowFieldStats("", &field, columns[i])
	}
}

func ShowFieldStats(indent string, field *arrow.Field, column arrow.Array) {
	if column.Len() == column.NullN() {
		fmt.Printf("# UNUSED %s%s:%s len=%d, nulls=%d\n", indent, field.Name, field.Type.Name(), column.Len(), column.NullN())
	} else {
		fmt.Printf("%s%s:%s len=%d, nulls=%d\n", indent, field.Name, field.Type.Name(), column.Len(), column.NullN())
	}

	switch dt := column.DataType().(type) {
	case *arrow.StructType:
		for i, child := range dt.Fields() {
			ShowFieldStats(indent+"  ", &child, column.(*array.Struct).Field(i))
		}
	case *arrow.ListType:
		elemField := dt.ElemField()
		ShowFieldStats("  ", &elemField, column.(*array.List).ListValues())
	case *arrow.DenseUnionType:
		for i, child := range dt.Fields() {
			ShowFieldStats(indent+"  ", &child, column.(*array.DenseUnion).Field(i))
		}
	case *arrow.SparseUnionType:
		for i, child := range dt.Fields() {
			ShowFieldStats(indent+"  ", &child, column.(*array.SparseUnion).Field(i))
		}
	case *arrow.MapType:
		keyField := dt.KeyField()
		valueField := dt.ValueField()
		ShowFieldStats(indent+"  ", &keyField, column.(*array.Map).Keys())
		ShowFieldStats(indent+"  ", &valueField, column.(*array.Map).Items())
	}
}
