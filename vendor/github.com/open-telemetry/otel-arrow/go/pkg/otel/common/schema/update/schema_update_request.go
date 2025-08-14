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

package update

import (
	"github.com/apache/arrow-go/v18/arrow"

	"github.com/open-telemetry/otel-arrow/go/pkg/otel/observer"
)

// SchemaUpdateRequest is a counter that keeps track of the number of schema
// update requests.
type SchemaUpdateRequest struct {
	events []FieldUpdateEvent
	count  int
}

// FieldUpdateEvent is an interface for all field update events.
type FieldUpdateEvent interface {
	// Notify notifies the observer of a specific field update event.
	Notify(recordName string, observer observer.ProducerObserver)
}

// NewFieldEvent is an event that is triggered when a new field is added to
// a schema.
type NewFieldEvent struct {
	FieldName string
}

// DictionaryUpgradeEvent is an event that is triggered when a dictionary
// is upgraded to a larger index type.
type DictionaryUpgradeEvent struct {
	FieldName     string
	PrevIndexType arrow.DataType
	NewIndexType  arrow.DataType
	Cardinality   uint64
	Total         uint64
}

// DictionaryResetEvent is an event that is triggered when a dictionary is reset
// instead of being overflowed. This happens when dictionary entries are reused
// in average more than a specific threshold.
type DictionaryResetEvent struct {
	FieldName   string
	IndexType   arrow.DataType
	Cardinality uint64
	Total       uint64
}

// DictionaryOverflowEvent is an event that is triggered when a dictionary
// overflows its index type.
type DictionaryOverflowEvent struct {
	FieldName     string
	PrevIndexType arrow.DataType
	NewIndexType  arrow.DataType
	Cardinality   uint64
	Total         uint64
}

// MetadataEvent is an event that is triggered when schema metadata are updated.
type MetadataEvent struct {
	MetadataKey string
}

// NewSchemaUpdateRequest creates a new SchemaUpdateRequest.
func NewSchemaUpdateRequest() *SchemaUpdateRequest {
	return &SchemaUpdateRequest{count: 0}
}

// Inc increments the counter of the schema update request by one.
func (r *SchemaUpdateRequest) Inc(event FieldUpdateEvent) {
	r.events = append(r.events, event)
	r.count++
}

// Count returns the current count of the schema update request.
func (r *SchemaUpdateRequest) Count() int {
	return r.count
}

// Reset resets the counter of the schema update request to zero.
func (r *SchemaUpdateRequest) Reset() {
	r.count = 0
	r.events = nil
}

// Notify notifies the observer passed as argument of all events that have
// occurred since the last call to Notify.
func (r *SchemaUpdateRequest) Notify(recordName string, observer observer.ProducerObserver) {
	for _, event := range r.events {
		event.Notify(recordName, observer)
	}
	r.events = nil
}

func (e *NewFieldEvent) Notify(recordName string, observer observer.ProducerObserver) {
	observer.OnNewField(recordName, e.FieldName)
}

func (e *DictionaryUpgradeEvent) Notify(recordName string, observer observer.ProducerObserver) {
	observer.OnDictionaryUpgrade(
		recordName,
		e.FieldName,
		e.PrevIndexType,
		e.NewIndexType,
		e.Cardinality,
		e.Total,
	)
}

func (e *DictionaryOverflowEvent) Notify(recordName string, observer observer.ProducerObserver) {
	observer.OnDictionaryOverflow(
		recordName,
		e.FieldName,
		e.Cardinality,
		e.Total,
	)
}

func (e *DictionaryResetEvent) Notify(recordName string, observer observer.ProducerObserver) {
	observer.OnDictionaryReset(
		recordName,
		e.FieldName,
		e.IndexType,
		e.Cardinality,
		e.Total,
	)
}

func (e *MetadataEvent) Notify(recordName string, observer observer.ProducerObserver) {
	observer.OnMetadataUpdate(
		recordName,
		e.MetadataKey,
	)
}
