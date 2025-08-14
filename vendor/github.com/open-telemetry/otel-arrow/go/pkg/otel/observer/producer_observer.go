package observer

import (
	"github.com/apache/arrow-go/v18/arrow"

	"github.com/open-telemetry/otel-arrow/go/pkg/record_message"
)

// ProducerObserver is an interface for observing the OTel Arrow producer.
type ProducerObserver interface {
	// OnNewField is called when a new field is added to the schema.
	OnNewField(recordName string, fieldPath string)

	// OnDictionaryUpgrade is called when a dictionary index is upgraded.
	OnDictionaryUpgrade(recordName string, fieldPath string, prevIndexType, newIndexType arrow.DataType, card, total uint64)

	// OnDictionaryOverflow is called when a dictionary index overflows, i.e.
	// the cardinality of the dictionary exceeds the maximum cardinality of the
	// index type.
	// The column type is no longer a dictionary and is downgraded to its value
	// type.
	OnDictionaryOverflow(recordName string, fieldPath string, card, total uint64)

	// OnSchemaUpdate is called when the schema is updated.
	OnSchemaUpdate(recordName string, old, new *arrow.Schema)

	// OnDictionaryReset is called when a dictionary is reset instead of being
	// overflowed. This happens when dictionary entries are reused in average
	// more than a specific threshold.
	OnDictionaryReset(recordName string, fieldPath string, indexType arrow.DataType, card, total uint64)

	// OnMetadataUpdate is called when schema metadata are updated.
	OnMetadataUpdate(recordName, metadataKey string)

	// OnRecord is called when a record is produced.
	OnRecord(arrow.Record, record_message.PayloadType)
}
