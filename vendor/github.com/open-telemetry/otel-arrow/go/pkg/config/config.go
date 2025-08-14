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

package config

// Main configuration object in the package.

import (
	"math"

	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/open-telemetry/otel-arrow/go/pkg/otel/observer"
)

type OrderSpanBy int8

// Enumeration defining how to order spans in a batch.
const (
	OrderSpanByNothing OrderSpanBy = iota
	OrderSpanByNameTraceID
	OrderSpanByTraceIDName
	OrderSpanByNameStartTime
	OrderSpanByNameTraceIdStartTime
	OrderSpanByStartTimeTraceIDName
	OrderSpanByStartTimeNameTraceID
)

// OrderSpanByVariants is a map of string to OrderSpanBy.
// It is used to iterate over the possible values of OrderSpanBy.
// This map must be kept in sync with the OrderSpanBy enumeration.
var OrderSpanByVariants = map[string]OrderSpanBy{
	"":                         OrderSpanByNothing,
	"name,trace_id":            OrderSpanByNameTraceID,
	"trace_id,name":            OrderSpanByTraceIDName,
	"name,start_time":          OrderSpanByNameStartTime,
	"name,trace_id,start_time": OrderSpanByNameTraceIdStartTime,
	"start_time,trace_id,name": OrderSpanByStartTimeTraceIDName,
	"start_time,name,trace_id": OrderSpanByStartTimeNameTraceID,
}

type OrderAttrs32By int8

// Enumeration defining how to order attributes in a batch
// (with 32bits attribute ID).
const (
	OrderAttrs32ByNothing OrderAttrs32By = iota
	OrderAttrs32ByTypeParentIdKeyValue
	OrderAttrs32ByTypeKeyParentIdValue
	OrderAttrs32ByTypeKeyValueParentId
	OrderAttrs32ByKeyValueParentId
)

// OrderAttrs32ByVariants is a map of string to OrderAttrs32By.
// It is used to iterate over the possible values of OrderAttrs32By.
// This map must be kept in sync with the OrderAttrs32By enumeration.
var OrderAttrs32ByVariants = map[string]OrderAttrs32By{
	"":                         OrderAttrs32ByNothing,
	"type,parent_id,key,value": OrderAttrs32ByTypeParentIdKeyValue,
	"type,key,parent_id,value": OrderAttrs32ByTypeKeyParentIdValue,
	"type,key,value,parent_id": OrderAttrs32ByTypeKeyValueParentId,
	"key,value,parent_id":      OrderAttrs32ByKeyValueParentId,
}

type OrderAttrs16By int8

// Enumeration defining how to order attributes in a batch
// (with 16bits attribute ID).
const (
	OrderAttrs16ByNothing OrderAttrs16By = iota
	OrderAttrs16ByParentIdKeyValue
	OrderAttrs16ByTypeKeyParentIdValue
	OrderAttrs16ByTypeKeyValueParentId
)

// OrderAttrs16ByVariants is a map of string to OrderAttrs16By.
// It is used to iterate over the possible values of OrderAttrs16By.
// This map must be kept in sync with the OrderAttrs16By enumeration.
var OrderAttrs16ByVariants = map[string]OrderAttrs16By{
	"":                         OrderAttrs16ByNothing,
	"parent_id,key,value":      OrderAttrs16ByParentIdKeyValue,
	"type,key,parent_id,value": OrderAttrs16ByTypeKeyParentIdValue,
	"type,key,value,parent_id": OrderAttrs16ByTypeKeyValueParentId,
}

type Config struct {
	Pool memory.Allocator

	// InitIndexSize sets the initial size of a dictionary index.
	InitIndexSize uint64
	// LimitIndexSize sets the maximum size of a dictionary index
	// before it is no longer encoded as a dictionary.
	LimitIndexSize uint64
	// DictResetThreshold specifies the ratio under which a dictionary overflow
	// is converted to a dictionary reset. This ratio is calculated as:
	//   (# of unique values in the dict) / (# of values inserted in the dict.)
	// This ratio characterizes the efficiency of the dictionary. Smaller is the
	// ratio, more efficient is the dictionary in term compression ratio because
	// it means that the dictionary entries are reused more often.
	DictResetThreshold float64

	// Zstd enables the use of ZSTD compression for IPC messages.
	Zstd bool // Use IPC ZSTD compression

	// SchemaStats enables the collection of statistics about Arrow schemas.
	SchemaStats bool
	// RecordStats enables the collection of statistics about Arrow records.
	RecordStats bool
	// Display schema updates
	SchemaUpdates bool
	// Display producer statistics
	ProducerStats bool
	// Display compression ratio statistics
	CompressionRatioStats bool
	// DumpRecordRows specifies the number of rows to dump for each record.
	// If not defined or set to 0, no rows are dumped.
	DumpRecordRows map[string]int

	// OrderSpanBy specifies how to order spans in a batch.
	OrderSpanBy OrderSpanBy
	// OrderAttrs16By specifies how to order attributes in a batch
	// (with 16bits attribute ID).
	OrderAttrs16By OrderAttrs16By
	// OrderAttrs32By specifies how to order attributes in a batch
	// (with 32bits attribute ID).
	OrderAttrs32By OrderAttrs32By

	// Observer is the optional observer to use for the producer.
	Observer observer.ProducerObserver
}

type Option func(*Config)

// DefaultConfig returns a Config with the following default values:
//   - Pool: memory.NewGoAllocator()
//   - InitIndexSize: math.MaxUint16
//   - LimitIndexSize: math.MaxUint32
//   - SchemaStats: false
//   - Zstd: true
func DefaultConfig() *Config {
	return &Config{
		Pool: memory.NewGoAllocator(),

		InitIndexSize: math.MaxUint16,
		// The default dictionary index limit is set to 2^16 - 1
		// to keep the overall memory usage of the encoder and decoder low.
		LimitIndexSize: math.MaxUint16,
		// The default dictionary reset threshold is set to 0.3 based on
		// empirical observations. I suggest to run more controlled experiments
		// to find a more optimal value for the majority of workloads.
		DictResetThreshold: 0.3,

		SchemaStats: false,
		Zstd:        true,

		OrderSpanBy:    OrderSpanByNameTraceID,
		OrderAttrs16By: OrderAttrs16ByTypeKeyValueParentId,
		OrderAttrs32By: OrderAttrs32ByTypeKeyValueParentId,
	}
}

// WithAllocator sets the allocator to use for the Producer.
func WithAllocator(allocator memory.Allocator) Option {
	return func(cfg *Config) {
		cfg.Pool = allocator
	}
}

// WithNoDictionary sets the Producer to not use dictionary encoding.
func WithNoDictionary() Option {
	return func(cfg *Config) {
		cfg.InitIndexSize = 0
		cfg.LimitIndexSize = 0
	}
}

// WithUint8InitDictIndex sets the Producer to use an uint8 index for all dictionaries.
func WithUint8InitDictIndex() Option {
	return func(cfg *Config) {
		cfg.InitIndexSize = math.MaxUint8
	}
}

// WithUint16InitDictIndex sets the Producer to use an uint16 index for all dictionaries.
func WithUint16InitDictIndex() Option {
	return func(cfg *Config) {
		cfg.InitIndexSize = math.MaxUint16
	}
}

// WithUint32LinitDictIndex sets the Producer to use an uint32 index for all dictionaries.
func WithUint32LinitDictIndex() Option {
	return func(cfg *Config) {
		cfg.InitIndexSize = math.MaxUint32
	}
}

// WithUint64InitDictIndex sets the Producer to use an uint64 index for all dictionaries.
func WithUint64InitDictIndex() Option {
	return func(cfg *Config) {
		cfg.InitIndexSize = math.MaxUint64
	}
}

// WithUint8LimitDictIndex sets the Producer to fall back to non dictionary encoding if the dictionary size exceeds an uint8 index.
func WithUint8LimitDictIndex() Option {
	return func(cfg *Config) {
		cfg.LimitIndexSize = math.MaxUint8
	}
}

// WithUint16LimitDictIndex sets the Producer to fall back to non dictionary encoding if the dictionary size exceeds an uint16 index.
func WithUint16LimitDictIndex() Option {
	return func(cfg *Config) {
		cfg.LimitIndexSize = math.MaxUint16
	}
}

// WithUint32LimitDictIndex sets the Producer to fall back to non dictionary encoding if the dictionary size exceeds an uint32 index.
func WithUint32LimitDictIndex() Option {
	return func(cfg *Config) {
		cfg.LimitIndexSize = math.MaxUint32
	}
}

// WithUint64LimitDictIndex sets the Producer to fall back to non dictionary encoding if the dictionary size exceeds an uint64 index.
func WithUint64LimitDictIndex() Option {
	return func(cfg *Config) {
		cfg.LimitIndexSize = math.MaxUint64
	}
}

// WithZstd sets the Producer to use Zstd compression at the Arrow IPC level.
func WithZstd() Option {
	return func(cfg *Config) {
		cfg.Zstd = true
	}
}

// WithNoZstd sets the Producer to not use Zstd compression at the Arrow IPC level.
func WithNoZstd() Option {
	return func(cfg *Config) {
		cfg.Zstd = false
	}
}

// WithSchemaStats enables the collection of statistics about Arrow schemas.
func WithSchemaStats() Option {
	return func(cfg *Config) {
		cfg.SchemaStats = true
	}
}

// WithSchemaUpdates enables the display of schema updates.
func WithSchemaUpdates() Option {
	return func(cfg *Config) {
		cfg.SchemaUpdates = true
	}
}

// WithRecordStats enables the collection of statistics about Arrow records.
func WithRecordStats() Option {
	return func(cfg *Config) {
		cfg.RecordStats = true
	}
}

// WithProducerStats enables the display of producer statistics.
func WithProducerStats() Option {
	return func(cfg *Config) {
		cfg.ProducerStats = true
	}
}

// WithCompressionRatioStats enables the display of compression ratio statistics.
func WithCompressionRatioStats() Option {
	return func(cfg *Config) {
		cfg.CompressionRatioStats = true
	}
}

// WithDumpRecordRows specifies the number of rows to dump for a specific
// payload type.
func WithDumpRecordRows(payloadType string, numRows int) Option {
	return func(cfg *Config) {
		if cfg.DumpRecordRows == nil {
			cfg.DumpRecordRows = make(map[string]int)
		}
		cfg.DumpRecordRows[payloadType] = numRows
	}
}

// WithOrderSpanBy specifies how to order spans in a batch.
func WithOrderSpanBy(orderSpanBy OrderSpanBy) Option {
	return func(cfg *Config) {
		cfg.OrderSpanBy = orderSpanBy
	}
}

// WithOrderAttrs32By specifies how to order attributes in a batch
// (with 32bits attribute ID).
func WithOrderAttrs32By(orderAttrs32By OrderAttrs32By) Option {
	return func(cfg *Config) {
		cfg.OrderAttrs32By = orderAttrs32By
	}
}

// WithOrderAttrs16By specifies how to order attributes in a batch
// (with 16bits attribute ID).
func WithOrderAttrs16By(orderAttrs16By OrderAttrs16By) Option {
	return func(cfg *Config) {
		cfg.OrderAttrs16By = orderAttrs16By
	}
}

// WithObserver sets the optional observer to use for the producer.
func WithObserver(observer observer.ProducerObserver) Option {
	return func(cfg *Config) {
		cfg.Observer = observer
	}
}

// WithDictResetThreshold sets the ratio under which a dictionary overflow
// is converted to a dictionary reset. This ratio is calculated as:
//
//	(# of unique values in the dict) / (# of values inserted in the dict.)
func WithDictResetThreshold(dictResetThreshold float64) Option {
	return func(cfg *Config) {
		cfg.DictResetThreshold = dictResetThreshold
	}
}
