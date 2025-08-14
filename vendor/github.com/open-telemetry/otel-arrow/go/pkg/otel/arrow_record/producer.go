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

package arrow_record

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	colarspb "github.com/open-telemetry/otel-arrow/go/api/experimental/arrow/v1"
	carrow "github.com/open-telemetry/otel-arrow/go/pkg/arrow"
	cfg "github.com/open-telemetry/otel-arrow/go/pkg/config"
	acommon "github.com/open-telemetry/otel-arrow/go/pkg/otel/common/arrow"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema/builder"
	config "github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema/config"
	logsarrow "github.com/open-telemetry/otel-arrow/go/pkg/otel/logs/arrow"
	metricsarrow "github.com/open-telemetry/otel-arrow/go/pkg/otel/metrics/arrow"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/observer"
	pstats "github.com/open-telemetry/otel-arrow/go/pkg/otel/stats"
	tracesarrow "github.com/open-telemetry/otel-arrow/go/pkg/otel/traces/arrow"
	"github.com/open-telemetry/otel-arrow/go/pkg/record_message"
	"github.com/open-telemetry/otel-arrow/go/pkg/werror"
)

// This file implements a generic producer API used to encode BatchArrowRecords messages from
// OTLP entities (i.e. pmetric.Metrics, plog.Logs, ptrace.Traces).
// The producer API is used by the OTLP Arrow exporter.

// ProducerAPI is the interface of a Producer considering all signals.
// This is useful for mock testing.
type ProducerAPI interface {
	BatchArrowRecordsFromTraces(ptrace.Traces) (*colarspb.BatchArrowRecords, error)
	BatchArrowRecordsFromLogs(plog.Logs) (*colarspb.BatchArrowRecords, error)
	BatchArrowRecordsFromMetrics(pmetric.Metrics) (*colarspb.BatchArrowRecords, error)
	Close() error
}

var _ ProducerAPI = &Producer{}

// Producer is a BatchArrowRecords producer.
type (
	Producer struct {
		pool            memory.Allocator // Use a custom memory allocator
		zstd            bool             // Use IPC ZSTD compression
		streamProducers map[string]*streamProducer
		nextSchemaId    int64
		batchId         int64

		// Builder for each OTEL entities
		metricsBuilder *metricsarrow.MetricsBuilder
		logsBuilder    *logsarrow.LogsBuilder
		tracesBuilder  *tracesarrow.TracesBuilder

		// Record builder for each OTEL entities
		metricsRecordBuilder *builder.RecordBuilderExt
		logsRecordBuilder    *builder.RecordBuilderExt
		tracesRecordBuilder  *builder.RecordBuilderExt

		// General stats for the producer
		stats *pstats.ProducerStats

		// Producer observer
		observer observer.ProducerObserver
	}

	consoleObserver struct {
		// Max number of rows to print per record
		maxRows int
		// Max number of prints per payload type
		maxPrints int

		counters map[record_message.PayloadType]int
	}

	streamProducer struct {
		output         bytes.Buffer
		ipcWriter      *ipc.Writer
		schemaID       string
		lastProduction time.Time
		schema         *arrow.Schema
		payloadType    record_message.PayloadType
	}
)

// NewProducer creates a new BatchArrowRecords producer.
//
// The method close MUST be called when the producer is not used anymore to release the memory and avoid memory leaks.
func NewProducer() *Producer {
	return NewProducerWithOptions( /* use default options */ )
}

// NewProducerWithOptions creates a new BatchArrowRecords producer with a set of options.
//
// The method close MUST be called when the producer is not used anymore to release the memory and avoid memory leaks.
func NewProducerWithOptions(options ...cfg.Option) *Producer {
	// Default configuration
	conf := cfg.DefaultConfig()
	for _, opt := range options {
		opt(conf)
	}

	// Configure the various level of statistics to collect and display.
	stats := pstats.NewProducerStats()
	stats.SchemaStats = conf.SchemaStats
	stats.SchemaUpdates = conf.SchemaUpdates
	stats.RecordStats = conf.RecordStats
	stats.DumpRecordRows = conf.DumpRecordRows
	stats.CompressionRatioStats = conf.CompressionRatioStats
	stats.ProducerStats = conf.ProducerStats

	// Record builders
	metricsRecordBuilder := builder.NewRecordBuilderExt(
		conf.Pool,
		metricsarrow.MetricsSchema,
		config.NewDictionary(conf.LimitIndexSize, conf.DictResetThreshold),
		stats,
		conf.Observer,
	)
	metricsRecordBuilder.SetLabel("metrics")

	logsRecordBuilder := builder.NewRecordBuilderExt(
		conf.Pool,
		logsarrow.LogsSchema,
		config.NewDictionary(conf.LimitIndexSize, conf.DictResetThreshold),
		stats,
		conf.Observer,
	)
	logsRecordBuilder.SetLabel("logs")

	tracesRecordBuilder := builder.NewRecordBuilderExt(
		conf.Pool,
		tracesarrow.TracesSchema,
		config.NewDictionary(conf.LimitIndexSize, conf.DictResetThreshold),
		stats,
		conf.Observer,
	)
	tracesRecordBuilder.SetLabel("traces")

	// Entity builders
	metricsBuilder, err := metricsarrow.NewMetricsBuilder(metricsRecordBuilder, metricsarrow.NewConfig(conf), stats, conf.Observer)
	if err != nil {
		panic(err)
	}

	logsBuilder, err := logsarrow.NewLogsBuilder(logsRecordBuilder, logsarrow.NewConfig(conf), stats, conf.Observer)
	if err != nil {
		panic(err)
	}

	traceCfg := tracesarrow.NewConfig(conf)
	traceCfg.Span.Sorter = tracesarrow.FindOrderByFunc(conf.OrderSpanBy)

	traceCfg.Attrs.Resource.Sorter = acommon.Attrs16FindOrderByFunc(conf.OrderAttrs16By)
	traceCfg.Attrs.Scope.Sorter = acommon.Attrs16FindOrderByFunc(conf.OrderAttrs16By)
	traceCfg.Attrs.Span.Sorter = acommon.Attrs16FindOrderByFunc(conf.OrderAttrs16By)

	traceCfg.Attrs.Event.Sorter = acommon.Attrs32FindOrderByFunc(conf.OrderAttrs32By)
	traceCfg.Attrs.Link.Sorter = acommon.Attrs32FindOrderByFunc(conf.OrderAttrs32By)

	tracesBuilder, err := tracesarrow.NewTracesBuilder(tracesRecordBuilder, traceCfg, stats, conf.Observer)
	if err != nil {
		panic(err)
	}

	return &Producer{
		pool:            conf.Pool,
		zstd:            conf.Zstd,
		streamProducers: make(map[string]*streamProducer),
		batchId:         0,

		metricsBuilder: metricsBuilder,
		logsBuilder:    logsBuilder,
		tracesBuilder:  tracesBuilder,

		metricsRecordBuilder: metricsRecordBuilder,
		logsRecordBuilder:    logsRecordBuilder,
		tracesRecordBuilder:  tracesRecordBuilder,

		stats:    stats,
		observer: conf.Observer,
	}
}

// SetObserver adds an observer to the producer.
func (p *Producer) SetObserver(observer observer.ProducerObserver) {
	p.observer = observer
}

// BatchArrowRecordsFromMetrics produces a BatchArrowRecords message from a [pmetric.Metrics] messages.
func (p *Producer) BatchArrowRecordsFromMetrics(metrics pmetric.Metrics) (*colarspb.BatchArrowRecords, error) {
	// Builds a main Record and n related Records from the metrics passed in
	// parameter. All these Arrow records are wrapped into a BatchArrowRecords
	// and will be released by the Producer.Produce method.
	record, err := recordBuilder[pmetric.Metrics](func() (acommon.EntityBuilder[pmetric.Metrics], error) {
		// Related entity builder must be reset before each use.
		// This is especially important after a schema update.
		p.metricsBuilder.RelatedData().Reset()
		return p.metricsBuilder, nil
	}, metrics, p.observer)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	// builds the related records (e.g. INT_SUM, INT_GAUGE, INT_GAUGE_ATTRS, ...)
	rms, err := p.metricsBuilder.RelatedData().BuildRecordMessages()
	if err != nil {
		return nil, werror.Wrap(err)
	}

	schemaID := p.metricsRecordBuilder.SchemaID()

	// The main record must be the first one to simplify the decoding
	// in the collector.
	rms = append([]*record_message.RecordMessage{record_message.NewMetricsMessage(schemaID, record)}, rms...)

	bar, err := p.Produce(rms)
	if err != nil {
		return nil, werror.Wrap(err)
	}
	p.stats.MetricsBatchesProduced++
	return bar, nil
}

// BatchArrowRecordsFromLogs produces a BatchArrowRecords message from a [plog.Logs] messages.
func (p *Producer) BatchArrowRecordsFromLogs(ls plog.Logs) (*colarspb.BatchArrowRecords, error) {
	// Builds a main Record and n related Records from the logs passed in
	// parameter. All these Arrow records are wrapped into a BatchArrowRecords
	// and will be released by the Producer.Produce method.
	record, err := recordBuilder[plog.Logs](func() (acommon.EntityBuilder[plog.Logs], error) {
		p.logsBuilder.RelatedData().Reset()
		return p.logsBuilder, nil
	}, ls, p.observer)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	rms, err := p.logsBuilder.RelatedData().BuildRecordMessages()
	if err != nil {
		return nil, werror.Wrap(err)
	}

	schemaID := p.logsRecordBuilder.SchemaID()
	// The main record must be the first one to simplify the decoding
	// in the collector.
	rms = append([]*record_message.RecordMessage{record_message.NewLogsMessage(schemaID, record)}, rms...)

	bar, err := p.Produce(rms)
	if err != nil {
		return nil, werror.Wrap(err)
	}
	p.stats.LogsBatchesProduced++
	return bar, nil
}

// BatchArrowRecordsFromTraces produces a BatchArrowRecords message from a [ptrace.Traces] messages.
func (p *Producer) BatchArrowRecordsFromTraces(ts ptrace.Traces) (*colarspb.BatchArrowRecords, error) {
	// Builds a main Record and n related Records from the traces passed in
	// parameter. All these Arrow records are wrapped into a BatchArrowRecords
	// and will be released by the Producer.Produce method.
	record, err := recordBuilder[ptrace.Traces](func() (acommon.EntityBuilder[ptrace.Traces], error) {
		p.tracesBuilder.RelatedData().Reset()
		return p.tracesBuilder, nil
	}, ts, p.observer)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	rms, err := p.tracesBuilder.RelatedData().BuildRecordMessages()
	if err != nil {
		return nil, werror.Wrap(err)
	}

	schemaID := p.tracesRecordBuilder.SchemaID()
	// The main record must be the first one to simplify the decoding
	// in the collector.
	rms = append([]*record_message.RecordMessage{record_message.NewTraceMessage(schemaID, record)}, rms...)

	bar, err := p.Produce(rms)
	if err != nil {
		return nil, werror.Wrap(err)
	}
	p.stats.TracesBatchesProduced++
	return bar, nil
}

// MetricsRecordBuilderExt returns the record builder used to encode metrics.
func (p *Producer) MetricsRecordBuilderExt() *builder.RecordBuilderExt {
	return p.metricsRecordBuilder
}

// LogsRecordBuilderExt returns the record builder used to encode logs.
func (p *Producer) LogsRecordBuilderExt() *builder.RecordBuilderExt {
	return p.logsRecordBuilder
}

// TracesRecordBuilderExt returns the record builder used to encode traces.
func (p *Producer) TracesRecordBuilderExt() *builder.RecordBuilderExt {
	return p.tracesRecordBuilder
}

func (p *Producer) MetricsBuilder() *metricsarrow.MetricsBuilder {
	return p.metricsBuilder
}

func (p *Producer) LogsBuilder() *logsarrow.LogsBuilder {
	return p.logsBuilder
}

func (p *Producer) TracesBuilder() *tracesarrow.TracesBuilder {
	return p.tracesBuilder
}

// Close closes all stream producers.
func (p *Producer) Close() error {
	p.metricsBuilder.Release()
	p.logsBuilder.Release()
	p.tracesBuilder.Release()

	p.metricsRecordBuilder.Release()
	p.logsRecordBuilder.Release()
	p.tracesRecordBuilder.Release()

	for _, sp := range p.streamProducers {
		if err := sp.ipcWriter.Close(); err != nil {
			return werror.Wrap(err)
		}
		p.stats.StreamProducersClosed++
	}
	return nil
}

// GetAndResetStats returns the stats and resets them.
func (p *Producer) GetAndResetStats() pstats.ProducerStats {
	return p.stats.GetAndReset()
}

// Produce takes a slice of RecordMessage and returns the corresponding BatchArrowRecords protobuf message.
func (p *Producer) Produce(rms []*record_message.RecordMessage) (*colarspb.BatchArrowRecords, error) {
	oapl := make([]*colarspb.ArrowPayload, len(rms))

	if p.stats.RecordStats {
		fmt.Printf("==> Batch id %d\n", p.batchId)
	}

	for i, rm := range rms {
		err := func() error {
			defer func() {
				rm.Record().Release()
			}()

			// Retrieves (or creates) the stream Producer for the schema id defined in the RecordMessage.
			sp := p.streamProducers[rm.SchemaID()]
			if sp == nil {
				// cleanup previous stream producer if any that have the same
				// PayloadType. The reasoning is that if we have a new
				// schema ID (i.e. schema change) we should no longer use
				// the previous stream producer for this PayloadType as schema
				// changes are only additive.
				// This will release the resources associated with the previous
				// stream producer.
				for ssID, sp := range p.streamProducers {
					if sp.payloadType == rm.PayloadType() {
						if err := sp.ipcWriter.Close(); err != nil {
							return werror.Wrap(err)
						}
						p.stats.StreamProducersClosed++
						delete(p.streamProducers, ssID)
					}
				}

				var buf bytes.Buffer
				sp = &streamProducer{
					output:      buf,
					schemaID:    fmt.Sprintf("%d", p.nextSchemaId),
					payloadType: rm.PayloadType(),
				}
				p.streamProducers[rm.SchemaID()] = sp
				p.nextSchemaId++
				p.stats.StreamProducersCreated++
			}

			sp.lastProduction = time.Now()
			sp.schema = rm.Record().Schema()

			if sp.ipcWriter == nil {
				options := []ipc.Option{
					ipc.WithAllocator(p.pool), // use allocator of the `Producer`
					ipc.WithSchema(rm.Record().Schema()),
					ipc.WithDictionaryDeltas(true), // enable dictionary deltas
				}
				if p.zstd {
					options = append(options, ipc.WithZstd())
				}
				sp.ipcWriter = ipc.NewWriter(&sp.output, options...)
			}

			if p.observer != nil {
				p.observer.OnRecord(rm.Record(), rm.PayloadType())
			}

			err := sp.ipcWriter.Write(rm.Record())
			if err != nil {
				return werror.Wrap(err)
			}
			outputBuf := sp.output.Bytes()
			buf := make([]byte, len(outputBuf))
			copy(buf, outputBuf)

			if p.stats.RecordStats || p.stats.CompressionRatioStats {
				payloadType := rm.PayloadType().String()
				recordSize := int64(len(buf))

				recordSizeDist, ok := p.stats.RecordBuilderStats.RecordSizeDistribution[payloadType]
				if !ok {
					recordSizeDist = &pstats.RecordSizeStats{
						TotalSize: 0,
						Dist:      hdrhistogram.New(0, 1<<32, 2),
					}
					p.stats.RecordBuilderStats.RecordSizeDistribution[payloadType] = recordSizeDist
				}

				recordSizeDist.TotalSize += recordSize
				if err := recordSizeDist.Dist.RecordValue(recordSize); err != nil {
					return werror.Wrap(err)
				}

				if p.stats.RecordStats {
					fmt.Printf("Record %q -> %d bytes\n", payloadType, len(buf))
					rowsToDisplay := p.stats.DumpRecordRows[payloadType]
					if rowsToDisplay > 0 {
						carrow.PrintRecord(payloadType, rm.Record(), rowsToDisplay)
					}
				}
			}

			// Reset the buffer
			sp.output.Reset()

			oapl[i] = &colarspb.ArrowPayload{
				SchemaId: sp.schemaID,
				Type:     rm.PayloadType(),
				Record:   buf,
			}
			return nil
		}()
		if err != nil {
			return nil, werror.Wrap(err)
		}
	}

	batchId := p.batchId
	p.batchId++

	return &colarspb.BatchArrowRecords{
		BatchId:       batchId,
		ArrowPayloads: oapl,
	}, nil
}

// ShowStats prints the stats to the console.
func (p *Producer) ShowStats() {
	if p.stats == nil {
		return
	}

	if p.stats.ProducerStats {
		println("\n== Producer Statistics ==============================================================================")
		p.stats.Show("")
	}

	if p.stats.SchemaStats {
		type TimeSchema struct {
			payloadType record_message.PayloadType
			time        time.Time
			schema      *arrow.Schema
		}

		var schemas []TimeSchema

		for _, producer := range p.streamProducers {
			schemas = append(schemas, TimeSchema{payloadType: producer.payloadType, time: producer.lastProduction, schema: producer.schema})
		}

		sort.Slice(schemas, func(i, j int) bool {
			return schemas[i].time.Before(schemas[j].time)
		})
		fmt.Printf("\n== Details on the Schema ============================================================\n")
		for _, s := range schemas {
			carrow.ShowSchema(s.schema, fmt.Sprintf("%q", s.payloadType), "")
		}
	}
}

// RecordSizeStats returns statistics per record payload type.
func (p *Producer) RecordSizeStats() map[string]*pstats.RecordSizeStats {
	return p.stats.RecordSizeStats()
}

// recordBuilder is a generic function that builds an Arrow Record from an OTel
// entity.
func recordBuilder[T pmetric.Metrics | plog.Logs | ptrace.Traces](
	builder func() (acommon.EntityBuilder[T], error),
	entity T,
	observer observer.ProducerObserver,
) (record arrow.Record, err error) {
	schemaNotUpToDateCount := 0

	// Build an Arrow Record from an OTEL entity.
	//
	// If a dictionary overflow is observed (see AdaptiveSchema, index type), during
	// the conversion, the record must be build again with an updated schema.
	for {
		var tb acommon.EntityBuilder[T]

		if tb, err = builder(); err != nil {
			return
		}

		if err = tb.Append(entity); err != nil {
			return
		}

		record, err = tb.Build()
		if err != nil {
			if record != nil {
				record.Release()
			}

			switch {
			case errors.Is(err, schema.ErrSchemaNotUpToDate):
				schemaNotUpToDateCount++
				if schemaNotUpToDateCount > 5 {
					panic("Too many consecutive schema updates. This shouldn't happen.")
				}
			default:
				return
			}
		} else {
			break
		}
	}
	return record, werror.Wrap(err)
}

func NewConsoleObserver(maxRows, maxPrints int) observer.ProducerObserver {
	return &consoleObserver{
		maxRows:   maxRows,
		maxPrints: maxPrints,
		counters:  make(map[record_message.PayloadType]int),
	}
}

func (o *consoleObserver) OnRecord(record arrow.Record, payloadType record_message.PayloadType) {
	count, found := o.counters[payloadType]
	if found && count >= o.maxPrints {
		// We already printed the max number of records for this payload type.
		return
	} else {
		count++
		o.counters[payloadType] = count
		carrow.PrintRecordWithProgression(payloadType.String(), record, o.maxRows, count, o.maxPrints)
	}
}

func (o *consoleObserver) OnNewField(recordName string, fieldPath string) {}

func (o *consoleObserver) OnDictionaryUpgrade(recordName string, fieldPath string, prevIndexType, newIndexType arrow.DataType, card, total uint64) {
}

func (o *consoleObserver) OnDictionaryOverflow(recordName string, fieldPath string, card, total uint64) {
}

func (o *consoleObserver) OnSchemaUpdate(recordName string, old, new *arrow.Schema) {}

func (o *consoleObserver) OnDictionaryReset(recordName string, fieldPath string, indexType arrow.DataType, card, total uint64) {
}
func (o *consoleObserver) OnMetadataUpdate(recordName, metadataKey string) {}
