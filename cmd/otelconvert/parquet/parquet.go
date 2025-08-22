package parquet

import (
	"fmt"
	v1 "go.opentelemetry.io/proto/otlp/metrics/v1"
	"log"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	otelconvarrow "github.com/grafana/mimir/cmd/otelconvert/arrow"
	otelarrow "github.com/open-telemetry/otel-arrow/go/pkg/arrow"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/arrow_record"
	"github.com/open-telemetry/otel-arrow/go/pkg/record_message"
)

var (
	consumer *arrow_record.Consumer
)

func init() {
	consumer = arrow_record.NewConsumer(arrow_record.WithMemoryLimit(3_000_000_000))
}

type parquetWriter struct {
	file      *os.File
	writer    *pqarrow.FileWriter
	writeChan chan *record_message.RecordMessage
	wg        sync.WaitGroup
	stopped   bool
	schemaID  string
}

func (w *parquetWriter) start() {
	w.wg.Add(1)
	for record := range w.writeChan {
		err := w.writer.Write(record.Record())
		if err != nil {
			panic(err)
		}
		record.Record().Release()
	}
	w.wg.Done()
}

func (w *parquetWriter) stop() {
	if w.stopped {
		return
	}

	close(w.writeChan)
	w.wg.Wait()

	w.writer.Close()
	w.file.Close()
	w.stopped = true
}

func newParquetWriter(file *os.File, writer *pqarrow.FileWriter, schemaID string) *parquetWriter {
	return &parquetWriter{
		file:      file,
		writer:    writer,
		writeChan: make(chan *record_message.RecordMessage, 100),
		schemaID:  schemaID,
	}
}

type WriteManager struct {
	processedMetrics uint64
	batchSize        uint64
	batchNum         uint64
	writers          map[string]*parquetWriter
	metricsPerBatch  uint64
	processedRecords uint64
}

func NewWriteManager(batchSize, metricsPerBatch uint64) *WriteManager {
	return &WriteManager{
		batchSize:       batchSize,
		metricsPerBatch: metricsPerBatch,

		// Write only happens in one thread but this should be threadsafe
		writers: map[string]*parquetWriter{},
	}
}

func (wm *WriteManager) Write(md *v1.MetricsData, dest string) error {
	arrowBatch, err := otelconvarrow.MarshalBatch(md)
	if err != nil {
		return fmt.Errorf("marshal arrow batch: %w", err)
	}

	records, err := consumer.Consume(arrowBatch)
	if err != nil {
		return err
	}

	for _, record := range records {
		pathPrefix := fmt.Sprintf("%s/", strings.ToLower(record.PayloadType().String()))
		if w, ok := wm.writers[pathPrefix]; !ok {
			writer, err := newWriter(dest, pathPrefix, record.Record().Schema(), wm.batchNum)
			if err != nil {
				return fmt.Errorf("create parquet writer: %w", err)
			}

			go writer.start()
			wm.writers[pathPrefix] = writer
		} else {
			if w.schemaID != otelarrow.SchemaToID(record.Record().Schema()) {
				w.stop()

				// Start a new writer when the schema changes
				writer, err := newWriter(dest, pathPrefix, record.Record().Schema(), wm.batchNum)
				if err != nil {
					return fmt.Errorf("create parquet writer: %w", err)
				}

				go writer.start()
				wm.writers[pathPrefix] = writer
			}
		}

		wm.writers[pathPrefix].writeChan <- record
		wm.processedRecords++
	}

	wm.processedMetrics += uint64(len(md.ResourceMetrics))
	//fmt.Printf("writing batch: %d, processed records: %d, processed metrics: %d\n", wm.batchNum, wm.processedRecords, wm.processedMetrics)
	if wm.processedMetrics > wm.batchSize {
		log.Printf("finished batch: %d, processed records: %d, processed metrics: %d\n", wm.batchNum, wm.processedRecords, wm.processedMetrics)

		// resets all the writers to create a new file
		wm.Stop()
		wm.batchNum++
		wm.processedMetrics = 0
	}

	return nil
}

func (wm *WriteManager) Stop() {
	for k, w := range wm.writers {
		w.stop()
		delete(wm.writers, k)
	}
}

func newWriter(dest, pathPrefix string, schema *arrow.Schema, batchNum uint64) (*parquetWriter, error) {
	dir := path.Join(dest, pathPrefix)
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return nil, fmt.Errorf("create parquet directory '%s': %w", dir, err)
	}

	fullPath, err := getFullPath(dir, batchNum)
	if err != nil {
		return nil, err
	}

	file, err := os.OpenFile(fullPath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0644)
	if err != nil {
		return nil, err
	}

	pWriter, err := pqarrow.NewFileWriter(
		schema,
		file,
		parquet.NewWriterProperties(
			parquet.WithCompression(compress.Codecs.Gzip),
		),
		pqarrow.DefaultWriterProps(),
	)
	if err != nil {
		return nil, err
	}

	schemaID := otelarrow.SchemaToID(schema)
	return newParquetWriter(file, pWriter, schemaID), nil
}

func getFullPath(dir string, batchNum uint64) (string, error) {
	return path.Join(dir, fmt.Sprintf("part-%d-%d.parquet", batchNum, time.Now().UnixMilli())), nil
}
