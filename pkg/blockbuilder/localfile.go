// SPDX-License-Identifier: AGPL-3.0-only

package blockbuilder

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"os"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/runutil"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/grafana/mimir/pkg/storage/ingest"
)

// localFileBatchSize bounds how many records are held in memory at once while
// streaming a dump file through the consumer. The consumer iterates a batch
// twice (once to size tenants, once to push), so the batch must be fully
// materialized, but the whole file need not be.
const localFileBatchSize = 5000

// consumeLocalFile is a hacky, non-upstream path that reads records from a dump
// file (as produced by `tools/kafkatool dump export`) instead of from Kafka,
// and builds blocks out of them. The file is JSON-lines of kgo.Record and may
// be gzipped. It is meant for local debugging, bypassing both the scheduler and
// Kafka entirely.
func (b *BlockBuilder) consumeLocalFile(ctx context.Context, path string) (err error) {
	logger := log.With(b.logger, "local_file", path)
	level.Info(logger).Log("msg", "consuming records from local file instead of Kafka")

	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("opening local file: %w", err)
	}
	defer runutil.CloseWithLogOnErr(logger, f, "closing local file")

	reader := bufio.NewReaderSize(f, 1<<20)
	var src io.Reader = reader
	if magic, perr := reader.Peek(2); perr == nil && magic[0] == 0x1f && magic[1] == 0x8b {
		gz, gerr := gzip.NewReader(reader)
		if gerr != nil {
			return fmt.Errorf("opening gzip reader: %w", gerr)
		}
		defer runutil.CloseWithLogOnErr(logger, gz, "closing gzip reader")
		src = gz
	}

	scanner := bufio.NewScanner(src)
	scanner.Buffer(make([]byte, 1<<20), 100_000_000) // Records can be large.

	var (
		builder   *TSDBBuilder
		consumer  ingest.RecordConsumer
		partition int32
		batch     = make([]*kgo.Record, 0, localFileBatchSize)
		total     int
		start     = time.Now()
	)

	asSeq := func(recs []*kgo.Record) iter.Seq[*kgo.Record] {
		return func(yield func(*kgo.Record) bool) {
			for _, rec := range recs {
				if !yield(rec) {
					return
				}
			}
		}
	}

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		// Mirror the real consume loop: a safety-net head compaction to bound
		// in-memory series. For the majority of cases it's a noop.
		if err := builder.CompactToReduceInMemorySeries(ctx); err != nil {
			level.Error(logger).Log("msg", "failed to run early head compaction", "err", err)
		}
		if err := consumer.Consume(ctx, asSeq(batch)); err != nil {
			return fmt.Errorf("consuming batch: %w", err)
		}
		batch = batch[:0]
		return nil
	}

	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}

		rec := &kgo.Record{}
		if err := json.Unmarshal(scanner.Bytes(), rec); err != nil {
			level.Warn(logger).Log("msg", "skipping corrupted record line", "record_idx", total, "err", err)
			continue
		}
		// Records fetched from Kafka carry a context (tracing baggage); the
		// consumer dereferences it, so it must be non-nil.
		rec.Context = ctx

		if builder == nil {
			partition = rec.Partition
			builder = NewTSDBBuilder(partition, b.cfg, b.limits, logger, b.tsdbBuilderMetrics, b.tsdbMetrics)
			defer runutil.CloseWithErrCapture(&err, builder, "closing tsdb builder")
			consumer = ingest.NewPusherConsumer(builder, b.cfg.Kafka, b.pusherConsumerMetrics, logger)
		} else if rec.Partition != partition {
			level.Warn(logger).Log("msg", "record from unexpected partition; consuming it under the first partition seen",
				"expected", partition, "got", rec.Partition, "offset", rec.Offset)
		}

		batch = append(batch, rec)
		total++

		if len(batch) >= localFileBatchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scanning local file: %w", err)
	}
	if err := flush(); err != nil {
		return err
	}

	if builder == nil {
		level.Info(logger).Log("msg", "no records were consumed from local file")
		return nil
	}

	metas, err := builder.CompactAndUpload(ctx, b.uploadBlocks)
	if err != nil {
		return fmt.Errorf("compacting and uploading: %w", err)
	}

	level.Info(logger).Log("msg", "done consuming local file", "duration", time.Since(start),
		"partition", partition, "records", total, "num_blocks", len(metas))
	return nil
}
