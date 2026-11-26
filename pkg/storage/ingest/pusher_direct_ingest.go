// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"iter"
	"time"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/user"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"

	util_log "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// ErrMalformedRecord is the error a Pusher wraps for a record it couldn't read.
var ErrMalformedRecord = errors.New("malformed record")

// consumeDirectIngest sends each tenant's records to one worker to preserve Kafka order.
// The workers must finish before the caller can release their records.
func (c PusherConsumer) consumeDirectIngest(ctx context.Context, records iter.Seq[*kgo.Record]) error {
	workers := c.kafkaConfig.IngestionConcurrencyMax
	if workers <= 1 {
		return c.ingestRecords(ctx, records)
	}

	group, workerCtx := errgroup.WithContext(ctx)
	queues := make([]chan *kgo.Record, workers)
	for index := range queues {
		queue := make(chan *kgo.Record, 64)
		queues[index] = queue
		group.Go(func() error {
			return c.ingestRecords(workerCtx, func(yield func(*kgo.Record) bool) {
				for rec := range queue {
					if !yield(rec) {
						return
					}
				}
			})
		})
	}
dispatch:
	for rec := range records {
		if workerCtx.Err() != nil {
			break
		}
		select {
		case queues[directIngestWorker(rec.Key, workers)] <- rec:
		case <-workerCtx.Done():
			break dispatch
		}
	}
	for _, queue := range queues {
		close(queue)
	}
	if err := group.Wait(); err != nil {
		return err
	}
	return context.Cause(ctx)
}

// directIngestWorker is the worker of a tenant's records, by a hash of its id.
func directIngestWorker(tenantID []byte, workers int) int {
	h := fnv.New32a()
	_, _ = h.Write(tenantID)
	return int(h.Sum32() % uint32(workers))
}

// ingestRecords pushes the records in order, stopping at the first server error.
func (c PusherConsumer) ingestRecords(ctx context.Context, records iter.Seq[*kgo.Record]) error {
	errorHandler := newPushErrorHandler(c.metrics.storagePusherMetrics, util_log.NewSampler(c.kafkaConfig.FallbackClientErrorSampleRate), c.logger)
	for rec := range records {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := c.ingestRecord(rec, errorHandler); err != nil {
			return err
		}
	}
	return nil
}

func (c PusherConsumer) ingestRecord(rec *kgo.Record, errorHandler *pushErrorHandler) error {
	tenantID := string(rec.Key)
	spanLog, ctx := spanlogger.New(user.InjectOrgID(rec.Context, tenantID), c.logger, tracer, "PusherConsumer.ingestRecord")
	defer spanLog.Finish()

	start := time.Now()
	err := c.pusher.IngestRecord(ctx, rec.Value, ParseRecordVersion(rec))
	c.metrics.storagePusherMetrics.processingTime.WithLabelValues("record").Observe(time.Since(start).Seconds())
	if errors.Is(err, ErrMalformedRecord) {
		level.Error(spanLog).Log("msg", "failed to parse write request; skipping", "err", err)
		return nil
	}
	if errorHandler.IsServerError(ctx, err) {
		return fmt.Errorf("consuming record at offset %d for tenant %s: %w", rec.Offset, tenantID, err)
	}
	return nil
}
