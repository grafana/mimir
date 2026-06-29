// SPDX-License-Identifier: AGPL-3.0-only

package blockbuilder

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"iter"
	"slices"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/pkg/blockbuilder/schedulerpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
)

const (
	// mergeSourceBufferSize bounds each per-WC prefetch channel, providing
	// backpressure so a fast WC can't run unboundedly ahead of a slow one.
	mergeSourceBufferSize = 1000
	// mergeMaxBatchRecords is how many merged records accumulate before a flush
	// to the builder.
	mergeMaxBatchRecords = 1000
)

type clusterOffsetRange struct {
	schedulerpb.OffsetRange
	clusterID int
}

// consumePartitionMerged consumes a compartment job's per-WC offset ranges and
// merges them, in record-timestamp order, into consumer. Each active range is
// consumed by its own producer (one per WC, on its own Kafka client) feeding a
// k-way merge that owns the builder.
//
// Consuming the job is all-or-nothing: every producer and the merge share a
// context, so the first one to fail cancels it, unwinding the merge and every
// sibling producer. The function then returns an error and consumeJob does not
// commit the job — a partial consume must never advance offsets past data we
// failed to read.
func (b *BlockBuilder) consumePartitionMerged(
	ctx context.Context,
	logger log.Logger,
	consumer ingest.RecordConsumer,
	builder *TSDBBuilder,
	spec schedulerpb.JobSpec,
) error {
	// activeRanges pairs each non-empty range with its WC ID (the OffsetRanges map
	// key). The map can't carry duplicate WC IDs, so no de-duplication is needed.
	activeRanges := make([]clusterOffsetRange, 0, len(spec.OffsetRanges))
	for key, rng := range spec.OffsetRanges {
		clusterID := int(key)
		if rng.StartOffset >= rng.EndOffset {
			continue // Empty range for this WC.
		}
		activeRanges = append(activeRanges, clusterOffsetRange{clusterID: clusterID, OffsetRange: rng})
	}

	switch len(activeRanges) {
	case 0:
		return nil
	case 1:
		singleRange := activeRanges[0]
		if err := b.consumePartitionSection(ctx, logger, singleRange.clusterID, consumer, builder, spec.Topic, spec.Partition, singleRange.StartOffset, singleRange.EndOffset); err != nil {
			return fmt.Errorf("consuming cluster %d: %w", singleRange.clusterID, err)
		}
		return nil
	}

	producerCtx, cancelProducers := context.WithCancel(ctx)
	defer cancelProducers()
	producerGroup, groupCtx := errgroup.WithContext(producerCtx)

	// TODO: per-WC resources scale with N concurrent WCs and may need dividing
	// across active WCs to keep peak usage independent of compartment count:
	//   - cfg.Kafka.MaxBufferedBytes (~1GB default): up to N× buffered fetch bytes.
	//   - cfg.Kafka.FetchConcurrencyMax (12 default): up to N×12 concurrent fetch
	//     requests, each holding an in-flight response buffer.
	//   - mergeSourceBufferSize: up to N× buffered records across source channels.
	// The Kafka limits would need threading (divided) through consumePartitionSection.
	sources := make([]*writeCompartmentSource, len(activeRanges))
	for i, wcRange := range activeRanges {
		source := &writeCompartmentSource{wcID: wcRange.clusterID, records: make(chan *kgo.Record, mergeSourceBufferSize)}
		sources[i] = source
		producerGroup.Go(func() error {
			// nil builder: producers must not touch the builder concurrently; the
			// merge goroutine below owns it.
			err := b.consumePartitionSection(groupCtx, logger, wcRange.clusterID, recordChannelConsumer{records: source.records}, nil, spec.Topic, spec.Partition, wcRange.StartOffset, wcRange.EndOffset)
			source.finalErr = err
			close(source.records)
			return err
		})
	}

	batcher := newRecordBatcher(consumer, mergeMaxBatchRecords)
	recordsSinceCompaction := 0
	mergeErr := mergeSourcesByTimestamp(groupCtx, sources, func(record *kgo.Record) error {
		if err := batcher.add(groupCtx, record); err != nil {
			return err
		}
		// Early head compaction runs here, on the merge goroutine that solely owns
		// the builder, at the batch cadence — the merge equivalent of the non-merge
		// path's per-fetch compaction. Producers pass a nil builder and never do this.
		recordsSinceCompaction++
		if recordsSinceCompaction >= mergeMaxBatchRecords {
			recordsSinceCompaction = 0
			if err := builder.CompactToReduceInMemorySeries(groupCtx); err != nil {
				level.Error(logger).Log("msg", "failed to run early head compaction", "err", err)
			}
		}
		return nil
	})
	if mergeErr == nil {
		mergeErr = batcher.flush(groupCtx)
	}
	cancelProducers() // unblock any producer still trying to send before waiting.
	producerErr := producerGroup.Wait()

	// A concrete error wins over a sibling's context.Canceled.
	switch {
	case producerErr != nil && !errors.Is(producerErr, context.Canceled):
		return producerErr
	case mergeErr != nil && !errors.Is(mergeErr, context.Canceled):
		return mergeErr
	case producerErr != nil:
		return producerErr
	default:
		return mergeErr
	}
}

// recordChannelConsumer is an ingest.RecordConsumer that forwards each record to
// a writeCompartmentSource's channel. Passing it to the unchanged
// consumePartitionSection lets that fetch loop feed the merge without any
// modification.
type recordChannelConsumer struct {
	records chan<- *kgo.Record
}

func (c recordChannelConsumer) Consume(ctx context.Context, records iter.Seq[*kgo.Record]) error {
	for record := range records {
		select {
		case c.records <- record:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// recordBatcher accumulates the merged record stream and forwards it to the
// builder via consumer in batches of up to maxBatch. Each ingest.RecordConsumer
// Consume call spins up an unmarshal goroutine and buffered pipeline, so batching
// both avoids that per-record cost and matches the per-fetch Consume cadence of
// the non-merge path.
type recordBatcher struct {
	consumer ingest.RecordConsumer
	maxBatch int
	batch    []*kgo.Record
}

func newRecordBatcher(consumer ingest.RecordConsumer, maxBatch int) *recordBatcher {
	return &recordBatcher{consumer: consumer, maxBatch: maxBatch}
}

func (b *recordBatcher) add(ctx context.Context, record *kgo.Record) error {
	b.batch = append(b.batch, record)
	if len(b.batch) >= b.maxBatch {
		return b.flush(ctx)
	}
	return nil
}

func (b *recordBatcher) flush(ctx context.Context) error {
	if len(b.batch) == 0 {
		return nil
	}
	if err := b.consumer.Consume(ctx, slices.Values(b.batch)); err != nil {
		return err
	}
	b.batch = b.batch[:0]
	return nil
}

// writeCompartmentSource streams one write compartment's records in offset order
// over records. The channel is closed at EOF or on error; after the close,
// finalErr holds any terminal error.
type writeCompartmentSource struct {
	wcID     int
	records  chan *kgo.Record
	finalErr error
}

// next returns the next record from the source. ok is false once the source is
// drained, in which case err carries any terminal error recorded by the producer.
func (s *writeCompartmentSource) next(ctx context.Context) (record *kgo.Record, ok bool, err error) {
	select {
	case record, open := <-s.records:
		if !open {
			return nil, false, s.finalErr
		}
		return record, true, nil
	case <-ctx.Done():
		return nil, false, ctx.Err()
	}
}

// mergeSourcesByTimestamp pulls from every source and calls emitRecord for each
// record in (timestamp, clusterID, offset) order, until all sources are drained. It
// always holds one head per live source, blocking on a lagging source until it
// produces its next record, so a faster source's later records never overtake a
// slower source's earlier ones. It returns the first error from emitRecord or
// from a source.
func mergeSourcesByTimestamp(ctx context.Context, sources []*writeCompartmentSource, emitRecord func(*kgo.Record) error) error {
	heads := make(sourceHeadHeap, 0, len(sources))
	for sourceIndex, source := range sources {
		record, ok, err := source.next(ctx)
		if err != nil {
			return err
		}
		if ok {
			heads = append(heads, sourceHead{rec: record, wcID: source.wcID, sourceIndex: sourceIndex})
		}
	}
	heap.Init(&heads)

	for heads.Len() > 0 {
		head := heap.Pop(&heads).(sourceHead)
		if err := emitRecord(head.rec); err != nil {
			return err
		}
		record, ok, err := sources[head.sourceIndex].next(ctx)
		if err != nil {
			return err
		}
		if ok {
			heap.Push(&heads, sourceHead{rec: record, wcID: head.wcID, sourceIndex: head.sourceIndex})
		}
	}
	return nil
}
