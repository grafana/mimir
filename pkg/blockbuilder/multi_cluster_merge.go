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
	// mergeSourceBufferSize bounds each per-kafka cluster prefetch channel, providing
	// backpressure so a fast cluster can't run unboundedly ahead of a slow one.
	mergeSourceBufferSize = 1000
	// mergeMaxBatchRecords is how many merged records accumulate before a flush to the
	// builder.
	mergeMaxBatchRecords = 1000
)

// kafkaClusterOffsetRange is a Kafka cluster's offset range to consume.
type kafkaClusterOffsetRange struct {
	schedulerpb.OffsetRange
	clusterID int
}

// consumeMultiCluster consumes a job's per-cluster offset ranges into builder via consumer. A
// single non-empty range is consumed directly; several are consumed concurrently, one producer per
// cluster on its own Kafka client, then k-way merged in record-timestamp order so cross-cluster
// appends stay roughly ordered rather than leaning on the TSDB out-of-order window.
//
// The merge holds one record per live cluster, so it blocks on the slowest before emitting.
// That's fine here because jobs are bounded and offline: every range must be fully read before the
// job completes anyway, so blocking on a laggard adds essentially no cost.
//
// It is all-or-nothing: the producers and the merge share a context, so the first failure cancels
// the rest and the function returns an error, leaving consumeJob to not commit the job — a partial
// consume must never advance offsets past data we failed to read.
func (b *BlockBuilder) consumeMultiCluster(
	ctx context.Context,
	logger log.Logger,
	consumer ingest.RecordConsumer,
	builder *TSDBBuilder,
	spec schedulerpb.JobSpec,
) error {
	// activeRanges pairs each non-empty range with its cluster ID (the OffsetRanges
	// map key). The map can't carry duplicate keys, so no de-duplication is needed.
	activeRanges := make([]kafkaClusterOffsetRange, 0, len(spec.OffsetRanges))
	for clusterID, rng := range spec.OffsetRanges {
		if int(clusterID) < 0 || int(clusterID) >= len(b.clusters) {
			return fmt.Errorf("job spec references kafka cluster %d, but only %d clusters are configured", clusterID, len(b.clusters))
		}
		if rng.StartOffset >= rng.EndOffset {
			continue // Empty range for this cluster
		}
		activeRanges = append(activeRanges, kafkaClusterOffsetRange{clusterID: int(clusterID), OffsetRange: rng})
	}

	switch len(activeRanges) {
	case 0:
		return nil
	case 1:
		rng := activeRanges[0]
		return b.consumePartitionSection(ctx, logger, b.clusters[rng.clusterID], consumer, builder, spec.Topic, spec.Partition, rng.StartOffset, rng.EndOffset)
	}

	producerCtx, cancelProducers := context.WithCancelCause(ctx)
	defer cancelProducers(context.Canceled)
	producerGroup, groupCtx := errgroup.WithContext(producerCtx)

	sources := make([]*kafkaClusterSource, len(activeRanges))
	for i, rng := range activeRanges {
		source := &kafkaClusterSource{clusterID: rng.clusterID, records: make(chan *kgo.Record, mergeSourceBufferSize)}
		sources[i] = source
		producerGroup.Go(func() error {
			// nil builder: producers must not touch the builder concurrently; the merge
			// goroutine below owns it.
			err := b.consumePartitionSection(groupCtx, logger, b.clusters[rng.clusterID], recordChannelConsumer{records: source.records}, nil, spec.Topic, spec.Partition, rng.StartOffset, rng.EndOffset)
			if err != nil {
				err = fmt.Errorf("consuming kafka cluster %d: %w", rng.clusterID, err)
			}
			source.finalErr = err
			close(source.records)
			return err
		})
	}

	batcher := newRecordBatcher(consumer, mergeMaxBatchRecords)
	mergeErr := mergeSourcesByTimestamp(groupCtx, sources, func(record *kgo.Record) error {
		flushed, err := batcher.add(groupCtx, record)
		if err != nil {
			return err
		}
		// Early head compaction runs here, on the merge goroutine that solely owns the
		// builder, at the batch-flush cadence — the merge equivalent of the non-merge path's
		// per-fetch compaction. Producers pass a nil builder and never do this.
		if flushed {
			if err := builder.CompactToReduceInMemorySeries(groupCtx); err != nil {
				level.Error(logger).Log("msg", "failed to run early head compaction", "err", err)
			}
		}
		return nil
	})
	if mergeErr == nil {
		mergeErr = batcher.flush(groupCtx)
	}
	cancelProducers(context.Canceled) // Unblock any producer still trying to send before waiting.
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

// recordChannelConsumer is an ingest.RecordConsumer that forwards each record to a channel.
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

// recordBatcher accumulates the merged record stream and forwards it to a
// consumer in batches of up to maxBatch.
type recordBatcher struct {
	consumer ingest.RecordConsumer
	maxBatch int
	batch    []*kgo.Record
}

func newRecordBatcher(consumer ingest.RecordConsumer, maxBatch int) *recordBatcher {
	return &recordBatcher{consumer: consumer, maxBatch: maxBatch}
}

// add appends record to the batch, flushing it to the consumer once it reaches maxBatch. It
// reports whether the batch was flushed.
func (b *recordBatcher) add(ctx context.Context, record *kgo.Record) (flushed bool, err error) {
	b.batch = append(b.batch, record)
	if len(b.batch) < b.maxBatch {
		return false, nil
	}
	if err := b.flush(ctx); err != nil {
		return false, err
	}
	return true, nil
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

// kafkaClusterSource streams one Kafka cluster's records in offset order over records.
// The channel is closed at EOF or on error; after the close, finalErr holds any terminal error.
type kafkaClusterSource struct {
	clusterID int
	records   chan *kgo.Record
	finalErr  error
}

// next returns the next record from the source, or a nil record once the source is drained,
// in which case err carries any terminal error recorded by the producer.
func (s *kafkaClusterSource) next(ctx context.Context) (*kgo.Record, error) {
	select {
	case record, open := <-s.records:
		if !open {
			return nil, s.finalErr
		}
		return record, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// mergeSourcesByTimestamp pulls from every source and calls emitRecord for each record in
// (timestamp, cluster ID, offset) order, until all sources are drained. It always
// holds one head per live source, blocking on a lagging source until it produces its next
// record, so a faster source's later records never overtake a slower source's earlier ones.
// It returns the first error from emitRecord or from a source.
func mergeSourcesByTimestamp(ctx context.Context, sources []*kafkaClusterSource, emitRecord func(*kgo.Record) error) error {
	heads := make(sourceHeadHeap, 0, len(sources))
	for sourceIndex, source := range sources {
		record, err := source.next(ctx)
		if err != nil {
			return err
		}
		if record != nil {
			heads = append(heads, sourceHead{rec: record, clusterID: source.clusterID, sourceIndex: sourceIndex})
		}
	}
	heap.Init(&heads)

	for heads.Len() > 0 {
		head := heap.Pop(&heads).(sourceHead)
		if err := emitRecord(head.rec); err != nil {
			return err
		}
		record, err := sources[head.sourceIndex].next(ctx)
		if err != nil {
			return err
		}
		if record != nil {
			heap.Push(&heads, sourceHead{rec: record, clusterID: head.clusterID, sourceIndex: head.sourceIndex})
		}
	}
	return nil
}
