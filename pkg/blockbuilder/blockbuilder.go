// SPDX-License-Identifier: AGPL-3.0-only

package blockbuilder

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"os"
	"path"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/runutil"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/objstore"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.uber.org/atomic"
	"google.golang.org/grpc"

	"github.com/grafana/mimir/pkg/blockbuilder/schedulerpb"
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/ingest"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
)

var tracer = otel.Tracer("pkg/blockbuilder")

type BlockBuilder struct {
	services.Service

	cfg             Config
	logger          log.Logger
	register        prometheus.Registerer
	limits          *validation.Overrides
	kafkaClient     *kgo.Client
	bucketClient    objstore.Bucket
	schedulerClient schedulerpb.SchedulerClient
	schedulerConn   *grpc.ClientConn

	// the current job iteration number. For tests.
	jobIteration atomic.Int64

	blockBuilderMetrics   blockBuilderMetrics
	tsdbBuilderMetrics    tsdbBuilderMetrics
	readerMetrics         *ingest.ReaderMetrics
	pusherConsumerMetrics *ingest.PusherConsumerMetrics
}

func New(
	cfg Config,
	logger log.Logger,
	reg prometheus.Registerer,
	limits *validation.Overrides,
) (*BlockBuilder, error) {
	return newWithSchedulerClient(cfg, logger, reg, limits, nil)
}

// newWithSchedulerClient creates a new BlockBuilder with the given scheduler client.
func newWithSchedulerClient(
	cfg Config,
	logger log.Logger,
	reg prometheus.Registerer,
	limits *validation.Overrides,
	schedulerClient schedulerpb.SchedulerClient,
) (*BlockBuilder, error) {
	var readerMetrics *ingest.ReaderMetrics
	if cfg.Kafka.FetchConcurrencyMax > 0 {
		// Reader metrics tracks some per-partition things, but block-builder
		// will always be dealing with different partitions. So we pass -1.
		m := ingest.NewReaderMetrics(-1, reg, &ingest.NoOpReaderMetricsSource{}, cfg.Kafka.Topic)
		readerMetrics = &m
	}

	b := &BlockBuilder{
		cfg:                   cfg,
		logger:                logger,
		register:              reg,
		limits:                limits,
		blockBuilderMetrics:   newBlockBuilderMetrics(reg),
		tsdbBuilderMetrics:    newTSDBBBuilderMetrics(reg),
		readerMetrics:         readerMetrics,
		pusherConsumerMetrics: ingest.NewPusherConsumerMetrics(reg),
	}

	bucketClient, err := bucket.NewClient(context.Background(), cfg.BlocksStorage.Bucket, "block-builder", logger, reg)
	if err != nil {
		return nil, fmt.Errorf("failed to create the bucket client: %w", err)
	}
	b.bucketClient = bucketClient

	if schedulerClient != nil {
		b.schedulerClient = schedulerClient
	} else {
		var err error
		if b.schedulerClient, b.schedulerConn, err = b.makeSchedulerClient(); err != nil {
			return nil, fmt.Errorf("make scheduler client: %w", err)
		}
	}

	b.Service = services.NewBasicService(b.starting, b.running, b.stopping)
	return b, nil
}

func (b *BlockBuilder) makeSchedulerClient() (schedulerpb.SchedulerClient, *grpc.ClientConn, error) {
	opts, err := b.cfg.SchedulerConfig.GRPCClientConfig.DialOption(
		nil,
		nil,
		util.NewInvalidClusterValidationReporter(b.cfg.SchedulerConfig.GRPCClientConfig.ClusterValidation.Label, b.blockBuilderMetrics.invalidClusterValidation, b.logger),
	)
	if err != nil {
		return nil, nil, err
	}
	opts = append(opts, grpc.WithStatsHandler(otelgrpc.NewClientHandler()))

	//nolint:staticcheck // grpc.Dial() has been deprecated; we'll address it before upgrading to gRPC 2.
	conn, err := grpc.Dial(b.cfg.SchedulerConfig.Address, opts...)
	if err != nil {
		return nil, nil, err
	}

	client, err := schedulerpb.NewSchedulerClient(
		b.cfg.InstanceID,
		schedulerpb.NewBlockBuilderSchedulerClient(conn),
		b.logger,
		b.cfg.SchedulerConfig.UpdateInterval,
		b.cfg.SchedulerConfig.MaxUpdateAge,
	)
	if err != nil {
		return nil, nil, err
	}

	return client, conn, nil
}

func (b *BlockBuilder) starting(context.Context) (err error) {
	// Empty any previous artifacts.
	if err := os.RemoveAll(b.cfg.DataDir); err != nil {
		return fmt.Errorf("removing data dir: %w", err)
	}
	if err := os.MkdirAll(b.cfg.DataDir, os.ModePerm); err != nil {
		return fmt.Errorf("creating data dir: %w", err)
	}

	b.kafkaClient, err = ingest.NewKafkaReaderClient(
		b.cfg.Kafka,
		ingest.NewKafkaReaderClientMetrics(ingest.ReaderMetricsPrefix, "block-builder", b.register),
		b.logger,
	)
	if err != nil {
		return fmt.Errorf("creating kafka reader: %w", err)
	}

	return nil
}

func (b *BlockBuilder) stopping(_ error) error {
	b.kafkaClient.Close()
	b.schedulerClient.Close()

	if b.schedulerConn != nil {
		return b.schedulerConn.Close()
	}
	return nil
}

// running learns about the jobs from a block-builder-scheduler, and consumes one job at a time.
func (b *BlockBuilder) running(ctx context.Context) error {
	// Kick off the scheduler client's run loop.
	go b.schedulerClient.Run(ctx)

	for {
		if err := ctx.Err(); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}

		key, spec, err := b.schedulerClient.GetJob(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			level.Error(b.logger).Log("msg", "failed to get job", "err", err)
			continue
		}

		// Once we've gotten a job, we attempt to complete it even if the context is cancelled.
		if _, err := b.consumeJob(context.WithoutCancel(ctx), key, spec); err != nil {
			level.Error(b.logger).Log("msg", "failed to consume job", "job_id", key.Id, "epoch", key.Epoch, "err", err)

			if err := b.schedulerClient.FailJob(key); err != nil {
				level.Error(b.logger).Log("msg", "failed to fail job", "job_id", key.Id, "epoch", key.Epoch, "err", err)
				panic("couldn't fail job")
			}

			continue
		}

		if err := b.schedulerClient.CompleteJob(key); err != nil {
			level.Error(b.logger).Log("msg", "failed to complete job", "job_id", key.Id, "epoch", key.Epoch, "err", err)
		}

		b.jobIteration.Inc()
	}
}

// consumeJob performs block consumption from Kafka into object storage based on the given job spec.
func (b *BlockBuilder) consumeJob(ctx context.Context, key schedulerpb.JobKey, spec schedulerpb.JobSpec) (lastOffset int64, err error) {
	defer func(start time.Time) {
		success := "true"
		if err != nil {
			success = "false"
		}
		b.blockBuilderMetrics.consumeJobDuration.WithLabelValues(success).Observe(time.Since(start).Seconds())
	}(time.Now())

	sp, ctx := spanlogger.New(ctx, b.logger, tracer, "BlockBuilder.consumeJob")
	defer sp.Finish()

	logger := log.With(sp, "partition", spec.Partition, "job_id", key.Id, "job_epoch", key.Epoch)

	builder := NewTSDBBuilder(logger, b.cfg.DataDir, spec.Partition, b.cfg.BlocksStorage, b.limits, b.tsdbBuilderMetrics, b.cfg.ApplyMaxGlobalSeriesPerUserBelow)
	defer runutil.CloseWithErrCapture(&err, builder, "closing tsdb builder")

	// TODO: the block-builder can skip unmarshaling of exemplars because TSDB doesn't persist them into blocks; find a way to let PusherConsumer know about it
	consumer := ingest.NewPusherConsumer(builder, b.cfg.Kafka, b.pusherConsumerMetrics, logger)

	return b.consumePartitionSection(ctx, logger, consumer, builder, spec.Partition, spec.StartOffset, spec.EndOffset)
}

type fetchPoller interface {
	PollFetches(context.Context) kgo.Fetches
}

type fetchWrapper struct {
	fetchers *ingest.ConcurrentFetchers
}

func (f *fetchWrapper) PollFetches(ctx context.Context) kgo.Fetches {
	fetch, _ := f.fetchers.PollFetches(ctx)
	return fetch
}

var _ fetchPoller = (*fetchWrapper)(nil)

// consumePartitionSection is for the use of scheduler-based architecture.
// startOffset is inclusive, endOffset is exclusive, and must be valid offsets and not something in the future (endOffset can be technically 1 offset in the future).
// All the records and samples between these offsets will be consumed and put into a block.
// The returned lastConsumedOffset is the offset of the last record consumed.
func (b *BlockBuilder) consumePartitionSection(
	ctx context.Context,
	logger log.Logger,
	consumer ingest.RecordConsumer,
	builder *TSDBBuilder,
	partition int32,
	startOffset, endOffset int64,
) (lastConsumedOffset int64, retErr error) {
	lastConsumedOffset = startOffset
	if startOffset >= endOffset {
		level.Info(logger).Log("msg", "nothing to consume")
		return
	}

	var blockMetas []tsdb.BlockMeta
	defer func(t time.Time) {
		// No need to log or track time of the unfinished section. Just bail out.
		if errors.Is(retErr, context.Canceled) {
			return
		}

		dur := time.Since(t)

		if retErr != nil {
			level.Error(logger).Log("msg", "partition consumption failed", "err", retErr, "duration", dur)
			return
		}

		b.blockBuilderMetrics.processPartitionDuration.WithLabelValues(fmt.Sprintf("%d", partition)).Observe(dur.Seconds())
		level.Info(logger).Log("msg", "done consuming", "duration", dur, "partition", partition,
			"start_offset", startOffset, "end_offset", endOffset,
			"last_consumed_offset", lastConsumedOffset, "num_blocks", len(blockMetas))
	}(time.Now())

	b.kafkaClient.ForceMetadataRefresh()
	b.kafkaClient.AddConsumePartitions(map[string]map[int32]kgo.Offset{
		b.cfg.Kafka.Topic: {
			partition: kgo.NewOffset().At(startOffset),
		},
	})
	defer b.kafkaClient.RemoveConsumePartitions(map[string][]int32{b.cfg.Kafka.Topic: {partition}})

	var fetchPoller fetchPoller

	if b.cfg.Kafka.FetchConcurrencyMax > 0 {
		if b.readerMetrics == nil {
			panic("readerMetrics should be non-nil when concurrent fetchers are used")
		}
		f, ferr := ingest.NewConcurrentFetchers(
			ctx,
			b.kafkaClient,
			logger,
			b.cfg.Kafka.Topic,
			partition,
			startOffset,
			b.cfg.Kafka.FetchConcurrencyMax,
			100_000_000,
			false,
			5*time.Second,
			nil, // Don't need a reader since we've provided the start offset.
			ingest.NewGenericOffsetReader(func(context.Context) (int64, error) { return 0, nil }, 5*time.Second, logger), // Dummy.
			backoff.Config{
				MinBackoff: 100 * time.Millisecond,
				MaxBackoff: 1 * time.Second,
			},
			b.readerMetrics)
		if ferr != nil {
			return lastConsumedOffset, fmt.Errorf("creating concurrent fetcher: %w", ferr)
		}

		f.Start(ctx)
		defer f.Stop()

		fetchPoller = &fetchWrapper{f}
	} else {
		fetchPoller = b.kafkaClient
	}

	level.Info(logger).Log("msg", "start consuming", "partition", partition, "start_offset", startOffset, "end_offset", endOffset)

	firstRecOffset := int64(-1)
	lastRecOffset := int64(-1)

	for lastRecOffset < endOffset-1 {
		if err := context.Cause(ctx); err != nil {
			return 0, err
		}

		// PollFetches can return a non-failed fetch with zero records. In such a case, with only the fetches at hands,
		// we cannot tell if the consumer has already reached the latest end of the partition, i.e. no more records to consume,
		// or there is more data in the backlog, and we must retry the poll. That's why the consumer loop above has to guard
		// the iterations against the endOffset, so it retries the polling up until the expected end of the partition is reached.
		fetches := fetchPoller.PollFetches(ctx)
		fetches.EachError(func(_ string, _ int32, err error) {
			if !errors.Is(err, context.Canceled) {
				level.Error(logger).Log("msg", "failed to fetch records", "err", err)
				b.blockBuilderMetrics.fetchErrors.WithLabelValues(fmt.Sprintf("%d", partition)).Inc()
			}
		})

		recordsAll := func(fetches kgo.Fetches) iter.Seq[*kgo.Record] {
			return func(yield func(*kgo.Record) bool) {
				for recIter := fetches.RecordIter(); !recIter.Done(); {
					rec := recIter.Next()
					// Stop consuming after we touched the endOffset.
					if rec.Offset >= endOffset {
						return
					}
					if !yield(rec) {
						return
					}
				}
			}
		}

		records := recordsAll(fetches)
		for rec := range records {
			lastRecOffset = rec.Offset
			if firstRecOffset == -1 {
				firstRecOffset = lastRecOffset
			}
		}

		err := consumer.Consume(ctx, records)
		if err != nil {
			return 0, fmt.Errorf("consume records in partition %d: %w", partition, err)
		}
	}

	// Nothing was consumed from Kafka at all.
	if firstRecOffset == -1 {
		level.Info(logger).Log("msg", "no records were consumed")
		return
	}
	var err error
	blockMetas, err = builder.CompactAndUpload(ctx, b.uploadBlocks)
	if err != nil {
		return 0, err
	}

	// TODO: figure out a way to track the blockCounts metrics.

	return lastRecOffset, nil
}

func (b *BlockBuilder) uploadBlocks(ctx context.Context, tenantID, dbDir string, metas []tsdb.BlockMeta) error {
	buc := bucket.NewUserBucketClient(tenantID, b.bucketClient, b.limits)
	for _, m := range metas {
		if m.Stats.NumSamples == 0 {
			// No need to upload empty block.
			level.Info(b.logger).Log("msg", "skip uploading empty block", "tenant", tenantID, "block", m.ULID.String())
			return nil
		}

		meta := &block.Meta{BlockMeta: m}
		blockDir := path.Join(dbDir, meta.ULID.String())

		meta.Thanos.Source = block.BlockBuilderSource
		meta.Thanos.SegmentFiles = block.GetSegmentFiles(blockDir)

		if meta.Compaction.FromOutOfOrder() && b.limits.OutOfOrderBlocksExternalLabelEnabled(tenantID) {
			// At this point the OOO data was already ingested and compacted, so there's no point in checking for the OOO feature flag
			if meta.Thanos.Labels == nil {
				meta.Thanos.Labels = map[string]string{}
			}
			meta.Thanos.Labels[mimir_tsdb.OutOfOrderExternalLabel] = mimir_tsdb.OutOfOrderExternalLabelValue
		}

		boff := backoff.New(ctx, backoff.Config{
			MinBackoff: 100 * time.Millisecond,
			MaxBackoff: time.Minute, // If there is a network hiccup, we prefer to wait longer retrying, than fail the whole section.
			MaxRetries: 10,
		})
		for boff.Ongoing() {
			err := block.Upload(ctx, b.logger, buc, blockDir, meta)
			if err == nil {
				break
			}
			level.Warn(b.logger).Log("msg", "failed to upload block; will retry", "err", err, "block", meta.ULID.String(), "tenant", tenantID)
			boff.Wait()
		}
		if err := boff.ErrCause(); err != nil {
			return fmt.Errorf("upload block %s (tenant %s): %w", meta.ULID.String(), tenantID, err)
		}
	}
	return nil
}
