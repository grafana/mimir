// SPDX-License-Identifier: AGPL-3.0-only

package blockbuilder

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kprom"

	"github.com/grafana/mimir/pkg/storage/bucket"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/util/validation"
)

type BlockBuilder struct {
	services.Service

	instanceID int

	cfg         Config
	logger      log.Logger
	register    prometheus.Registerer
	limits      *validation.Overrides
	kafkaClient *kgo.Client
	bucket      objstore.Bucket

	parts []int32
	// fallbackOffset is the low watermark from where a partition which doesn't have a commit will be consumed.
	// It can be either a timestamp or the kafkaStartOffset.
	// TODO(v): to support for setting it to kafkaStartOffset we need to rework how a lagging cycle is chopped into time frames.
	fallbackOffset int64

	metrics blockBuilderMetrics

	// for testing
	tsdbBuilder func() builder
}

func New(
	cfg Config,
	logger log.Logger,
	reg prometheus.Registerer,
	limits *validation.Overrides,
) (_ *BlockBuilder, err error) {
	b := &BlockBuilder{
		cfg:      cfg,
		logger:   logger,
		register: reg,
		limits:   limits,
		metrics:  newBlockBuilderMetrics(reg),
	}

	id, err := parseIDFromInstance(b.cfg.InstanceID)
	if err != nil {
		return nil, fmt.Errorf("failed to parse instance id: %w", err)
	}

	b.instanceID = id
	level.Info(b.logger).Log("msg", "initialising block builder", "id", b.instanceID)

	var ok bool
	b.parts, ok = b.cfg.Kafka.PartitionAssignment[b.instanceID]
	if !ok {
		return nil, fmt.Errorf("instance id %d must be present in partition assignment", b.instanceID)
	}

	b.tsdbBuilder = func() builder {
		return newTSDBBuilder(b.logger, b.limits, b.cfg.BlocksStorageConfig)
	}

	bucketClient, err := bucket.NewClient(context.Background(), cfg.BlocksStorageConfig.Bucket, "blockbuilder", logger, reg)
	if err != nil {
		return nil, fmt.Errorf("failed to create the bucket client: %w", err)
	}
	b.bucket = bucketClient

	b.Service = services.NewBasicService(b.starting, b.running, b.stopping)

	return b, nil
}

func parseIDFromInstance(instanceID string) (int, error) {
	splits := strings.Split(instanceID, "-")
	if len(splits) == 0 {
		return 0, fmt.Errorf("malformed instance id %s", instanceID)
	}

	partID, err := strconv.Atoi(splits[len(splits)-1])
	if err != nil {
		return 0, fmt.Errorf("parse instance id %s, expect sequence number: %w", instanceID, err)
	}

	return partID, nil
}

func (b *BlockBuilder) starting(ctx context.Context) (err error) {
	const fetchMaxBytes = 100_000_000

	defer func() {
		if err != nil {
			if b.kafkaClient != nil {
				b.kafkaClient.Close()
			}
		}
	}()

	// Empty any previous artifacts.
	if err := os.RemoveAll(b.cfg.BlocksStorageConfig.TSDB.Dir); err != nil {
		return fmt.Errorf("removing tsdb dir: %w", err)
	}
	if err := os.MkdirAll(b.cfg.BlocksStorageConfig.TSDB.Dir, os.ModePerm); err != nil {
		return fmt.Errorf("creating tsdb dir: %w", err)
	}

	// TODO: add a test to test the case where the consumption on startup happens
	// after the LookbackOnNoCommit with records before the after the consumption
	// start point.
	startAtOffsets, fallbackOffset, err := b.findOffsetsToStartAt(ctx, nil)
	if err != nil {
		return fmt.Errorf("find offsets to start at: %w", err)
	}
	b.fallbackOffset = fallbackOffset

	opts := []kgo.Opt{
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			b.cfg.Kafka.Topic: startAtOffsets,
		}),
		kgo.FetchMinBytes(128),
		kgo.FetchMaxBytes(fetchMaxBytes),
		kgo.FetchMaxWait(5 * time.Second),
		kgo.FetchMaxPartitionBytes(50_000_000),
		// BrokerMaxReadBytes sets the maximum response size that can be read from
		// Kafka. This is a safety measure to avoid OOMing on invalid responses.
		// franz-go recommendation is to set it 2x FetchMaxBytes.
		kgo.BrokerMaxReadBytes(2 * fetchMaxBytes),
	}

	// TODO: metrics without clashes
	metrics := kprom.NewMetrics(
		"cortex_blockbuilder_kafka",
		kprom.Registerer(b.register),
		kprom.FetchAndProduceDetail(kprom.ByNode, kprom.Batches, kprom.Records, kprom.CompressedBytes, kprom.UncompressedBytes),
	)
	opts = append(
		commonKafkaClientOptions(b.cfg.Kafka, b.logger, metrics),
		opts...,
	)
	b.kafkaClient, err = kgo.NewClient(opts...)
	if err != nil {
		return fmt.Errorf("creating kafka client: %w", err)
	}

	level.Info(b.logger).Log("msg", "waiting until kafka brokers are reachable", "parts", fmt.Sprintf("%v", b.parts))
	err = b.waitKafka(ctx)
	if err != nil {
		return err
	}

	b.kafkaClient.PauseFetchPartitions(map[string][]int32{b.cfg.Kafka.Topic: b.parts})

	return nil
}

// kafkaOffsetStart is a special offset value that means the beginning of the partition.
const kafkaOffsetStart = int64(-2)

func (b *BlockBuilder) findOffsetsToStartAt(ctx context.Context, metrics *kprom.Metrics) (map[int32]kgo.Offset, int64, error) {
	// We use an ephemeral client to fetch the offset and then create a new client with this offset.
	// The reason for this is that changing the offset of an existing client requires to have used this client for fetching at least once.
	// We don't want to do noop fetches just to warm up the client, so we create a new client instead.
	cl, err := kgo.NewClient(commonKafkaClientOptions(b.cfg.Kafka, b.logger, metrics)...)
	if err != nil {
		return nil, -1, fmt.Errorf("unable to create bootstrap client: %w", err)
	}
	defer cl.Close()

	admClient := kadm.NewClient(cl)

	var fallbackOffset int64
	defaultKOffset := kgo.NewOffset()

	// When LookbackOnNoCommit is positive, its value is used to calculate the timestamp after which the consumption starts,
	// otherwise it is a special offset for the "start" of the partition.
	if b.cfg.LookbackOnNoCommit > 0 {
		ts := time.Now().Add(-b.cfg.LookbackOnNoCommit)
		fallbackOffset = ts.UnixMilli()
		defaultKOffset = defaultKOffset.AfterMilli(fallbackOffset)
	} else {
		fallbackOffset = int64(b.cfg.LookbackOnNoCommit)
		switch fallbackOffset {
		case kafkaOffsetStart:
			defaultKOffset = defaultKOffset.AtStart()
		default:
			// We don't support consuming from the "end" of the partition because starting a cycle from the end always results to a zero lag.
			// This may be done later.
			return nil, -1, fmt.Errorf("unexpected fallback offset value %v", b.cfg.LookbackOnNoCommit)
		}
	}

	fetchOffsets := func(ctx context.Context) (offsets map[int32]kgo.Offset, err error) {
		resp, err := admClient.FetchOffsets(ctx, b.cfg.Kafka.ConsumerGroup)
		if err == nil {
			err = resp.Error()
		}
		// Either success or the requested group does not exist.
		if errors.Is(err, kerr.GroupIDNotFound) || errors.Is(err, kerr.UnknownTopicOrPartition) {
			offsets = make(map[int32]kgo.Offset)
		} else if err != nil {
			return nil, fmt.Errorf("unable to fetch group offsets: %w", err)
		} else {
			offsets = resp.KOffsets()[b.cfg.Kafka.Topic]
		}

		// Look over the assigned partitions and fallback to the ConsumerResetOffset if a partition doesn't have any commit for the configured group.
		for _, part := range b.parts {
			if _, ok := offsets[part]; ok {
				level.Info(b.logger).Log("msg", "consuming from last consumed offset", "consumer_group", b.cfg.Kafka.ConsumerGroup, "part", part, "offset", offsets[part].String())
			} else {
				offsets[part] = defaultKOffset
				level.Info(b.logger).Log("msg", "consuming from partition look back because no offset has been found", "consumer_group", b.cfg.Kafka.ConsumerGroup, "part", part, "offset", offsets[part].String())
			}
		}
		return offsets, nil
	}

	boff := backoff.New(ctx, backoff.Config{
		MinBackoff: 100 * time.Millisecond,
		MaxBackoff: 2 * time.Second,
		MaxRetries: 10,
	})
	for boff.Ongoing() {
		var offsets map[int32]kgo.Offset
		offsets, err = fetchOffsets(ctx)
		if err == nil {
			return offsets, fallbackOffset, nil
		}
		level.Warn(b.logger).Log("msg", "failed to fetch startup offsets", "err", err)
		boff.Wait()
	}
	// Handle the case the context was canceled before the first attempt.
	if err == nil {
		err = boff.Err()
	}
	return nil, -1, err
}

func commonKafkaClientOptions(cfg KafkaConfig, logger log.Logger, metrics *kprom.Metrics) []kgo.Opt {
	opts := []kgo.Opt{
		kgo.ClientID(cfg.ClientID),
		kgo.SeedBrokers(cfg.Address),
		kgo.DialTimeout(cfg.DialTimeout),
		kgo.MetadataMinAge(10 * time.Second),
		kgo.MetadataMaxAge(10 * time.Second),
		kgo.WithLogger(newKafkaLogger(logger)),
	}
	if metrics != nil {
		opts = append(opts, kgo.WithHooks(metrics))
	}
	return opts
}

func (b *BlockBuilder) waitKafka(ctx context.Context) error {
	boff := backoff.New(ctx, backoff.Config{
		MinBackoff: 100 * time.Millisecond,
		MaxBackoff: time.Second,
		MaxRetries: 0,
	})
	for boff.Ongoing() {
		err := b.kafkaClient.Ping(ctx)
		if err == nil {
			return nil
		}

		level.Error(b.logger).Log(
			"msg", "waiting for kafka ping to succeed",
			"part", fmt.Sprintf("%v", b.parts),
			"num_retries", boff.NumRetries(),
		)
		boff.Wait()
	}
	return boff.Err()
}

func (b *BlockBuilder) stopping(_ error) error {
	b.kafkaClient.Close()

	return nil
}

func (b *BlockBuilder) running(ctx context.Context) error {
	// Do initial consumption on start using current time as the point up to which we are consuming.
	// To avoid small blocks at startup, we consume until the last hour boundary + buffer.
	cycleEnd := cycleEndAtStartup(b.cfg.ConsumeInterval, b.cfg.ConsumeIntervalBuffer)
	err := b.NextConsumeCycle(ctx, cycleEnd)
	if err != nil {
		return err
	}

	nextCycleTime := time.Now().Truncate(b.cfg.ConsumeInterval).Add(b.cfg.ConsumeInterval + b.cfg.ConsumeIntervalBuffer)
	waitTime := time.Until(nextCycleTime)
	for waitTime > b.cfg.ConsumeInterval {
		// NB: at now=14:12, next cycle starts at 14:15 (startup cycle ended at 13:15)
		//     at now=14:17, next cycle starts at 15:15 (startup cycle ended at 14:15)
		nextCycleTime = nextCycleTime.Add(-b.cfg.ConsumeInterval)
		waitTime -= b.cfg.ConsumeInterval
	}

	for {
		select {
		case <-time.After(waitTime):
			cycleEnd := nextCycleTime
			level.Info(b.logger).Log("msg", "triggering next consume from running", "cycle_end", cycleEnd, "cycle_time", nextCycleTime)
			err := b.NextConsumeCycle(ctx, cycleEnd)
			if err != nil {
				b.metrics.consumeCycleFailures.Inc()
				level.Error(b.logger).Log("msg", "consume cycle failed", "cycle_end", cycleEnd, "err", err)
			}

			// If we took more than consumptionItvl time to consume the records, this
			// will immediately start the next consumption.
			nextCycleTime = nextCycleTime.Add(b.cfg.ConsumeInterval)
			waitTime = time.Until(nextCycleTime)
		// TODO(codesome): track "-waitTime" (when waitTime < 0), which is the time we ran over. Should have an alert on this.
		case <-ctx.Done():
			level.Info(b.logger).Log("msg", "context cancelled, stopping block builder")
			return nil
		}
	}
}

func cycleEndAtStartup(interval, buffer time.Duration) time.Time {
	cycleEnd := time.Now().Truncate(interval).Add(buffer)
	if cycleEnd.After(time.Now()) {
		cycleEnd = cycleEnd.Add(-interval)
	}
	return cycleEnd
}

// NextConsumeCycle manages consumption of currently assigned partitions.
// The cycleEnd argument indicates the timestamp (relative to Kafka records) up until which to consume from partitions
// in this cycle. That is, Kafka records produced after the mark will be consumed in the next cycle.
func (b *BlockBuilder) NextConsumeCycle(ctx context.Context, cycleEnd time.Time) error {
	defer func(t time.Time) {
		b.metrics.consumeCycleDuration.Observe(time.Since(t).Seconds())
	}(time.Now())

	for _, part := range b.parts {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if part < 0 {
			return nil
		}

		level.Debug(b.logger).Log("msg", "next consume cycle", "part", part)

		// TODO(v): calculating lag for each individual partition is redundant, as it requests the data for the whole group all the time.
		// Since we don't have a rebalance in the middle of the cycle now, can we do it once in the beginning of the cycle?
		lag, err := b.getLagForPartition(ctx, kadm.NewClient(b.kafkaClient), part)
		if err != nil {
			level.Error(b.logger).Log("msg", "failed to get partition lag", "err", err, "part", part)
			continue
		}

		b.metrics.consumerLag.WithLabelValues(lag.Topic, fmt.Sprintf("%d", lag.Partition)).Set(float64(lag.Lag))

		if lag.Lag <= 0 {
			if err := lag.Err; err != nil {
				level.Error(b.logger).Log("msg", "failed to get partition lag", "err", err, "part", part)
			} else {
				level.Info(b.logger).Log(
					"msg", "nothing to consume in partition",
					"part", part,
					"offset", lag.Commit.At,
					"end_offset", lag.End.Offset,
					"lag", lag.Lag,
				)
			}
			continue
		}

		// We look at the commit offset timestamp to determine how far behind we are lagging
		// relative to the cycleEnd. We will consume the partition in parts accordingly.
		offset := lag.Commit
		commitRecTs, seenTillOffset, lastBlockEnd, err := unmarshallCommitMeta(offset.Metadata)
		if err != nil {
			// If there is an error in unmarshalling the metadata, treat it as if
			// we have no commit. There is no reason to stop the cycle for this.
			level.Warn(b.logger).Log("msg", "error unmarshalling commit metadata", "err", err, "part", part, "offset", offset.At, "metadata", offset.Metadata)
		}

		pl := partitionInfo{
			Partition:      part,
			Lag:            lag.Lag,
			Commit:         offset,
			CommitRecTs:    commitRecTs,
			SeenTillOffset: seenTillOffset,
			LastBlockEnd:   lastBlockEnd,
		}
		if err := b.nextConsumeCycle(ctx, b.kafkaClient, pl, cycleEnd); err != nil {
			level.Error(b.logger).Log("msg", "failed to consume partition", "err", err, "part", part)
		}
	}
	return nil
}

func (b *BlockBuilder) getLagForPartition(ctx context.Context, admClient *kadm.Client, part int32) (kadm.GroupMemberLag, error) {
	boff := backoff.New(ctx, backoff.Config{
		MinBackoff: 100 * time.Millisecond,
		MaxBackoff: time.Second,
		MaxRetries: 10,
	})
	var lastErr error
	for boff.Ongoing() {
		if lastErr != nil {
			level.Error(b.logger).Log("msg", "failed to get consumer group lag", "err", lastErr, "part", part)
		}
		groupLag, err := getGroupLag(ctx, admClient, b.cfg.Kafka.Topic, b.cfg.Kafka.ConsumerGroup, b.fallbackOffset)
		if err != nil {
			lastErr = fmt.Errorf("get consumer group lag: %w", err)
			continue
		}

		lag, ok := groupLag.Lookup(b.cfg.Kafka.Topic, part)
		if ok {
			return lag, nil
		}
		lastErr = fmt.Errorf("partition %d not found in lag response", part)
		boff.Wait()
	}

	return kadm.GroupMemberLag{}, lastErr
}

// getGroupLag is the inlined version of `kadm.Client.Lag` but updated to work with when the group
// doesn't have live participants.
// Similar to `kadm.CalculateGroupLagWithStartOffsets`, it takes into account that the group may not have any commits.
func getGroupLag(ctx context.Context, admClient *kadm.Client, topic, group string, fallbackOffset int64) (kadm.GroupLag, error) {
	offsets, err := admClient.FetchOffsets(ctx, group)
	if err != nil {
		if !errors.Is(err, kerr.GroupIDNotFound) {
			return nil, fmt.Errorf("fetch offsets: %w", err)
		}
	}
	if err := offsets.Error(); err != nil {
		return nil, fmt.Errorf("fetch offsets got error in response: %w", err)
	}

	startOffsets, err := admClient.ListStartOffsets(ctx, topic)
	if err != nil {
		return nil, err
	}
	endOffsets, err := admClient.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, err
	}

	resolveFallbackOffsets := sync.OnceValues(func() (kadm.ListedOffsets, error) {
		if fallbackOffset == kafkaOffsetStart {
			return startOffsets, nil
		}
		if fallbackOffset > 0 {
			return admClient.ListOffsetsAfterMilli(ctx, fallbackOffset, topic)
		}
		// This should not happen because fallbackOffset already went through the validation by this point.
		return nil, fmt.Errorf("cannot resolve fallback offset for value %v", fallbackOffset)
	})
	// If the group-partition in offsets doesn't have a commit, fall back depending on where fallbackOffset points at.
	for topic, pt := range startOffsets.Offsets() {
		for part := range pt {
			if _, ok := offsets.Lookup(topic, part); ok {
				continue
			}
			fallbackOffsets, err := resolveFallbackOffsets()
			if err != nil {
				return nil, fmt.Errorf("resolve fallback offsets: %w", err)
			}
			o, ok := fallbackOffsets.Lookup(topic, part)
			if !ok {
				return nil, fmt.Errorf("partition %d not found in fallback offsets for topic %s", part, topic)
			}
			offsets.Add(kadm.OffsetResponse{Offset: kadm.Offset{
				Topic:       o.Topic,
				Partition:   o.Partition,
				At:          o.Offset,
				LeaderEpoch: o.LeaderEpoch,
			}})
		}
	}

	return calculateGroupLag(offsets, startOffsets, endOffsets), nil
}

var errListMissing = errors.New("missing from list offsets")

// Inlined from https://github.com/grafana/mimir/blob/04df05d32320cd19281591a57c19cceba63ceabf/vendor/github.com/twmb/franz-go/pkg/kadm/groups.go#L1711
func calculateGroupLag(commit kadm.OffsetResponses, startOffsets, endOffsets kadm.ListedOffsets) kadm.GroupLag {
	l := make(map[string]map[int32]kadm.GroupMemberLag)
	for t, ps := range commit {
		lt := l[t]
		if lt == nil {
			lt = make(map[int32]kadm.GroupMemberLag)
			l[t] = lt
		}
		tstart := startOffsets[t]
		tend := endOffsets[t]
		for p, pcommit := range ps {
			var (
				pend = kadm.ListedOffset{
					Topic:     t,
					Partition: p,
					Err:       errListMissing,
				}
				pstart = pend
				perr   error
			)

			// In order of priority, perr (the error on the Lag
			// calculation) is non-nil if:
			//
			//  * The topic is missing from end ListOffsets
			//  * The partition is missing from end ListOffsets
			//  * OffsetFetch has an error on the partition
			//  * ListOffsets has an error on the partition
			//
			// If we have no error, then we can calculate lag.
			// We *do* allow an error on start ListedOffsets;
			// if there are no start offsets or the start offset
			// has an error, it is not used for lag calculation.
			perr = errListMissing
			if tend != nil {
				if pendActual, ok := tend[p]; ok {
					pend = pendActual
					perr = nil
				}
			}
			if perr == nil {
				if perr = pcommit.Err; perr == nil {
					perr = pend.Err
				}
			}
			if tstart != nil {
				if pstartActual, ok := tstart[p]; ok {
					pstart = pstartActual
				}
			}

			lag := int64(-1)
			if perr == nil {
				lag = pend.Offset
				if pstart.Err == nil {
					lag = pend.Offset - pstart.Offset
				}
				if pcommit.At >= 0 {
					lag = pend.Offset - pcommit.At
				}
				if lag < 0 {
					lag = 0
				}
			}

			lt[p] = kadm.GroupMemberLag{
				Topic:     t,
				Partition: p,
				Commit:    pcommit.Offset,
				Start:     pstart,
				End:       pend,
				Lag:       lag,
				Err:       perr,
			}
		}
	}

	// Now we look at all topics that we calculated lag for, and check out
	// the partitions we listed. If those partitions are missing from the
	// lag calculations above, the partitions were not committed to and we
	// count that as entirely lagging.
	for t, lt := range l {
		tstart := startOffsets[t]
		tend := endOffsets[t]
		for p, pend := range tend {
			if _, ok := lt[p]; ok {
				continue
			}
			pcommit := kadm.Offset{
				Topic:       t,
				Partition:   p,
				At:          -1,
				LeaderEpoch: -1,
			}
			perr := pend.Err
			lag := int64(-1)
			if perr == nil {
				lag = pend.Offset
			}
			pstart := kadm.ListedOffset{
				Topic:     t,
				Partition: p,
				Err:       errListMissing,
			}
			if tstart != nil {
				if pstartActual, ok := tstart[p]; ok {
					pstart = pstartActual
					if pstart.Err == nil {
						lag = pend.Offset - pstart.Offset
						if lag < 0 {
							lag = 0
						}
					}
				}
			}
			lt[p] = kadm.GroupMemberLag{
				Topic:     t,
				Partition: p,
				Commit:    pcommit,
				Start:     pstart,
				End:       pend,
				Lag:       lag,
				Err:       perr,
			}
		}
	}

	return l
}

type partitionInfo struct {
	Partition int32
	Lag       int64

	Commit         kadm.Offset
	CommitRecTs    int64
	SeenTillOffset int64
	LastBlockEnd   int64
}

func (b *BlockBuilder) nextConsumeCycle(ctx context.Context, client *kgo.Client, pl partitionInfo, cycleEnd time.Time) error {
	var commitRecTime time.Time
	if pl.CommitRecTs > 0 {
		commitRecTime = time.UnixMilli(pl.CommitRecTs)
	}
	if commitRecTime.IsZero() && b.fallbackOffset > 0 {
		// If there is no commit metadata, we use the lookback config to replay a set amount of
		// records because it is non-trivial to peek at the first record in a partition to determine
		// the range of replay required. Without knowing the range, we might end up trying to consume
		// a lot of records in a single partition consumption call and end up in an OOM loop.
		// TODO(codesome): add a test for this.
		commitRecTime = time.UnixMilli(b.fallbackOffset).Truncate(b.cfg.ConsumeInterval)
	}

	lagging := cycleEnd.Sub(commitRecTime) > 3*b.cfg.ConsumeInterval/2
	if !lagging {
		// Either we did not find a commit offset or we are not lagging behind by
		// more than 1.5 times the consume interval.
		// When there is no kafka commit, we play safe and assume seenTillOffset and
		// lastBlockEnd was 0 to not discard any samples unnecessarily.
		_, err := b.consumePartition(ctx, client, pl, cycleEnd)
		if err != nil {
			return fmt.Errorf("consume partition %d: %w", pl.Partition, err)
		}

		return nil
	}

	// We are lagging behind. We need to consume the partition in parts.
	// We iterate through all the cycleEnds starting from the first one after commit until the cycleEnd.
	cycleEndStartAt := commitRecTime.Truncate(b.cfg.ConsumeInterval).Add(b.cfg.ConsumeInterval + b.cfg.ConsumeIntervalBuffer)
	level.Info(b.logger).Log("msg", "partition is lagging behind", "part", pl.Partition, "lag", pl.Lag, "cycle_end_start", cycleEndStartAt, "cycle_end", cycleEnd, "last_commit_rec_time", commitRecTime, "last_commit_rec_ts", pl.CommitRecTs)
	for ce := cycleEndStartAt; cycleEnd.Sub(ce) >= 0; ce = ce.Add(b.cfg.ConsumeInterval) {
		// Instead of looking for the commit metadata for each iteration, we use the data returned by consumePartition
		// in the next iteration.
		var err error
		pl, err = b.consumePartition(ctx, client, pl, ce)
		if err != nil {
			return fmt.Errorf("consume partition %d: %w", pl.Partition, err)
		}

		// If adding the ConsumeInterval takes it beyond the cycleEnd, we set it to the cycleEnd to not
		// exit the loop without consuming until cycleEnd.
		if ce.Compare(cycleEnd) != 0 && ce.Add(b.cfg.ConsumeInterval).After(cycleEnd) {
			ce = cycleEnd
		}
	}

	return nil
}

// consumePartition consumes records from the given partition until the cycleEnd timestamp.
// If the partition is lagging behind, the caller of consumePartition needs to take care of
// calling consumePartition in parts.
// consumePartition returns
// * retPl: updated partitionInfo after consuming the partition.
func (b *BlockBuilder) consumePartition(
	ctx context.Context,
	client *kgo.Client,
	pl partitionInfo,
	cycleEnd time.Time,
) (retPl partitionInfo, retErr error) {
	var (
		part           = pl.Partition
		seenTillOffset = pl.SeenTillOffset
		lastBlockEnd   = pl.LastBlockEnd
		lastCommit     = pl.Commit
		lag            = pl.Lag

		blockEndAt = cycleEnd.Truncate(b.cfg.ConsumeInterval)
		blockEnd   = blockEndAt.UnixMilli()
	)

	// TopicPartition to resume consuming on this iteration.
	// Note: pause/resume is a client-local state. On restart or a crash, the client will be assigned its share of partitions,
	// during consumer group's rebalancing, and it will continue consuming as usual.
	tp := map[string][]int32{b.cfg.Kafka.Topic: {part}}
	client.ResumeFetchPartitions(tp)
	defer client.PauseFetchPartitions(tp)

	var (
		numBlocks     int
		compactionDur time.Duration
	)

	defer func(t time.Time, startingLag int64) {
		dur := time.Since(t)
		if retErr != nil {
			level.Error(b.logger).Log("msg", "partition consumption failed", "part", part, "dur", dur, "lag", lag, "err", retErr)
			return
		}
		b.metrics.processPartitionDuration.WithLabelValues(fmt.Sprintf("%d", part)).Observe(dur.Seconds())
		level.Info(b.logger).Log("msg", "done consuming partition", "part", part, "dur", dur,
			"start_lag", startingLag, "cycle_end", cycleEnd,
			"last_block_end", time.UnixMilli(lastBlockEnd), "curr_block_end", time.UnixMilli(retPl.LastBlockEnd),
			"last_seen_till", seenTillOffset, "curr_seen_till", retPl.SeenTillOffset,
			"num_blocks", numBlocks, "compact_and_upload_dur", compactionDur)
	}(time.Now(), lag)

	builder := b.tsdbBuilder()
	defer builder.close() // TODO: handle error

	level.Info(b.logger).Log(
		"msg", "consuming partition", "part", part, "lag", lag,
		"cycle_end", cycleEnd, "last_block_end", time.UnixMilli(lastBlockEnd), "curr_block_end", blockEndAt,
		"last_seen_till", seenTillOffset)

	var (
		done                         bool
		commitRec, firstRec, lastRec *kgo.Record
	)
	for !done {
		if err := context.Cause(ctx); err != nil {
			return pl, err
		}
		// Limit the time the consumer blocks waiting for a new batch. If not set, the consumer will hang
		// when it lands on an inactive partition.
		ctx1, cancel := context.WithTimeout(ctx, b.cfg.Kafka.PollTimeout)
		fetches := client.PollFetches(ctx1)
		cancel()

		if fetches.IsClientClosed() {
			level.Warn(b.logger).Log("msg", "client closed when fetching records")
			return pl, nil
		}

		fetches.EachError(func(_ string, _ int32, err error) {
			if !errors.Is(err, context.DeadlineExceeded) {
				level.Error(b.logger).Log("msg", "failed to fetch records", "part", part, "err", err)
				b.metrics.fetchErrors.Inc()
			}
		})

		numRecs := fetches.NumRecords()
		if numRecs == 0 && lag <= 0 {
			level.Warn(b.logger).Log("msg", "got empty fetches from broker", "part", part)
			break
		}

		b.metrics.fetchRecordsTotal.Add(float64(numRecs))

		recIter := fetches.RecordIter()
		for !recIter.Done() {
			rec := recIter.Next()

			if firstRec == nil {
				firstRec = rec
			}

			level.Debug(b.logger).Log("msg", "process record", "offset", rec.Offset, "rec", rec.Timestamp, "last_bmax", lastBlockEnd, "bmax", blockEnd)
			// Stop consuming after we reached the cycleEnd marker.
			// NOTE: the timestamp of the record is when the record was produced relative to distributor's time.
			if rec.Timestamp.After(cycleEnd) {
				done = true
				break
			}

			lag--

			recProcessedBefore := rec.Offset <= seenTillOffset
			allSamplesProcessed, err := builder.process(ctx, rec, lastBlockEnd, blockEnd, recProcessedBefore)
			if err != nil {
				// TODO(codesome): do we just ignore this? What if it was Mimir's issue and this leading to data loss?
				level.Error(b.logger).Log("msg", "failed to process record", "part", part, "key", string(rec.Key), "err", err)
				continue
			}
			if !allSamplesProcessed && commitRec == nil {
				// If block builder restarts, it will start consuming from the record after this from kafka (from the commit record).
				// So the commit record should be the last record that was fully processed and not the
				// first record that was not fully processed.
				commitRec = lastRec

				// The first record itself was not fully processed, meaning the record before
				// this is the commit point.
				if commitRec == nil {
					commitRec = &kgo.Record{
						Timestamp:   time.UnixMilli(pl.CommitRecTs),
						Topic:       lastCommit.Topic,
						Partition:   lastCommit.Partition,
						LeaderEpoch: lastCommit.LeaderEpoch,
						Offset:      lastCommit.At - 1, // TODO(v): lastCommit.At can be zero if there wasn't any commit yet
					}
				}
			}
			lastRec = rec
		}
	}

	compactStart := time.Now()
	var err error
	numBlocks, err = builder.compactAndUpload(ctx, b.blockUploaderForUser)
	if err != nil {
		return pl, err
	}
	compactionDur = time.Since(compactStart)
	b.metrics.compactAndUploadDuration.WithLabelValues(fmt.Sprintf("%d", part)).Observe(compactionDur.Seconds())

	// Nothing was processed, including that there was not discarded record.
	if lastRec == nil && firstRec == nil {
		level.Info(b.logger).Log("msg", "no records were processed in consumePartition", "part", part)
		return pl, nil
	}

	// No records were for this cycle.
	if lastRec == nil {
		// The first record fetched was from the next cycle.
		// Rewind partition's offset and re-consume this record again on the next cycle.
		// No need to re-commit since the commit point did not change.
		level.Info(b.logger).Log("msg", "skip commit record due to first record fetched is from next cycle", "part", part, "first_rec_offset", firstRec.Offset)
		rec := kgo.EpochOffset{
			Epoch:  firstRec.LeaderEpoch,
			Offset: firstRec.Offset,
		}
		b.seekPartition(client, part, rec)
		return pl, nil
	}

	// All samples in all records were processed. We can commit the last record's offset.
	if commitRec == nil {
		commitRec = lastRec
	}

	// If there were records that we consumed but didn't process, we must rewind the partition's offset
	// to the commit record. This is so on the next cycle, when the partition is read again, the consumer
	// starts at the commit point.
	defer func() {
		rec := kgo.EpochOffset{
			Epoch:  commitRec.LeaderEpoch,
			Offset: commitRec.Offset + 1, // offset+1 means everything up (including) to commitRec was processed
		}
		b.seekPartition(client, part, rec)
	}()

	// We should take the max of "seen till" offset. If the partition was lagging
	// due to some record not being processed because of a future sample, we might be
	// coming back to the same consume cycle again.
	commitSeenTillOffset := seenTillOffset
	if lastRec != nil && commitSeenTillOffset < lastRec.Offset {
		commitSeenTillOffset = lastRec.Offset
	}
	// Take the max of block max times because of same reasons above.
	commitBlockEnd := blockEnd
	if lastBlockEnd > blockEnd {
		commitBlockEnd = lastBlockEnd
	}

	pl = partitionInfo{
		Partition: pl.Partition,
		Lag:       lag,
		Commit: kadm.Offset{
			Topic:       commitRec.Topic,
			Partition:   commitRec.Partition,
			At:          commitRec.Offset + 1,
			LeaderEpoch: commitRec.LeaderEpoch,
			Metadata:    marshallCommitMeta(commitRec.Timestamp.UnixMilli(), commitSeenTillOffset, commitBlockEnd),
		},
		CommitRecTs:    commitRec.Timestamp.UnixMilli(),
		SeenTillOffset: commitSeenTillOffset,
		LastBlockEnd:   commitBlockEnd,
	}

	return pl, commitOffset(ctx, b.logger, client, b.cfg.Kafka.ConsumerGroup, pl.Commit)
}

func (b *BlockBuilder) seekPartition(client *kgo.Client, part int32, rec kgo.EpochOffset) {
	offsets := map[string]map[int32]kgo.EpochOffset{
		b.cfg.Kafka.Topic: {
			part: rec,
		},
	}
	client.SetOffsets(offsets)
}

func (b *BlockBuilder) blockUploaderForUser(ctx context.Context, userID string) blockUploader {
	buc := bucket.NewUserBucketClient(userID, b.bucket, b.limits)
	return func(blockDir string) error {
		meta, err := block.ReadMetaFromDir(blockDir)
		if err != nil {
			return err
		}

		if meta.Stats.NumSamples == 0 {
			// No need to upload empty block.
			return nil
		}

		meta.Thanos.Source = block.BlockBuilderSource
		meta.Thanos.SegmentFiles = block.GetSegmentFiles(blockDir)

		if meta.Compaction.FromOutOfOrder() && b.limits.OutOfOrderBlocksExternalLabelEnabled(userID) {
			// At this point the OOO data was already ingested and compacted, so there's no point in checking for the OOO feature flag
			meta.Thanos.Labels[mimir_tsdb.OutOfOrderExternalLabel] = mimir_tsdb.OutOfOrderExternalLabelValue
		}

		// Upload block with custom metadata.
		return block.Upload(ctx, b.logger, buc, blockDir, meta)
	}
}

func commitOffset(ctx context.Context, l log.Logger, client *kgo.Client, group string, offset kadm.Offset) error {
	offsets := make(kadm.Offsets)
	offsets.Add(offset)
	if err := kadm.NewClient(client).CommitAllOffsets(ctx, group, offsets); err != nil {
		return fmt.Errorf("commit with part %d, offset %d: %w", offset.Partition, offset.At, err)
	}

	level.Info(l).Log("msg", "successfully committed to Kafka", "part", offset.Partition, "offset", offset.At)

	return nil
}

const (
	kafkaCommitMetaV1 = 1
)

// commitRecTs: timestamp of the record which was committed (and not the commit time).
// lastRecOffset: offset of the last record processed (which will be >= commit record offset).
// blockEnd: timestamp of the block end in this cycle.
func marshallCommitMeta(commitRecTs, lastRecOffset, blockEnd int64) string {
	return fmt.Sprintf("%d,%d,%d,%d", kafkaCommitMetaV1, commitRecTs, lastRecOffset, blockEnd)
}

// commitRecTs: timestamp of the record which was committed (and not the commit time).
// lastRecOffset: offset of the last record processed (which will be >= commit record offset).
// blockEnd: timestamp of the block end in this cycle.
func unmarshallCommitMeta(meta string) (commitRecTs, lastRecOffset, blockEnd int64, err error) {
	if meta == "" {
		return
	}
	var (
		version int
		s       string
	)
	_, err = fmt.Sscanf(meta, "%d,%s", &version, &s)
	if err != nil {
		return
	}

	switch version {
	case kafkaCommitMetaV1:
		_, err = fmt.Sscanf(s, "%d,%d,%d", &commitRecTs, &lastRecOffset, &blockEnd)
	default:
		err = fmt.Errorf("unsupported commit meta version %d", version)
	}
	return
}

type Config struct {
	InstanceID            string        `yaml:"instance_id" doc:"default=<hostname>" category:"advanced"`
	ConsumeInterval       time.Duration `yaml:"consume_interval"`
	ConsumeIntervalBuffer time.Duration `yaml:"consume_interval_buffer"`
	LookbackOnNoCommit    time.Duration `yaml:"lookback_on_no_commit" category:"advanced"`

	Kafka               KafkaConfig                    `yaml:"kafka"`
	BlocksStorageConfig mimir_tsdb.BlocksStorageConfig `yaml:"-"` // TODO(codesome): check how this is passed. Copied over form ingester.
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	hostname, err := os.Hostname()
	if err != nil {
		level.Error(logger).Log("msg", "failed to get hostname", "err", err)
		os.Exit(1)
	}

	cfg.Kafka.RegisterFlagsWithPrefix("block-builder.", f)

	f.StringVar(&cfg.InstanceID, "block-builder.instance-id", hostname, "Instance id.")
	f.DurationVar(&cfg.ConsumeInterval, "block-builder.consume-interval", time.Hour, "Interval between block consumption cycles.")
	f.DurationVar(&cfg.ConsumeIntervalBuffer, "block-builder.consume-interval-buffer", 15*time.Minute, "Extra buffer between subsequent block consumption cycles to avoid small blocks.")
	f.DurationVar(&cfg.LookbackOnNoCommit, "block-builder.lookback-on-no-commit", 12*time.Hour, "How much of the historical records to look back when there is no kafka commit for a partition.")
}

func (cfg *Config) Validate() error {
	if err := cfg.Kafka.Validate(); err != nil {
		return err
	}
	// TODO(codesome): validate the consumption interval. Must be <=2h and can divide 2h into an integer.
	if cfg.ConsumeInterval < 0 {
		return fmt.Errorf("-consume-interval must be non-negative")
	}

	return nil
}

type KafkaConfig struct {
	Address             string          `yaml:"address"`
	Topic               string          `yaml:"topic"`
	ClientID            string          `yaml:"client_id"`
	DialTimeout         time.Duration   `yaml:"dial_timeout"`
	PollTimeout         time.Duration   `yaml:"poll_timeout"`
	ConsumerGroup       string          `yaml:"consumer_group"`
	PartitionAssignment map[int][]int32 `yaml:"partition_assignment" category:"experimental"`
}

func (cfg *KafkaConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.Address, prefix+"kafka.address", "", "The Kafka seed broker address.")
	f.StringVar(&cfg.Topic, prefix+"kafka.topic", "", "The Kafka topic name.")
	f.StringVar(&cfg.ClientID, prefix+"kafka.client-id", "", "The Kafka client ID.")
	f.DurationVar(&cfg.DialTimeout, prefix+"kafka.dial-timeout", 5*time.Second, "The maximum time allowed to open a connection to a Kafka broker.")
	f.DurationVar(&cfg.PollTimeout, prefix+"kafka.poll-timeout", 5*time.Second, "The maximum time allowed to block if data is not available in the broker to consume.")
	f.StringVar(&cfg.ConsumerGroup, prefix+"kafka.consumer-group", "", "The consumer group used by the consumer to track the last consumed offset.")
	f.Var(newPartitionAssignmentVar(&cfg.PartitionAssignment), prefix+"kafka.partition-assignment", "Static partition assignment map.")
}

func (cfg *KafkaConfig) Validate() error {
	if len(cfg.PartitionAssignment) == 0 {
		return fmt.Errorf("partition assignment is required")
	}
	return nil
}

type partitionAssignmentVar map[int][]int32

func newPartitionAssignmentVar(p *map[int][]int32) *partitionAssignmentVar {
	return (*partitionAssignmentVar)(p)
}

func (v *partitionAssignmentVar) Set(s string) error {
	if s == "" {
		return nil
	}
	val := make(map[int][]int32)
	err := json.Unmarshal([]byte(s), &val)
	if err != nil {
		return err
	}
	*v = val
	return nil
}

func (v partitionAssignmentVar) String() string {
	return fmt.Sprintf("%v", map[int][]int32(v))
}

type kafkaLogger struct {
	logger log.Logger
}

func newKafkaLogger(logger log.Logger) *kafkaLogger {
	return &kafkaLogger{
		logger: log.With(logger, "component", "kafka_client"),
	}
}

func (l *kafkaLogger) Level() kgo.LogLevel {
	// The Kafka client calls Level() to check whether debug level is enabled or not.
	// To keep it simple, we always return Info, so the Kafka client will never try
	// to log expensive debug messages.
	return kgo.LogLevelInfo
}

func (l *kafkaLogger) Log(lev kgo.LogLevel, msg string, keyvals ...any) {
	if lev == kgo.LogLevelNone {
		return
	}
	keyvals = append([]any{"msg", msg}, keyvals...)
	switch lev {
	case kgo.LogLevelDebug:
		level.Debug(l.logger).Log(keyvals...)
	case kgo.LogLevelInfo:
		level.Info(l.logger).Log(keyvals...)
	case kgo.LogLevelWarn:
		level.Warn(l.logger).Log(keyvals...)
	case kgo.LogLevelError:
		level.Error(l.logger).Log(keyvals...)
	}
}
