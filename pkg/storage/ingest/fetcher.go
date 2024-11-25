// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/instrument"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/plugin/kotel"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/util/spanlogger"
)

const (
	// unknownBroker duplicates a constant from franz-go because it isn't exported.
	unknownBroker = "unknown broker"

	// chosenBrokerDied duplicates a constant from franz-go because it isn't exported.
	chosenBrokerDied = "the internal broker struct chosen to issue this request has died--either the broker id is migrating or no longer exists"

	// initialBytesPerRecord is the initial number of estimated bytes per record.
	// We start with an estimation, we will update it as we consume.
	initialBytesPerRecord = 10_000

	// forcedMinValueForMaxBytes is the lowest value we set in Fetch request's MaxBytes.
	// When we're fetching few records, we can afford to over-fetch to avoid more requests.
	forcedMinValueForMaxBytes = 1_000_000
)

type fetcher interface {
	// PollFetches fetches records from Kafka and returns them.
	// The returned context is the context of fetching. It can also be equal to the passed context.
	// The returned context may contain spans that were used to fetch from Kafka.
	// Each record in the returned fetches also contains a context.
	// You should use that when doing something specific to a tenant or a
	// record and use the returned context when doing something that is common to all records.
	PollFetches(context.Context) (kgo.Fetches, context.Context)

	// Update updates the fetcher with the given concurrency.
	Update(ctx context.Context, concurrency int)

	// Stop stops the fetcher.
	Stop()

	// BufferedRecords returns the number of records that have been fetched but not yet consumed.
	BufferedRecords() int64

	// BufferedBytes returns the number of bytes that have been fetched but not yet consumed.
	BufferedBytes() int64

	// BytesPerRecord returns the current estimation for how many bytes each record is.
	BytesPerRecord() int64
}

// fetchWant represents a range of offsets to fetch.
// Based on a given number of records, it tries to estimate how many bytes we need to fetch, given there's no support for fetching offsets directly.
// fetchWant also contains the channel on which to send the fetched records for the offset range.
type fetchWant struct {
	startOffset             int64 // inclusive
	endOffset               int64 // exclusive
	estimatedBytesPerRecord int
	targetMaxBytes          int

	// result should be closed when there are no more fetches for this partition. It is ok to send multiple times on the channel.
	result chan fetchResult
}

func fetchWantFrom(offset int64, targetMaxBytes, estimatedBytesPerRecord int) fetchWant {
	estimatedBytesPerRecord = max(estimatedBytesPerRecord, 1)
	estimatedNumberOfRecords := max(1, targetMaxBytes/estimatedBytesPerRecord)
	return fetchWant{
		startOffset:             offset,
		endOffset:               offset + int64(estimatedNumberOfRecords),
		targetMaxBytes:          targetMaxBytes,
		estimatedBytesPerRecord: estimatedBytesPerRecord,
		result:                  make(chan fetchResult),
	}
}

// Next returns the fetchWant for the next numRecords starting from the last known offset.
func (w fetchWant) Next() fetchWant {
	n := fetchWantFrom(w.endOffset, w.targetMaxBytes, w.estimatedBytesPerRecord)
	n.estimatedBytesPerRecord = w.estimatedBytesPerRecord
	return n
}

// MaxBytes returns the maximum number of bytes we can fetch in a single request.
// It's capped at math.MaxInt32 to avoid overflow, and it'll always fetch a minimum of 1MB.
func (w fetchWant) MaxBytes() int32 {
	fetchBytes := w.expectedBytes()
	if fetchBytes > math.MaxInt32 || fetchBytes < 0 {
		// This shouldn't happen because w should have been trimmed before sending the request.
		// But we definitely don't want to request negative bytes by casting to int32, so add this safeguard.
		return math.MaxInt32
	}
	fetchBytes = max(forcedMinValueForMaxBytes, fetchBytes)
	return int32(fetchBytes)
}

// UpdateBytesPerRecord updates the expected bytes per record based on the results of the last fetch and trims the fetchWant if MaxBytes() would now exceed math.MaxInt32.
func (w fetchWant) UpdateBytesPerRecord(lastFetchBytes int, lastFetchNumberOfRecords int) fetchWant {
	// Smooth over the estimation to avoid having outlier fetches from throwing off the estimation.
	// We don't want a fetch of 5 records to determine how we fetch the next fetch of 6000 records.
	// Ideally we weigh the estimation on the number of records observed, but it's simpler to smooth it over with a constant factor.
	const currentEstimateWeight = 0.8

	actualBytesPerRecord := float64(lastFetchBytes) / float64(lastFetchNumberOfRecords)
	w.estimatedBytesPerRecord = int(currentEstimateWeight*float64(w.estimatedBytesPerRecord) + (1-currentEstimateWeight)*actualBytesPerRecord)

	return w
}

// expectedBytes returns how many bytes we'd need to accommodate the range of offsets using estimatedBytesPerRecord.
// They may be more than the kafka protocol supports (> MaxInt32). Use MaxBytes.
func (w fetchWant) expectedBytes() int64 {
	// We over-fetch bytes to reduce the likelihood of under-fetching and having to run another request.
	// Based on some testing 65% of under-estimations are by less than 5%. So we account for that.
	const overFetchBytesFactor = 1.05
	return int64(overFetchBytesFactor * float64(int64(w.estimatedBytesPerRecord)*(w.endOffset-w.startOffset)))
}

type fetchResult struct {
	kgo.FetchPartition
	ctx          context.Context
	fetchedBytes int

	waitingToBePickedUpFromOrderedFetchesSpan opentracing.Span
}

func (fr *fetchResult) logCompletedFetch(fetchStartTime time.Time, w fetchWant) {
	var logger log.Logger = spanlogger.FromContext(fr.ctx, log.NewNopLogger())

	msg := "fetched records"
	if fr.Err != nil {
		msg = "received an error while fetching records; will retry after processing received records (if any)"
	}
	var (
		gotRecords   = int64(len(fr.Records))
		askedRecords = w.endOffset - w.startOffset
	)
	switch {
	case fr.Err == nil, errors.Is(fr.Err, kerr.OffsetOutOfRange):
		logger = level.Debug(logger)
	default:
		logger = level.Error(logger)
	}
	var firstTimestamp, lastTimestamp string
	if gotRecords > 0 {
		firstTimestamp = fr.Records[0].Timestamp.String()
		lastTimestamp = fr.Records[gotRecords-1].Timestamp.String()
	}
	logger.Log(
		"msg", msg,
		"duration", time.Since(fetchStartTime),
		"start_offset", w.startOffset,
		"end_offset", w.endOffset,
		"asked_records", askedRecords,
		"got_records", gotRecords,
		"diff_records", askedRecords-gotRecords,
		"asked_bytes", w.MaxBytes(),
		"got_bytes", fr.fetchedBytes,
		"diff_bytes", int(w.MaxBytes())-fr.fetchedBytes,
		"first_timestamp", firstTimestamp,
		"last_timestamp", lastTimestamp,
		"hwm", fr.HighWatermark,
		"lso", fr.LogStartOffset,
		"err", fr.Err,
	)
}

func (fr *fetchResult) startWaitingForConsumption() {
	fr.waitingToBePickedUpFromOrderedFetchesSpan, fr.ctx = opentracing.StartSpanFromContext(fr.ctx, "fetchResult.waitingForConsumption")
}

func (fr *fetchResult) finishWaitingForConsumption() {
	if fr.waitingToBePickedUpFromOrderedFetchesSpan == nil {
		fr.waitingToBePickedUpFromOrderedFetchesSpan, fr.ctx = opentracing.StartSpanFromContext(fr.ctx, "fetchResult.noWaitingForConsumption")
	}
	fr.waitingToBePickedUpFromOrderedFetchesSpan.Finish()
}

// Merge merges older with into fr. Merge keeps most of the fields of fr and assumes they
// are more up-to-date than older.
//
// This function panics if one of the two fetchResult has Err set (errors MUST not be merged).
func (fr *fetchResult) Merge(older fetchResult) fetchResult {
	if fr.Err != nil {
		panic("fetchResult.Merge() has been called on a fetchResult with Err set")
	}
	if older.Err != nil {
		panic("fetchResult.Merge() has been called with older fetchResult with Err set")
	}

	if older.ctx != nil {
		level.Debug(spanlogger.FromContext(older.ctx, log.NewNopLogger())).Log("msg", "merged fetch result with the next result")
	}

	// older.Records are older than fr.Records, so we append them first.
	fr.Records = append(older.Records, fr.Records...)

	// We ignore HighWatermark, LogStartOffset, LastStableOffset because this result should be more up to date.
	fr.fetchedBytes += older.fetchedBytes
	return *fr
}

// newEmptyFetchResult creates a new fetchResult with empty partition and no error set.
func newEmptyFetchResult(ctx context.Context, partitionID int32) fetchResult {
	return fetchResult{
		ctx:          ctx,
		fetchedBytes: 0,
		FetchPartition: kgo.FetchPartition{
			Partition: partitionID,
		},
	}
}

// newEmptyFetchResult creates a new fetchResult with a partition with the input error set.
// This function intentionally panics if the error is not provided (it's a logic bug).
func newErrorFetchResult(ctx context.Context, partitionID int32, err error) fetchResult {
	if err == nil {
		panic("an error must be provided to newErrorFetchResult()")
	}

	return fetchResult{
		ctx:          ctx,
		fetchedBytes: 0,
		FetchPartition: kgo.FetchPartition{
			Partition: partitionID,
			Err:       err,
		},
	}
}

type concurrentFetchers struct {
	wg   sync.WaitGroup
	done chan struct{}

	client      *kgo.Client
	logger      log.Logger
	partitionID int32
	topicID     [16]byte
	topicName   string
	metrics     *readerMetrics
	tracer      *kotel.Tracer

	minBytesWaitTime time.Duration

	// orderedFetches is a channel where we write fetches that are ready to be polled by PollFetches().
	// Since all records must be polled in order, the fetches written to this channel are after
	// ordering.
	orderedFetches chan fetchResult

	lastReturnedOffset int64
	startOffsets       *genericOffsetReader[int64]

	// fetchBackoffConfig is the config to use for the backoff in case of Fetch errors.
	// We set it here so that tests can override it run faster.
	fetchBackoffConfig backoff.Config

	// trackCompressedBytes controls whether to calculate MaxBytes for fetch requests based on previous responses' compressed or uncompressed bytes.
	trackCompressedBytes  bool
	maxBufferedBytesLimit int32

	bufferedFetchedRecords  *atomic.Int64
	bufferedFetchedBytes    *atomic.Int64
	estimatedBytesPerRecord *atomic.Int64
}

// newConcurrentFetchers creates a new concurrentFetchers. startOffset can be kafkaOffsetStart, kafkaOffsetEnd or a specific offset.
func newConcurrentFetchers(
	ctx context.Context,
	client *kgo.Client,
	logger log.Logger,
	topic string,
	partition int32,
	startOffset int64,
	concurrency int,
	maxBufferedBytesLimit int32,
	trackCompressedBytes bool,
	minBytesWaitTime time.Duration,
	offsetReader *partitionOffsetClient,
	startOffsetsReader *genericOffsetReader[int64],
	fetchBackoffConfig backoff.Config,
	metrics *readerMetrics,
) (*concurrentFetchers, error) {
	if fetchBackoffConfig.MaxBackoff == 0 {
		// Ensure it's not the zero value, which means we haven't got the backoff config due to a bug.
		return nil, errors.New("fetchBackoffConfig.MaxBackoff has not been set")
	}
	if fetchBackoffConfig.MaxRetries != 0 {
		// It's critical for concurrentFetchers that failed Fetch requests are retried forever.
		return nil, errors.New("fetchBackoffConfig.MaxRetries must be 0")
	}

	var err error
	switch startOffset {
	case kafkaOffsetStart:
		startOffset, err = offsetReader.FetchPartitionStartOffset(ctx, partition)
	case kafkaOffsetEnd:
		startOffset, err = offsetReader.FetchPartitionLastProducedOffset(ctx, partition)
		// End (-1) means "ignore all existing records". FetchPartitionLastProducedOffset returns the offset of an existing record.
		// We need to start from the next one, which is still not produced.
		startOffset++
	}
	if err != nil {
		return nil, fmt.Errorf("resolving offset to start consuming from: %w", err)
	}

	if maxBufferedBytesLimit <= 0 {
		maxBufferedBytesLimit = math.MaxInt32
	}
	f := &concurrentFetchers{
		bufferedFetchedRecords:  atomic.NewInt64(0),
		bufferedFetchedBytes:    atomic.NewInt64(0),
		estimatedBytesPerRecord: atomic.NewInt64(0),
		client:                  client,
		logger:                  logger,
		topicName:               topic,
		partitionID:             partition,
		metrics:                 metrics,
		minBytesWaitTime:        minBytesWaitTime,
		lastReturnedOffset:      startOffset - 1,
		startOffsets:            startOffsetsReader,
		trackCompressedBytes:    trackCompressedBytes,
		maxBufferedBytesLimit:   maxBufferedBytesLimit,
		tracer:                  recordsTracer(),
		orderedFetches:          make(chan fetchResult),
		done:                    make(chan struct{}),
		fetchBackoffConfig:      fetchBackoffConfig,
	}

	topics, err := kadm.NewClient(client).ListTopics(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("failed to find topic ID: %w", err)
	}
	if !topics.Has(topic) {
		return nil, fmt.Errorf("failed to find topic ID: topic not found")
	}
	if err := topics.Error(); err != nil {
		return nil, fmt.Errorf("failed to find topic ID: %w", err)
	}
	f.topicID = topics[topic].ID

	f.wg.Add(1)
	go f.start(ctx, startOffset, concurrency)

	return f, nil
}

// BufferedRecords implements fetcher.
func (r *concurrentFetchers) BufferedRecords() int64 {
	return r.bufferedFetchedRecords.Load()
}

// BufferedBytes implements fetcher.
func (r *concurrentFetchers) BufferedBytes() int64 {
	return r.bufferedFetchedBytes.Load()
}

func (r *concurrentFetchers) BytesPerRecord() int64 {
	return r.estimatedBytesPerRecord.Load()
}

// Stop implements fetcher.
func (r *concurrentFetchers) Stop() {
	// Ensure it's not already stopped.
	select {
	case _, ok := <-r.done:
		if !ok {
			return
		}
	default:
	}

	close(r.done)
	r.wg.Wait()

	// When the fetcher is stopped, buffered records are intentionally dropped. For this reason,
	// we do reset the counter of buffered records here.
	r.bufferedFetchedRecords.Store(0)
	r.bufferedFetchedBytes.Store(0)

	level.Info(r.logger).Log("msg", "stopped concurrent fetchers", "last_returned_offset", r.lastReturnedOffset)
}

// Update implements fetcher
func (r *concurrentFetchers) Update(ctx context.Context, concurrency int) {
	r.Stop()
	r.done = make(chan struct{})

	r.wg.Add(1)
	go r.start(ctx, r.lastReturnedOffset+1, concurrency)
}

// PollFetches implements fetcher
func (r *concurrentFetchers) PollFetches(ctx context.Context) (kgo.Fetches, context.Context) {
	waitStartTime := time.Now()
	select {
	case <-ctx.Done():
		return kgo.Fetches{}, ctx
	case f := <-r.orderedFetches:
		// The records have been polled from the buffer, so we can now decrease the number of
		// buffered records. It's important to note that we decrease it by the number of actually
		// buffered records and not by the number of records returned by PollFetchers(), which
		// could be lower if some records are discarded because "old" (already returned by previous
		// PollFetches() calls).
		r.bufferedFetchedRecords.Sub(int64(len(f.Records)))

		firstUnreturnedRecordIdx := recordIndexAfterOffset(f.Records, r.lastReturnedOffset)
		r.recordOrderedFetchTelemetry(f, firstUnreturnedRecordIdx, waitStartTime)

		f.Records = f.Records[firstUnreturnedRecordIdx:]
		if len(f.Records) > 0 {
			r.lastReturnedOffset = f.Records[len(f.Records)-1].Offset
		}

		return kgo.Fetches{{
			Topics: []kgo.FetchTopic{
				{
					Topic:      r.topicName,
					Partitions: []kgo.FetchPartition{f.FetchPartition},
				},
			},
		}}, f.ctx
	}
}

func instrumentGaps(gaps []offsetRange, records prometheus.Counter, logger log.Logger) {
	for _, gap := range gaps {
		level.Error(logger).Log(
			"msg", "there is a gap in consumed offsets; it is likely that there was data loss; see runbook for MimirIngesterMissedRecordsFromKafka",
			"records_offset_gap_start_inclusive", gap.start,
			"records_offset_gap_end_exclusive", gap.end,
		)
		records.Add(float64(gap.numOffsets()))
	}
}

type offsetRange struct {
	// start is inclusive
	start int64

	// end is exclusive
	end int64
}

func (g offsetRange) numOffsets() int64 {
	return g.end - g.start
}

func findGapsInRecords(records kgo.Fetches, lastSeenOffset int64) []offsetRange {
	var gaps []offsetRange
	if lastSeenOffset < 0 && records.NumRecords() > 0 {
		// The offset may be -1 when we haven't seen ANY offsets. In this case we don't know what's the first available offset.
		// So we assume that the first record is the first offset from the ones we're being passed.
		firstRecord := records.RecordIter().Next()
		lastSeenOffset = firstRecord.Offset
	}
	records.EachRecord(func(r *kgo.Record) {
		if r.Offset > lastSeenOffset+1 {
			gaps = append(gaps, offsetRange{start: lastSeenOffset + 1, end: r.Offset})
		}
		lastSeenOffset = r.Offset
	})
	return gaps
}

func recordIndexAfterOffset(records []*kgo.Record, offset int64) int {
	for i, r := range records {
		if r.Offset > offset {
			return i
		}
	}
	return len(records)
}

func (r *concurrentFetchers) recordOrderedFetchTelemetry(f fetchResult, firstReturnedRecordIndex int, waitStartTime time.Time) {
	waitDuration := time.Since(waitStartTime)
	level.Debug(r.logger).Log("msg", "received ordered fetch", "num_records", len(f.Records), "wait_duration", waitDuration)
	instrument.ObserveWithExemplar(f.ctx, r.metrics.fetchWaitDuration, waitDuration.Seconds())

	var (
		doubleFetchedBytes             = 0
		skippedRecordsCount            = 0
		firstSkippedRecordOffset int64 = -1
		lastSkippedRecordOffset  int64 = -1
	)

	for i, record := range f.Records {
		if i < firstReturnedRecordIndex {
			doubleFetchedBytes += len(record.Value)

			// Keep track of first/last skipped record offsets, just for debugging purposes.
			skippedRecordsCount++
			lastSkippedRecordOffset = record.Offset
			if firstSkippedRecordOffset < 0 {
				firstSkippedRecordOffset = record.Offset
			}
		}
		r.tracer.OnFetchRecordUnbuffered(record, true)
	}
	r.metrics.fetchedDiscardedRecordBytes.Add(float64(doubleFetchedBytes))

	if skippedRecordsCount > 0 {
		spanlogger.FromContext(f.Records[0].Context, r.logger).DebugLog(
			"msg", "skipped records because it is already returned",
			"skipped_records_count", skippedRecordsCount,
			"first_skipped_offset", firstSkippedRecordOffset,
			"last_skipped_offset", lastSkippedRecordOffset)
	}
}

// fetchSingle attempts to find out the leader Kafka broker for a partition and then sends a fetch request
// to the leader to fetch the range of records requested in the input fetchWant. If the request is successful,
// it then parses the responses.
//
// The returned fetchResult may be a success or failure. If it's a success, it may or may not fulfil the
// entire fetchWant (only a subset of records may have been fetched, potentially even none of them). If it's
// a failure, the fetchResult.Err is valued. This function guarantees that the returned fetchResult doesn't
// contain any record in case an error is also returned.
//
// If ctx is cancelled, fetchSingle will return an empty fetchResult without an error.
func (r *concurrentFetchers) fetchSingle(ctx context.Context, fw fetchWant) (fr fetchResult) {
	defer func(fetchStartTime time.Time) {
		fr.logCompletedFetch(fetchStartTime, fw)
	}(time.Now())

	// Lookup the leader broker for the topic partition.
	leaderID, leaderEpoch, err := r.client.PartitionLeader(r.topicName, r.partitionID)
	if err != nil || (leaderID == -1 && leaderEpoch == -1) {
		if err != nil {
			return newErrorFetchResult(ctx, r.partitionID, fmt.Errorf("finding leader for partition: %w", err))
		}
		return newErrorFetchResult(ctx, r.partitionID, errUnknownPartitionLeader)
	}

	// Build and send the Fetch request to the leader broker.
	req := r.buildFetchRequest(fw, leaderEpoch)
	resp, err := req.RequestWith(ctx, r.client.Broker(int(leaderID)))
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return newEmptyFetchResult(ctx, r.partitionID)
		}
		return newErrorFetchResult(ctx, r.partitionID, fmt.Errorf("fetching from kafka: %w", err))
	}

	return r.parseFetchResponse(ctx, fw.startOffset, resp)
}

func (r *concurrentFetchers) buildFetchRequest(fw fetchWant, leaderEpoch int32) kmsg.FetchRequest {
	req := kmsg.NewFetchRequest()
	req.MinBytes = 1 // Warpstream ignores this field. This means that the WaitTime below is always waited and MaxBytes play a bigger role in how fast Ws responds.
	req.Version = 13
	req.MaxWaitMillis = int32(r.minBytesWaitTime / time.Millisecond)
	req.MaxBytes = fw.MaxBytes()

	reqTopic := kmsg.NewFetchRequestTopic()
	reqTopic.Topic = r.topicName
	reqTopic.TopicID = r.topicID

	reqPartition := kmsg.NewFetchRequestTopicPartition()
	reqPartition.Partition = r.partitionID
	reqPartition.FetchOffset = fw.startOffset
	reqPartition.PartitionMaxBytes = req.MaxBytes
	reqPartition.CurrentLeaderEpoch = leaderEpoch

	reqTopic.Partitions = append(reqTopic.Partitions, reqPartition)
	req.Topics = append(req.Topics, reqTopic)
	return req
}

// This function guarantees that the returned fetchResult doesn't contain any record in case an error is also returned.
func (r *concurrentFetchers) parseFetchResponse(ctx context.Context, startOffset int64, resp *kmsg.FetchResponse) fetchResult {
	// We ignore rawPartitionResp.PreferredReadReplica to keep the code simpler. We don't provide any rack in the FetchRequest,
	// so the broker _probably_ doesn't have a recommended replica for us.

	// Ensure we got the expected partition and no error occurred. If we get something we didn't expect, maybe we're sending
	// the wrong request or there's a bug in the kafka implementation. Even in case of errors we get the topic partition.
	if resp.ErrorCode != 0 {
		// The FetchResponse.ErrorCode should be never set for our use case. This error code was added for support for KIP-227
		// and is only set if we're using fetch sessions. We don't use fetch sessions. However, we check it anyway to make
		// in case it will change in future (or some Kafka-compatible backends set it even when sessions are not in use).
		return newErrorFetchResult(ctx, r.partitionID, fmt.Errorf("received error code %d", resp.ErrorCode))
	}

	// Sanity check for the response we get.
	if expected, actual := 1, len(resp.Topics); expected != actual {
		return newErrorFetchResult(ctx, r.partitionID, fmt.Errorf("unexpected number of topics in the Fetch response (expected: %d got: %d)", expected, actual))
	}
	if expected, actual := r.topicID, resp.Topics[0].TopicID; expected != actual {
		return newErrorFetchResult(ctx, r.partitionID, fmt.Errorf("unexpected topic ID in the Fetch response (expected: %s got %s)", expected, actual))
	}
	if expected, actual := 1, len(resp.Topics[0].Partitions); expected != actual {
		return newErrorFetchResult(ctx, r.partitionID, fmt.Errorf("unexpected number of partitions in the Fetch response (expected: %d got: %d)", expected, actual))
	}
	if expected, actual := r.partitionID, resp.Topics[0].Partitions[0].Partition; expected != actual {
		return newErrorFetchResult(ctx, r.partitionID, fmt.Errorf("unexpected partition ID in the Fetch response (expected: %d got %d)", expected, actual))
	}
	if code := resp.Topics[0].Partitions[0].ErrorCode; code != 0 {
		return newErrorFetchResult(ctx, r.partitionID, fmt.Errorf("fetch request failed with error: %w", kerr.ErrorForCode(code)))
	}

	parseOptions := kgo.ProcessFetchPartitionOptions{
		KeepControlRecords: false,
		Offset:             startOffset,
		IsolationLevel:     kgo.ReadUncommitted(), // we don't produce in transactions, but leaving this here so it's explicit.
		Topic:              r.topicName,
		Partition:          r.partitionID,
	}

	observeMetrics := func(m kgo.FetchBatchMetrics) {
		brokerMeta := kgo.BrokerMetadata{} // leave it empty because kprom doesn't use it, and we don't exactly have all the metadata
		r.metrics.kprom.OnFetchBatchRead(brokerMeta, r.topicName, r.partitionID, m)
	}
	rawPartitionResp := resp.Topics[0].Partitions[0]
	partition, _ := kgo.ProcessRespPartition(parseOptions, &rawPartitionResp, observeMetrics)
	if partition.Err != nil {
		// This should never happen because we already check the ErrorCode above, but we keep this double check
		// in case Err will be set because of other reasons by kgo.ProcessRespPartition() in the future.
		return newErrorFetchResult(ctx, r.partitionID, partition.Err)
	}

	partition.EachRecord(r.tracer.OnFetchRecordBuffered)
	partition.EachRecord(func(r *kgo.Record) {
		spanlogger.FromContext(r.Context, log.NewNopLogger()).DebugLog("msg", "received record")
	})

	fetchedBytes := len(rawPartitionResp.RecordBatches)
	if !r.trackCompressedBytes {
		fetchedBytes = sumRecordLengths(partition.Records)
	}

	return fetchResult{
		ctx:            ctx,
		FetchPartition: partition,
		fetchedBytes:   fetchedBytes,
	}
}

func sumRecordLengths(records []*kgo.Record) (sum int) {
	for _, r := range records {
		sum += len(r.Value)
	}
	return sum
}

func (r *concurrentFetchers) run(ctx context.Context, wants chan fetchWant, logger log.Logger, highWatermark *atomic.Int64) {
	defer r.wg.Done()

	errBackoff := backoff.New(ctx, r.fetchBackoffConfig)

	for w := range wants {
		// Start new span for each fetchWant. We want to record the lifecycle of a single record from being fetched to being ingested.
		wantSpan, ctx := spanlogger.NewWithLogger(ctx, logger, "concurrentFetcher.fetch")
		wantSpan.SetTag("start_offset", w.startOffset)
		wantSpan.SetTag("end_offset", w.endOffset)

		// This current buffered fetchResult that has not been sent to the result channel yet.
		// This is empty at the beginning, then we merge records as soon as we receive them
		// from the Fetch response(s).
		var bufferedResult fetchResult

		for attempt := 0; errBackoff.Ongoing() && w.endOffset > w.startOffset; attempt++ {
			attemptSpan, ctx := spanlogger.NewWithLogger(ctx, logger, "concurrentFetcher.fetch.attempt")
			attemptSpan.SetTag("attempt", attempt)

			// Run a single Fetch request.
			if res := r.fetchSingle(ctx, w); res.Err != nil {
				// We got an error. We handle it and then discard this fetch result content.
				w = handleKafkaFetchErr(res.Err, w, errBackoff, r.startOffsets, r.client, attemptSpan)
			} else {
				// We increase the count of buffered records as soon as we fetch them.
				r.bufferedFetchedRecords.Add(int64(len(res.Records)))

				// Update the high watermark.
				if hwm := res.HighWatermark; hwm >= 0 {
					casHWM(highWatermark, hwm)
				}

				// Merge the last fetch result if the previous buffered result (if any).
				// Keep non-mergeable fields from res, because the last response is the most updated one.
				bufferedResult = res.Merge(bufferedResult)
			}

			if len(bufferedResult.Records) == 0 {
				// If we have no buffered records to try to send to the result channel then we retry with another
				// Fetch attempt. However, before doing it, we check if we've been told to stop (if so, we should honor it).
				attemptSpan.Finish()

				select {
				case <-r.done:
					wantSpan.Finish()
					close(w.result)
					return
				default:
				}

				continue
			}

			// Next attempt will be from the last record onwards.
			w.startOffset = bufferedResult.Records[len(bufferedResult.Records)-1].Offset + 1
			w = w.UpdateBytesPerRecord(bufferedResult.fetchedBytes, len(bufferedResult.Records)) // This takes into account the previous fetch too. This should give us a better average than using just the records from the last attempt.

			// We reset the backoff if we received any records whatsoever. A received record means _some_ success.
			// We don't want to slow down until we hit a larger error.
			errBackoff.Reset()

			select {
			case <-r.done:
				wantSpan.Finish()
				attemptSpan.Finish()
				close(w.result)
				return
			case w.result <- bufferedResult:
				bufferedResult = fetchResult{}
			case <-ctx.Done():
			default:
				if w.startOffset >= w.endOffset {
					// We've fetched all we were asked for the whole batch is ready, and we definitely have to wait to send on the channel now.
					bufferedResult.startWaitingForConsumption()
					select {
					case <-r.done:
						wantSpan.Finish()
						attemptSpan.Finish()
						close(w.result)
						return
					case w.result <- bufferedResult:
						bufferedResult = fetchResult{}
					case <-ctx.Done():
					}
				}
			}
			attemptSpan.Finish()
		}
		wantSpan.Finish()
		close(w.result)
	}
}

func casHWM(highWwatermark *atomic.Int64, newHWM int64) {
	for hwm := highWwatermark.Load(); hwm < newHWM; hwm = highWwatermark.Load() {
		if highWwatermark.CompareAndSwap(hwm, newHWM) {
			break
		}
	}
}

type inflightFetchWants struct {
	// wants is the list of all fetchResult of all inflight fetch operations. Pending results
	// are ordered in the same order these results should be returned to PollFetches(), so the first one
	// in the list is the next one that should be returned.
	wants list.List

	// bytes is the sum of the MaxBytes of all fetchWants that are currently inflight.
	bytes *atomic.Int64
}

// peekNextResult is the channel where we expect a worker will write the result of the next fetch
// operation. This result is the next result that will be returned to PollFetches(), guaranteeing
// records ordering. The channel can be closed. In this case you are expected to call removeNextResult.
func (w *inflightFetchWants) peekNextResult() chan fetchResult {
	if w.wants.Len() == 0 {
		return nil
	}
	return w.wants.Front().Value.(fetchWant).result
}

func (w *inflightFetchWants) count() int {
	return w.wants.Len()
}

func (w *inflightFetchWants) append(nextFetch fetchWant) {
	w.bytes.Add(int64(nextFetch.MaxBytes()))
	w.wants.PushBack(nextFetch)
}

func (w *inflightFetchWants) removeNextResult() {
	head := w.wants.Front()
	// The MaxBytes of the fetchWant might have changed as it was being fetched (e.g. UpdateBytesPerRecord).
	// But we don't care about that here because we're only interested in the MaxBytes when the fetchWant was added to the inflight fetchWants.
	w.bytes.Sub(int64(head.Value.(fetchWant).MaxBytes()))
	w.wants.Remove(head)
}

func (r *concurrentFetchers) start(ctx context.Context, startOffset int64, concurrency int) {
	targetBytesPerFetcher := int(r.maxBufferedBytesLimit) / concurrency
	level.Info(r.logger).Log("msg", "starting concurrent fetchers", "start_offset", startOffset, "concurrency", concurrency, "bytes_per_fetch_request", targetBytesPerFetcher)

	// HWM is updated by the fetchers. A value of 0 is the same as there not being any produced records.
	// A value of 0 doesn't prevent progress because we ensure there is at least one inflight fetchWant.
	highWatermark := atomic.NewInt64(0)

	wants := make(chan fetchWant)
	defer close(wants)
	r.wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		logger := log.With(r.logger, "fetcher", i)
		go r.run(ctx, wants, logger, highWatermark)
	}

	// We need to make sure we don't leak any goroutine given that start is called within a goroutine.
	defer r.wg.Done()

	var (
		// nextFetch is the next records fetch operation we want to issue to one of the running workers.
		// It contains the offset range to fetch and a channel where the result should be written to.
		nextFetch = fetchWantFrom(startOffset, targetBytesPerFetcher, initialBytesPerRecord)

		// inflight is the list of all fetchWants that are currently in flight.
		inflight = inflightFetchWants{bytes: r.bufferedFetchedBytes}

		// bufferedResult is the next fetch that should be polled by PollFetches().
		bufferedResult fetchResult

		// readyBufferedResults channel gets continuously flipped between nil and the actual channel
		// where PollFetches() reads from. This channel is nil when there are no ordered buffered
		// records ready to be written to the channel where PollFetches(), and is non-nil when there
		// are some ordered buffered records ready.
		//
		// It is guaranteed that this channel is non-nil when bufferedResult is non-empty.
		//
		// The idea is that we don't want to block the main loop when we have records ready to be consumed.
		readyBufferedResults chan fetchResult
	)

	for {
		// refillBufferedResult is the channel of the next fetch result. This variable is valued (non-nil) only when
		// we're ready to actually read the result, so that we don't try to read the next result if we're not ready.
		refillBufferedResult := inflight.peekNextResult()
		if readyBufferedResults != nil {
			// We have a single result that's still not consumed.
			// So we don't try to get new results from the fetchers.
			refillBufferedResult = nil
		}
		dispatchNextWant := chan fetchWant(nil)
		wouldExceedInflightBytesLimit := inflight.bytes.Load()+int64(nextFetch.MaxBytes()) > int64(r.maxBufferedBytesLimit)
		if inflight.count() == 0 || (!wouldExceedInflightBytesLimit && nextFetch.startOffset <= highWatermark.Load()) {
			// In Warpstream fetching past the end induced more delays than MinBytesWaitTime.
			// So we dispatch a fetch only if it's fetching an existing offset.
			// This shouldn't noticeably affect performance with Apache Kafka, after all franz-go only has a concurrency of 1 per partition.
			//
			// We also don't want to have too many fetches in flight, so we only dispatch a fetch if it wouldn't exceed the memory limit.
			//
			// At the same time we don't want to reach a deadlock where the HWM is not updated and there are no fetches in flight.
			// When there isn't a fetch in flight the HWM will never be updated, we will dispatch the next fetchWant even if that means it's above the HWM.
			dispatchNextWant = wants
		}
		select {
		case <-r.done:
			return
		case <-ctx.Done():
			return

		case dispatchNextWant <- nextFetch:
			inflight.append(nextFetch)
			nextFetch = nextFetch.Next()

		case result, moreLeft := <-refillBufferedResult:
			if !moreLeft {
				inflight.removeNextResult()
				continue
			}
			nextFetch = nextFetch.UpdateBytesPerRecord(result.fetchedBytes, len(result.Records))

			// We have some ordered records ready to be sent to PollFetches(). We store the fetch
			// result in bufferedResult, and we flip readyBufferedResults to the channel used by
			// PollFetches().
			bufferedResult = result
			readyBufferedResults = r.orderedFetches

		case readyBufferedResults <- bufferedResult:
			bufferedResult.finishWaitingForConsumption()
			readyBufferedResults = nil
			bufferedResult = fetchResult{}
		}
	}
}

type waiter interface {
	Wait()
}

type metadataRefresher interface {
	ForceMetadataRefresh()
}

// handleKafkaFetchErr handles all the errors listed in the franz-go documentation as possible errors when fetching records.
// For most of them we just apply a backoff. They are listed here so we can be explicit in what we're handling and how.
// It may also return an adjusted fetchWant in case the error indicated, we were consuming not yet produced records or records already deleted due to retention.
func handleKafkaFetchErr(err error, fw fetchWant, longBackoff waiter, partitionStartOffset *genericOffsetReader[int64], refresher metadataRefresher, logger log.Logger) fetchWant {
	// Typically franz-go will update its own metadata when it detects a change in brokers. But it's hard to verify this.
	// So we force a metadata refresh here to be sure.
	// It's ok to call this from multiple fetchers concurrently. franz-go will only be sending one metadata request at a time (whether automatic, periodic, or forced).
	//
	// Metadata refresh is asynchronous. So even after forcing the refresh we might have outdated metadata.
	// Hopefully the backoff that will follow is enough to get the latest metadata.
	// If not, the fetcher will end up here again on the next attempt.
	triggerMetadataRefresh := refresher.ForceMetadataRefresh
	var errString string
	if err != nil {
		errString = err.Error()
	}

	switch {
	case err == nil:
	case errors.Is(err, kerr.OffsetOutOfRange):
		// We're either consuming from before the first offset or after the last offset.
		partitionStart, err := partitionStartOffset.CachedOffset()
		logger = log.With(logger, "log_start_offset", partitionStart, "start_offset", fw.startOffset, "end_offset", fw.endOffset)
		if err != nil {
			level.Error(logger).Log("msg", "failed to find start offset to readjust on OffsetOutOfRange; retrying same records range", "err", err)
			break
		}

		if fw.startOffset < partitionStart {
			// We're too far behind.
			if partitionStart >= fw.endOffset {
				// The next fetch want is responsible for this range. We set startOffset=endOffset to effectively mark this fetch as complete.
				fw.startOffset = fw.endOffset
				level.Debug(logger).Log("msg", "we're too far behind aborting fetch")
				break
			}
			// Only some of the offsets of our want are out of range, so let's fast-forward.
			fw.startOffset = partitionStart
			level.Debug(logger).Log("msg", "part of fetch want is outside of available offsets, adjusted start offset")
		} else {
			// If the broker is behind or if we are requesting offsets which have not yet been produced, we end up here.
			// We set a MaxWaitMillis on fetch requests, but even then there may be no records for some time.
			// Wait for a short time to allow the broker to catch up or for new records to be produced.
			level.Debug(logger).Log("msg", "offset out of range; waiting for new records to be produced")
		}
	case errors.Is(err, kerr.TopicAuthorizationFailed):
		longBackoff.Wait()
	case errors.Is(err, kerr.UnknownTopicOrPartition):
		longBackoff.Wait()
	case errors.Is(err, kerr.UnsupportedCompressionType):
		level.Error(logger).Log("msg", "received UNSUPPORTED_COMPRESSION_TYPE from kafka; this shouldn't happen; please report this as a bug", "err", err)
		longBackoff.Wait() // this shouldn't happen - only happens when the request version was under 10, but we always use 13 - log error and backoff - we can't afford to lose records
	case errors.Is(err, kerr.UnsupportedVersion):
		level.Error(logger).Log("msg", "received UNSUPPORTED_VERSION from kafka; the Kafka cluster is probably too old", "err", err)
		longBackoff.Wait() // in this case our client is too old, not much we can do. This will probably continue logging the error until someone upgrades their Kafka cluster.
	case errors.Is(err, kerr.KafkaStorageError):
		longBackoff.Wait() // server-side error, effectively same as HTTP 500
	case errors.Is(err, kerr.UnknownTopicID):
		longBackoff.Wait() // Maybe it wasn't created by the producers yet.
	case errors.Is(err, kerr.OffsetMovedToTieredStorage):
		level.Error(logger).Log("msg", "received OFFSET_MOVED_TO_TIERED_STORAGE from kafka; this shouldn't happen; please report this as a bug", "err", err)
		longBackoff.Wait() // This should be only intra-broker error, and we shouldn't get it.
	case errors.Is(err, kerr.NotLeaderForPartition):
		// We're asking a broker which is no longer the leader. For a partition. We should refresh our metadata and try again.
		triggerMetadataRefresh()
		longBackoff.Wait()
	case errors.Is(err, kerr.ReplicaNotAvailable):
		// Maybe the replica hasn't replicated the log yet, or it is no longer a replica for this partition.
		// We should refresh and try again with a leader or replica which is up to date.
		triggerMetadataRefresh()
		longBackoff.Wait()
	case errors.Is(err, kerr.UnknownLeaderEpoch):
		// Maybe there's an ongoing election. We should refresh our metadata and try again with a leader in the current epoch.
		triggerMetadataRefresh()
		longBackoff.Wait()
	case errors.Is(err, kerr.FencedLeaderEpoch):
		// We missed a new epoch (leader election). We should refresh our metadata and try again with a leader in the current epoch.
		triggerMetadataRefresh()
		longBackoff.Wait()
	case errors.Is(err, kerr.LeaderNotAvailable):
		// This isn't listed in the possible errors in franz-go, but Apache Kafka returns it when the partition has no leader.
		triggerMetadataRefresh()
		longBackoff.Wait()
	case errors.Is(err, kerr.BrokerNotAvailable):
		// This isn't listed in the possible errors in franz-go, but Warpstream returns it.
		triggerMetadataRefresh()
		longBackoff.Wait()
	case errors.Is(err, errUnknownPartitionLeader):
		triggerMetadataRefresh()
		longBackoff.Wait()
	case errors.Is(err, &kgo.ErrFirstReadEOF{}):
		longBackoff.Wait()
	case strings.Contains(errString, unknownBroker):
		// The client's metadata refreshed after we called Broker(). It should already be refreshed, so we can retry immediately.
	case strings.Contains(errString, chosenBrokerDied):
		// The client's metadata refreshed after we called Broker(). It should already be refreshed, so we can retry immediately.
	case strings.Contains(errString, "use of closed network connection"):
		// The client usually immediately handles closed connections, so we can retry immediately.
	case strings.Contains(errString, "i/o timeout"):
		// Maybe the broker went away ungracefully; let's refresh our metadata and try again.
		triggerMetadataRefresh()
		longBackoff.Wait()

	default:
		level.Error(logger).Log("msg", "received an error we're not prepared to handle; this shouldn't happen; please report this as a bug", "err", err)
		longBackoff.Wait()
	}
	return fw
}
