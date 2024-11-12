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
	"github.com/opentracing/opentracing-go"
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
)

type fetcher interface {
	// PollFetches fetches records from Kafka and returns them.
	// The returned context is the context of fetching. It can also be equal to the passed context.
	// The returned context may contain spans that were used to fetch from Kafka.
	// Each record in the returned fetches also contains a context.
	// You should use that when doing something specific to a tenant or a
	// record and use the returned context when doing something that is common to all records.
	PollFetches(context.Context) (kgo.Fetches, context.Context)

	// Update updates the fetcher with the given concurrency and records.
	Update(ctx context.Context, concurrency, records int)

	// Stop stops the fetcher.
	Stop()
}

// fetchWant represents a range of offsets to fetch.
// Based on a given number of records, it tries to estimate how many bytes we need to fetch, given there's no support for fetching offsets directly.
// fetchWant also contains the channel on which to send the fetched records for the offset range.
type fetchWant struct {
	startOffset    int64 // inclusive
	endOffset      int64 // exclusive
	bytesPerRecord int

	// result should be closed when there are no more fetches for this partition. It is ok to send multiple times on the channel.
	result chan fetchResult
}

func fetchWantFrom(offset int64, recordsPerFetch int) fetchWant {
	return fetchWant{
		startOffset: offset,
		endOffset:   offset + int64(recordsPerFetch),
		result:      make(chan fetchResult),
	}
}

// Next returns the fetchWant for the next numRecords starting from the last known offset.
func (w fetchWant) Next(numRecords int) fetchWant {
	n := fetchWantFrom(w.endOffset, numRecords)
	n.bytesPerRecord = w.bytesPerRecord
	return n.trimIfTooBig()
}

// MaxBytes returns the maximum number of bytes we can fetch in a single request.
// It's capped at math.MaxInt32 to avoid overflow, and it'll always fetch a minimum of 1MB.
func (w fetchWant) MaxBytes() int32 {
	fetchBytes := w.expectedBytes()
	if fetchBytes > math.MaxInt32 {
		// This shouldn't happen because w should have been trimmed before sending the request.
		// But we definitely don't want to request negative bytes by casting to int32, so add this safeguard.
		return math.MaxInt32
	}
	fetchBytes = max(1_000_000, fetchBytes) // when we're fetching few records, we can afford to over-fetch to avoid more requests.
	return int32(fetchBytes)
}

// UpdateBytesPerRecord updates the expected bytes per record based on the results of the last fetch and trims the fetchWant if MaxBytes() would now exceed math.MaxInt32.
func (w fetchWant) UpdateBytesPerRecord(lastFetchBytes int, lastFetchNumberOfRecords int) fetchWant {
	// Smooth over the estimation to avoid having outlier fetches from throwing off the estimation.
	// We don't want a fetch of 5 records to determine how we fetch the next fetch of 6000 records.
	// Ideally we weigh the estimation on the number of records observed, but it's simpler to smooth it over with a constant factor.
	const currentEstimateWeight = 0.8

	actualBytesPerRecord := float64(lastFetchBytes) / float64(lastFetchNumberOfRecords)
	w.bytesPerRecord = int(currentEstimateWeight*float64(w.bytesPerRecord) + (1-currentEstimateWeight)*actualBytesPerRecord)

	return w.trimIfTooBig()
}

// expectedBytes returns how many bytes we'd need to accommodate the range of offsets using bytesPerRecord.
// They may be more than the kafka protocol supports (> MaxInt32). Use MaxBytes.
func (w fetchWant) expectedBytes() int {
	// We over-fetch bytes to reduce the likelihood of under-fetching and having to run another request.
	// Based on some testing 65% of under-estimations are by less than 5%. So we account for that.
	const overFetchBytesFactor = 1.05
	return int(overFetchBytesFactor * float64(w.bytesPerRecord*int(w.endOffset-w.startOffset)))
}

// trimIfTooBig adjusts the end offset if we expect to fetch too many bytes.
// It's capped at math.MaxInt32 bytes.
func (w fetchWant) trimIfTooBig() fetchWant {
	if w.expectedBytes() <= math.MaxInt32 {
		return w
	}
	// We are overflowing, so we need to trim the end offset.
	// We do this by calculating how many records we can fetch with the max bytes, and then setting the end offset to that.
	w.endOffset = w.startOffset + int64(math.MaxInt32/w.bytesPerRecord)
	return w
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

// Merge merges other with an older fetchResult. Merge keeps most of the fields of fr and assumes they are more up-to-date than older.
func (fr *fetchResult) Merge(older fetchResult) fetchResult {
	if older.ctx != nil {
		level.Debug(spanlogger.FromContext(older.ctx, log.NewNopLogger())).Log("msg", "merged fetch result with the next result")
	}

	// older.Records are older than fr.Records, so we append them first.
	fr.Records = append(older.Records, fr.Records...)

	// We ignore HighWatermark, LogStartOffset, LastStableOffset because this result should be more up to date.
	fr.fetchedBytes += older.fetchedBytes
	return *fr
}

func newEmptyFetchResult(ctx context.Context, err error) fetchResult {
	return fetchResult{
		ctx:            ctx,
		fetchedBytes:   0,
		FetchPartition: kgo.FetchPartition{Err: err},
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

	orderedFetches     chan fetchResult
	lastReturnedRecord int64
	startOffsets       *genericOffsetReader[int64]

	// trackCompressedBytes controls whether to calculate MaxBytes for fetch requests based on previous responses' compressed or uncompressed bytes.
	trackCompressedBytes bool
}

// Stop implements fetcher
func (r *concurrentFetchers) Stop() {
	close(r.done)

	r.wg.Wait()
	level.Info(r.logger).Log("msg", "stopped concurrent fetchers", "last_returned_record", r.lastReturnedRecord)
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
	recordsPerFetch int,
	trackCompressedBytes bool,
	minBytesWaitTime time.Duration,
	offsetReader *partitionOffsetClient,
	startOffsetsReader *genericOffsetReader[int64],
	metrics *readerMetrics,
) (*concurrentFetchers, error) {

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
	f := &concurrentFetchers{
		client:               client,
		logger:               logger,
		topicName:            topic,
		partitionID:          partition,
		metrics:              metrics,
		minBytesWaitTime:     minBytesWaitTime,
		lastReturnedRecord:   startOffset - 1,
		startOffsets:         startOffsetsReader,
		trackCompressedBytes: trackCompressedBytes,
		tracer:               recordsTracer(),
		orderedFetches:       make(chan fetchResult),
		done:                 make(chan struct{}),
	}

	topics, err := kadm.NewClient(client).ListTopics(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("failed to find topic ID: %w", err)
	}
	if !topics.Has(topic) {
		return nil, fmt.Errorf("failed to find topic ID: topic not found")
	}
	f.topicID = topics[topic].ID

	f.wg.Add(1)
	go f.start(ctx, startOffset, concurrency, recordsPerFetch)

	return f, nil
}

// Update implements fetcher
func (r *concurrentFetchers) Update(ctx context.Context, concurrency, records int) {
	r.Stop()
	r.done = make(chan struct{})

	r.wg.Add(1)
	go r.start(ctx, r.lastReturnedRecord+1, concurrency, records)
}

// PollFetches implements fetcher
func (r *concurrentFetchers) PollFetches(ctx context.Context) (kgo.Fetches, context.Context) {
	waitStartTime := time.Now()
	select {
	case <-ctx.Done():
		return kgo.Fetches{}, ctx
	case f := <-r.orderedFetches:
		firstUnreturnedRecordIdx := recordIndexAfterOffset(f.Records, r.lastReturnedRecord)
		r.recordOrderedFetchTelemetry(f, firstUnreturnedRecordIdx, waitStartTime)

		f.Records = f.Records[firstUnreturnedRecordIdx:]
		if len(f.Records) > 0 {
			r.lastReturnedRecord = f.Records[len(f.Records)-1].Offset
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
	r.metrics.fetchWaitDuration.Observe(waitDuration.Seconds())

	doubleFetchedBytes := 0
	for i, record := range f.Records {
		if i < firstReturnedRecordIndex {
			doubleFetchedBytes += len(record.Value)
			spanlogger.FromContext(record.Context, r.logger).DebugLog("msg", "skipping record because it has already been returned", "offset", record.Offset)
		}
		r.tracer.OnFetchRecordUnbuffered(record, true)
	}
	r.metrics.fetchedDiscardedRecordBytes.Add(float64(doubleFetchedBytes))
}

// fetchSingle attempts to find out the leader Kafka broker for a partition and then sends a fetch request to the leader of the fetchWant request and parses the responses
// fetchSingle returns a fetchResult which may or may not fulfil the entire fetchWant.
// If ctx is cancelled, fetchSingle will return an empty fetchResult without an error.
func (r *concurrentFetchers) fetchSingle(ctx context.Context, fw fetchWant) (fr fetchResult) {
	defer func(fetchStartTime time.Time) {
		fr.logCompletedFetch(fetchStartTime, fw)
	}(time.Now())

	leaderID, leaderEpoch, err := r.client.PartitionLeader(r.topicName, r.partitionID)
	if err != nil || (leaderID == -1 && leaderEpoch == -1) {
		if err != nil {
			return newEmptyFetchResult(ctx, fmt.Errorf("finding leader for partition: %w", err))
		}
		return newEmptyFetchResult(ctx, errUnknownPartitionLeader)
	}

	req := r.buildFetchRequest(fw, leaderEpoch)

	resp, err := req.RequestWith(ctx, r.client.Broker(int(leaderID)))
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return newEmptyFetchResult(ctx, nil)
		}
		return newEmptyFetchResult(ctx, fmt.Errorf("fetching from kafka: %w", err))
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

func (r *concurrentFetchers) parseFetchResponse(ctx context.Context, startOffset int64, resp *kmsg.FetchResponse) fetchResult {
	// Here we ignore resp.ErrorCode. That error code was added for support for KIP-227 and is only set if we're using fetch sessions. We don't use fetch sessions.
	// We also ignore rawPartitionResp.PreferredReadReplica to keep the code simpler. We don't provide any rack in the FetchRequest, so the broker _probably_ doesn't have a recommended replica for us.

	// Sanity check for the response we get.
	// If we get something we didn't expect, maybe we're sending the wrong request or there's a bug in the kafka implementation.
	// Even in case of errors we get the topic partition.
	err := assertResponseContainsPartition(resp, r.topicID, r.partitionID)
	if err != nil {
		return newEmptyFetchResult(ctx, err)
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

func assertResponseContainsPartition(resp *kmsg.FetchResponse, topicID kadm.TopicID, partitionID int32) error {
	if topics := resp.Topics; len(topics) < 1 || topics[0].TopicID != topicID {
		receivedTopicID := kadm.TopicID{}
		if len(topics) > 0 {
			receivedTopicID = topics[0].TopicID
		}
		return fmt.Errorf("didn't find expected topic %s in fetch response; received topic %s", topicID, receivedTopicID)
	}
	if partitions := resp.Topics[0].Partitions; len(partitions) < 1 || partitions[0].Partition != partitionID {
		receivedPartitionID := int32(-1)
		if len(partitions) > 0 {
			receivedPartitionID = partitions[0].Partition
		}
		return fmt.Errorf("didn't find expected partition %d in fetch response; received partition %d", partitionID, receivedPartitionID)
	}
	return nil
}

func sumRecordLengths(records []*kgo.Record) (sum int) {
	for _, r := range records {
		sum += len(r.Value)
	}
	return sum
}

func (r *concurrentFetchers) run(ctx context.Context, wants chan fetchWant, logger log.Logger, highWatermark *atomic.Int64) {
	defer r.wg.Done()

	errBackoff := backoff.New(ctx, backoff.Config{
		MinBackoff: 250 * time.Millisecond,
		MaxBackoff: 2 * time.Second,
		MaxRetries: 0, // retry forever
	})

	for w := range wants {
		// Start new span for each fetchWant. We want to record the lifecycle of a single record from being fetched to being ingested.
		wantSpan, ctx := spanlogger.NewWithLogger(ctx, logger, "concurrentFetcher.fetch")
		wantSpan.SetTag("start_offset", w.startOffset)
		wantSpan.SetTag("end_offset", w.endOffset)

		var previousResult fetchResult
		for attempt := 0; errBackoff.Ongoing() && w.endOffset > w.startOffset; attempt++ {
			attemptSpan, ctx := spanlogger.NewWithLogger(ctx, logger, "concurrentFetcher.fetch.attempt")
			attemptSpan.SetTag("attempt", attempt)

			f := r.fetchSingle(ctx, w)
			f = f.Merge(previousResult)
			previousResult = f
			if f.Err != nil {
				w = handleKafkaFetchErr(f.Err, w, errBackoff, r.startOffsets, r.client, attemptSpan)
			}
			if hwm := f.HighWatermark; hwm >= 0 {
				casHWM(highWatermark, hwm)
			}
			if len(f.Records) == 0 {
				// Typically if we had an error, then there wouldn't be any records.
				// But it's hard to verify this for all errors from the Kafka API docs, so just to be sure, we process any records we might have received.
				attemptSpan.Finish()

				// There is a chance we've been told to stop even when we have no records.
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
			w.startOffset = f.Records[len(f.Records)-1].Offset + 1
			w = w.UpdateBytesPerRecord(f.fetchedBytes, len(f.Records)) // This takes into account the previousFetch too. This should give us a better average than using just the records from the last attempt.

			// We reset the backoff if we received any records whatsoever. A received record means _some_ success.
			// We don't want to slow down until we hit a larger error.
			errBackoff.Reset()

			select {
			case <-r.done:
				wantSpan.Finish()
				attemptSpan.Finish()
				close(w.result)
				return
			case w.result <- f:
				previousResult = fetchResult{}
			case <-ctx.Done():
			default:
				if w.startOffset >= w.endOffset {
					// We've fetched all we were asked for the whole batch is ready, and we definitely have to wait to send on the channel now.
					f.startWaitingForConsumption()
					select {
					case <-r.done:
						wantSpan.Finish()
						attemptSpan.Finish()
						close(w.result)
						return
					case w.result <- f:
						previousResult = fetchResult{}
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

func (r *concurrentFetchers) start(ctx context.Context, startOffset int64, concurrency, recordsPerFetch int) {
	level.Info(r.logger).Log("msg", "starting concurrent fetchers", "start_offset", startOffset, "concurrency", concurrency, "recordsPerFetch", recordsPerFetch)

	// HWM is updated by the fetchers. A value of 0 is the same as there not being any produced records.
	// A value of 0 doesn't prevent progress because we ensure there is at least one dispatched fetchWant.
	highWatermark := atomic.NewInt64(0)

	wants := make(chan fetchWant)
	defer close(wants)
	r.wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		logger := log.With(r.logger, "fetcher", i)
		go r.run(ctx, wants, logger, highWatermark)
	}

	var (
		nextFetch      = fetchWantFrom(startOffset, recordsPerFetch)
		nextResult     chan fetchResult
		pendingResults = list.New()

		bufferedResult       fetchResult
		readyBufferedResults chan fetchResult // this is non-nil when bufferedResult is non-empty
	)
	nextFetch.bytesPerRecord = 10_000 // start with an estimation, we will update it as we consume

	// We need to make sure we don't leak any goroutine given that start is called within a goroutine.
	defer r.wg.Done()
	for {
		refillBufferedResult := nextResult
		if readyBufferedResults != nil {
			// We have a single result that's still not consumed.
			// So we don't try to get new results from the fetchers.
			refillBufferedResult = nil
		}
		dispatchNextWant := chan fetchWant(nil)
		if nextResult == nil || nextFetch.startOffset <= highWatermark.Load() {
			// In Warpstream fetching past the end induced more delays than MinBytesWaitTime.
			// So we dispatch a fetch only if it's fetching an existing offset.
			// This shouldn't noticeably affect performance with Apache Kafka, after all franz-go only has a concurrency of 1 per partition.
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
			pendingResults.PushBack(nextFetch.result)
			if nextResult == nil {
				// In case we previously exhausted pendingResults, we just created
				nextResult = pendingResults.Front().Value.(chan fetchResult)
				pendingResults.Remove(pendingResults.Front())
			}
			nextFetch = nextFetch.Next(recordsPerFetch)

		case result, moreLeft := <-refillBufferedResult:
			if !moreLeft {
				if pendingResults.Len() > 0 {
					nextResult = pendingResults.Front().Value.(chan fetchResult)
					pendingResults.Remove(pendingResults.Front())
				} else {
					nextResult = nil
				}
				continue
			}
			nextFetch = nextFetch.UpdateBytesPerRecord(result.fetchedBytes, len(result.Records))
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
