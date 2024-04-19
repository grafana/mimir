// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

var (
	errPartitionOffsetReaderStopped = errors.New("partition offset reader is stopped")
)

// partitionOffsetReader is responsible to read the offsets of a single partition.
//
// If in the future we'll need to read offsets of multiple partitions at once, then we shouldn't use
// this structure but create a new one which fetches multiple partition offsets in a single request.
type partitionOffsetReader struct {
	services.Service

	client      *kgo.Client
	logger      log.Logger
	topic       string
	partitionID int32

	// nextResultPromise is the promise that will be notified about the result of the *next* "last produced offset"
	// request that will be issued (not the current in-flight one, if any).
	nextResultPromiseMx sync.RWMutex
	nextResultPromise   *resultPromise[int64]

	// Metrics.
	lastProducedOffsetRequestsTotal   prometheus.Counter
	lastProducedOffsetFailuresTotal   prometheus.Counter
	lastProducedOffsetLatency         prometheus.Histogram
	partitionStartOffsetRequestsTotal prometheus.Counter
	partitionStartOffsetFailuresTotal prometheus.Counter
	partitionStartOffsetLatency       prometheus.Histogram
}

func newPartitionOffsetReader(client *kgo.Client, topic string, partitionID int32, pollInterval time.Duration, reg prometheus.Registerer, logger log.Logger) *partitionOffsetReader {
	p := &partitionOffsetReader{
		client:            client,
		topic:             topic,
		partitionID:       partitionID,
		logger:            logger, // Do not wrap with partition ID because it's already done by the caller.
		nextResultPromise: newResultPromise[int64](),

		lastProducedOffsetRequestsTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name:        "cortex_ingest_storage_reader_last_produced_offset_requests_total",
			Help:        "Total number of requests issued to get the last produced offset.",
			ConstLabels: prometheus.Labels{"partition": strconv.Itoa(int(partitionID))},
		}),
		lastProducedOffsetFailuresTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name:        "cortex_ingest_storage_reader_last_produced_offset_failures_total",
			Help:        "Total number of failed requests to get the last produced offset.",
			ConstLabels: prometheus.Labels{"partition": strconv.Itoa(int(partitionID))},
		}),
		lastProducedOffsetLatency: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:                            "cortex_ingest_storage_reader_last_produced_offset_request_duration_seconds",
			Help:                            "The duration of requests to fetch the last produced offset of a given partition.",
			ConstLabels:                     prometheus.Labels{"partition": strconv.Itoa(int(partitionID))},
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: 1 * time.Hour,
			Buckets:                         prometheus.DefBuckets,
		}),

		partitionStartOffsetRequestsTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name:        "cortex_ingest_storage_reader_partition_start_offset_requests_total",
			Help:        "Total number of requests issued to get the partition start offset.",
			ConstLabels: prometheus.Labels{"partition": strconv.Itoa(int(partitionID))},
		}),
		partitionStartOffsetFailuresTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name:        "cortex_ingest_storage_reader_partition_start_offset_failures_total",
			Help:        "Total number of failed requests to get the partition start offset.",
			ConstLabels: prometheus.Labels{"partition": strconv.Itoa(int(partitionID))},
		}),
		partitionStartOffsetLatency: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:                            "cortex_ingest_storage_reader_partition_start_offset_request_duration_seconds",
			Help:                            "The duration of requests to fetch the start offset of a given partition.",
			ConstLabels:                     prometheus.Labels{"partition": strconv.Itoa(int(partitionID))},
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: 1 * time.Hour,
			Buckets:                         prometheus.DefBuckets,
		}),
	}

	p.Service = services.NewTimerService(pollInterval, nil, p.onPollInterval, p.stopping)

	return p
}

func (p *partitionOffsetReader) onPollInterval(ctx context.Context) error {
	// The following call blocks until the last produced offset has been fetched from Kafka. If fetching
	// the offset takes longer than the poll interval, than we'll poll less frequently than configured.
	p.getAndNotifyLastProducedOffset(ctx)

	// Never return error, otherwise the service stops.
	return nil
}

func (p *partitionOffsetReader) stopping(_ error) error {
	// Release any waiting goroutine without swapping the result promise so that if any other goroutine
	// will watch it after this point it will get immediately notified.
	p.nextResultPromiseMx.Lock()
	p.nextResultPromise.notify(0, errPartitionOffsetReaderStopped)
	p.nextResultPromiseMx.Unlock()

	return nil
}

// getAndNotifyLastProducedOffset fetches the last produced offset for a partition and notifies all waiting
// goroutines (if any).
func (p *partitionOffsetReader) getAndNotifyLastProducedOffset(ctx context.Context) {
	// Swap the next promise with a new one.
	p.nextResultPromiseMx.Lock()
	promise := p.nextResultPromise
	p.nextResultPromise = newResultPromise[int64]()
	p.nextResultPromiseMx.Unlock()

	// We call FetchLastProducedOffset() even if there are no goroutines waiting on the result in order to get
	// a constant load on the Kafka backend. In other words, the load produced on Kafka by this component is
	// constant, regardless the number of received queries with strong consistency enabled.
	offset, err := p.FetchLastProducedOffset(ctx)
	if err != nil {
		level.Warn(p.logger).Log("msg", "failed to fetch the last produced offset", "err", err)
	}

	// Notify whoever was waiting for it.
	promise.notify(offset, err)
}

// FetchLastProducedOffset fetches and returns the last produced offset for a partition, or -1 if no record has
// been ever produced in the partition. This function issues a single request, but the Kafka client used under the
// hood may retry a failed request until the retry timeout is hit.
func (p *partitionOffsetReader) FetchLastProducedOffset(ctx context.Context) (_ int64, returnErr error) {
	startTime := time.Now()

	p.lastProducedOffsetRequestsTotal.Inc()
	defer func() {
		// We track the latency also in case of error, so that if the request times out it gets
		// pretty clear looking at latency too.
		p.lastProducedOffsetLatency.Observe(time.Since(startTime).Seconds())

		if returnErr != nil {
			p.lastProducedOffsetFailuresTotal.Inc()
		}
	}()

	offset, err := p.fetchPartitionOffset(ctx, kafkaEndOffset)
	if err != nil {
		return 0, err
	}

	// The offset we get is the offset at which the next message will be written, so to get the last produced offset
	// we have to subtract 1. See DESIGN.md for more details.
	return offset - 1, nil
}

// FetchPartitionStartOffset fetches and returns the start offset for a partition. This function returns 0 if no record has
// been ever produced in the partition. This function issues a single request, but the Kafka client used under the
// hood may retry a failed request until the retry timeout is hit.
func (p *partitionOffsetReader) FetchPartitionStartOffset(ctx context.Context) (_ int64, returnErr error) {
	startTime := time.Now()

	p.partitionStartOffsetRequestsTotal.Inc()
	defer func() {
		// We track the latency also in case of error, so that if the request times out it gets
		// pretty clear looking at latency too.
		p.partitionStartOffsetLatency.Observe(time.Since(startTime).Seconds())

		if returnErr != nil {
			p.partitionStartOffsetFailuresTotal.Inc()
		}
	}()

	return p.fetchPartitionOffset(ctx, kafkaStartOffset)
}

func (p *partitionOffsetReader) fetchPartitionOffset(ctx context.Context, position int64) (int64, error) {
	// Create a custom request to fetch the latest offset of a specific partition.
	partitionReq := kmsg.NewListOffsetsRequestTopicPartition()
	partitionReq.Partition = p.partitionID
	partitionReq.Timestamp = position

	topicReq := kmsg.NewListOffsetsRequestTopic()
	topicReq.Topic = p.topic
	topicReq.Partitions = []kmsg.ListOffsetsRequestTopicPartition{partitionReq}

	req := kmsg.NewPtrListOffsetsRequest()
	req.IsolationLevel = 0 // 0 means READ_UNCOMMITTED.
	req.Topics = []kmsg.ListOffsetsRequestTopic{topicReq}

	// Even if we share the same client, other in-flight requests are not canceled once this context is canceled
	// (or its deadline is exceeded). We've verified it with a unit test.
	resps := p.client.RequestSharded(ctx, req)

	// Since we issued a request for only 1 partition, we expect exactly 1 response.
	if expected := 1; len(resps) != expected {
		return 0, fmt.Errorf("unexpected number of responses (expected: %d, got: %d)", expected, len(resps))
	}

	// Ensure no error occurred.
	res := resps[0]
	if res.Err != nil {
		return 0, res.Err
	}

	// Parse the response.
	listRes, ok := res.Resp.(*kmsg.ListOffsetsResponse)
	if !ok {
		return 0, errors.New("unexpected response type")
	}
	if expected, actual := 1, len(listRes.Topics); actual != expected {
		return 0, fmt.Errorf("unexpected number of topics in the response (expected: %d, got: %d)", expected, actual)
	}
	if expected, actual := p.topic, listRes.Topics[0].Topic; expected != actual {
		return 0, fmt.Errorf("unexpected topic in the response (expected: %s, got: %s)", expected, actual)
	}
	if expected, actual := 1, len(listRes.Topics[0].Partitions); actual != expected {
		return 0, fmt.Errorf("unexpected number of partitions in the response (expected: %d, got: %d)", expected, actual)
	}
	if expected, actual := p.partitionID, listRes.Topics[0].Partitions[0].Partition; actual != expected {
		return 0, fmt.Errorf("unexpected partition in the response (expected: %d, got: %d)", expected, actual)
	}
	if err := kerr.ErrorForCode(listRes.Topics[0].Partitions[0].ErrorCode); err != nil {
		return 0, err
	}

	return listRes.Topics[0].Partitions[0].Offset, nil
}

// WaitNextFetchLastProducedOffset returns the result of the *next* "last produced offset" request
// that will be issued.
//
// The "last produced offset" is the offset of the last message written to the partition (starting from 0), or -1 if no
// message has been written yet.
func (p *partitionOffsetReader) WaitNextFetchLastProducedOffset(ctx context.Context) (int64, error) {
	// Get the promise for the result of the next request that will be issued.
	p.nextResultPromiseMx.RLock()
	promise := p.nextResultPromise
	p.nextResultPromiseMx.RUnlock()

	return promise.wait(ctx)
}
