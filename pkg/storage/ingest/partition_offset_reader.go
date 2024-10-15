// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/atomic"
)

var (
	errPartitionOffsetReaderStopped = errors.New("partition offset reader is stopped")
)

// genericOffsetReader is the base implementation used by offset readers.
type genericOffsetReader[O any] struct {
	services.Service

	logger log.Logger

	// fetchLastProducedOffset is the implementation of the function used to fetch the last produced offset.
	fetchLastProducedOffset func(context.Context) (O, error)

	// nextResultPromise is the promise that will be notified about the result of the *next* "last produced offset"
	// request that will be issued (not the current in-flight one, if any).
	nextResultPromiseMx sync.RWMutex
	nextResultPromise   *resultPromise[O]

	// lastResultPromise is the last returned offset.
	lastResultPromise *atomic.Pointer[resultPromise[O]]
}

func newGenericOffsetReader[O any](fetchLastProducedOffset func(context.Context) (O, error), pollInterval time.Duration, logger log.Logger) *genericOffsetReader[O] {
	p := &genericOffsetReader[O]{
		logger:                  logger,
		fetchLastProducedOffset: fetchLastProducedOffset,
		nextResultPromise:       newResultPromise[O](),
		lastResultPromise:       atomic.NewPointer(newResultPromise[O]()),
	}

	// Run the poll interval once at startup so we can cache the offset.
	p.Service = services.NewTimerService(pollInterval, p.onPollInterval, p.onPollInterval, p.stopping)

	return p
}

func (r *genericOffsetReader[O]) onPollInterval(ctx context.Context) error {
	// The following call blocks until the last produced offset has been fetched from Kafka. If fetching
	// the offset takes longer than the poll interval, than we'll poll less frequently than configured.
	r.getAndNotifyLastProducedOffset(ctx)

	// Never return error, otherwise the service stops.
	return nil
}

func (r *genericOffsetReader[O]) stopping(_ error) error {
	var zero O

	// Release any waiting goroutine without swapping the result promise so that if any other goroutine
	// will watch it after this point it will get immediately notified.
	r.nextResultPromiseMx.Lock()
	r.nextResultPromise.notify(zero, errPartitionOffsetReaderStopped)
	r.nextResultPromiseMx.Unlock()

	return nil
}

// getAndNotifyLastProducedOffset fetches the last produced offset for a partition and notifies all waiting
// goroutines (if any).
func (r *genericOffsetReader[O]) getAndNotifyLastProducedOffset(ctx context.Context) {
	// Swap the next promise with a new one.
	r.nextResultPromiseMx.Lock()
	promise := r.nextResultPromise
	r.nextResultPromise = newResultPromise[O]()
	r.nextResultPromiseMx.Unlock()

	// We call fetchLastProducedOffset() even if there are no goroutines waiting on the result in order to get
	// a constant load on the Kafka backend. In other words, the load produced on Kafka by this component is
	// constant, regardless the number of received queries with strong consistency enabled.
	offset, err := r.fetchLastProducedOffset(ctx)
	if err != nil {
		level.Warn(r.logger).Log("msg", "failed to fetch the last produced offset", "err", err)
	}

	// Notify whoever was waiting for it.
	promise.notify(offset, err)
	r.lastResultPromise.Store(promise)
}

// WaitNextFetchLastProducedOffset returns the result of the *next* "last produced offset" request
// that will be issued.
//
// The "last produced offset" is the offset of the last message written to the partition (starting from 0), or -1 if no
// message has been written yet.
func (r *genericOffsetReader[O]) WaitNextFetchLastProducedOffset(ctx context.Context) (O, error) {
	// Get the promise for the result of the next request that will be issued.
	r.nextResultPromiseMx.RLock()
	promise := r.nextResultPromise
	r.nextResultPromiseMx.RUnlock()

	return promise.wait(ctx)
}

// CachedOffset returns the last result of fetching the offset. This is likely outdated, but it's useful to get a directionally correct value quickly.
func (r *genericOffsetReader[O]) CachedOffset() (O, error) {
	c := r.lastResultPromise.Load()
	return c.resultValue, c.resultErr
}

// partitionOffsetReader is responsible to read the offsets of a single partition.
type partitionOffsetReader struct {
	*genericOffsetReader[int64]

	client      *partitionOffsetClient
	logger      log.Logger
	partitionID int32
}

func newPartitionOffsetReader(client *kgo.Client, topic string, partitionID int32, pollInterval time.Duration, reg prometheus.Registerer, logger log.Logger) *partitionOffsetReader {
	offsetClient := newPartitionOffsetClient(client, topic, reg, logger)
	return newPartitionOffsetReaderWithOffsetClient(offsetClient, partitionID, pollInterval, logger)
}

func newPartitionOffsetReaderWithOffsetClient(offsetClient *partitionOffsetClient, partitionID int32, pollInterval time.Duration, logger log.Logger) *partitionOffsetReader {
	r := &partitionOffsetReader{
		client:      offsetClient,
		partitionID: partitionID,
		logger:      logger, // Do not wrap with partition ID because it's already done by the caller.
	}

	r.genericOffsetReader = newGenericOffsetReader[int64](r.FetchLastProducedOffset, pollInterval, logger)

	return r
}

// FetchLastProducedOffset fetches and returns the last produced offset for a partition, or -1 if no record has
// been ever produced in the partition. This function issues a single request, but the Kafka client used under the
// hood may retry a failed request until the retry timeout is hit.
func (p *partitionOffsetReader) FetchLastProducedOffset(ctx context.Context) (_ int64, returnErr error) {
	return p.client.FetchPartitionLastProducedOffset(ctx, p.partitionID)
}

// FetchPartitionStartOffset fetches and returns the start offset for a partition. This function returns 0 if no record has
// been ever produced in the partition. This function issues a single request, but the Kafka client used under the
// hood may retry a failed request until the retry timeout is hit.
func (p *partitionOffsetReader) FetchPartitionStartOffset(ctx context.Context) (_ int64, returnErr error) {
	return p.client.FetchPartitionStartOffset(ctx, p.partitionID)
}

type GetPartitionIDsFunc func(ctx context.Context) ([]int32, error)

// TopicOffsetsReader is responsible to read the offsets of partitions in a topic.
type TopicOffsetsReader struct {
	*genericOffsetReader[map[int32]int64]

	client          *partitionOffsetClient
	topic           string
	getPartitionIDs GetPartitionIDsFunc
	logger          log.Logger
}

func NewTopicOffsetsReader(client *kgo.Client, topic string, getPartitionIDs GetPartitionIDsFunc, pollInterval time.Duration, reg prometheus.Registerer, logger log.Logger) *TopicOffsetsReader {
	r := &TopicOffsetsReader{
		client:          newPartitionOffsetClient(client, topic, reg, logger),
		topic:           topic,
		getPartitionIDs: getPartitionIDs,
		logger:          logger,
	}

	r.genericOffsetReader = newGenericOffsetReader[map[int32]int64](r.FetchLastProducedOffset, pollInterval, logger)

	return r
}

// NewTopicOffsetsReaderForAllPartitions returns a TopicOffsetsReader instance that fetches the offsets for all
// existing partitions in a topic. The list of partitions is refreshed each time FetchLastProducedOffset() is called,
// so using a TopicOffsetsReader created by this function adds an extra latency to refresh partitions each time.
func NewTopicOffsetsReaderForAllPartitions(client *kgo.Client, topic string, pollInterval time.Duration, reg prometheus.Registerer, logger log.Logger) *TopicOffsetsReader {
	offsetsClient := newPartitionOffsetClient(client, topic, reg, logger)

	return NewTopicOffsetsReader(client, topic, offsetsClient.ListTopicPartitionIDs, pollInterval, reg, logger)
}

// FetchLastProducedOffset fetches and returns the last produced offset for each requested partition in the topic.
// The offset is -1 if a partition has been created but no record has been produced yet.
func (p *TopicOffsetsReader) FetchLastProducedOffset(ctx context.Context) (map[int32]int64, error) {
	partitionIDs, err := p.getPartitionIDs(ctx)
	if err != nil {
		return nil, err
	}

	return p.client.FetchPartitionsLastProducedOffsets(ctx, partitionIDs)
}
