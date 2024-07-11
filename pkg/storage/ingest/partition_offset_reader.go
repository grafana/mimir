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

	client      *partitionOffsetClient
	logger      log.Logger
	partitionID int32

	// nextResultPromise is the promise that will be notified about the result of the *next* "last produced offset"
	// request that will be issued (not the current in-flight one, if any).
	nextResultPromiseMx sync.RWMutex
	nextResultPromise   *resultPromise[int64]
}

func newPartitionOffsetReader(client *kgo.Client, topic string, partitionID int32, pollInterval time.Duration, reg prometheus.Registerer, logger log.Logger) *partitionOffsetReader {
	p := &partitionOffsetReader{
		client:            newPartitionOffsetClient(client, topic, reg, logger),
		partitionID:       partitionID,
		logger:            logger, // Do not wrap with partition ID because it's already done by the caller.
		nextResultPromise: newResultPromise[int64](),
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
	return p.client.FetchLastProducedOffset(ctx, p.partitionID)
}

// FetchPartitionStartOffset fetches and returns the start offset for a partition. This function returns 0 if no record has
// been ever produced in the partition. This function issues a single request, but the Kafka client used under the
// hood may retry a failed request until the retry timeout is hit.
func (p *partitionOffsetReader) FetchPartitionStartOffset(ctx context.Context) (_ int64, returnErr error) {
	return p.client.FetchPartitionStartOffset(ctx, p.partitionID)
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
