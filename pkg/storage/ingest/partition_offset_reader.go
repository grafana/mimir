// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
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

	metadataStore *MetadataStore
	logger        log.Logger
	partitionID   int32

	// nextResultPromise is the promise that will be notified about the result of the *next* "last produced offset"
	// request that will be issued (not the current in-flight one, if any).
	nextResultPromiseMx sync.RWMutex
	nextResultPromise   *resultPromise[int64]

	// Metrics.
	lastProducedOffsetRequestsTotal prometheus.Counter
	lastProducedOffsetFailuresTotal prometheus.Counter
	lastProducedOffsetLatency       prometheus.Summary
}

func newPartitionOffsetReader(metadataStore *MetadataStore, partitionID int32, pollInterval time.Duration, reg prometheus.Registerer, logger log.Logger) *partitionOffsetReader {
	p := &partitionOffsetReader{
		metadataStore:     metadataStore,
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
		lastProducedOffsetLatency: promauto.With(reg).NewSummary(prometheus.SummaryOpts{
			Name:        "cortex_ingest_storage_reader_last_produced_offset_request_duration_seconds",
			Help:        "The duration of requests to fetch the last produced offset of a given partition.",
			ConstLabels: prometheus.Labels{"partition": strconv.Itoa(int(partitionID))},
			Objectives:  latencySummaryObjectives,
			MaxAge:      time.Minute,
			AgeBuckets:  10,
		}),
	}

	p.Service = services.NewTimerService(pollInterval, nil, p.onPollInterval, p.stopping)

	return p
}

func (p *partitionOffsetReader) onPollInterval(ctx context.Context) error {
	// The following call blocks until the last produced offset has been fetched from metadata store. If fetching
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

	// We call getLastProducedOffset() even if there are no goroutines waiting on the result in order to get
	// a constant load on the metadata store. In other words, the load produced on metadata store by this component is
	// constant, regardless the number of received queries with strong consistency enabled.
	offset, err := p.getLastProducedOffset(ctx)
	if err != nil {
		level.Warn(p.logger).Log("msg", "failed to fetch the last produced offset", "err", err)
	}

	// Notify whoever was waiting for it.
	promise.notify(offset, err)
}

// getLastProducedOffset fetches and returns the last produced offset for a partition, or -1 if the
// partition is empty. This function issues a single request.
func (p *partitionOffsetReader) getLastProducedOffset(ctx context.Context) (_ int64, returnErr error) {
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

	return p.metadataStore.GetLastProducedOffsetID(ctx, p.partitionID)
}

// FetchLastProducedOffset returns the result of the *next* "last produced offset" request
// that will be issued.
//
// The "last produced offset" is the offset of the last message written to the partition (starting from 0), or -1 if no
// message has been written yet.
func (p *partitionOffsetReader) FetchLastProducedOffset(ctx context.Context) (int64, error) {
	// Get the promise for the result of the next request that will be issued.
	p.nextResultPromiseMx.RLock()
	promise := p.nextResultPromise
	p.nextResultPromiseMx.RUnlock()

	return promise.wait(ctx)
}
