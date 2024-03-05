// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/ingest/ingestpb"
)

type WriteAgent struct {
	*services.BasicService

	logger              log.Logger
	dependencies        *services.Manager
	segmentStorage      *SegmentStorage
	flushInterval       time.Duration
	maxSegmentSizeBytes int64
	uploadHedgeDelay    time.Duration

	segmentUpdateAllowedMu  sync.Mutex
	segmentUpdateAllowed    bool
	segmentUpdateInProgress sync.WaitGroup // increased only with segmentUpdateAllowedMu lock held, and when segmentUpdateAllowed is true.

	partitionSegmentsMu sync.RWMutex
	partitionSegments   map[int32]*partitionSegmentWithWaiters

	asyncFlushWaitingGroup sync.WaitGroup

	flushLatency prometheus.Histogram
}

func NewWriteAgent(cfg Config, logger log.Logger, reg prometheus.Registerer) (*WriteAgent, error) {
	bucketClient, err := bucket.NewClient(context.Background(), cfg.Bucket, "write-agent-segment-store", logger, reg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create segment store bucket client")
	}

	var (
		wrappedReg     = prometheus.WrapRegistererWith(prometheus.Labels{"component": "write-agent"}, reg)
		metadataStore  = NewMetadataStore(NewMetadataStorePostgresql(cfg.PostgresConfig), logger)
		segmentStorage = NewSegmentStorage(bucketClient, metadataStore, wrappedReg)
	)

	mgr, err := services.NewManager(metadataStore)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create write agent dependencies")
	}

	return newWriteAgent(cfg.FlushInterval, 4*1024*1024, cfg.UploadHedgeDelay, segmentStorage, logger, wrappedReg, mgr), nil
}

func newWriteAgent(flushInterval time.Duration, maxSegmentSizeBytes int64, uploadHedgeDelay time.Duration, segmentStorage *SegmentStorage, logger log.Logger, reg prometheus.Registerer, dependencies *services.Manager) *WriteAgent {
	a := &WriteAgent{
		segmentStorage:      segmentStorage,
		logger:              logger,
		flushInterval:       flushInterval,
		uploadHedgeDelay:    uploadHedgeDelay,
		maxSegmentSizeBytes: maxSegmentSizeBytes,
		partitionSegments:   map[int32]*partitionSegmentWithWaiters{},
		dependencies:        dependencies,
	}

	a.flushLatency = promauto.With(reg).NewHistogram(
		prometheus.HistogramOpts{
			Name:                            "cortex_write_agent_flush_latency_seconds",
			Help:                            "Histogram of flush latency in seconds",
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: 1 * time.Hour,
		})

	a.BasicService = services.NewBasicService(a.starting, a.running, a.stopping)
	return a
}

func (a *WriteAgent) starting(ctx context.Context) error {
	// Start dependencies.
	if a.dependencies != nil {
		if err := services.StartManagerAndAwaitHealthy(ctx, a.dependencies); err != nil {
			return err
		}

		// TODO: listen for failures and stop write agent if any dependency fails.
	}

	// Enable segment updates in starting, so that when client observes Running state, this is already set.
	a.segmentUpdateAllowedMu.Lock()
	a.segmentUpdateAllowed = true
	a.segmentUpdateAllowedMu.Unlock()
	return nil
}

func (a *WriteAgent) running(ctx context.Context) error {
	timer := time.NewTicker(a.flushInterval)
	defer timer.Stop()

	// Disable segment updates when running function stops.
	defer func() {
		a.segmentUpdateAllowedMu.Lock()
		a.segmentUpdateAllowed = false
		a.segmentUpdateAllowedMu.Unlock()
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			// Trigger a flush of pending segments without waiting.
			a.asyncFlushPendingSegments(ctx)
		}
	}
}

func (a *WriteAgent) stopping(_ error) error {
	// No new segments can be created at this point, but we should write all pending segments one more time.
	// But first wait until all segment updates are done. (This is safe to call,
	// because no new segment updates can be started anymore -- running function disabled those).
	a.segmentUpdateInProgress.Wait()

	// Trigger another async flush of pending segments (there may still be others still running from running() function).
	a.asyncFlushPendingSegments(context.Background())

	// Wait until all async flushes completed.
	a.asyncFlushWaitingGroup.Wait()

	// Stop dependencies.
	if a.dependencies != nil {
		if err := services.StopManagerAndAwaitStopped(context.Background(), a.dependencies); err != nil {
			level.Warn(a.logger).Log("msg", "failed to stop write agent dependencies", "err", err)
		}
	}

	return nil
}

func (a *WriteAgent) asyncFlushPendingSegments(ctx context.Context) {
	a.asyncFlushWaitingGroup.Add(1)

	go func() {
		defer a.asyncFlushWaitingGroup.Done()
		a.syncFlushPendingSegments(ctx)
	}()
}

func (a *WriteAgent) syncFlushPendingSegments(ctx context.Context) {
	// Let's make a copy of partitionSegments that we will flush.
	partitionsToFlush := map[int32]*partitionSegmentWithWaiters{}

	a.partitionSegmentsMu.RLock()
	for p, ps := range a.partitionSegments {
		partitionsToFlush[p] = ps
	}
	a.partitionSegmentsMu.RUnlock()

	// Now let's flush all partitions, without holding partitionSegmentsMu lock. We do that concurrently. We don't use
	// our existing concurrency methods, because we actually want to ignore context **in this method** and flush all partition,
	// even if it means that flush will end up with error due to context cancellation.
	// But it's important to send those notifications to unblock calls to Write.
	wg := sync.WaitGroup{}
	for p, ps := range partitionsToFlush {
		p, ps := p, ps

		wg.Add(1)
		go func() {
			defer wg.Done()

			a.flushPartitionSegmentAndNotifyWaiters(ctx, p, ps, 0)
		}()
	}
	wg.Wait()
}

func (a *WriteAgent) flushPartitionSegmentAndNotifyWaiters(ctx context.Context, partition int32, ps *partitionSegmentWithWaiters, minSizeForFlushing int64) {
	segment, waiters, hasCutNewSegment := ps.getCurrentSegmentAndReplaceItWithNewOne(minSizeForFlushing)

	// A new segment may have not been cut if the minSizeForFlushing is not honored.
	// If no new segment has been cut, we have to skip the flushing and we can't notify waiters (yet).
	if !hasCutNewSegment {
		return
	}

	if len(segment.Pieces) == 0 {
		sendErrorToWaiters(waiters, nil)
		return
	}

	start := time.Now()
	segmentRef, err := a.segmentStorage.CommitSegment(ctx, partition, segment, a.uploadHedgeDelay, time.Now())
	elapsed := time.Since(start)

	a.flushLatency.Observe(elapsed.Seconds())

	if err == nil {
		level.Debug(a.logger).Log("msg", "flushing partition succeeded", "partition", partition, "write_requests", len(segment.Pieces), "elapsed", elapsed, "segmentRef", segmentRef)
	} else {
		level.Error(a.logger).Log("msg", "flushing partition failed", "partition", partition, "write_requests", len(segment.Pieces), "elapsed", elapsed, "segmentRef", segmentRef, "err", err)
	}

	sendErrorToWaiters(waiters, err)
}

func (a *WriteAgent) asyncFlushPartitionSegmentAndNotifyWaiters(partition int32, ps *partitionSegmentWithWaiters, minSizeForFlushing int64) {
	a.asyncFlushWaitingGroup.Add(1)

	go func() {
		defer a.asyncFlushWaitingGroup.Done()

		a.flushPartitionSegmentAndNotifyWaiters(a.BasicService.ServiceContext(), partition, ps, minSizeForFlushing)
	}()
}

func (a *WriteAgent) getOrCreatePartitionSegment(partition int32) *partitionSegmentWithWaiters {
	a.partitionSegmentsMu.RLock()
	s := a.partitionSegments[partition]
	a.partitionSegmentsMu.RUnlock()

	if s != nil {
		return s
	}

	// create new outside of lock.
	ns := newPartitionSegmentWithWaiters()

	a.partitionSegmentsMu.Lock()
	defer a.partitionSegmentsMu.Unlock()

	// maybe it exists now?
	s = a.partitionSegments[partition]
	if s != nil {
		return s
	}
	a.partitionSegments[partition] = ns
	return ns
}

func (a *WriteAgent) checkIfSegmentUpdatesAreAllowedAndIncreaseUpdatesInProgress() bool {
	a.segmentUpdateAllowedMu.Lock()
	defer a.segmentUpdateAllowedMu.Unlock()
	if a.segmentUpdateAllowed {
		a.segmentUpdateInProgress.Add(1)
		return true
	}
	return false
}

// append write request to the current segment, and wait until it's written to storage.
func (a *WriteAgent) Write(ctx context.Context, wr *ingestpb.WriteRequest) (*ingestpb.WriteResponse, error) {
	if !a.checkIfSegmentUpdatesAreAllowedAndIncreaseUpdatesInProgress() {
		return nil, errors.New("WriteAgent is not running.")
	}

	ps := a.getOrCreatePartitionSegment(wr.PartitionId)
	ch, segmentSize := ps.addPieceToCurrentSegment(wr.Piece)

	if segmentSize > a.maxSegmentSizeBytes {
		a.asyncFlushPartitionSegmentAndNotifyWaiters(wr.PartitionId, ps, a.maxSegmentSizeBytes)
	}

	// Indicate that segment update was finished, and segments can be written (used when WriteAgent is stopping).
	// IMPORTANT: this needs to be called after asyncFlushPartitionSegmentAndNotifyWaiters().
	a.segmentUpdateInProgress.Done()

	// Wait for response from writing.
	select {
	case err := <-ch:
		if err == nil {
			return &ingestpb.WriteResponse{}, nil
		}
		return nil, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type partitionSegmentWithWaiters struct {
	currentSegmentMu        sync.Mutex
	currentSegment          *ingestpb.Segment
	currentSegmentSizeBytes int64
	currentSegmentWaiters   []chan<- error
}

func newPartitionSegmentWithWaiters() *partitionSegmentWithWaiters {
	return &partitionSegmentWithWaiters{
		currentSegment:          newSegment(),
		currentSegmentSizeBytes: 0,
		currentSegmentWaiters:   newWaiters(),
	}
}

func (ps *partitionSegmentWithWaiters) addPieceToCurrentSegment(piece *ingestpb.Piece) (<-chan error, int64) {
	ch := make(chan error, 1)

	ps.currentSegmentMu.Lock()
	defer ps.currentSegmentMu.Unlock()

	ps.currentSegment.Pieces = append(ps.currentSegment.Pieces, piece)
	ps.currentSegmentSizeBytes += int64(piece.Size())
	ps.currentSegmentWaiters = append(ps.currentSegmentWaiters, ch)

	return ch, ps.currentSegmentSizeBytes
}

func (ps *partitionSegmentWithWaiters) getCurrentSegmentAndReplaceItWithNewOne(minSizeForFlushing int64) (*ingestpb.Segment, []chan<- error, bool) {
	// Optimistically assume the new segment will be cut, so allocate new segments and waiters before taking the lock.
	ns := newSegment()
	nw := newWaiters()

	ps.currentSegmentMu.Lock()
	defer ps.currentSegmentMu.Unlock()

	// First of all, check if the min size for flushing is honored.
	if ps.currentSegmentSizeBytes < minSizeForFlushing {
		return nil, nil, false
	}

	segmentToWrite := ps.currentSegment
	waiters := ps.currentSegmentWaiters

	ps.currentSegment = ns
	ps.currentSegmentSizeBytes = 0
	ps.currentSegmentWaiters = nw

	return segmentToWrite, waiters, true
}

func newSegment() *ingestpb.Segment {
	return &ingestpb.Segment{Pieces: make([]*ingestpb.Piece, 0, 128)}
}

func newWaiters() []chan<- error {
	return make([]chan<- error, 0, 128)
}

func sendErrorToWaiters(waiters []chan<- error, err error) {
	for _, w := range waiters {
		select {
		case w <- err: // ok
		default: // don't block
		}
		close(w)
	}
}
