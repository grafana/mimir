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

	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/ingest/ingestpb"
)

type WriteAgent struct {
	services.Service

	logger         log.Logger
	metadataStore  *MetadataStore
	segmentStorage *SegmentStorage

	segmentUpdateAllowedMu  sync.Mutex
	segmentUpdateAllowed    bool
	segmentUpdateInProgress sync.WaitGroup // increased only with segmentUpdateAllowedMu lock held, and when segmentUpdateAllowed is true.

	partitionSegmentsMu sync.RWMutex
	partitionSegments   map[int32]*partitionSegmentWithWaiters
}

func NewWriteAgent(cfg Config, logger log.Logger, reg prometheus.Registerer) (*WriteAgent, error) {
	bucketClient, err := bucket.NewClient(context.Background(), cfg.Bucket, "segment-store", logger, reg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create segment store bucket client")
	}

	metadataStore := NewMetadataStore(NewMetadataStorePostgresql(cfg.PostgresConfig), logger)
	segmentStorage := NewSegmentStorage(bucketClient, metadataStore)

	a := &WriteAgent{
		metadataStore:  metadataStore,
		segmentStorage: segmentStorage,
		logger:         logger,
	}

	a.Service = services.NewBasicService(a.starting, a.running, a.stopping)
	return a, nil
}

func (a *WriteAgent) starting(ctx context.Context) error {
	// Start dependencies.
	if err := services.StartAndAwaitRunning(ctx, a.metadataStore); err != nil {
		return err
	}

	return nil
}

func (a *WriteAgent) running(ctx context.Context) error {
	timer := time.NewTicker(250 * time.Millisecond)
	defer timer.Stop()

	a.segmentUpdateAllowedMu.Lock()
	a.segmentUpdateAllowed = true
	a.segmentUpdateAllowedMu.Unlock()

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
			a.flushPendingSegments(ctx)
		}
	}
}

func (a *WriteAgent) stopping(_ error) error {
	// No new segments can be created at this point, but we should write all pending segments one more time.
	// But first wait until all segment updates are done. (This is safe to call,
	// because no new segment updates can be started anymore).
	a.segmentUpdateInProgress.Wait()

	a.flushPendingSegments(context.Background())

	// Stop dependencies.
	if err := services.StopAndAwaitTerminated(context.Background(), a.metadataStore); err != nil {
		level.Warn(a.logger).Log("msg", "failed to stop write agent dependencies", "err", err)
	}

	return nil
}

func (a *WriteAgent) flushPendingSegments(ctx context.Context) {
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

			a.flushPartitionSegmentAndNotifyWaiters(ctx, p, ps)
		}()
	}
	wg.Wait()
}

func (a *WriteAgent) flushPartitionSegmentAndNotifyWaiters(ctx context.Context, partition int32, ps *partitionSegmentWithWaiters) {
	segment, waiters := ps.getCurrentSegmentAndReplaceItWithNewOne()

	if len(segment.Pieces) == 0 {
		sendErrorToWaitersWithoutBlocking(waiters, nil)
		return
	}

	_, err := a.segmentStorage.CommitSegment(ctx, partition, segment)
	sendErrorToWaitersWithoutBlocking(waiters, err)
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
	ch := ps.addPieceToCurrentSegment(wr.Piece)

	// Indicate that segment update was finished, and segments can be written (used when WriteAgent is stopping).
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
	currentSegmentMu      sync.Mutex
	currentSegment        *ingestpb.Segment
	currentSegmentWaiters []chan<- error
}

func newPartitionSegmentWithWaiters() *partitionSegmentWithWaiters {
	return &partitionSegmentWithWaiters{
		currentSegment:        newSegment(),
		currentSegmentWaiters: newWaiters(),
	}
}

func (ps *partitionSegmentWithWaiters) addPieceToCurrentSegment(piece *ingestpb.Piece) <-chan error {
	ch := make(chan error, 1)

	ps.currentSegmentMu.Lock()
	defer ps.currentSegmentMu.Unlock()

	ps.currentSegment.Pieces = append(ps.currentSegment.Pieces, piece)
	ps.currentSegmentWaiters = append(ps.currentSegmentWaiters, ch)

	return ch
}

func (ps *partitionSegmentWithWaiters) getCurrentSegmentAndReplaceItWithNewOne() (*ingestpb.Segment, []chan<- error) {
	ns := newSegment()
	nw := newWaiters()

	ps.currentSegmentMu.Lock()
	segmentToWrite := ps.currentSegment
	waiters := ps.currentSegmentWaiters

	ps.currentSegment = ns
	ps.currentSegmentWaiters = nw
	ps.currentSegmentMu.Unlock()

	return segmentToWrite, waiters
}

func newSegment() *ingestpb.Segment {
	return &ingestpb.Segment{Pieces: make([]*ingestpb.Piece, 0, 128)}
}

func newWaiters() []chan<- error {
	return make([]chan<- error, 0, 128)
}

func sendErrorToWaitersWithoutBlocking(waiters []chan<- error, err error) {
	for _, w := range waiters {
		select {
		case w <- err: // ok
		default: // don't block
		}
		close(w)
	}
}
