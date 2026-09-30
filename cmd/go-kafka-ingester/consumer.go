// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"runtime"
	"sync"
	"time"

	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/kafka"
	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/protection"
	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/record"
	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/segment"
	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/store"
)

const (
	applyBatch = 64
	// About a thousand series of fetched records, below which decoding them in parallel costs
	// more than it saves.
	parallelPrepareBytes = 512 * 1024
)

type applyKind uint8

const (
	// A record as fetched, which the applier decodes.
	applyFetched applyKind = iota
	applyRecord
	applyMaintain
)

type applyCommand struct {
	kind          applyKind
	fetched       kafka.RawRecord
	offset        int64
	timestampMs   int64
	highWatermark int64
	bytes         int
	tenant        string
	// Nil for a record without a payload.
	request *record.DecodedRequest
	keys    []segment.SeriesKey
	hashes  []store.OptionalHash
}

func prepareRecord(command applyCommand) (applyCommand, error) {
	raw := command.fetched
	decoded, spans, ok, err := raw.DecodeWithLabelSpans()
	if err != nil {
		return applyCommand{}, fmt.Errorf("decode offset %d: %w", raw.Offset, err)
	}
	prepared := applyCommand{
		kind:          applyRecord,
		offset:        raw.Offset,
		timestampMs:   raw.TimestampMs,
		highWatermark: command.highWatermark,
		bytes:         len(raw.Payload),
		tenant:        raw.Tenant,
	}
	// Hashing the series for the segment log runs here, in parallel, and the ordered applier only
	// looks the keys up.
	if ok {
		prepared.keys = segment.SeriesKeysWithLabelBytes(raw.Tenant, &decoded, raw.Payload, spans)
		prepared.hashes = store.SeriesHashes(&decoded)
		prepared.request = &decoded
	}
	return prepared, nil
}

type applier struct {
	store              *store.Store
	consistency        *kafka.Consistency
	segmentLog         *segment.Log
	cluster            int
	fatal              *signals
	coveragePath       string
	coveragePending    bool
	lastTimestampMs    int64
	pushCircuitBreaker *protection.CircuitBreaker
	// The last offset of the startup replay, which the Go ingester consumes while starting, before
	// its push circuit breaker is activated.
	replayTarget int64
}

// loop applies the queued commands in order until commands is closed, and returns the log's
// position for the head snapshot.
func (a *applier) loop(commands <-chan applyCommand) store.SnapshotOffset {
	failed := false
	for command := range commands {
		batch := []applyCommand{command}
	drain:
		for len(batch) < applyBatch {
			select {
			case next, ok := <-commands:
				if !ok {
					break drain
				}
				batch = append(batch, next)
			default:
				break drain
			}
		}
		// A failed applier keeps receiving, so its consumer is never blocked, until the shutdown
		// the fatal error starts closes the queue.
		if failed {
			continue
		}
		if err := a.applyBatch(batch); err != nil {
			log.Printf("Kafka cluster %d failed: %v", a.cluster, err)
			a.fatal.fire()
			failed = true
		}
	}
	return a.snapshotOffset()
}

func (a *applier) snapshotOffset() store.SnapshotOffset {
	if err := a.segmentLog.Close(); err != nil {
		log.Printf("Kafka cluster %d final persistence sync failed: %v", a.cluster, err)
		a.fatal.fire()
	}
	offset, ok := a.segmentLog.LastOffset()
	return store.SnapshotOffset{Offset: offset, HasOffset: ok, TimestampMs: a.lastTimestampMs}
}

// applyBatch applies queued commands in order, turning each run of records into one parallel
// store batch.
func (a *applier) applyBatch(commands []applyCommand) error {
	commands, err := prepareFetched(commands)
	if err != nil {
		return err
	}
	records := make([]applyCommand, 0, len(commands))
	for _, command := range commands {
		switch command.kind {
		case applyRecord:
			records = append(records, command)
		case applyMaintain:
			if err := a.applyRecords(records); err != nil {
				return err
			}
			records = records[:0]
			if err := a.segmentLog.Maintain(); err != nil {
				return err
			}
			if a.cluster == 0 {
				if err := a.store.PruneExpired(); err != nil {
					return err
				}
			}
		default:
			return errors.New("fetched records are prepared first")
		}
	}
	return a.applyRecords(records)
}

// prepareFetched decodes fetched records here rather than each on its own goroutine: once caught
// up, handing a small record between goroutines cost more than decoding it, and a replay's full
// batches are decoded with one fan-out.
func prepareFetched(commands []applyCommand) ([]applyCommand, error) {
	fetchedBytes := 0
	for _, command := range commands {
		if command.kind == applyFetched {
			fetchedBytes += len(command.fetched.Payload)
		}
	}
	prepared := make([]applyCommand, len(commands))
	errs := make([]error, len(commands))
	prepare := func(index int) {
		if commands[index].kind != applyFetched {
			prepared[index] = commands[index]
			return
		}
		prepared[index], errs[index] = prepareRecord(commands[index])
	}
	if fetchedBytes < parallelPrepareBytes {
		for index := range commands {
			prepare(index)
		}
	} else {
		var wg sync.WaitGroup
		next := make(chan int)
		for range min(runtime.GOMAXPROCS(0), len(commands)) {
			wg.Go(func() {
				for index := range next {
					prepare(index)
				}
			})
		}
		for index := range commands {
			next <- index
		}
		close(next)
		wg.Wait()
	}
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}
	return prepared, nil
}

var emptyRequest = &record.DecodedRequest{}

func (a *applier) applyRecords(records []applyCommand) error {
	if len(records) == 0 {
		return nil
	}
	lastOffset := records[len(records)-1].offset
	// Rejects a duplicate or old offset before any record reaches the store, so the store never
	// holds data the segment log does not.
	previous, hasPrevious := a.segmentLog.LastOffset()
	for _, command := range records {
		if hasPrevious && command.offset <= previous {
			return fmt.Errorf("Kafka offset %d is not after offset %s", command.offset, optionalOffset(previous, hasPrevious))
		}
		previous, hasPrevious = command.offset, true
	}
	ingestedMs := nowMs()
	if err := a.segmentLog.BeginBatch(ingestedMs); err != nil {
		return err
	}
	frames := make([]*segment.CompressedFrame, len(records))
	batch := make([]store.IngestRecord, 0, len(records))
	for index, command := range records {
		request := command.request
		if request == nil {
			request = emptyRequest
		}
		frame, err := a.segmentLog.Encode(command.offset, command.timestampMs, ingestedMs, command.tenant, request, command.keys)
		if err != nil {
			return fmt.Errorf("encode offset %d: %w", command.offset, err)
		}
		frames[index] = frame
		if command.request != nil {
			batch = append(batch, store.IngestRecord{
				Tenant:       command.tenant,
				Request:      *command.request,
				IngestedMs:   ingestedMs,
				TrackRate:    true,
				Bytes:        command.bytes,
				SeriesHashes: command.hashes,
			})
		}
	}
	// Like the Go ingester, whose pusher retries records while its push circuit breaker is open,
	// and counts each head append.
	var permit protection.Permit
	breaker := a.pushCircuitBreaker
	if breaker != nil && lastOffset > a.replayTarget {
		for {
			acquired, err := breaker.TryAcquire()
			var open *protection.OpenError
			if errors.As(err, &open) {
				time.Sleep(max(open.Remaining, 10*time.Millisecond))
				continue
			}
			if err != nil {
				return err
			}
			permit = acquired
			break
		}
	}
	flushes, err := a.store.IngestFlushes(batch)
	if err != nil {
		return fmt.Errorf("apply records through offset %d: %w", lastOffset, err)
	}
	if breaker != nil && permit.Active() {
		breaker.FinishAll(permit, int(flushes))
	}
	for index, command := range records {
		if err := a.segmentLog.AppendCompressed(frames[index]); err != nil {
			return err
		}
		a.consistency.Consumed(a.cluster, command.offset, command.highWatermark, command.timestampMs)
		a.lastTimestampMs = max(a.lastTimestampMs, command.timestampMs)
		if a.coveragePending {
			if err := raiseCoverage(a.coveragePath, command.timestampMs); err != nil {
				return err
			}
			a.coveragePending = false
		}
	}
	return nil
}

type fetched struct {
	record        kafka.RawRecord
	highWatermark int64
	err           error
}

type consumer struct {
	cluster        int
	partition      int32
	config         kafka.Config
	client         *kafka.PartitionClient
	consistency    *kafka.Consistency
	initialOffset  int64
	replayTarget   int64
	replayComplete bool
	latestOffset   int64
	syncInterval   time.Duration
	shutdown       *signals
	warmup         *signals
	ready          chan struct{}
	applier        *applier
}

// startFetcher fetches records into the returned channel until stopped.
func startFetcher(client *kafka.PartitionClient) (<-chan fetched, func()) {
	ctx, cancel := context.WithCancel(context.Background())
	records := make(chan fetched)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			next, highWatermark, err := client.NextRaw(ctx)
			if ctx.Err() != nil {
				return
			}
			select {
			case records <- fetched{record: next, highWatermark: highWatermark, err: err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				select {
				case <-time.After(100 * time.Millisecond):
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return records, func() {
		cancel()
		<-done
	}
}

// fetchStalled reports whether a fetch that made no progress for a while stalled, given the
// partition's latest offset then. An unknown latest offset (-1, as a client that lost the
// partition reports it) is a stall: taking it for "nothing new" kept a pod from ever reconnecting.
func fetchStalled(latest int64, err error, nextOffset int64) bool {
	return err != nil || latest < 0 || latest > nextOffset
}

// waitWarmup blocks until every cluster replayed, and reports false on shutdown.
func (c *consumer) waitWarmup() bool {
	select {
	case <-c.warmup.closed:
		return true
	case <-c.shutdown.closed:
		return false
	}
}

func (c *consumer) run() store.SnapshotOffset {
	commands := make(chan applyCommand, 64)
	applied := make(chan store.SnapshotOffset, 1)
	go func() { applied <- c.applier.loop(commands) }()
	records, stopFetcher := startFetcher(c.client)
	nextOffset := c.initialOffset
	lastConsumeLog, lastReplayLog := time.Now(), time.Now()
	loggedFirstConsume := false
	const stallTimeout = 30 * time.Second
	stall := time.NewTimer(stallTimeout)
	defer stall.Stop()
	syncTicker := time.NewTicker(c.syncInterval)
	defer syncTicker.Stop()
	watermarkTicker := time.NewTicker(30 * time.Second)
	defer watermarkTicker.Stop()
	readyPending := true
	stopped := false
	if c.replayComplete {
		log.Printf("phase=kafka_replay_complete cluster=%d partition=%d latest_offset=%d", c.cluster, c.partition, c.latestOffset)
		close(c.ready)
		readyPending = false
		stopped = !c.waitWarmup()
		stall.Reset(stallTimeout)
	}
loop:
	for !stopped {
		select {
		case <-c.shutdown.closed:
			break loop
		case <-c.applier.fatal.closed:
			break loop
		case next := <-records:
			if next.err != nil {
				log.Printf("Kafka cluster %d: %v", c.cluster, next.err)
				continue
			}
			offset := next.record.Offset
			commands <- applyCommand{kind: applyFetched, fetched: next.record, highWatermark: next.highWatermark}
			nextOffset = offset + 1
			stall.Reset(stallTimeout)
			if !loggedFirstConsume || time.Since(lastConsumeLog) >= time.Minute {
				log.Printf("phase=kafka_consume_progress cluster=%d partition=%d offset=%d latest_offset=%d lag=%d",
					c.cluster, c.partition, offset, next.highWatermark, max(next.highWatermark-nextOffset, 0))
				lastConsumeLog, loggedFirstConsume = time.Now(), true
			}
			if readyPending && time.Since(lastReplayLog) >= 30*time.Second {
				log.Printf("phase=kafka_replay_progress cluster=%d partition=%d offset=%d latest_offset=%d lag=%d",
					c.cluster, c.partition, offset, next.highWatermark, max(next.highWatermark-(offset+1), 0))
				lastReplayLog = time.Now()
			}
			if readyPending && offset >= c.replayTarget {
				log.Printf("phase=kafka_replay_complete cluster=%d partition=%d offset=%d latest_offset=%d", c.cluster, c.partition, offset, next.highWatermark)
				close(c.ready)
				readyPending = false
				if !c.waitWarmup() {
					break loop
				}
				stall.Reset(stallTimeout)
			}
		case <-stall.C:
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			latest, err := c.client.GetOffset(ctx, kafka.Latest)
			cancel()
			if !fetchStalled(latest, err, nextOffset) {
				stall.Reset(stallTimeout)
				continue
			}
			log.Printf("phase=kafka_fetch_timeout cluster=%d partition=%d next_offset=%d latest_offset=%d error=%v", c.cluster, c.partition, nextOffset, latest, err)
			if refreshed, err := kafka.Connect(c.config); err != nil {
				log.Printf("phase=kafka_client_reconnect_failed cluster=%d partition=%d error=%v", c.cluster, c.partition, err)
			} else if err := refreshed.Assign(nextOffset); err != nil {
				log.Printf("phase=kafka_client_reconnect_failed cluster=%d partition=%d error=%v", c.cluster, c.partition, err)
				refreshed.Close()
			} else {
				stopFetcher()
				c.client.Close()
				c.client = refreshed
				c.consistency.SetClient(c.cluster, refreshed)
				records, stopFetcher = startFetcher(refreshed)
				log.Printf("phase=kafka_client_reconnected cluster=%d partition=%d next_offset=%d", c.cluster, c.partition, nextOffset)
			}
			stall.Reset(stallTimeout)
		case <-syncTicker.C:
			commands <- applyCommand{kind: applyMaintain}
			if nextOffset > c.initialOffset {
				if err := c.client.Commit(nextOffset); err != nil {
					log.Printf("phase=kafka_commit_failed cluster=%d partition=%d error=%v", c.cluster, c.partition, err)
				}
			}
		case <-watermarkTicker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			if offset, err := c.client.GetOffset(ctx, kafka.Latest); err == nil {
				c.consistency.HighWatermark(c.cluster, offset)
			}
			cancel()
		}
	}
	stopFetcher()
	close(commands)
	offset := <-applied
	c.client.Close()
	return offset
}
