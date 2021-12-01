package chunks

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

type chunkWriteJob struct {
	cutFile   bool
	seriesRef HeadSeriesRef
	mint      int64
	maxt      int64
	chk       chunkenc.Chunk
	ref       ChunkDiskMapperRef
	callback  func(error)
}

type chunkWriteQueue struct {
	jobMtx      sync.RWMutex
	jobs        []chunkWriteJob
	chunkRefMap map[ChunkDiskMapperRef]int
	headPos     int
	tailPos     int

	size      int
	sizeLimit chan struct{}

	workerCtrl chan struct{}
	workerWg   sync.WaitGroup

	writeChunk writeChunkF

	operationsMetric *prometheus.CounterVec
}

type writeChunkF func(HeadSeriesRef, int64, int64, chunkenc.Chunk, ChunkDiskMapperRef, bool) error

func newChunkWriteQueue(reg prometheus.Registerer, size int, writeChunk writeChunkF) *chunkWriteQueue {
	q := &chunkWriteQueue{
		size:        size,
		jobs:        make([]chunkWriteJob, size),
		chunkRefMap: make(map[ChunkDiskMapperRef]int, size),
		headPos:     -1,
		tailPos:     -1,
		sizeLimit:   make(chan struct{}, size),
		workerCtrl:  make(chan struct{}, size),
		writeChunk:  writeChunk,

		operationsMetric: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "prometheus_tsdb_chunk_write_queue_operations_total",
				Help: "Number of operations on the chunk_write_queue.",
			},
			[]string{"operation"},
		),
	}

	if reg != nil {
		reg.MustRegister(q.operationsMetric)
	}

	q.start()
	return q
}

func (c *chunkWriteQueue) start() {
	c.workerWg.Add(1)

	go func() {
		defer c.workerWg.Done()

		for range c.workerCtrl {
			for !c.IsEmpty() {
				c.processJob()
			}
		}
	}()
}

func (c *chunkWriteQueue) IsEmpty() bool {
	c.jobMtx.RLock()
	defer c.jobMtx.RUnlock()

	return c.isEmpty()
}

func (c *chunkWriteQueue) isEmpty() bool {
	return c.headPos < 0 || c.tailPos < 0
}

func (c *chunkWriteQueue) IsFull() bool {
	c.jobMtx.RLock()
	defer c.jobMtx.RUnlock()

	return c.isFull()
}

func (c *chunkWriteQueue) isFull() bool {
	return (c.headPos+1)%c.size == c.tailPos
}

func (c *chunkWriteQueue) processJob() {
	job, ok := c.getJob()
	if !ok {
		return
	}

	err := c.writeChunk(job.seriesRef, job.mint, job.maxt, job.chk, job.ref, job.cutFile)
	if job.callback != nil {
		job.callback(err)
	}

	c.operationsMetric.WithLabelValues("complete").Inc()

	c.advanceTail()
}

func (c *chunkWriteQueue) advanceTail() {
	c.jobMtx.Lock()
	defer c.jobMtx.Unlock()

	delete(c.chunkRefMap, c.jobs[c.tailPos].ref)
	c.jobs[c.tailPos] = chunkWriteJob{}

	if c.tailPos == c.headPos {
		// Queue is empty.
		c.tailPos = -1
		c.headPos = -1
	} else {
		c.tailPos = (c.tailPos + 1) % c.size
	}

	<-c.sizeLimit
}

func (c *chunkWriteQueue) getJob() (chunkWriteJob, bool) {
	c.jobMtx.RLock()
	defer c.jobMtx.RUnlock()

	if c.isEmpty() {
		return chunkWriteJob{}, false
	}

	return c.jobs[c.tailPos], true
}

func (c *chunkWriteQueue) addJob(job chunkWriteJob) {
	// if queue is full then block here
	c.sizeLimit <- struct{}{}

	c.operationsMetric.WithLabelValues("add").Inc()

	c.jobMtx.Lock()
	defer c.jobMtx.Unlock()

	c.headPos = (c.headPos + 1) % c.size
	c.jobs[c.headPos] = job
	c.chunkRefMap[job.ref] = c.headPos
	if c.tailPos < 0 {
		c.tailPos = c.headPos
	}

	select {
	// non-blocking write to wake up worker because there is at least one job ready to consume
	case c.workerCtrl <- struct{}{}:
	default:
	}
}

func (c *chunkWriteQueue) get(ref ChunkDiskMapperRef) chunkenc.Chunk {
	c.jobMtx.RLock()
	defer c.jobMtx.RUnlock()

	pos, ok := c.chunkRefMap[ref]
	if !ok || pos < 0 || pos >= len(c.jobs) {
		return nil
	}

	c.operationsMetric.WithLabelValues("get").Inc()

	return c.jobs[pos].chk
}

func (c *chunkWriteQueue) stop() {
	close(c.workerCtrl)
	c.workerWg.Wait()

	// restore workerCtrl to make it possible to add jobs to the queue while it is stopped.
	c.workerCtrl = make(chan struct{}, c.size)
}
