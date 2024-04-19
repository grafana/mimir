package cache

import (
	"sync"

	"github.com/pkg/errors"
)

var errAsyncQueueFull = errors.New("the async queue is full")

type asyncQueue struct {
	stopCh  chan struct{}
	queueCh chan func()

	workers sync.WaitGroup
}

func newAsyncQueue(length, maxConcurrency int) *asyncQueue {
	q := &asyncQueue{
		stopCh:  make(chan struct{}),
		queueCh: make(chan func(), length),
	}
	// Start a number of goroutines - processing async operations - equal
	// to the max concurrency we have.
	q.workers.Add(maxConcurrency)
	for i := 0; i < maxConcurrency; i++ {
		go q.asyncQueueProcessLoop()
	}
	return q
}

// submit adds an operation to the queue or returns an error if the queue is full
func (q *asyncQueue) submit(op func()) error {
	select {
	case q.queueCh <- op:
		return nil
	default:
		return errAsyncQueueFull
	}
}

func (q *asyncQueue) stop() {
	close(q.stopCh)
	q.workers.Wait()
}

func (q *asyncQueue) asyncQueueProcessLoop() {
	defer q.workers.Done()

	for {
		select {
		case op := <-q.queueCh:
			op()
		case <-q.stopCh:
			return
		}
	}
}
