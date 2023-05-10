package limiter

import (
	"container/list"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/platinummonkey/go-concurrency-limits/core"
)

// EvictFunc is a type denoting a function used to evict an
// element from a QueueBlockingLimiter backlog
type EvictFunc func()

// QueueOrdering is an enum used for configuring the order in
// which elements in a QueueBlockingLimiter backlog are consumed
type QueueOrdering string

const (
	// OrderingFIFO is an enum constant used to represent
	// a first-in first-out ordering for queue elements.
	// This means that the oldest elements in a queue are
	// the first to be consumed
	OrderingFIFO QueueOrdering = "fifo"

	// OrderingLIFO is an enum constant used to represent
	// a last-in first-out ordering for queue elements.
	// This means that the newest elements in a queue are
	// the first to be consumed
	OrderingLIFO QueueOrdering = "lifo"

	metricTagOrdering = "ordering"
)

type queueElement struct {
	ctx         context.Context
	releaseChan chan<- core.Listener
	next, prev  *queueElement
}

func (e *queueElement) setListener(listener core.Listener) bool {
	select {
	case e.releaseChan <- listener:
		close(e.releaseChan)
		return true
	default:
		// timeout has expired or context has been cancelled
		return false
	}
}

func (q *queue) evictionFunc(e *list.Element) func() {
	return func() {
		q.mu.Lock()
		defer q.mu.Unlock()
		q.list.Remove(e)
	}
}

type queue struct {
	list     *list.List
	ordering QueueOrdering
	mu       sync.RWMutex
}

func (q *queue) len() uint64 {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return uint64(q.list.Len())
}

func (q *queue) push(ctx context.Context) (EvictFunc, <-chan core.Listener) {
	q.mu.Lock()
	defer q.mu.Unlock()
	releaseChan := make(chan core.Listener)

	e := &queueElement{ctx: ctx, releaseChan: releaseChan}

	// We always push to the front of the list regardless of
	// queue order. As usage of the list will always assume
	// Front == newest and Back == Oldest
	listElement := q.list.PushFront(e)

	return q.evictionFunc(listElement), releaseChan
}

func (q *queue) pop() *queueElement {
	evict, ele := q.peek()
	if evict != nil {
		evict()
	}

	return ele
}

// peek will return the next queue element to process
// depending on the ordering configured in queue.
// The element returned is not evicted from the queue
// until EvictFunc is invoked
func (q *queue) peek() (EvictFunc, *queueElement) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	var element *list.Element

	switch q.ordering {
	case OrderingFIFO:
		element = q.list.Back()
	case OrderingLIFO:
		element = q.list.Front()
	}

	if element != nil {
		return q.evictionFunc(element), element.Value.(*queueElement)
	}

	return nil, nil
}

// QueueBlockingListener implements a blocking listener for the QueueBlockingLimiter
type QueueBlockingListener struct {
	delegateListener core.Listener
	limiter          *QueueBlockingLimiter
}

func (l *QueueBlockingListener) unblock() {
	l.limiter.mu.Lock()
	defer l.limiter.mu.Unlock()

	evict, nextEvent := l.limiter.backlog.peek()

	// The queue is empty
	if nextEvent == nil {
		return
	}

	listener, ok := l.limiter.delegate.Acquire(nextEvent.ctx)

	if ok && listener != nil {
		// We successfully acquired a listener from the
		// delegate. Now we can evict the element from
		// the queue
		evict()

		// If the listener is not accepted due to subtle timings
		// between setListener being invoked and the element
		// expiration elapsing we need to be sure to release it.
		if accepted := nextEvent.setListener(listener); !accepted {
			listener.OnIgnore()
		}
	}
	// otherwise: still can't acquire the limit.  unblock will be called again next time the limit is released.
}

// OnDropped is called to indicate the request failed and was dropped due to being rejected by an external limit or
// hitting a timeout.  Loss based Limit implementations will likely do an aggressive reducing in limit when this
// happens.
func (l *QueueBlockingListener) OnDropped() {
	l.delegateListener.OnDropped()
	l.unblock()
}

// OnIgnore is called to indicate the operation failed before any meaningful RTT measurement could be made and
// should be ignored to not introduce an artificially low RTT.
func (l *QueueBlockingListener) OnIgnore() {
	l.delegateListener.OnIgnore()
	l.unblock()
}

// OnSuccess is called as a notification that the operation succeeded and internally measured latency should be
// used as an RTT sample.
func (l *QueueBlockingListener) OnSuccess() {
	l.delegateListener.OnSuccess()
	l.unblock()
}

// QueueBlockingLimiter implements a Limiter that blocks the caller when the limit has been reached.  This strategy
// ensures the resource is properly protected but favors availability over latency by not fast failing requests when
// the limit has been reached.  To help keep success latencies low and minimize timeouts any blocked requests are
// processed in last in/first out order.
//
// Use this limiter only when the concurrency model allows the limiter to be blocked.
type QueueBlockingLimiter struct {
	delegate            core.Limiter
	maxBacklogSize      uint64
	maxBacklogTimeout   time.Duration
	backlogEvictDoneCtx bool
	ordering            QueueOrdering

	backlog *queue
	mu      sync.RWMutex
}

// QueueLimiterConfig is a struct used to encapsulate the constructor arguments
// needed for creating a QueueBlockingLimiter instance
type QueueLimiterConfig struct {
	Ordering            QueueOrdering `yaml:"ordering,omitempty" json:"ordering,omitempty"`
	MaxBacklogSize      int           `yaml:"maxBacklogSize,omitempty" json:"maxBacklogSize,omitempty"`
	MaxBacklogTimeout   time.Duration `yaml:"maxBacklogTimeout,omitempty" json:"maxBacklogTimeout,omitempty"`
	BacklogEvictDoneCtx bool          `yaml:"backlogEvictDoneCtx,omitempty" json:"backlogEvictDoneCtx,omitempty"`

	MetricRegistry core.MetricRegistry
	Tags           []string `yaml:"tags,omitempty" json:"tags,omitempty"`
}

// ApplyDefaults is used by QueueBlockingLimiter constructors
// to set defaults for optional limiter configuration arguments
func (c *QueueLimiterConfig) ApplyDefaults() {

	if c.MaxBacklogSize <= 0 {
		c.MaxBacklogSize = 100
	}

	if c.MaxBacklogTimeout == 0 {
		c.MaxBacklogTimeout = time.Millisecond * 1000
	}

	if c.MetricRegistry == nil {
		c.MetricRegistry = &core.EmptyMetricRegistry{}
	}

	if c.Ordering == "" {
		c.Ordering = OrderingLIFO
	}

	c.Tags = append(c.Tags, metricTagOrdering, string(c.Ordering))
}

// NewQueueBlockingLimiterFromConfig will create a new QueueBlockingLimiter
func NewQueueBlockingLimiterFromConfig(
	delegate core.Limiter,
	config QueueLimiterConfig,
) *QueueBlockingLimiter {

	config.ApplyDefaults()

	l := &QueueBlockingLimiter{
		delegate:            delegate,
		maxBacklogSize:      uint64(config.MaxBacklogSize),
		maxBacklogTimeout:   config.MaxBacklogTimeout,
		backlogEvictDoneCtx: config.BacklogEvictDoneCtx,
		backlog: &queue{
			list:     list.New(),
			ordering: OrderingFIFO,
		},
	}

	config.MetricRegistry.RegisterGauge(
		core.MetricQueueLimit, core.NewIntMetricSupplierWrapper(func() int {
			return config.MaxBacklogSize
		}), config.Tags...)

	config.MetricRegistry.RegisterGauge(
		core.MetricQueueSize, core.NewUint64MetricSupplierWrapper(l.backlog.len), config.Tags...)

	return l
}

// NewQueueBlockingLimiterWithDefaults will create a new QueueBlockingLimiter with default values.
func NewQueueBlockingLimiterWithDefaults(
	delegate core.Limiter,
) *QueueBlockingLimiter {
	return NewQueueBlockingLimiterFromConfig(
		delegate,
		QueueLimiterConfig{},
	)
}

func (l *QueueBlockingLimiter) tryAcquire(ctx context.Context) core.Listener {
	// Try to acquire a token and return immediately if successful
	listener, ok := l.delegate.Acquire(ctx)
	if ok && listener != nil {
		return listener
	}

	// Restrict backlog size so the queue doesn't grow unbounded during an outage
	if l.backlog.len() >= l.maxBacklogSize {
		return nil
	}

	// Create a holder for a listener and block until a listener is released by another
	// operation.  Holders will be unblocked in LIFO or FIFO order depending on whatever
	// ordering was configured when backlog was instantiated
	evict, eventReleaseChan := l.backlog.push(ctx)

	// We're using a nil chan so that we
	// can avoid needing to duplicate the
	// following select statement to support
	// a conditional case.
	var ctxDone <-chan struct{}
	if l.backlogEvictDoneCtx {
		ctxDone = ctx.Done()
	}

	select {
	case listener = <-eventReleaseChan:
		// If we have received a listener then that means
		// that 'unblock' has already evicted this element
		// from the queue for us.
		return listener
	case <-time.After(l.maxBacklogTimeout):
		// Remove the holder from the backlog.
		evict()
		return nil
	case <-ctxDone:
		// The context has been cancelled before `maxBacklogTimeout`
		// could elapse. Since this context no longer needs a listener
		// we evict it from the backlog to free up space.
		evict()
		return nil
	}
}

// Acquire a token from the limiter.  Returns an Optional.empty() if the limit has been exceeded.
// If acquired the caller must call one of the Listener methods when the operation has been completed to release
// the count.
//
// ctx Context for the request. The context is used by advanced strategies such as LookupPartitionStrategy
// and early queue eviction on context cancellation.
func (l *QueueBlockingLimiter) Acquire(ctx context.Context) (core.Listener, bool) {
	delegateListener := l.tryAcquire(ctx)
	if delegateListener == nil {
		return nil, false
	}
	return &QueueBlockingListener{
		delegateListener: delegateListener,
		limiter:          l,
	}, true
}

func (l *QueueBlockingLimiter) String() string {
	return fmt.Sprintf("QueueBlockingLimiter{delegate=%v, maxBacklogSize=%d, maxBacklogTimeout=%v, ordering=%v}",
		l.delegate, l.maxBacklogSize, l.maxBacklogTimeout, l.ordering)
}
