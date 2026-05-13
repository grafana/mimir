// SPDX-License-Identifier: AGPL-3.0-only

package usagetracker

import (
	"context"
	"errors"
	"hash/fnv"
	"runtime"
	"sync"
	"time"

	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/usagetracker/clock"
	"github.com/grafana/mimir/pkg/usagetracker/tenantshard"
)

// shardOp is a single unit of work submitted to a shard worker. The submitter
// fills in the input fields, sends the op via shardWorkerPool.submit, and then
// waits on done.
//
// shardOp is not safe for concurrent use; callers must not touch any field of
// the op (input or output) between submission and the close of done.
type shardOp struct {
	// inputs (immutable for the lifetime of the op)
	m             *tenantshard.Map
	series        []uint64
	now           clock.Minutes
	track         bool           // true = enforce limit (TrackSeries); false = unconditional load (event replay)
	seriesCounter *atomic.Uint64 // tenant.series; atomically incremented on Put-create
	limitProvider *atomic.Uint64 // tenant.currentLimit; nil if track == false

	// outputs (worker writes; submitter reads after done is closed)
	createdRefs  []uint64
	rejectedRefs []uint64

	// done is closed by the worker when processing has completed.
	done chan struct{}
}

func newShardOp(m *tenantshard.Map, series []uint64, now clock.Minutes, track bool, seriesCounter, limitProvider *atomic.Uint64) *shardOp {
	return &shardOp{
		m:             m,
		series:        series,
		now:           now,
		track:         track,
		seriesCounter: seriesCounter,
		limitProvider: limitProvider,
		done:          make(chan struct{}),
	}
}

// shardWorkerPool is a fixed-size pool of worker goroutines that serve shardOps.
// Submitters route to a worker by hashing (tenant, shard) so that ops for the
// same tenantshard.Map always land on the same worker; this gives each map a
// single writer (in worker mode) without needing a goroutine per map.
//
// With ~1800 tenants × 16 shards on a single instance, a per-(tenant, shard)
// worker design would cost ~28k goroutines per partition; a fixed pool of
// GOMAXPROCS workers is two orders of magnitude cheaper.
//
// The cost of pooling is that a slow op can stall other (tenant, shard) pairs
// hashed to the same worker. Inboxes are bounded so the pool's saturation is
// observable via the cortex_usage_tracker_shard_worker_submit_blocked_total
// counter, and submitters block on the request context (per F=block-while-ctx
// design) so a saturated worker doesn't pile up unbounded goroutines either.
type shardWorkerPool struct {
	workers []*shardWorker

	stop     chan struct{}
	stopOnce sync.Once

	// Shared metrics, see trackerStore.shardWorker* fields.
	submitBlocked   *atomic.Int64
	submitWaitNanos *atomic.Int64
	opsTotal        *atomic.Int64
}

func newShardWorkerPool(numWorkers, inboxDepth int, submitBlocked, submitWaitNanos, opsTotal *atomic.Int64) *shardWorkerPool {
	if numWorkers <= 0 {
		numWorkers = runtime.GOMAXPROCS(0)
	}
	p := &shardWorkerPool{
		workers:         make([]*shardWorker, numWorkers),
		stop:            make(chan struct{}),
		submitBlocked:   submitBlocked,
		submitWaitNanos: submitWaitNanos,
		opsTotal:        opsTotal,
	}
	for i := range p.workers {
		p.workers[i] = newShardWorker(inboxDepth, p.stop, p.opsTotal)
		go p.workers[i].run()
	}
	return p
}

// workerFor returns the worker that owns ops for the given (tenant, shard) pair.
// Routing is consistent: the same (tenantHash, shard) always returns the same
// worker, which preserves the single-writer property of the underlying
// tenantshard.Map.
func (p *shardWorkerPool) workerFor(tenantHash uint64, shard uint8) *shardWorker {
	// Mix the shard index into the tenant hash so that the 16 shards of a
	// single tenant land on different workers (assuming N > NumShards).
	const shardMixer = 0x9E3779B97F4A7C15
	idx := (tenantHash + uint64(shard)*shardMixer) % uint64(len(p.workers))
	return p.workers[idx]
}

// submit enqueues op to the worker selected for (tenantHash, shard), blocking
// the caller until either the op is accepted, the context is cancelled, or the
// pool has been stopped.
//
// On error the op is *not* enqueued and op.done will not be closed; callers
// must not wait on op.done after an error.
func (p *shardWorkerPool) submit(ctx context.Context, tenantHash uint64, shard uint8, op *shardOp) error {
	w := p.workerFor(tenantHash, shard)

	select {
	case w.inbox <- op:
		return nil
	default:
	}

	if p.submitBlocked != nil {
		p.submitBlocked.Inc()
	}
	start := time.Now()
	select {
	case w.inbox <- op:
		if p.submitWaitNanos != nil {
			p.submitWaitNanos.Add(time.Since(start).Nanoseconds())
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-p.stop:
		return errShardWorkerStopped
	}
}

// shutdown signals all workers to stop and waits for them to drain their
// inboxes and exit. Safe to call multiple times.
func (p *shardWorkerPool) shutdown() {
	p.stopOnce.Do(func() {
		close(p.stop)
	})
	for _, w := range p.workers {
		<-w.done
	}
}

// numWorkers returns the configured worker count. Useful for tests and metrics.
func (p *shardWorkerPool) numWorkers() int { return len(p.workers) }

// shardWorker drains shardOps from a single inbox channel and processes them
// serially. Workers do not own any specific tenantshard.Map; ownership is a
// property of routing (see shardWorkerPool.workerFor): the routing function
// guarantees that all ops for a given (tenant, shard) reach the same worker,
// so the underlying map sees only a single writer at a time.
type shardWorker struct {
	inbox chan *shardOp

	// stop is shared with the pool; closed by shardWorkerPool.shutdown.
	stop chan struct{}
	// done is closed by run() when the worker goroutine exits.
	done chan struct{}

	// opsTotal is shared with the pool.
	opsTotal *atomic.Int64
}

func newShardWorker(inboxDepth int, stop chan struct{}, opsTotal *atomic.Int64) *shardWorker {
	return &shardWorker{
		inbox:    make(chan *shardOp, inboxDepth),
		stop:     stop,
		done:     make(chan struct{}),
		opsTotal: opsTotal,
	}
}

func (w *shardWorker) run() {
	defer close(w.done)
	for {
		select {
		case op := <-w.inbox:
			w.process(op)
		case <-w.stop:
			// Drain any ops already queued so submitters don't block forever.
			for {
				select {
				case op := <-w.inbox:
					w.process(op)
				default:
					return
				}
			}
		}
	}
}

func (w *shardWorker) process(op *shardOp) {
	if w.opsTotal != nil {
		w.opsTotal.Inc()
	}

	// We still take the map's mutex because other code paths (cleanup, snapshot,
	// event replay) currently access the map directly. Contention here is
	// near-zero for track ops: routing guarantees only one worker writes to the
	// map at a time, so the lock is uncontended on the worker path. Step 3 of
	// the rollout migrates the remaining paths and removes the mutex entirely.
	op.m.Lock()
	if op.track {
		for _, ref := range op.series {
			created, rejected := op.m.Put(ref, op.now, op.seriesCounter, op.limitProvider, true)
			if created {
				op.createdRefs = append(op.createdRefs, ref)
			} else if rejected {
				op.rejectedRefs = append(op.rejectedRefs, ref)
			}
		}
	} else {
		for _, ref := range op.series {
			_, _ = op.m.Put(ref, op.now, op.seriesCounter, nil, false)
		}
	}
	op.m.Unlock()

	close(op.done)
}

var errShardWorkerStopped = errors.New("shard worker pool stopped")

// hashTenantID returns a stable 64-bit hash of tenantID. Used by
// shardWorkerPool.workerFor as the routing key. Computed once per tenant in
// trackerStore.getOrCreateTenant and cached on trackedTenant.tenantHash so the
// hot path doesn't pay for string hashing on every TrackSeries call.
func hashTenantID(tenantID string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(tenantID))
	return h.Sum64()
}
