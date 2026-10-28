// SPDX-License-Identifier: AGPL-3.0-only

package queue

import (
	"container/heap"
	"errors"
	"math/rand/v2" //#nosec G404 -- The reaper uses it only to spread its re-checks in time, no need for a CSPRNG -- nosemgrep: math-random-used
	"sync"
	"time"

	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/atomic"
)

var (
	ErrReservationExists     = errors.New("a reservation with this ID is already open")
	ErrReservationExceedsCap = errors.New("the reservation requests more slots than the per-tenant queue cap")
	// ErrReservationNotFound means that no reservation is open at the key. The reservation completed,
	// was aborted or timed out, or the Queue lost it in a restart.
	ErrReservationNotFound = errors.New("no reservation is open with this ID")
	// ErrReservationOtherTenant and ErrReservationNoIdleSlot are contract violations by the caller:
	// one reservation serves one tenant, and the caller keeps at most slots items of it queued.
	ErrReservationOtherTenant = errors.New("the reservation belongs to another tenant")
	ErrReservationNoIdleSlot  = errors.New("the reservation has no idle slot")

	errInvalidReservationTimeout = errors.New("the reservation timeout must be greater than 0")
)

// ReservationKey identifies a reservation. SubmitCloseReservationsOfOwner closes all reservations
// of one Owner. The query-scheduler uses the frontend address as the Owner.
type ReservationKey struct {
	Owner string
	ID    string
}

// ReservationOutcome is the reason why a reservation closed.
type ReservationOutcome string

const (
	ReservationCompleted         ReservationOutcome = "completed"
	ReservationAborted           ReservationOutcome = "aborted"
	ReservationTimedOut          ReservationOutcome = "timed_out"
	ReservationOwnerDisconnected ReservationOutcome = "frontend_disconnected"
)

// Reservation reserves a fixed number of queue slots for a group of items that its owner inserts.
// Each queued item of the reservation uses one slot, so at most slots items are queued at one time.
// While the reservation is open, a dequeue returns the item's slot to it.
//
// The reservation closes when its owner completes or aborts it, when the Queue closes all
// reservations of its owner, or when it times out. A close releases the idle slots. An item that
// is still queued keeps its slot until its dequeue.
//
// Only dispatcherLoop uses the fields, except inflight and lastActivityAfterOpen. opened does not
// change after the open.
type Reservation struct {
	key      ReservationKey
	tenantID string
	// slots is the number of reserved queue slots this reservation can concurrently consume.
	slots int
	// inserted counts the total number of items enqueued through this reservation.
	inserted int
	// queued counts the current number of items queued through this reservation.
	queued    int
	closed    bool
	heapIndex int
	nextCheck time.Time
	// opened is the open time. The activity time is an offset from it, so the reaper compares
	// monotonic clock readings and a wall clock step does not move a deadline.
	opened time.Time

	// inflight counts the items that are queued or with a consumer: inserted and not yet finished.
	// The reaper does not close a reservation while it is above 0.
	inflight atomic.Int64
	// lastActivityAfterOpen is the last activity time, in nanoseconds after opened.
	lastActivityAfterOpen atomic.Int64
}

// ItemFinished records that a consumer finished one item tracked by this reservation.
// The caller must call it exactly once for each item.
func (r *Reservation) ItemFinished(now time.Time) {
	// The activity store comes first. A reaper that sees inflight at 0 then also sees the new
	// activity time, and does not close the reservation at once.
	r.storeLastActivity(now)
	r.inflight.Dec()
}

// storeLastActivity moves lastActivityAfterOpen forward to now. Concurrent callers can read the clock
// out of order, so it never moves the time back.
func (r *Reservation) storeLastActivity(now time.Time) {
	n := int64(now.Sub(r.opened))
	for {
		old := r.lastActivityAfterOpen.Load()
		if n <= old || r.lastActivityAfterOpen.CompareAndSwap(old, n) {
			return
		}
	}
}

// ClosedReservation describes a reservation that closed. Released is the number of idle slots
// that the close released.
type ClosedReservation struct {
	ID       string
	TenantID string
	Inserted int
	Released int
	Outcome  ReservationOutcome
}

// reservationHeap orders the open reservations by nextCheck, so the reaper visits only the
// reservations that are due.
type reservationHeap []*Reservation

func (h *reservationHeap) Len() int           { return len(*h) }
func (h *reservationHeap) Less(i, j int) bool { return (*h)[i].nextCheck.Before((*h)[j].nextCheck) }

func (h *reservationHeap) Swap(i, j int) {
	(*h)[i], (*h)[j] = (*h)[j], (*h)[i]
	(*h)[i].heapIndex = i
	(*h)[j].heapIndex = j
}

func (h *reservationHeap) Push(x any) {
	res := x.(*Reservation)
	res.heapIndex = len(*h)
	*h = append(*h, res)
}

func (h *reservationHeap) Pop() any {
	old := *h
	n := len(old)
	res := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return res
}

// IdleSlotsGauge is a prometheus.Collector that reports the idle slots of each tenant: the
// slots that the tenant's open reservations hold and no queued item uses. A tenant with no idle
// slot has no series, so the inactive-user cleanup has no series to delete.
type IdleSlotsGauge struct {
	desc *prometheus.Desc

	mtx     sync.Mutex
	tenants map[string]int
}

// NewIdleSlotsGauge returns an IdleSlotsGauge with the given metric name and help, and the
// variable label "user". The caller registers it.
func NewIdleSlotsGauge(name, help string) *IdleSlotsGauge {
	return &IdleSlotsGauge{
		desc:    prometheus.NewDesc(name, help, []string{"user"}, nil),
		tenants: map[string]int{},
	}
}

func (g *IdleSlotsGauge) set(tenantID string, slots int) {
	g.mtx.Lock()
	defer g.mtx.Unlock()

	if slots <= 0 {
		delete(g.tenants, tenantID)
		return
	}
	g.tenants[tenantID] = slots
}

func (g *IdleSlotsGauge) Describe(ch chan<- *prometheus.Desc) {
	ch <- g.desc
}

// Collect emits one series per tenant. It copies the values under the lock and sends them after
// the unlock, because dispatcherLoop takes the same lock.
func (g *IdleSlotsGauge) Collect(ch chan<- prometheus.Metric) {
	type tenantSlots struct {
		tenantID string
		slots    int
	}

	g.mtx.Lock()
	snapshot := make([]tenantSlots, 0, len(g.tenants))
	for tenantID, slots := range g.tenants {
		snapshot = append(snapshot, tenantSlots{tenantID: tenantID, slots: slots})
	}
	g.mtx.Unlock()

	for _, s := range snapshot {
		ch <- prometheus.MustNewConstMetric(g.desc, prometheus.GaugeValue, float64(s.slots), s.tenantID)
	}
}

// openReservation applies an opOpenReservation. Only dispatcherLoop calls it.
func (q *Queue) openReservation(r producerOperation) error {
	if _, exists := q.reservations[r.reservationKey]; exists {
		return ErrReservationExists
	}
	if r.slots <= 0 {
		return errInvalidReservationSlots
	}
	if err := q.queueBroker.reserveForTenant(r.tenantID, r.slots); err != nil {
		if _, ok := errors.AsType[*ReservationRejectedError](err); ok {
			// discardedRequests counts only the items that the cap rejects, so a reservation does not change it.
			q.rejectedReservations.WithLabelValues(r.tenantID).Inc()
		}
		return err
	}

	res := &Reservation{
		key:      r.reservationKey,
		tenantID: r.tenantID,
		slots:    r.slots,
		// The next reap computes the real deadline from the last activity.
		nextCheck: r.now,
		opened:    r.now,
	}
	q.reservations[res.key] = res
	owned := q.reservationsByOwner[res.key.Owner]
	if owned == nil {
		owned = map[ReservationKey]*Reservation{}
		q.reservationsByOwner[res.key.Owner] = owned
	}
	owned[res.key] = res
	heap.Push(&q.reservationHeap, res)

	q.reservationSlots.Observe(float64(r.slots))
	q.updateIdleSlots(res.tenantID)
	return nil
}

// enqueueIntoReservation applies an opEnqueueIntoReservation. Only dispatcherLoop calls it.
func (q *Queue) enqueueIntoReservation(r producerOperation) error {
	res := q.reservations[r.reservationKey]
	switch {
	case res == nil:
		return ErrReservationNotFound
	case res.tenantID != r.tenantID:
		return ErrReservationOtherTenant
	case res.queued >= res.slots:
		return ErrReservationNoIdleSlot
	}

	return q.enqueue(&r, res)
}

// returnReservedSlot handles the dequeue of an item that used a slot of res. While res is open,
// the slot goes back to res. Only dispatcherLoop calls it.
func (q *Queue) returnReservedSlot(res *Reservation) {
	res.queued--
	if res.closed {
		return
	}
	q.queueBroker.returnSlotForTenant(res.tenantID)
	q.updateIdleSlots(res.tenantID)
}

// closeReservation removes res from the open reservations and releases its idle slots. Only
// dispatcherLoop calls it.
func (q *Queue) closeReservation(res *Reservation, outcome ReservationOutcome) ClosedReservation {
	released := res.slots - res.queued
	if leaked := q.queueBroker.releaseForTenant(res.tenantID, released); leaked > 0 {
		level.Warn(q.log).Log(
			"msg", "reservation release exceeded the idle slots; this is a bug",
			"tenant", res.tenantID, "leaked", leaked,
		)
	}
	res.closed = true

	delete(q.reservations, res.key)
	owned := q.reservationsByOwner[res.key.Owner]
	delete(owned, res.key)
	if len(owned) == 0 {
		delete(q.reservationsByOwner, res.key.Owner)
	}
	heap.Remove(&q.reservationHeap, res.heapIndex)

	q.updateIdleSlots(res.tenantID)
	return ClosedReservation{
		ID:       res.key.ID,
		TenantID: res.tenantID,
		Inserted: res.inserted,
		Released: released,
		Outcome:  outcome,
	}
}

// reapReservations closes the due reservations that have no inflight item and no activity for
// at least timeout. Only dispatcherLoop calls it.
func (q *Queue) reapReservations(r producerOperation) error {
	if r.timeout <= 0 {
		return errInvalidReservationTimeout
	}
	for len(q.reservationHeap) > 0 {
		res := q.reservationHeap[0]
		if res.nextCheck.After(r.now) {
			return nil
		}
		if res.inflight.Load() > 0 {
			// A random delay in [timeout/2, timeout] spreads out a cohort of busy reservations that
			// opened in one tick. The delay is new at each check, because a fixed delay per
			// reservation lines the cohort up again at common multiples of the delays. A delay of at
			// most timeout never delays a close.
			res.nextCheck = r.now.Add(r.timeout - rand.N(r.timeout/2+1))
		} else if idleUntil := res.opened.Add(time.Duration(res.lastActivityAfterOpen.Load()) + r.timeout); idleUntil.After(r.now) {
			res.nextCheck = idleUntil
		} else {
			r.result.closed = append(r.result.closed, q.closeReservation(res, ReservationTimedOut))
			continue
		}
		heap.Fix(&q.reservationHeap, 0)
	}
	return nil
}

func (q *Queue) updateIdleSlots(tenantID string) {
	q.idleSlots.set(tenantID, q.queueBroker.idleSlotsByTenant[tenantID])
}
