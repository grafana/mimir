// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/ring"
	"google.golang.org/grpc"

	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/nautilus/assignment"
)

// This file is the deterministic-time test harness for the
// rebalancer. The goal is to drive multi-round timelines (lease
// aging, cooldown expiry, ring churn, RPC failures) without any
// sleeps or real network — every clock tick is explicit and every
// readcache peer is an in-memory stub.
//
// Layered design:
//
//   - fakeClock satisfies Clock with an Advance(d) knob.
//   - fakeReadcache satisfies readcacheRPC. It owns an in-memory
//     copy of the (partition, ranges) the rebalancer pushed via
//     SetHashRanges and answers HashRangeStats / GetHashRanges from
//     that state. Optional knobs let a test inject RPC failures or
//     scripted load signals per (partition, range).
//   - fakeFleet satisfies readcacheFleet. It maintains an instance
//     set and routes clientFor() calls to the matching fakeReadcache.
//   - fakeReadcacheRing wraps fakeFleet so the slicer's tier-2
//     instance discovery (r.activeReadcacheInstances) and the
//     fleet's RPC routing always agree on which readcaches exist.
//   - harness ties everything together and exposes test-facing
//     helpers (advance, runRound, ownersByInstance, etc.) plus
//     access to the internal stores for assertions.

// fakeClock is a controllable Clock for tests. Advance is the only
// way time moves; this lets tests express timelines like "round 1,
// then 3 minutes pass, then round 2" without sleeping.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock(t time.Time) *fakeClock { return &fakeClock{t: t} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// fakeReadcache is the test double for one readcache pod. It
// satisfies readcacheRPC by tracking which (partition, ranges) it
// "owns" (per the last SetHashRanges call) and synthesising
// HashRangeStats / GetHashRanges responses from that state.
//
// Mutexed because the rebalancer fans out RPCs in parallel
// (concurrency.ForEachJob).
type fakeReadcache struct {
	id   string
	addr string

	mu      sync.Mutex
	owned   map[int32][]assignment.HashRange // partitionID -> ranges currently owned
	rates   map[partitionRangeKey]float64    // optional per-(P,R) sample rate
	series  map[partitionRangeKey]int64      // optional per-(P,R) series count
	pSeries map[int32]int64                  // optional per-partition total head series
	pQuery  map[int32]float64                // optional per-partition query-samples EWMA
	unnamed float64                          // optional unnamed query EWMA

	// Failure-injection knobs. Setting any of these makes the
	// matching RPC return the error instead of touching state.
	hashRangeStatsErr error
	setHashRangesErr  error
	getHashRangesErr  error
}

func newFakeReadcache(id string) *fakeReadcache {
	return &fakeReadcache{
		id:      id,
		addr:    id, // address == id keeps assertions readable
		owned:   make(map[int32][]assignment.HashRange),
		rates:   make(map[partitionRangeKey]float64),
		series:  make(map[partitionRangeKey]int64),
		pSeries: make(map[int32]int64),
		pQuery:  make(map[int32]float64),
	}
}

// setLoad sets the sample-rate and series-count this readcache will
// report for (pid, hr) on the next HashRangeStats call. Used by
// tests that want to exercise the slicer's load-balancing path.
func (f *fakeReadcache) setLoad(pid int32, hr assignment.HashRange, sampleRate float64, series int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := partitionRangeKey{partitionID: pid, hr: hr}
	f.rates[k] = sampleRate
	f.series[k] = series
	f.pSeries[pid] += series
}

// HashRangeStats implements readcacheRPC. Walks the owned set and
// builds a response containing one HashRangeRate per (partition,
// range) the readcache owns, plus per-partition totals. Returns
// hashRangeStatsErr when set, simulating a transient RPC failure.
func (f *fakeReadcache) HashRangeStats(_ context.Context, _ *ingester_client.HashRangeStatsRequest, _ ...grpc.CallOption) (*ingester_client.HashRangeStatsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.hashRangeStatsErr != nil {
		return nil, f.hashRangeStatsErr
	}

	resp := &ingester_client.HashRangeStatsResponse{
		UnnamedQuerySamplesEwma: f.unnamed,
	}
	for pid, ranges := range f.owned {
		for _, hr := range ranges {
			k := partitionRangeKey{partitionID: pid, hr: hr}
			resp.Rates = append(resp.Rates, ingester_client.HashRangeRate{
				Lo:           hr.Lo,
				Hi:           hr.Hi,
				PartitionId:  pid,
				ActiveSeries: f.series[k],
				SampleRate:   f.rates[k],
			})
			resp.TotalActiveSeries += f.series[k]
		}
		resp.PartitionActiveSeries = append(resp.PartitionActiveSeries, ingester_client.PartitionActiveSeries{
			PartitionId:  pid,
			ActiveSeries: f.pSeries[pid],
		})
		if v, ok := f.pQuery[pid]; ok {
			resp.PartitionQueryLoads = append(resp.PartitionQueryLoads, ingester_client.PartitionQueryLoad{
				PartitionId: pid,
				SamplesEwma: v,
			})
		}
	}
	// Sort everything so test assertions on the response are
	// deterministic regardless of map iteration order.
	sort.Slice(resp.Rates, func(i, j int) bool {
		if resp.Rates[i].PartitionId != resp.Rates[j].PartitionId {
			return resp.Rates[i].PartitionId < resp.Rates[j].PartitionId
		}
		return resp.Rates[i].Lo < resp.Rates[j].Lo
	})
	sort.Slice(resp.PartitionActiveSeries, func(i, j int) bool {
		return resp.PartitionActiveSeries[i].PartitionId < resp.PartitionActiveSeries[j].PartitionId
	})
	sort.Slice(resp.PartitionQueryLoads, func(i, j int) bool {
		return resp.PartitionQueryLoads[i].PartitionId < resp.PartitionQueryLoads[j].PartitionId
	})
	return resp, nil
}

// SetHashRanges implements readcacheRPC. Replaces the owned set with
// the supplied (partition, range) entries — mirroring the real
// readcache where SetHashRanges fully reconciles the partition's
// range set rather than appending.
func (f *fakeReadcache) SetHashRanges(_ context.Context, in *ingester_client.SetHashRangesRequest, _ ...grpc.CallOption) (*ingester_client.SetHashRangesResponse, error) {
	if f.setHashRangesErr != nil {
		return nil, f.setHashRangesErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.owned = make(map[int32][]assignment.HashRange)
	for _, e := range in.Ranges {
		f.owned[e.PartitionId] = append(f.owned[e.PartitionId], assignment.HashRange{Lo: e.Lo, Hi: e.Hi})
	}
	// Stable order within each partition for deterministic
	// GetHashRanges responses.
	for pid, rs := range f.owned {
		sort.Slice(rs, func(i, j int) bool { return rs[i].Lo < rs[j].Lo })
		f.owned[pid] = rs
	}
	return &ingester_client.SetHashRangesResponse{}, nil
}

// GetHashRanges implements readcacheRPC. Flattens the owned map into
// the wire response that reconstructAssignmentFromReadcache consumes.
func (f *fakeReadcache) GetHashRanges(_ context.Context, _ *ingester_client.GetHashRangesRequest, _ ...grpc.CallOption) (*ingester_client.GetHashRangesResponse, error) {
	if f.getHashRangesErr != nil {
		return nil, f.getHashRangesErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	resp := &ingester_client.GetHashRangesResponse{}
	for pid, ranges := range f.owned {
		for _, hr := range ranges {
			resp.Ranges = append(resp.Ranges, ingester_client.HashRangeEntry{
				Lo:          hr.Lo,
				Hi:          hr.Hi,
				PartitionId: pid,
			})
		}
	}
	sort.Slice(resp.Ranges, func(i, j int) bool {
		if resp.Ranges[i].PartitionId != resp.Ranges[j].PartitionId {
			return resp.Ranges[i].PartitionId < resp.Ranges[j].PartitionId
		}
		return resp.Ranges[i].Lo < resp.Ranges[j].Lo
	})
	return resp, nil
}

// ownedPartitions returns the partition IDs this readcache currently
// holds (per the last SetHashRanges call). Used by tests to assert
// fleet-wide distribution.
func (f *fakeReadcache) ownedPartitions() []int32 {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]int32, 0, len(f.owned))
	for pid := range f.owned {
		out = append(out, pid)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// fakeFleet implements readcacheFleet. Instances can be added or
// removed at any point to simulate ring churn (scale-up, scale-down,
// pod restart with new IP).
type fakeFleet struct {
	mu         sync.Mutex
	instances  map[string]*fakeReadcache
	healthyErr error // when non-nil, healthyInstances returns this error
}

func newFakeFleet() *fakeFleet {
	return &fakeFleet{instances: make(map[string]*fakeReadcache)}
}

func (f *fakeFleet) healthyInstances() ([]ring.InstanceDesc, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.healthyErr != nil {
		return nil, f.healthyErr
	}
	out := make([]ring.InstanceDesc, 0, len(f.instances))
	for _, rc := range f.instances {
		out = append(out, ring.InstanceDesc{Id: rc.id, Addr: rc.addr})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Id < out[j].Id })
	return out, nil
}

func (f *fakeFleet) clientFor(_ context.Context, inst ring.InstanceDesc) (readcacheRPC, error) {
	f.mu.Lock()
	rc, ok := f.instances[inst.Id]
	f.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("fakeFleet: no readcache for id=%s", inst.Id)
	}
	return rc, nil
}

// addReadcache registers a fresh fakeReadcache under id and returns
// a handle so the test can mutate state or assertions on it.
func (f *fakeFleet) addReadcache(id string) *fakeReadcache {
	rc := newFakeReadcache(id)
	f.mu.Lock()
	f.instances[id] = rc
	f.mu.Unlock()
	return rc
}

// removeReadcache simulates a pod leaving the ring (drain, OOM,
// scale-down). Subsequent healthyInstances calls won't include it,
// and clientFor will return an error for its id.
func (f *fakeFleet) removeReadcache(id string) {
	f.mu.Lock()
	delete(f.instances, id)
	f.mu.Unlock()
}

// instanceIDs returns the current instance set as a sorted slice.
// Used by fakeReadcacheRing so the slicer's tier-2 instance
// discovery sees the same set as the fleet's RPC routing.
func (f *fakeFleet) instanceIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.instances))
	for id := range f.instances {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// fakeReadcacheRing wraps fakeFleet so the rebalancer's
// activeReadcacheInstances() helper agrees with the fleet's RPC
// routing. Without this, a test could end up in the misleading
// state of "the slicer thinks readcache-3 exists but the fleet
// refuses to dial it" — exactly the kind of inconsistency we want
// to keep out of the harness.
type fakeReadcacheRing struct{ fleet *fakeFleet }

func (r fakeReadcacheRing) GetAllHealthy(_ ring.Operation) (ring.ReplicationSet, error) {
	insts, err := r.fleet.healthyInstances()
	if err != nil {
		return ring.ReplicationSet{}, err
	}
	return ring.ReplicationSet{Instances: insts}, nil
}

// harness is the test-side driver for a Rebalancer wired with a
// fake clock and fake fleet. Construct via newHarness; drive with
// advance() and runRound(); inspect with tier1Active(),
// tier2Active(), ownersByInstance(), and logContains().
type harness struct {
	t       *testing.T
	cfg     Config
	clock   *fakeClock
	fleet   *fakeFleet
	r       *Rebalancer
	logBuf  *bytes.Buffer
	logger  log.Logger
	ctx     context.Context
	cancel  context.CancelFunc
}

// harnessOpts is the optional configuration for newHarness. Zero
// value gives a sane default: 4-partition cluster, 5min lease, slicer
// disabled (Phase 2A bring-up shape).
type harnessOpts struct {
	cfg            Config
	startTime      time.Time
	captureLogs    bool   // capture log output into a buffer for assertions
	staticReadcacheInstances bool // set ReadcacheSlicer.Instances from fleet instead of using fakeReadcacheRing
}

// newHarness builds a Rebalancer with all dependencies stubbed and
// returns the harness wrapper. The fleet starts empty; tests add
// readcaches via h.addReadcache before running rounds. partitions
// defaults to [0..3] if cfg.PartitionCount is zero.
func newHarness(t *testing.T, opts harnessOpts) *harness {
	t.Helper()

	cfg := opts.cfg
	if cfg.PartitionCount == 0 && cfg.ActivePartitionCount == 0 {
		cfg.PartitionCount = 4
	}
	if cfg.LeaseDuration == 0 {
		cfg.LeaseDuration = 5 * time.Minute
	}
	if cfg.LeaseLookahead == 0 {
		cfg.LeaseLookahead = 90 * time.Second
	}
	if cfg.EntryRetention == 0 {
		cfg.EntryRetention = 24 * time.Hour
	}
	if cfg.MinRebalanceInterval == 0 {
		cfg.MinRebalanceInterval = 30 * time.Second
	}
	if cfg.MaxRebalanceInterval == 0 {
		cfg.MaxRebalanceInterval = 5 * time.Minute
	}
	if cfg.IngesterRPCConcurrency == 0 {
		cfg.IngesterRPCConcurrency = 4
	}

	startTime := opts.startTime
	if startTime.IsZero() {
		startTime = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	}

	logBuf := &bytes.Buffer{}
	var logger log.Logger
	if opts.captureLogs {
		logger = log.NewLogfmtLogger(logBuf)
	} else {
		logger = log.NewNopLogger()
	}

	fleet := newFakeFleet()
	clock := newFakeClock(startTime)

	r := &Rebalancer{
		cfg:                cfg,
		logger:             logger,
		fleet:              fleet,
		store:              newLogStore(),
		readcacheStore:     newReadcacheLogStore(),
		moveCooldowns:      make(map[assignment.HashRange]time.Time),
		readcacheCooldowns: make(readcacheMoveCooldowns),
		metrics:            newMetrics(nil),
		clock:              clock,
	}
	if opts.staticReadcacheInstances {
		// Static instance list synced from the fleet on demand; the
		// caller is responsible for re-syncing after addReadcache /
		// removeReadcache if they care about the slicer picking up
		// fleet churn.
		r.cfg.ReadcacheSlicer.Instances = flagext.StringSliceCSV(fleet.instanceIDs())
	} else {
		// Tier-2 instance discovery shares the fleet's instance set
		// so churn is automatically reflected in the slicer.
		r.readcacheRing = fakeReadcacheRing{fleet: fleet}
	}

	ctx, cancel := context.WithCancel(context.Background())
	h := &harness{
		t:      t,
		cfg:    cfg,
		clock:  clock,
		fleet:  fleet,
		r:      r,
		logBuf: logBuf,
		logger: logger,
		ctx:    ctx,
		cancel: cancel,
	}
	t.Cleanup(cancel)
	return h
}

// addReadcache registers a new fakeReadcache and returns it. When
// staticReadcacheInstances is set on the harness opts, the caller
// must call h.syncStaticInstances() afterwards to make the slicer
// notice the addition.
func (h *harness) addReadcache(id string) *fakeReadcache {
	return h.fleet.addReadcache(id)
}

// removeReadcache drops a readcache from the fleet (simulates ring
// drop / pod death). See addReadcache for the staticReadcacheInstances
// caveat.
func (h *harness) removeReadcache(id string) {
	h.fleet.removeReadcache(id)
}

// syncStaticInstances copies the current fleet membership into
// cfg.ReadcacheSlicer.Instances. Only needed in the
// staticReadcacheInstances=true mode; with the default fakeReadcacheRing
// the slicer discovers churn automatically.
func (h *harness) syncStaticInstances() {
	h.r.cfg.ReadcacheSlicer.Instances = flagext.StringSliceCSV(h.fleet.instanceIDs())
}

// advance moves the fake clock forward by d. Has no other side
// effects — rounds only run when the test calls runRound.
func (h *harness) advance(d time.Duration) { h.clock.Advance(d) }

// runRound drives one rebalance() invocation at the current fake-clock
// time. Returns whatever rebalance() returned (nil in nearly every
// path; the helper does its own checks rather than the test repeating
// require.NoError).
func (h *harness) runRound() error {
	h.t.Helper()
	return h.r.rebalance(h.ctx)
}

// tier1Active returns the tier-1 hash-range assignment currently
// active at the harness's clock. Nil means "no active leases" — i.e.
// the next round will fall into the cold-start path.
func (h *harness) tier1Active() *assignment.Assignment {
	return h.r.store.latestActiveAssignment(h.clock.Now())
}

// tier2Active returns the active (partition -> readcache) entries.
// Used to assert that the slicer (or admin reset, or refresh-only
// pass) successfully populated the tier-2 log.
func (h *harness) tier2Active() []int32 {
	out := []int32{}
	now := h.clock.Now()
	for _, e := range h.r.readcacheStore.snapshot() {
		if e.ActiveAt(now) {
			out = append(out, e.PartitionID)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// ownersByInstance returns the (instance -> partition count) map
// derived from the tier-2 active leases. Used to assert distribution
// (e.g. "no instance holds more than ceil(P/N) partitions").
func (h *harness) ownersByInstance() map[string]int {
	out := make(map[string]int)
	now := h.clock.Now()
	for _, e := range h.r.readcacheStore.snapshot() {
		if e.ActiveAt(now) {
			out[e.InstanceID]++
		}
	}
	return out
}

// logOutput returns the captured log buffer (only populated when
// the harness was built with captureLogs=true).
func (h *harness) logOutput() string {
	if h.logBuf == nil {
		return ""
	}
	return h.logBuf.String()
}
