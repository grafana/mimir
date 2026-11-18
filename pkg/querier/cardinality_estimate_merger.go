// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"fmt"
	"strings"

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// Memory the merger charges to the request's memory consumption tracker: a hash, a group.
const (
	estimateHashBytes  = cardinality.HashBytes
	estimateGroupBytes = 64
)

// estimateMerger combines the groups the sources of an estimate send: it adds counts when the sources share no series, otherwise unions hashes, also those of a group a source sent in several parts. It is not safe for concurrent use.
type estimateMerger struct {
	dedup  bool
	held   int64
	counts map[string][]int64
	hashes map[string]map[uint64]struct{}

	hashBytes int64

	// buckets, when positive, is how many counts every group must carry.
	buckets int

	// tracker, when set, bounds the memory the groups and hashes hold.
	tracker *limiter.MemoryConsumptionTracker
}

// newEstimateMerger returns a merger that unions hashes when dedup is set, and charges what it holds to the memory consumption tracker in ctx, if any.
func newEstimateMerger(ctx context.Context, dedup bool) *estimateMerger {
	tracker, _ := limiter.MemoryConsumptionTrackerFromContext(ctx)
	return &estimateMerger{
		tracker: tracker,
		dedup:   dedup,
		counts:  map[string][]int64{},
		hashes:  map[string]map[uint64]struct{}{},
	}
}

// chargeEstimateGroup charges one new group to tracker, which may be nil.
func chargeEstimateGroup(tracker *limiter.MemoryConsumptionTracker, value string, buckets int) error {
	return (&estimateMerger{tracker: tracker}).charge(estimateGroupCharge(value, buckets))
}

// estimateGroupCharge is the memory a group is charged.
func estimateGroupCharge(value string, buckets int) uint64 {
	return uint64(estimateGroupBytes + len(value) + 8*buckets)
}

// charge adds b bytes to the request's memory consumption.
func (m *estimateMerger) charge(b uint64) error {
	if m.tracker == nil || b == 0 {
		return nil
	}
	if err := m.tracker.IncreaseMemoryConsumption(b, limiter.CardinalityEstimateGroups); err != nil {
		return refusedByLimit(refusedByMemory, "the groups and hashes this estimate holds in the querier: %w", err)
	}
	return nil
}

// add merges one group from source.
func (m *estimateMerger) add(source, value string, counts []int64, hashes []uint64) error {
	if m.dedup {
		m.hashBytes += 8 * int64(len(hashes))
		set := m.hashes[value]
		if set == nil {
			// A group that holds hashes is its own set, charged as the store-gateway charges it.
			if err := m.charge(uint64(cardinality.GroupBytes + len(value))); err != nil {
				return err
			}
			set = map[uint64]struct{}{}
			m.hashes[value] = set
		}
		added := 0
		for _, h := range hashes {
			if _, dup := set[h]; !dup {
				set[h] = struct{}{}
				m.held++
				added++
			}
		}
		return m.charge(uint64(added * estimateHashBytes))
	}
	// A group without one count per bucket is malformed: the table and the response index counts by bucket.
	if want := max(1, m.buckets); len(counts) != want {
		return fmt.Errorf("%s returned %d buckets for %q instead of %d", aSource(source), len(counts), value, want)
	}
	sum := m.counts[value]
	if sum == nil {
		if err := m.charge(uint64(estimateGroupBytes + len(value) + 8*len(counts))); err != nil {
			return err
		}
		sum = make([]int64, len(counts))
		m.counts[value] = sum
	}
	for i, n := range counts {
		sum[i] += n
	}
	return nil
}

// finish writes the merged counts into res.
func (m *estimateMerger) finish(res *CardinalityEstimateResult) error {
	if res.Counts == nil {
		res.Counts = map[string][]int64{}
	}
	for v, c := range m.counts {
		res.Counts[v] = c
	}
	for v, set := range m.hashes {
		res.Counts[v] = []int64{int64(len(set))}
	}
	res.HashBytes += m.hashBytes
	if m.dedup {
		res.DedupHashes = max(res.DedupHashes, m.held)
	}
	return nil
}

// aSource names one source of a kind, such as "a store-gateway", without its address.
func aSource(kind string) string {
	if strings.HasPrefix(kind, "i") {
		return "an " + kind
	}
	return "a " + kind
}
