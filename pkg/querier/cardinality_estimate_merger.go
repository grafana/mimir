// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"fmt"
	"strings"

	"github.com/axiomhq/hyperloglog"

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// Memory the merger charges to the request's memory consumption tracker: a hash, a group, a sketch.
const (
	estimateHashBytes   = cardinality.HashBytes
	estimateGroupBytes  = 64
	estimateSketchBytes = 1 << cardinality.SketchPrecision
)

// estimateMerger combines the groups the sources of an estimate send: it adds counts when the sources share no series, otherwise unions hashes and merges HLL sketches. It is not safe for concurrent use.
type estimateMerger struct {
	dedup    bool
	held     int64
	counts   map[string][]int64
	hashes   map[string]map[uint64]struct{}
	sketches map[string]*hyperloglog.Sketch

	hashBytes, sketchBytes int64

	// buckets, when positive, is how many counts every group must carry.
	buckets int

	// tracker, when set, bounds the memory the groups and hashes hold.
	tracker *limiter.MemoryConsumptionTracker
}

// newEstimateMerger returns a merger that unions hashes when dedup is set, and charges what it holds to the memory consumption tracker in ctx, if any.
func newEstimateMerger(ctx context.Context, dedup bool) *estimateMerger {
	tracker, _ := limiter.MemoryConsumptionTrackerFromContext(ctx)
	return &estimateMerger{
		tracker:  tracker,
		dedup:    dedup,
		counts:   map[string][]int64{},
		hashes:   map[string]map[uint64]struct{}{},
		sketches: map[string]*hyperloglog.Sketch{},
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
func (m *estimateMerger) add(source, value string, counts []int64, hashes []uint64, sketch []byte) error {
	if len(hashes) > 0 && len(sketch) > 0 {
		// A group is either hashes or a sketch, taking the sketch would drop the hashes.
		return olderVersionEstimate("%s sent both label-set hashes and a sketch for %q; it may run a version this querier doesn't know, retry once every %s is updated", aSource(source), value, source)
	}
	if m.dedup && len(sketch) > 0 {
		sk, err := hyperloglog.NewSketch(cardinality.SketchPrecision, false)
		if err != nil {
			return err
		}
		if err := sk.UnmarshalBinary(sketch); err != nil {
			return fmt.Errorf("%s returned a bad sketch for %q: %w", aSource(source), value, err)
		}
		if into := m.sketches[value]; into != nil {
			if err := into.Merge(sk); err != nil {
				return err
			}
		} else {
			if err := m.charge(uint64(estimateGroupBytes + len(value) + estimateSketchBytes)); err != nil {
				return err
			}
			m.sketches[value] = sk
		}
		m.sketchBytes += int64(len(sketch))
		return nil
	}
	if m.dedup {
		// A source asked for hashes never sends counts, unless it is an older version; taking them as an empty hash set would count the group as 0.
		if len(hashes) == 0 && len(counts) > 0 {
			return olderVersionEstimate("%s sent counts instead of the label-set hashes asked for %q; it may run a version that can't deduplicate, retry once every %s is updated", aSource(source), value, source)
		}
		m.hashBytes += 8 * int64(len(hashes))
		set := m.hashes[value]
		if set == nil {
			if err := m.charge(uint64(estimateGroupBytes + len(value))); err != nil {
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
	// A group with no counts is malformed and would leave an empty slice in the table.
	if want := max(1, m.buckets); len(counts) != want {
		return olderVersionEstimate("%s returned %d buckets for %q instead of the %d asked for with step_ms; it may run an older version, retry once every %s is updated", aSource(source), len(counts), value, want, source)
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
		if sk := m.sketches[v]; sk != nil {
			for h := range set {
				sk.InsertHash(h)
			}
			continue
		}
		res.Counts[v] = []int64{int64(len(set))}
	}
	for v, sk := range m.sketches {
		if res.Estimated == nil {
			res.Estimated = map[string]bool{}
		}
		res.Estimated[v] = true
		res.Counts[v] = []int64{int64(sk.Estimate())}
	}
	res.HashBytes += m.hashBytes
	res.SketchBytes += m.sketchBytes
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
