// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"fmt"

	"github.com/axiomhq/hyperloglog"

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// Memory the merger charges to the request's memory consumption tracker:
// a held hash as a map entry, a group as its value plus a map entry and
// its count slice, and a sketch as its registers.
const (
	estimateHashBytes   = 24
	estimateGroupBytes  = 64
	estimateSketchBytes = 1 << cardinality.SketchPrecision
)

// estimateMerger combines the groups that the sources of a cardinality
// estimate send: it adds up counts when the sources share no series, and
// otherwise unions label-set hashes and merges HLL sketches. A group that
// any source sent as a sketch is estimated, and the other sources' hashes
// for it go into the same sketch. It is not safe for concurrent use.
type estimateMerger struct {
	dedup    bool
	maxHeld  int64
	held     int64
	counts   map[string][]int64
	hashes   map[string]map[uint64]struct{}
	sketches map[string]*hyperloglog.Sketch

	hashBytes, sketchBytes int64

	// tracker, when set, bounds the memory the groups and hashes hold.
	tracker *limiter.MemoryConsumptionTracker
}

// newEstimateMerger returns a merger that unions hashes when dedup is set,
// refusing once it holds more than maxHeld hashes (0 for no limit), and
// charges what it holds to the memory consumption tracker in ctx, if any.
func newEstimateMerger(ctx context.Context, dedup bool, maxHeld int64) *estimateMerger {
	tracker, _ := limiter.MemoryConsumptionTrackerFromContext(ctx)
	return &estimateMerger{
		tracker:  tracker,
		dedup:    dedup,
		maxHeld:  maxHeld,
		counts:   map[string][]int64{},
		hashes:   map[string]map[uint64]struct{}{},
		sketches: map[string]*hyperloglog.Sketch{},
	}
}

// chargeEstimateGroup charges one new group, named value, to tracker, which
// may be nil, for the paths that build their table without a merger.
func chargeEstimateGroup(tracker *limiter.MemoryConsumptionTracker, value string) error {
	return (&estimateMerger{tracker: tracker}).charge(uint64(estimateGroupBytes + len(value) + 8))
}

// charge adds b bytes to the request's memory consumption, refusing the
// request once the tenant's limit is passed.
func (m *estimateMerger) charge(b uint64) error {
	if m.tracker == nil || b == 0 {
		return nil
	}
	if err := m.tracker.IncreaseMemoryConsumption(b, limiter.CardinalityEstimateGroups); err != nil {
		return refusedEstimate("the groups and hashes this estimate holds in the querier: %w", err)
	}
	return nil
}

// add merges one group from source.
func (m *estimateMerger) add(source, value string, counts []int64, hashes []uint64, sketch []byte) error {
	if m.dedup && len(sketch) > 0 {
		sk, err := hyperloglog.NewSketch(cardinality.SketchPrecision, false)
		if err != nil {
			return err
		}
		if err := sk.UnmarshalBinary(sketch); err != nil {
			return fmt.Errorf("%s returned a bad sketch for %q: %w", source, value, err)
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
		if err := m.charge(uint64(added * estimateHashBytes)); err != nil {
			return err
		}
		if m.maxHeld > 0 && m.held > m.maxHeld {
			return refusedEstimate("deduplicating this window holds more than %d series hashes in the querier (the -querier.cardinality-estimate.max-dedup-series limit); narrow the window or lower sketch_above", m.maxHeld)
		}
		return nil
	}
	sum := m.counts[value]
	if sum == nil {
		if err := m.charge(uint64(estimateGroupBytes + len(value) + 8*len(counts))); err != nil {
			return err
		}
		sum = make([]int64, len(counts))
		m.counts[value] = sum
	}
	if len(sum) != len(counts) {
		return fmt.Errorf("%s returned %d buckets for %q, expected %d", source, len(counts), value, len(sum))
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
	return nil
}
