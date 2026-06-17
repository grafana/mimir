// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

// computeRateZeroExclusions returns the set of partitions whose
// reported sample rate is suspicious — zero, even though the
// partition holds in-memory series (L > 0). The slicer treats those
// partitions as "rate unknown" and skips them in Phase 3's hot/cold
// selection for one round; see the long comment in runPhase3 for
// the rationale.
//
// The predicate is intentionally narrow:
//
//   - rate == 0:        only a complete absence of reports counts;
//     a partition with even a small positive rate
//     is treated as "rate known, just cold".
//   - L > 0:            the partition has series in some readcache's
//     TSDB head, so the rate=0 cannot reflect a
//     genuinely empty partition.
//
// Truly empty partitions (rate == 0 && L == 0) — for example a
// freshly-created partition that has not received any writes yet, or
// a partition whose entire range set was just merged away — are NOT
// excluded; treating them as cold destinations is the correct
// behavior at cold-start and after a tier-1 split.
//
// Returns nil when no partitions match, so callers can rely on
// `len(out) == 0` and `out == nil` as equivalent quick checks.
func computeRateZeroExclusions(
	partitionRateByPID map[int32]float64,
	partitionLByPID map[int32]int64,
	activePartitions []int32,
) map[int32]bool {
	if len(activePartitions) == 0 {
		return nil
	}
	var out map[int32]bool
	for _, pid := range activePartitions {
		if partitionRateByPID[pid] != 0 {
			continue
		}
		if partitionLByPID[pid] <= 0 {
			continue
		}
		if out == nil {
			out = make(map[int32]bool, 4)
		}
		out[pid] = true
	}
	return out
}
