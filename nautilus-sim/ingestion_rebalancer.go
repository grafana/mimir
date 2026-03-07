package main

import "sort"

func (sim *Simulation) runIngestionRebalancer() (int, float64) {
	if computeMaxMean(sim.partitionIngestion) <= sim.cfg.BalanceTarget {
		return 0, 0
	}
	sim.ingestionMergePhase()
	moves, churn := sim.ingestionWeightedMovePhase()
	sim.ingestionSplitPhase()
	return moves, churn
}

// Phase 1: merge adjacent cold hash ranges on the same partition to defragment.
func (sim *Simulation) ingestionMergePhase() {
	for _, p := range sim.partitions {
		if len(p.HashRanges) <= sim.cfg.MinRangesPerPartition {
			continue
		}

		for {
			if len(p.HashRanges) <= sim.cfg.MinRangesPerPartition {
				break
			}

			ranges := sortedRanges(p.HashRanges)

			totalLoad := 0.0
			for _, hr := range ranges {
				totalLoad += sim.rangeIngestion[hr]
			}
			meanLoad := totalLoad / float64(len(ranges))

			merged := false
			for i := 0; i < len(ranges)-1; i++ {
				a, b := ranges[i], ranges[i+1]
				if !a.CanMergeWith(b) {
					continue
				}
				combined := sim.rangeIngestion[a] + sim.rangeIngestion[b]
				if combined > meanLoad {
					continue
				}
				m := MergeRanges(a, b)
				delete(p.HashRanges, a)
				delete(p.HashRanges, b)
				p.HashRanges[m] = struct{}{}
				sim.rangeIngestion[m] = combined
				delete(sim.rangeIngestion, a)
				delete(sim.rangeIngestion, b)
				merged = true
				break
			}
			if !merged {
				break
			}
		}
	}
}

// Phase 2: weighted moves from overloaded to underloaded partitions.
func (sim *Simulation) ingestionWeightedMovePhase() (int, float64) {
	moves := 0
	churnUsed := 0.0

	for moves < sim.cfg.IngestionMoveBudget && churnUsed < sim.cfg.MoveChurnFraction {
		mean := meanFloat64(sim.partitionIngestion)
		if mean == 0 {
			break
		}

		maxPID := maxIndex(sim.partitionIngestion)
		if sim.partitionIngestion[maxPID]/mean <= sim.cfg.BalanceTarget {
			break
		}

		minPID := minIndex(sim.partitionIngestion)
		if maxPID == minPID {
			break
		}

		bestWeight := -1.0
		var bestRange HashRange
		bestFound := false

		for hr := range sim.partitions[maxPID].HashRanges {
			rangeLoad := sim.rangeIngestion[hr]
			churnFrac := hr.FractionOfSpace()
			if churnFrac == 0 {
				continue
			}

			newMaxLoad := sim.partitionIngestion[maxPID] - rangeLoad
			newMinLoad := sim.partitionIngestion[minPID] + rangeLoad

			newMax := 0.0
			for i, l := range sim.partitionIngestion {
				nl := l
				if i == maxPID {
					nl = newMaxLoad
				} else if i == minPID {
					nl = newMinLoad
				}
				if nl > newMax {
					newMax = nl
				}
			}

			improvement := (sim.partitionIngestion[maxPID] / mean) - (newMax / mean)
			if improvement <= 0 {
				continue
			}

			weight := improvement / churnFrac
			if weight > bestWeight {
				bestWeight = weight
				bestRange = hr
				bestFound = true
			}
		}

		if !bestFound {
			break
		}

		rangeLoad := sim.rangeIngestion[bestRange]
		delete(sim.partitions[maxPID].HashRanges, bestRange)
		sim.partitions[minPID].HashRanges[bestRange] = struct{}{}
		sim.partitionIngestion[maxPID] -= rangeLoad
		sim.partitionIngestion[minPID] += rangeLoad
		churnUsed += bestRange.FractionOfSpace()
		moves++
	}

	return moves, churnUsed
}

// Phase 3: recursively split hot hash ranges until no range exceeds 2x mean
// or the per-partition range count limit is reached. Recursive splitting is
// necessary because the Nautilus hash packs each metric into a narrow band
// (~1024 values for 22/10 bit layout) that may require many bisections to
// isolate from a large initial range.
func (sim *Simulation) ingestionSplitPhase() {
	for {
		totalLoad := 0.0
		totalRanges := 0
		for _, p := range sim.partitions {
			for hr := range p.HashRanges {
				totalLoad += sim.rangeIngestion[hr]
				totalRanges++
			}
		}
		if totalRanges == 0 {
			return
		}
		meanLoad := totalLoad / float64(totalRanges)

		anySplit := false
		for _, p := range sim.partitions {
			if len(p.HashRanges) >= sim.cfg.MaxRangesPerPartition {
				continue
			}

			var toSplit []HashRange
			for hr := range p.HashRanges {
				if sim.rangeIngestion[hr] >= 2*meanLoad && hr.Size() > 1 {
					toSplit = append(toSplit, hr)
				}
			}

			for _, hr := range toSplit {
				if len(p.HashRanges) >= sim.cfg.MaxRangesPerPartition {
					break
				}
				a, b := hr.Split()
				delete(p.HashRanges, hr)
				p.HashRanges[a] = struct{}{}
				p.HashRanges[b] = struct{}{}

			aIng, aQry := sim.computeRangeLoads(a)
			bIng, bQry := sim.computeRangeLoads(b)
			delete(sim.rangeIngestion, hr)
			delete(sim.rangeQuery, hr)
			sim.rangeIngestion[a] = aIng
			sim.rangeIngestion[b] = bIng
			sim.rangeQuery[a] = aQry
			sim.rangeQuery[b] = bQry
			anySplit = true
			}
		}

		if !anySplit {
			break
		}
	}
}

func sortedRanges(hrs map[HashRange]struct{}) []HashRange {
	out := make([]HashRange, 0, len(hrs))
	for hr := range hrs {
		out = append(out, hr)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Lo < out[j].Lo })
	return out
}
