package main

func (sim *Simulation) runQueryRebalancer() int {
	if computeMaxMean(sim.ingesterQuery) <= sim.cfg.BalanceTarget {
		return 0
	}

	totalQuery := sumFloat64(sim.ingesterQuery)
	totalIngestion := sumFloat64(sim.partitionIngestion)

	moves := 0
	for moves < sim.cfg.QueryMoveBudget {
		mean := totalQuery / float64(len(sim.ingesterQuery))
		if mean == 0 {
			break
		}

		maxIng := maxIndex(sim.ingesterQuery)
		if sim.ingesterQuery[maxIng]/mean <= sim.cfg.BalanceTarget {
			break
		}

		minIng := minIndex(sim.ingesterQuery)
		if maxIng == minIng {
			break
		}

		bestWeight := -1.0
		bestPID := int32(-1)

		for pid := range sim.ingesters[maxIng].Partitions {
			if sim.partitionInCatchUp(pid) {
				continue
			}

			pQuery := sim.partitionQuery[pid]
			if pQuery == 0 {
				continue
			}

			// Evaluate steady-state benefit (after catch-up completes).
			newMaxLoad := sim.ingesterQuery[maxIng] - pQuery
			newMinLoad := sim.ingesterQuery[minIng] + pQuery

			newMax := 0.0
			for i, l := range sim.ingesterQuery {
				nl := l
				switch i {
				case maxIng:
					nl = newMaxLoad
				case minIng:
					nl = newMinLoad
				}
				if nl > newMax {
					newMax = nl
				}
			}

			improvement := (sim.ingesterQuery[maxIng] / mean) - (newMax / mean)
			if improvement <= 0 {
				continue
			}

			churnFrac := pQuery / totalQuery
			replayFrac := 0.0
			if totalIngestion > 0 {
				replayFrac = (sim.cfg.CatchupCostFactor * sim.partitionIngestion[pid]) / totalIngestion
			}
			moveCost := churnFrac + replayFrac
			if moveCost <= 0 {
				continue
			}

			weight := improvement / moveCost
			if weight > bestWeight {
				bestWeight = weight
				bestPID = pid
			}
		}

		if bestPID < 0 {
			break
		}

		ingRate := sim.partitionIngestion[bestPID]
		replayLoad := sim.cfg.CatchupCostFactor * ingRate

		delete(sim.ingesters[maxIng].Partitions, bestPID)
		sim.ingesters[minIng].Partitions[bestPID] = struct{}{}

		sim.catchUps = append(sim.catchUps, CatchUpEntry{
			PartitionID:    bestPID,
			OldIngesterID:  maxIng,
			NewIngesterID:  minIng,
			RemainingSteps: sim.cfg.PartitionCatchupSteps,
			ReplayLoad:     replayLoad,
		})

		// Immediate effect on tracked loads: replay overhead on the target.
		// Query load stays on the source ingester during catch-up.
		sim.ingesterQuery[minIng] += replayLoad
		totalQuery += replayLoad

		moves++
	}

	return moves
}
