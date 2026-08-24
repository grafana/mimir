package wgo

import "errors"

// HealthCheckConfig holds the per-agent "is this agent unhealthy?" signals
// shared by every component that reads cluster stats. Both the Hedger
// (which decides when to fire a fallback request) and the Demoter (which
// decides when to reroute away from an agent and probe it occasionally)
// classify an agent's health using the same definitions:
//
//   - SlowMultiplier / MaxSlowFraction define what "slow" means and when
//     widespread slowness suppresses single-agent action.
//   - FaultyThreshold / MaxFaultyFraction define what "faulty" means and
//     when widespread faults suppress single-agent action.
//
// Keeping these in one struct avoids the bug-prone situation of the
// Hedger and Demoter disagreeing on "is agent X unhealthy?" because the
// operator forgot to keep two sets of flags in sync.
type HealthCheckConfig struct {
	// SlowMultiplier marks an agent as slow when its window-average
	// latency exceeds the cluster baseline by this factor. Must be >= 1.
	SlowMultiplier float64

	// MaxSlowFraction suppresses slow-based action when this fraction
	// (or more) of agents is slow — a widespread latency issue should
	// not be made worse by amplifying load.
	MaxSlowFraction float64

	// FaultyThreshold marks an agent as faulty when its window error
	// rate exceeds this absolute fraction (in (0,1]).
	FaultyThreshold float64

	// MaxFaultyFraction suppresses faulty-based action when this
	// fraction (or more) of agents is faulty — a cluster-wide error
	// spike should not be made worse by removing most of the pool.
	MaxFaultyFraction float64
}

// Validate returns an error if the config is invalid.
func (c *HealthCheckConfig) Validate() error {
	if c.SlowMultiplier < 1 {
		return errors.New("health check slow multiplier must be >= 1")
	}
	if c.MaxSlowFraction < 0 || c.MaxSlowFraction > 1 {
		return errors.New("health check max slow fraction must be between 0 and 1")
	}
	if c.FaultyThreshold <= 0 || c.FaultyThreshold > 1 {
		return errors.New("health check faulty threshold must be greater than 0 and at most 1")
	}
	if c.MaxFaultyFraction < 0 || c.MaxFaultyFraction > 1 {
		return errors.New("health check max faulty fraction must be between 0 and 1")
	}
	return nil
}
