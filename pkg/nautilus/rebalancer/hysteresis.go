// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"time"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
)

type partitionRoleCooldowns struct {
	recentSources      map[int32]time.Time
	recentDestinations map[int32]time.Time
}

func newPartitionRoleCooldowns() partitionRoleCooldowns {
	return partitionRoleCooldowns{
		recentSources:      map[int32]time.Time{},
		recentDestinations: map[int32]time.Time{},
	}
}

func (c *partitionRoleCooldowns) ensureMaps() {
	if c.recentSources == nil {
		c.recentSources = map[int32]time.Time{}
	}
	if c.recentDestinations == nil {
		c.recentDestinations = map[int32]time.Time{}
	}
}

func (c *partitionRoleCooldowns) record(now time.Time, duration time.Duration, actions []Action) {
	if duration <= 0 {
		return
	}
	c.ensureMaps()
	until := now.Add(duration)
	for _, action := range actions {
		if action.Kind != ActionMove {
			continue
		}
		c.recentSources[action.FromPart] = until
		c.recentDestinations[action.ToPart] = until
	}
}

func (c *partitionRoleCooldowns) prune(now time.Time) {
	c.ensureMaps()
	for pid, until := range c.recentSources {
		if !until.After(now) {
			delete(c.recentSources, pid)
		}
	}
	for pid, until := range c.recentDestinations {
		if !until.After(now) {
			delete(c.recentDestinations, pid)
		}
	}
}

func (c *partitionRoleCooldowns) blocksSource(pid int32, now time.Time) bool {
	c.ensureMaps()
	return c.recentDestinations[pid].After(now)
}

func (c *partitionRoleCooldowns) blocksDestination(pid int32, now time.Time) bool {
	c.ensureMaps()
	return c.recentSources[pid].After(now)
}

func (r *Rebalancer) recordStructuralCooldowns(now time.Time, actions []Action) {
	if r.cfg.StructuralCooldown <= 0 {
		return
	}
	if r.structuralCooldowns == nil {
		r.structuralCooldowns = map[assignment.HashRange]time.Time{}
	}
	until := now.Add(r.cfg.StructuralCooldown)
	for _, action := range actions {
		if action.Kind == ActionMerge || action.Kind == ActionSplit {
			r.structuralCooldowns[action.Range] = until
		}
	}
}

func (r *Rebalancer) pruneStabilizationCooldowns(now time.Time) {
	r.partitionRoles.prune(now)
	for hr, until := range r.structuralCooldowns {
		if !until.After(now) {
			delete(r.structuralCooldowns, hr)
		}
	}
}
