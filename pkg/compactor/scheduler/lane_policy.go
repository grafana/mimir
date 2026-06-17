// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"flag"
	"fmt"
	"slices"

	"github.com/grafana/mimir/pkg/compactor/scheduler/compactorschedulerpb"
)

// lane is an in-memory identifier of pending work logically enqueued together.
type lane uint8

const (
	lanePolicySimple = "simple"

	planLane lane = iota
	compactionLane
)

type laneTransition struct {
	lane lane
	kind rotationTransition
}

// Defines how to map jobs and requests into lanes
type lanePolicy interface {
	AllLanes() []lane                                                      // All possible lanes defined by this policy
	LaneForJob(TrackedJob) lane                                            // The lane this job is assigned to. A job must always map to some lane.
	LanesForRequest(*compactorschedulerpb.LeaseJobRequest) ([]lane, error) // The lanes this worker requested, or an error
}

type LanePolicyConfig struct {
	policy string
}

func (cfg *LanePolicyConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.policy, prefix+".policy", "simple", "The lane policy the compactor scheduler should use. Valid values: "+fmt.Sprintf("(%s)", lanePolicySimple))
}

func newLanePolicy(cfg LanePolicyConfig) (lanePolicy, error) {
	switch cfg.policy {
	case "simple":
		return newSimpleLanePolicy(), nil
	default:
		return nil, fmt.Errorf("unrecognized lane policy: %s", cfg.policy)
	}
}

// simpleLanePolicy assigns a lane per job type
type simpleLanePolicy struct {
	allLanes []lane
}

func newSimpleLanePolicy() lanePolicy {
	return &simpleLanePolicy{
		allLanes: []lane{planLane, compactionLane},
	}
}

func (slp *simpleLanePolicy) LaneForJob(j TrackedJob) lane {
	if j.ID() == planJobId {
		return planLane
	}
	return compactionLane
}

func (slp *simpleLanePolicy) AllLanes() []lane {
	return slp.allLanes
}

// requestedLanes maps a worker-requested lane to the internal lanes it should be served from, in priority order.
func (slp *simpleLanePolicy) LanesForRequest(req *compactorschedulerpb.LeaseJobRequest) ([]lane, error) {
	numLanes := len(req.LaneRequests)
	if numLanes == 0 {
		// No lanes supplied, provide a default
		return slp.AllLanes(), nil
	}
	if numLanes > len(slp.allLanes) {
		return nil, fmt.Errorf("at most %d lanes supported, provided %d", len(slp.allLanes), numLanes)
	}

	lanes := make([]lane, 0, numLanes)
	for _, ln := range req.LaneRequests {
		var l lane
		switch ln.JobType {
		case compactorschedulerpb.JOB_TYPE_PLANNING:
			l = planLane
		case compactorschedulerpb.JOB_TYPE_COMPACTION:
			l = compactionLane
		default:
			return nil, fmt.Errorf("unknown job type in lane request: %q", ln.JobType.String())
		}
		if slices.Contains(lanes, l) {
			return nil, fmt.Errorf("duplicate lane in request: %q", ln.JobType.String())
		}
		lanes = append(lanes, l)
	}
	return lanes, nil
}
