// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"errors"
	"flag"
	"fmt"
	"slices"
	"time"

	"github.com/grafana/mimir/pkg/compactor/scheduler/compactorschedulerpb"
)

// lane is an in-memory identifier of pending work logically enqueued together. Its value is
// exported as a metric label, so renaming one changes existing metrics.
type lane string

const (
	lanePolicySimple  = "simple"
	lanePolicyUrgency = "urgency"

	planLane             lane = "plan"
	compactionLane       lane = "compaction"
	compactionUrgentLane lane = "compaction-urgent"
	compactionDeferLane  lane = "compaction-defer"
)

type laneTransition struct {
	lane lane
	kind rotationTransition
}

// Defines how to map jobs and requests into lanes
type lanePolicy interface {
	AllLanes() []lane                                                      // All possible lanes defined by this policy.
	CompactionLanes() []lane                                               // The lanes that carry compaction jobs.
	LaneForJob(TrackedJob) lane                                            // The lane this job is assigned to. A job must always map to some lane.
	LanesForRequest(*compactorschedulerpb.LeaseJobRequest) ([]lane, error) // The lanes this worker requested, or an error.
}

type UrgencyConfig struct {
	MaxSpan    time.Duration `yaml:"max_span" category:"experimental"`
	OutOfOrder bool          `yaml:"out_of_order" category:"experimental"`
}

func (cfg *UrgencyConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.DurationVar(&cfg.MaxSpan, prefix+".max-span", 2*time.Hour, "Compaction jobs whose source blocks span at most this duration are served from the urgent lane. Jobs spanning longer are served from the defer lane.")
	f.BoolVar(&cfg.OutOfOrder, prefix+".out-of-order", true, "Serve out-of-order compaction jobs from the urgent lane regardless of the duration they span. Disable if out-of-order jobs for a tenant grow large enough to dominate the urgent lane.")
}

func (cfg *UrgencyConfig) Validate(prefix string) error {
	if cfg.MaxSpan <= 0 {
		return errors.New(prefix + ".max-span must be positive")
	}
	return nil
}

// isUrgent treats out-of-order jobs as urgent because they are recompacted repeatedly, and so keep
// spanning a wide time range while staying small. Such a job is only recognizable while it
// carries the "from-out-of-order" hint, which the compactor drops past the first level, or if the
// tenant has -blocks-storage.tsdb.out-of-order-blocks-external-label-enabled set.
func (cfg UrgencyConfig) isUrgent(j *CompactionJob) bool {
	return (cfg.OutOfOrder && j.outOfOrder) || j.Duration() <= cfg.MaxSpan
}

type LanePolicyConfig struct {
	Policy  string        `yaml:"policy" category:"experimental"`
	Urgency UrgencyConfig `yaml:"urgency"`
}

func (cfg *LanePolicyConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.Policy, prefix+".policy", lanePolicySimple, "The lane policy the compactor scheduler should use. Valid values: "+lanePolicySimple+", "+lanePolicyUrgency)
	cfg.Urgency.RegisterFlagsWithPrefix(prefix+".urgency", f)
}

func (cfg *LanePolicyConfig) Validate(prefix string) error {
	if _, err := newLanePolicy(*cfg); err != nil {
		return err
	}
	return cfg.Urgency.Validate(prefix + ".urgency")
}

func newLanePolicy(cfg LanePolicyConfig) (lanePolicy, error) {
	switch cfg.Policy {
	case lanePolicySimple:
		return newSimpleLanePolicy(), nil
	case lanePolicyUrgency:
		return newUrgencyLanePolicy(cfg.Urgency), nil
	default:
		return nil, fmt.Errorf("unrecognized lane policy: %s", cfg.Policy)
	}
}

// simpleLanePolicy assigns a lane per job type
type simpleLanePolicy struct {
	allLanes        []lane
	compactionLanes []lane
}

func newSimpleLanePolicy() lanePolicy {
	return &simpleLanePolicy{
		allLanes:        []lane{planLane, compactionLane},
		compactionLanes: []lane{compactionLane},
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

func (slp *simpleLanePolicy) CompactionLanes() []lane {
	return slp.compactionLanes
}

// requestedLanes maps a lease request to scheduler lanes
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

// urgencyLanePolicy gives urgent and deferrable compaction their own lanes, so that workers can be
// dedicated to one and sized independently of the other.
//
// Dedicate them: Rotator.LeaseJob walks lanes in the requested order and only round-robins tenants
// within a lane, so a worker asking for both drains urgent work across every tenant before any
// tenant's deferred work. Under simpleLanePolicy the tenant rotation is the outermost fairness
// rule, so this is a weaker guarantee: one tenant's urgent backlog can starve another tenant's
// deferred work.
type urgencyLanePolicy struct {
	allLanes        []lane
	compactionLanes []lane
	urgencyCfg      UrgencyConfig
}

func newUrgencyLanePolicy(urgencyCfg UrgencyConfig) lanePolicy {
	return &urgencyLanePolicy{
		allLanes:        []lane{planLane, compactionUrgentLane, compactionDeferLane},
		compactionLanes: []lane{compactionUrgentLane, compactionDeferLane},
		urgencyCfg:      urgencyCfg,
	}
}

func (clp *urgencyLanePolicy) LaneForJob(j TrackedJob) lane {
	if j.ID() == planJobId {
		return planLane
	}
	// Every non-plan job is a compaction job; anything else is a programming error.
	cj := j.(*TrackedCompactionJob)
	if clp.urgencyCfg.isUrgent(cj.value) {
		return compactionUrgentLane
	}
	return compactionDeferLane
}

func (clp *urgencyLanePolicy) AllLanes() []lane {
	return clp.allLanes
}

func (clp *urgencyLanePolicy) CompactionLanes() []lane {
	return clp.compactionLanes
}

// LanesForRequest serves both urgency lanes to a compaction request that names no urgency.
func (clp *urgencyLanePolicy) LanesForRequest(req *compactorschedulerpb.LeaseJobRequest) ([]lane, error) {
	numLanes := len(req.LaneRequests)
	if numLanes == 0 {
		// No lanes supplied, provide a default
		return clp.AllLanes(), nil
	}
	if numLanes > len(clp.allLanes) {
		return nil, fmt.Errorf("at most %d lanes supported, provided %d", len(clp.allLanes), numLanes)
	}

	lanes := make([]lane, 0, len(clp.allLanes))
	for _, ln := range req.LaneRequests {
		var requested []lane
		switch ln.JobType {
		case compactorschedulerpb.JOB_TYPE_PLANNING:
			requested = []lane{planLane}
		case compactorschedulerpb.JOB_TYPE_COMPACTION:
			switch ln.CompactionUrgency {
			case compactorschedulerpb.COMPACTION_URGENCY_URGENT:
				requested = []lane{compactionUrgentLane}
			case compactorschedulerpb.COMPACTION_URGENCY_DEFER:
				requested = []lane{compactionDeferLane}
			default:
				requested = clp.compactionLanes
			}
		default:
			return nil, fmt.Errorf("unknown job type in lane request: %q", ln.JobType.String())
		}
		for _, l := range requested {
			if slices.Contains(lanes, l) {
				return nil, fmt.Errorf("duplicate lane in request: %q", l)
			}
			lanes = append(lanes, l)
		}
	}
	return lanes, nil
}
