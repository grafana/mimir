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
	planLane         lane = "plan"
	compactionLane   lane = "compaction"
	compactionP1Lane lane = "compaction-p1"
	compactionP2Lane lane = "compaction-p2"
)

const (
	lanePolicySimple            = "simple"
	lanePolicyCompactionUrgency = "compaction-urgency"
)

type laneTransition struct {
	lane lane
	kind rotationTransition
}

// Defines how to map jobs and requests into lanes
type lanePolicy interface {
	// AllLanes returns every lane this policy defines. The scheduler tracks exactly these, and
	// serves them in this order to a worker that names no job type.
	AllLanes() []lane

	// CompactionLanes returns the lanes carrying compaction jobs.
	CompactionLanes() []lane

	// LaneForJob returns the job's lane. It must always return the same lane for a job for the
	// lifetime of the process, as different callers may re-derive it at different times.
	LaneForJob(TrackedJob) lane

	// LanesForRequest returns the lanes this worker requested.
	LanesForRequest(*compactorschedulerpb.LeaseJobRequest) ([]lane, error)
}

type CompactionUrgencyConfig struct {
	P1MaxSpan    time.Duration `yaml:"p1_max_span" category:"experimental"`
	OutOfOrderP1 bool          `yaml:"out_of_order_p1" category:"experimental"`
}

func (cfg *CompactionUrgencyConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.DurationVar(&cfg.P1MaxSpan, prefix+".p1-max-span", 2*time.Hour, "Compaction jobs whose source blocks span at most this duration are served from the p1 lane. Jobs spanning longer are served from the p2 lane.")
	f.BoolVar(&cfg.OutOfOrderP1, prefix+".out-of-order-p1", true, "Serve out-of-order compaction jobs from the p1 lane regardless of the duration they span. Disable if out-of-order jobs for a tenant grow large enough to dominate the p1 lane.")
}

func (cfg *CompactionUrgencyConfig) Validate(prefix string) error {
	if cfg.P1MaxSpan <= 0 {
		return errors.New(prefix + ".p1-max-span must be positive")
	}
	return nil
}

// isP1 treats out-of-order jobs as p1 because they are recompacted repeatedly, and so keep
// spanning a wide time range while staying small. Such a job is only recognizable while it
// carries the "from-out-of-order" hint, which the compactor drops past the first level, or if the
// tenant has -blocks-storage.tsdb.out-of-order-blocks-external-label-enabled set.
func (cfg CompactionUrgencyConfig) isP1(j *CompactionJob) bool {
	return (cfg.OutOfOrderP1 && j.outOfOrder) || j.Duration() <= cfg.P1MaxSpan
}

type LanePolicyConfig struct {
	Policy            string                  `yaml:"policy" category:"experimental"`
	CompactionUrgency CompactionUrgencyConfig `yaml:"compaction_urgency"`
}

func (cfg *LanePolicyConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.Policy, prefix+".policy", lanePolicySimple, "The lane policy the compactor scheduler should use. Valid values: "+lanePolicySimple+", "+lanePolicyCompactionUrgency)
	cfg.CompactionUrgency.RegisterFlagsWithPrefix(prefix+".compaction-urgency", f)
}

func (cfg *LanePolicyConfig) Validate(prefix string) error {
	if _, err := newLanePolicy(*cfg); err != nil {
		return err
	}
	return cfg.CompactionUrgency.Validate(prefix + ".compaction-urgency")
}

func newLanePolicy(cfg LanePolicyConfig) (lanePolicy, error) {
	switch cfg.Policy {
	case lanePolicySimple:
		return newSimpleLanePolicy(), nil
	case lanePolicyCompactionUrgency:
		return newCompactionUrgencyLanePolicy(cfg.CompactionUrgency), nil
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

// compactionUrgencyLanePolicy gives each urgency level its own lane, so that workers can be
// dedicated to one level and sized independently of the other.
//
// Dedicate them: Rotator.LeaseJob walks lanes in the requested order and only round-robins tenants
// within a lane, so a worker asking for both levels drains p1 work across every tenant before any
// tenant's p2 work. Under simpleLanePolicy the tenant rotation is the outermost fairness rule, so
// this is a weaker guarantee: one tenant's p1 backlog can starve another tenant's p2 work.
type compactionUrgencyLanePolicy struct {
	allLanes        []lane
	compactionLanes []lane
	urgencyCfg      CompactionUrgencyConfig
}

func newCompactionUrgencyLanePolicy(urgencyCfg CompactionUrgencyConfig) lanePolicy {
	return &compactionUrgencyLanePolicy{
		allLanes:        []lane{planLane, compactionP1Lane, compactionP2Lane},
		compactionLanes: []lane{compactionP1Lane, compactionP2Lane},
		urgencyCfg:      urgencyCfg,
	}
}

func (clp *compactionUrgencyLanePolicy) LaneForJob(j TrackedJob) lane {
	if j.ID() == planJobId {
		return planLane
	}
	// Every non-plan job is a compaction job; anything else is a programming error.
	cj := j.(*TrackedCompactionJob)
	if clp.urgencyCfg.isP1(cj.value) {
		return compactionP1Lane
	}
	return compactionP2Lane
}

func (clp *compactionUrgencyLanePolicy) AllLanes() []lane {
	return clp.allLanes
}

func (clp *compactionUrgencyLanePolicy) CompactionLanes() []lane {
	return clp.compactionLanes
}

// LanesForRequest serves both urgency lanes to a compaction request that names no level.
func (clp *compactionUrgencyLanePolicy) LanesForRequest(req *compactorschedulerpb.LeaseJobRequest) ([]lane, error) {
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
			case compactorschedulerpb.COMPACTION_URGENCY_P1:
				requested = []lane{compactionP1Lane}
			case compactorschedulerpb.COMPACTION_URGENCY_P2:
				requested = []lane{compactionP2Lane}
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
