// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"flag"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/compactor/scheduler/compactorschedulerpb"
)

func testCompactionUrgencyConfig() CompactionUrgencyConfig {
	var cfg CompactionUrgencyConfig
	cfg.RegisterFlagsWithPrefix("test", flag.NewFlagSet("test", flag.ContinueOnError))
	return cfg
}

func TestCompactionUrgencyConfig_IsP1(t *testing.T) {
	const hour = int64(time.Hour / time.Millisecond)

	tests := map[string]struct {
		cfg        CompactionUrgencyConfig
		job        *CompactionJob
		expectedP1 bool
	}{
		"in-order job within the p1 span": {
			cfg:        CompactionUrgencyConfig{P1MaxSpan: 2 * time.Hour, OutOfOrderP1: true},
			job:        &CompactionJob{minTime: 0, maxTime: hour},
			expectedP1: true,
		},
		"in-order job exactly at the p1 span": {
			cfg:        CompactionUrgencyConfig{P1MaxSpan: 2 * time.Hour, OutOfOrderP1: true},
			job:        &CompactionJob{minTime: 0, maxTime: 2 * hour},
			expectedP1: true,
		},
		"in-order job beyond the p1 span": {
			cfg:        CompactionUrgencyConfig{P1MaxSpan: 2 * time.Hour, OutOfOrderP1: true},
			job:        &CompactionJob{minTime: 0, maxTime: 2*hour + 1},
			expectedP1: false,
		},
		"in-order 24h job": {
			cfg:        CompactionUrgencyConfig{P1MaxSpan: 2 * time.Hour, OutOfOrderP1: true},
			job:        &CompactionJob{minTime: 0, maxTime: 24 * hour},
			expectedP1: false,
		},
		"out-of-order 24h job": {
			cfg:        CompactionUrgencyConfig{P1MaxSpan: 2 * time.Hour, OutOfOrderP1: true},
			job:        &CompactionJob{minTime: 0, maxTime: 24 * hour, outOfOrder: true},
			expectedP1: true,
		},
		"out-of-order 24h job with out-of-order p1 classification disabled": {
			cfg:        CompactionUrgencyConfig{P1MaxSpan: 2 * time.Hour, OutOfOrderP1: false},
			job:        &CompactionJob{minTime: 0, maxTime: 24 * hour, outOfOrder: true},
			expectedP1: false,
		},
		"out-of-order short job with out-of-order p1 classification disabled": {
			cfg:        CompactionUrgencyConfig{P1MaxSpan: 2 * time.Hour, OutOfOrderP1: false},
			job:        &CompactionJob{minTime: 0, maxTime: hour, outOfOrder: true},
			expectedP1: true,
		},
		// Timestamps arrive from a worker, so an inverted range must not overflow into a huge duration.
		"inverted range is treated as spanning nothing": {
			cfg:        CompactionUrgencyConfig{P1MaxSpan: 2 * time.Hour, OutOfOrderP1: false},
			job:        &CompactionJob{minTime: 24 * hour, maxTime: 0},
			expectedP1: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.expectedP1, tc.cfg.isP1(tc.job))
		})
	}
}

func TestCompactionUrgencyLanePolicy_LaneForJob(t *testing.T) {
	const hour = int64(time.Hour / time.Millisecond)
	policy := newCompactionUrgencyLanePolicy(testCompactionUrgencyConfig())

	newJob := func(job *CompactionJob) TrackedJob {
		return NewTrackedCompactionJob("id", job, 1, time.Now())
	}

	require.Equal(t, planLane, policy.LaneForJob(NewTrackedPlanJob(time.Now())))
	require.Equal(t, compactionP1Lane, policy.LaneForJob(newJob(&CompactionJob{maxTime: hour})))
	require.Equal(t, compactionP2Lane, policy.LaneForJob(newJob(&CompactionJob{maxTime: 24 * hour})))
	require.Equal(t, compactionP1Lane, policy.LaneForJob(newJob(&CompactionJob{maxTime: 24 * hour, outOfOrder: true})))
}

func TestLanePolicy_AllLanesCoversEveryJobType(t *testing.T) {
	for name, policy := range map[string]lanePolicy{
		lanePolicySimple:            newSimpleLanePolicy(),
		lanePolicyCompactionUrgency: newCompactionUrgencyLanePolicy(testCompactionUrgencyConfig()),
	} {
		t.Run(name, func(t *testing.T) {
			// Rotator and JobTracker only track lanes reported by AllLanes, so any lane a lease
			// request can return must appear there.
			for _, jobType := range []compactorschedulerpb.JobType{
				compactorschedulerpb.JOB_TYPE_PLANNING,
				compactorschedulerpb.JOB_TYPE_COMPACTION,
			} {
				lanes, err := policy.LanesForRequest(&compactorschedulerpb.LeaseJobRequest{
					LaneRequests: []*compactorschedulerpb.LaneRequest{{JobType: jobType}},
				})
				require.NoError(t, err)
				for _, l := range lanes {
					require.Contains(t, policy.AllLanes(), l, "lane %q served for %s is not in AllLanes", l, jobType)
				}
			}
			// Plan jobs carry no bytes, so tracking the plan lane would only export zeroes.
			require.NotContains(t, policy.CompactionLanes(), planLane)
		})
	}
}

func TestLanesForRequest(t *testing.T) {
	laneRequests := func(types ...compactorschedulerpb.JobType) *compactorschedulerpb.LeaseJobRequest {
		req := &compactorschedulerpb.LeaseJobRequest{}
		for _, jt := range types {
			req.LaneRequests = append(req.LaneRequests, &compactorschedulerpb.LaneRequest{JobType: jt})
		}
		return req
	}
	compactionUrgency := func(urgency compactorschedulerpb.CompactionUrgency) *compactorschedulerpb.LeaseJobRequest {
		return &compactorschedulerpb.LeaseJobRequest{
			LaneRequests: []*compactorschedulerpb.LaneRequest{
				{JobType: compactorschedulerpb.JOB_TYPE_COMPACTION, CompactionUrgency: urgency},
			},
		}
	}

	tests := map[string]struct {
		policy      lanePolicy
		req         *compactorschedulerpb.LeaseJobRequest
		expected    []lane
		expectedErr string
	}{
		"simple: no lane requests is served every lane": {
			policy:   newSimpleLanePolicy(),
			req:      laneRequests(),
			expected: []lane{planLane, compactionLane},
		},
		"simple: compaction then planning preserves request order": {
			policy:   newSimpleLanePolicy(),
			req:      laneRequests(compactorschedulerpb.JOB_TYPE_COMPACTION, compactorschedulerpb.JOB_TYPE_PLANNING),
			expected: []lane{compactionLane, planLane},
		},
		"compaction urgency: no lane requests is served every lane": {
			policy:   newCompactionUrgencyLanePolicy(testCompactionUrgencyConfig()),
			req:      laneRequests(),
			expected: []lane{planLane, compactionP1Lane, compactionP2Lane},
		},
		"compaction urgency: compaction expands to both urgency lanes": {
			policy:   newCompactionUrgencyLanePolicy(testCompactionUrgencyConfig()),
			req:      laneRequests(compactorschedulerpb.JOB_TYPE_COMPACTION),
			expected: []lane{compactionP1Lane, compactionP2Lane},
		},
		"compaction urgency: planning only": {
			policy:   newCompactionUrgencyLanePolicy(testCompactionUrgencyConfig()),
			req:      laneRequests(compactorschedulerpb.JOB_TYPE_PLANNING),
			expected: []lane{planLane},
		},
		"compaction urgency: compaction then planning preserves request order": {
			policy:   newCompactionUrgencyLanePolicy(testCompactionUrgencyConfig()),
			req:      laneRequests(compactorschedulerpb.JOB_TYPE_COMPACTION, compactorschedulerpb.JOB_TYPE_PLANNING),
			expected: []lane{compactionP1Lane, compactionP2Lane, planLane},
		},
		"compaction urgency: p1 only": {
			policy:   newCompactionUrgencyLanePolicy(testCompactionUrgencyConfig()),
			req:      compactionUrgency(compactorschedulerpb.COMPACTION_URGENCY_P1),
			expected: []lane{compactionP1Lane},
		},
		"compaction urgency: p2 only": {
			policy:   newCompactionUrgencyLanePolicy(testCompactionUrgencyConfig()),
			req:      compactionUrgency(compactorschedulerpb.COMPACTION_URGENCY_P2),
			expected: []lane{compactionP2Lane},
		},
		// The simple policy has one compaction lane, so an urgency request collapses onto it. This
		// keeps worker configuration valid whichever policy the scheduler runs.
		"simple: urgency request collapses to the single compaction lane": {
			policy:   newSimpleLanePolicy(),
			req:      compactionUrgency(compactorschedulerpb.COMPACTION_URGENCY_P2),
			expected: []lane{compactionLane},
		},
		// An urgency level is only meaningful for compaction, so naming one elsewhere is ignored.
		"compaction urgency: planning narrowed to a level is served the plan lane": {
			policy: newCompactionUrgencyLanePolicy(testCompactionUrgencyConfig()),
			req: &compactorschedulerpb.LeaseJobRequest{
				LaneRequests: []*compactorschedulerpb.LaneRequest{
					{JobType: compactorschedulerpb.JOB_TYPE_PLANNING, CompactionUrgency: compactorschedulerpb.COMPACTION_URGENCY_P1},
				},
			},
			expected: []lane{planLane},
		},
		"compaction urgency: a level already served by an unnarrowed request is rejected": {
			policy: newCompactionUrgencyLanePolicy(testCompactionUrgencyConfig()),
			req: &compactorschedulerpb.LeaseJobRequest{
				LaneRequests: []*compactorschedulerpb.LaneRequest{
					{JobType: compactorschedulerpb.JOB_TYPE_COMPACTION},
					{JobType: compactorschedulerpb.JOB_TYPE_COMPACTION, CompactionUrgency: compactorschedulerpb.COMPACTION_URGENCY_P2},
				},
			},
			expectedErr: `duplicate lane in request: "compaction-p2"`,
		},
		"compaction urgency: unknown job type is rejected": {
			policy:      newCompactionUrgencyLanePolicy(testCompactionUrgencyConfig()),
			req:         laneRequests(compactorschedulerpb.JOB_TYPE_UNKNOWN),
			expectedErr: `unknown job type in lane request: "JOB_TYPE_UNKNOWN"`,
		},
		"compaction urgency: more requests than lanes is rejected": {
			policy: newCompactionUrgencyLanePolicy(testCompactionUrgencyConfig()),
			req: laneRequests(
				compactorschedulerpb.JOB_TYPE_PLANNING,
				compactorschedulerpb.JOB_TYPE_COMPACTION,
				compactorschedulerpb.JOB_TYPE_PLANNING,
				compactorschedulerpb.JOB_TYPE_COMPACTION,
			),
			expectedErr: "at most 3 lanes supported, provided 4",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			lanes, err := tc.policy.LanesForRequest(tc.req)
			if tc.expectedErr != "" {
				require.EqualError(t, err, tc.expectedErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expected, lanes)
		})
	}
}

func TestNewLanePolicy(t *testing.T) {
	cfg := LanePolicyConfig{CompactionUrgency: testCompactionUrgencyConfig()}

	cfg.Policy = lanePolicySimple
	policy, err := newLanePolicy(cfg)
	require.NoError(t, err)
	require.IsType(t, &simpleLanePolicy{}, policy)

	cfg.Policy = lanePolicyCompactionUrgency
	policy, err = newLanePolicy(cfg)
	require.NoError(t, err)
	require.IsType(t, &compactionUrgencyLanePolicy{}, policy)

	cfg.Policy = "nope"
	_, err = newLanePolicy(cfg)
	require.EqualError(t, err, "unrecognized lane policy: nope")
}

func TestCompactionUrgencyConfig_Validate(t *testing.T) {
	tests := map[string]struct {
		duration time.Duration
		expected string
	}{
		"positive duration": {duration: 2 * time.Hour},
		"zero duration":     {duration: 0, expected: "test.p1-max-span must be positive"},
		"negative duration": {duration: -time.Hour, expected: "test.p1-max-span must be positive"},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			cfg := CompactionUrgencyConfig{P1MaxSpan: tc.duration}
			err := cfg.Validate("test")
			if tc.expected == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tc.expected)
			}
		})
	}
}
