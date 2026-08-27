// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"flag"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/compactor/scheduler/compactorschedulerpb"
)

func testUrgencyConfig() UrgencyConfig {
	var cfg UrgencyConfig
	cfg.RegisterFlagsWithPrefix("test", flag.NewFlagSet("test", flag.ContinueOnError))
	return cfg
}

func TestUrgencyConfig_IsUrgent(t *testing.T) {
	const hour = int64(time.Hour / time.Millisecond)

	tests := map[string]struct {
		cfg            UrgencyConfig
		job            *CompactionJob
		expectedUrgent bool
	}{
		"in-order job within the urgent span": {
			cfg:            UrgencyConfig{MaxSpan: 2 * time.Hour, OutOfOrder: true},
			job:            &CompactionJob{minTime: 0, maxTime: hour},
			expectedUrgent: true,
		},
		"in-order job exactly at the urgent span": {
			cfg:            UrgencyConfig{MaxSpan: 2 * time.Hour, OutOfOrder: true},
			job:            &CompactionJob{minTime: 0, maxTime: 2 * hour},
			expectedUrgent: true,
		},
		"in-order job beyond the urgent span": {
			cfg:            UrgencyConfig{MaxSpan: 2 * time.Hour, OutOfOrder: true},
			job:            &CompactionJob{minTime: 0, maxTime: 2*hour + 1},
			expectedUrgent: false,
		},
		"in-order 24h job": {
			cfg:            UrgencyConfig{MaxSpan: 2 * time.Hour, OutOfOrder: true},
			job:            &CompactionJob{minTime: 0, maxTime: 24 * hour},
			expectedUrgent: false,
		},
		"out-of-order 24h job": {
			cfg:            UrgencyConfig{MaxSpan: 2 * time.Hour, OutOfOrder: true},
			job:            &CompactionJob{minTime: 0, maxTime: 24 * hour, outOfOrder: true},
			expectedUrgent: true,
		},
		"out-of-order 24h job with out-of-order urgency disabled": {
			cfg:            UrgencyConfig{MaxSpan: 2 * time.Hour, OutOfOrder: false},
			job:            &CompactionJob{minTime: 0, maxTime: 24 * hour, outOfOrder: true},
			expectedUrgent: false,
		},
		"out-of-order short job with out-of-order urgency disabled": {
			cfg:            UrgencyConfig{MaxSpan: 2 * time.Hour, OutOfOrder: false},
			job:            &CompactionJob{minTime: 0, maxTime: hour, outOfOrder: true},
			expectedUrgent: true,
		},
		// Timestamps arrive from a worker, so an inverted range must not overflow into a huge duration.
		"inverted range is treated as spanning nothing": {
			cfg:            UrgencyConfig{MaxSpan: 2 * time.Hour, OutOfOrder: false},
			job:            &CompactionJob{minTime: 24 * hour, maxTime: 0},
			expectedUrgent: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.expectedUrgent, tc.cfg.isUrgent(tc.job))
		})
	}
}

func TestUrgencyLanePolicy_LaneForJob(t *testing.T) {
	const hour = int64(time.Hour / time.Millisecond)
	policy := newUrgencyLanePolicy(testUrgencyConfig())

	newJob := func(job *CompactionJob) TrackedJob {
		return NewTrackedCompactionJob("id", job, 1, time.Now())
	}

	require.Equal(t, planLane, policy.LaneForJob(NewTrackedPlanJob(time.Now())))
	require.Equal(t, compactionUrgentLane, policy.LaneForJob(newJob(&CompactionJob{maxTime: hour})))
	require.Equal(t, compactionDeferLane, policy.LaneForJob(newJob(&CompactionJob{maxTime: 24 * hour})))
	require.Equal(t, compactionUrgentLane, policy.LaneForJob(newJob(&CompactionJob{maxTime: 24 * hour, outOfOrder: true})))
}

func TestLanePolicy_AllLanesCoversEveryJobType(t *testing.T) {
	for name, policy := range map[string]lanePolicy{
		lanePolicySimple:  newSimpleLanePolicy(),
		lanePolicyUrgency: newUrgencyLanePolicy(testUrgencyConfig()),
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
			policy:   newUrgencyLanePolicy(testUrgencyConfig()),
			req:      laneRequests(),
			expected: []lane{planLane, compactionUrgentLane, compactionDeferLane},
		},
		"compaction urgency: compaction expands to both urgency lanes": {
			policy:   newUrgencyLanePolicy(testUrgencyConfig()),
			req:      laneRequests(compactorschedulerpb.JOB_TYPE_COMPACTION),
			expected: []lane{compactionUrgentLane, compactionDeferLane},
		},
		"compaction urgency: planning only": {
			policy:   newUrgencyLanePolicy(testUrgencyConfig()),
			req:      laneRequests(compactorschedulerpb.JOB_TYPE_PLANNING),
			expected: []lane{planLane},
		},
		"compaction urgency: compaction then planning preserves request order": {
			policy:   newUrgencyLanePolicy(testUrgencyConfig()),
			req:      laneRequests(compactorschedulerpb.JOB_TYPE_COMPACTION, compactorschedulerpb.JOB_TYPE_PLANNING),
			expected: []lane{compactionUrgentLane, compactionDeferLane, planLane},
		},
		"compaction urgency: urgent only": {
			policy:   newUrgencyLanePolicy(testUrgencyConfig()),
			req:      compactionUrgency(compactorschedulerpb.COMPACTION_URGENCY_URGENT),
			expected: []lane{compactionUrgentLane},
		},
		"compaction urgency: defer only": {
			policy:   newUrgencyLanePolicy(testUrgencyConfig()),
			req:      compactionUrgency(compactorschedulerpb.COMPACTION_URGENCY_DEFER),
			expected: []lane{compactionDeferLane},
		},
		// The simple policy has one compaction lane, so an urgency request collapses onto it. This
		// keeps worker configuration valid whichever policy the scheduler runs.
		"simple: urgency request collapses to the single compaction lane": {
			policy:   newSimpleLanePolicy(),
			req:      compactionUrgency(compactorschedulerpb.COMPACTION_URGENCY_DEFER),
			expected: []lane{compactionLane},
		},
		"simple: both urgency lanes and planning collapse to the lanes it has": {
			policy: newSimpleLanePolicy(),
			req: &compactorschedulerpb.LeaseJobRequest{
				LaneRequests: []*compactorschedulerpb.LaneRequest{
					{JobType: compactorschedulerpb.JOB_TYPE_COMPACTION, CompactionUrgency: compactorschedulerpb.COMPACTION_URGENCY_URGENT},
					{JobType: compactorschedulerpb.JOB_TYPE_COMPACTION, CompactionUrgency: compactorschedulerpb.COMPACTION_URGENCY_DEFER},
					{JobType: compactorschedulerpb.JOB_TYPE_PLANNING},
				},
			},
			expected: []lane{compactionLane, planLane},
		},
		// An urgency is only meaningful for compaction, so naming one elsewhere is ignored.
		"compaction urgency: planning narrowed to an urgency is served the plan lane": {
			policy: newUrgencyLanePolicy(testUrgencyConfig()),
			req: &compactorschedulerpb.LeaseJobRequest{
				LaneRequests: []*compactorschedulerpb.LaneRequest{
					{JobType: compactorschedulerpb.JOB_TYPE_PLANNING, CompactionUrgency: compactorschedulerpb.COMPACTION_URGENCY_URGENT},
				},
			},
			expected: []lane{planLane},
		},
		"compaction urgency: an urgency already served by an unnarrowed request is skipped": {
			policy: newUrgencyLanePolicy(testUrgencyConfig()),
			req: &compactorschedulerpb.LeaseJobRequest{
				LaneRequests: []*compactorschedulerpb.LaneRequest{
					{JobType: compactorschedulerpb.JOB_TYPE_COMPACTION},
					{JobType: compactorschedulerpb.JOB_TYPE_COMPACTION, CompactionUrgency: compactorschedulerpb.COMPACTION_URGENCY_DEFER},
				},
			},
			expected: []lane{compactionUrgentLane, compactionDeferLane},
		},
		"compaction urgency: unknown job type is rejected": {
			policy:      newUrgencyLanePolicy(testUrgencyConfig()),
			req:         laneRequests(compactorschedulerpb.JOB_TYPE_UNKNOWN),
			expectedErr: `unknown job type in lane request: "JOB_TYPE_UNKNOWN"`,
		},
		"compaction urgency: repeated requests collapse onto the lanes they name": {
			policy: newUrgencyLanePolicy(testUrgencyConfig()),
			req: laneRequests(
				compactorschedulerpb.JOB_TYPE_PLANNING,
				compactorschedulerpb.JOB_TYPE_COMPACTION,
				compactorschedulerpb.JOB_TYPE_PLANNING,
				compactorschedulerpb.JOB_TYPE_COMPACTION,
			),
			expected: []lane{planLane, compactionUrgentLane, compactionDeferLane},
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
	cfg := LanePolicyConfig{Urgency: testUrgencyConfig()}

	cfg.Policy = lanePolicySimple
	policy, err := newLanePolicy(cfg)
	require.NoError(t, err)
	require.IsType(t, &simpleLanePolicy{}, policy)

	cfg.Policy = lanePolicyUrgency
	policy, err = newLanePolicy(cfg)
	require.NoError(t, err)
	require.IsType(t, &urgencyLanePolicy{}, policy)

	cfg.Policy = "nope"
	_, err = newLanePolicy(cfg)
	require.EqualError(t, err, "unrecognized lane policy: nope")
}

func TestUrgencyConfig_Validate(t *testing.T) {
	tests := map[string]struct {
		duration time.Duration
		expected string
	}{
		"positive duration": {duration: 2 * time.Hour},
		"zero duration":     {duration: 0, expected: "test.max-span must be positive"},
		"negative duration": {duration: -time.Hour, expected: "test.max-span must be positive"},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			cfg := UrgencyConfig{MaxSpan: tc.duration}
			err := cfg.Validate("test")
			if tc.expected == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tc.expected)
			}
		})
	}
}
