// SPDX-License-Identifier: AGPL-3.0-only

package costattribution

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/costattribution/testutils"
	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestSampleTracker_hasSameLabels(t *testing.T) {
	st := newTestManager().SampleTracker("user1")
	assert.True(t, st.hasSameLabels([]string{"team"}), "Expected cost attribution labels mismatch")
}

func TestSampleTracker_IncrementReceviedSamples(t *testing.T) {
	tManager := newTestManager()
	st := tManager.SampleTracker("user4")
	t.Run("One Single Series in Request", func(t *testing.T) {
		st.IncrementReceivedSamples(testutils.CreateRequest([]testutils.Series{{LabelValues: []string{"platform", "foo", "service", "dodo"}, SamplesCount: 3}}), time.Unix(10, 0))

		expectedMetrics := `
	# HELP cortex_distributor_received_attributed_samples_total The total number of samples that were received per attribution.
	# TYPE cortex_distributor_received_attributed_samples_total counter
	cortex_distributor_received_attributed_samples_total{platform="foo",tenant="user4",tracker="cost-attribution"} 3
	`
		assert.NoError(t, testutil.GatherAndCompare(tManager.reg, strings.NewReader(expectedMetrics), "cortex_distributor_received_attributed_samples_total"))
	})
	t.Run("Multiple Different Series in Request", func(t *testing.T) {
		st.IncrementReceivedSamples(testutils.CreateRequest([]testutils.Series{
			{LabelValues: []string{"platform", "foo", "service", "dodo"}, SamplesCount: 3},
			{LabelValues: []string{"platform", "bar", "service", "yoyo"}, SamplesCount: 5},
		}), time.Unix(20, 0))

		expectedMetrics := `
	# HELP cortex_distributor_received_attributed_samples_total The total number of samples that were received per attribution.
	# TYPE cortex_distributor_received_attributed_samples_total counter
	cortex_distributor_received_attributed_samples_total{platform="foo",tenant="user4",tracker="cost-attribution"} 6
	cortex_distributor_received_attributed_samples_total{platform="bar",tenant="user4",tracker="cost-attribution"} 5
	`
		assert.NoError(t, testutil.GatherAndCompare(tManager.reg, strings.NewReader(expectedMetrics), "cortex_distributor_received_attributed_samples_total"))
	})

	t.Run("Multiple Series in Request with Same Labels", func(t *testing.T) {
		st.IncrementReceivedSamples(testutils.CreateRequest([]testutils.Series{
			{LabelValues: []string{"platform", "foo", "service", "dodo"}, SamplesCount: 3},
			{LabelValues: []string{"platform", "foo", "service", "yoyo"}, SamplesCount: 5},
		}), time.Unix(30, 0))

		expectedMetrics := `
	# HELP cortex_distributor_received_attributed_samples_total The total number of samples that were received per attribution.
	# TYPE cortex_distributor_received_attributed_samples_total counter
	cortex_distributor_received_attributed_samples_total{platform="foo",tenant="user4",tracker="cost-attribution"} 14
	cortex_distributor_received_attributed_samples_total{platform="bar",tenant="user4",tracker="cost-attribution"} 5
	`
		assert.NoError(t, testutil.GatherAndCompare(tManager.reg, strings.NewReader(expectedMetrics), "cortex_distributor_received_attributed_samples_total"))
	})
}

func TestSampleTracker_IncrementDiscardedSamples(t *testing.T) {
	st := newTestManager().SampleTracker("user3")
	lbls1 := []mimirpb.LabelAdapter{{Name: "department", Value: "foo"}, {Name: "service", Value: "bar"}}
	lbls2 := []mimirpb.LabelAdapter{{Name: "department", Value: "bar"}, {Name: "service", Value: "baz"}}
	lbls3 := []mimirpb.LabelAdapter{{Name: "department", Value: "baz"}, {Name: "service", Value: "foo"}}

	st.IncrementDiscardedSamples(lbls1, 1, "", time.Unix(1, 0))
	assert.True(t, st.overflowSince.IsZero(), "First observation, should not overflow")
	assert.Equal(t, 1, len(st.observed))

	st.IncrementDiscardedSamples(lbls2, 1, "", time.Unix(2, 0))
	assert.True(t, st.overflowSince.IsZero(), "Second observation, should not overflow")
	assert.Equal(t, 2, len(st.observed))

	st.IncrementDiscardedSamples(lbls3, 1, "", time.Unix(3, 0))
	assert.Equal(t, time.Unix(3, 0), st.overflowSince, "Third observation, should overflow")
	assert.Equal(t, 2, len(st.observed))

	st.IncrementDiscardedSamples(lbls3, 1, "", time.Unix(4, 0))
	assert.Equal(t, time.Unix(3, 0), st.overflowSince, "Fourth observation, should stay overflow")
	assert.Equal(t, 2, len(st.observed))
}

func TestSampleTracker_inactiveObservations(t *testing.T) {
	// Setup the test environment: create a st for user1 with a "team" label and max cardinality of 5.
	st := newTestManager().SampleTracker("user1")

	// Create two observations with different last update timestamps.
	observations := [][]mimirpb.LabelAdapter{
		{{Name: "team", Value: "foo"}},
		{{Name: "team", Value: "bar"}},
		{{Name: "team", Value: "baz"}},
	}

	// Simulate samples discarded with different timestamps.
	st.IncrementDiscardedSamples(observations[0], 1, "invalid-metrics-name", time.Unix(1, 0))
	st.IncrementDiscardedSamples(observations[1], 2, "out-of-window-sample", time.Unix(12, 0))
	st.IncrementDiscardedSamples(observations[2], 3, "invalid-metrics-name", time.Unix(20, 0))

	// Ensure that two observations were successfully added to the tracker.
	require.Len(t, st.observed, 3)

	// Purge observations that haven't been updated in the last 10 seconds.
	st.cleanupInactiveObservations(time.Unix(0, 0))
	require.Len(t, st.observed, 3)

	st.cleanupInactiveObservations(time.Unix(10, 0))
	assert.Len(t, st.observed, 2)

	st.cleanupInactiveObservations(time.Unix(15, 0))
	assert.Len(t, st.observed, 1)

	st.cleanupInactiveObservations(time.Unix(25, 0))
	assert.Len(t, st.observed, 0)
}

func TestSampleTracker_Concurrency(t *testing.T) {
	m := newTestManager()
	st := m.SampleTracker("user1")

	var wg sync.WaitGroup
	var i int64
	for i = 0; i < 100; i++ {
		wg.Add(1)
		go func(i int64) {
			defer wg.Done()
			st.IncrementReceivedSamples(testutils.CreateRequest([]testutils.Series{{LabelValues: []string{"team", string(rune('A' + (i % 26)))}, SamplesCount: 1}}), time.Unix(i, 0))
			st.IncrementDiscardedSamples([]mimirpb.LabelAdapter{{Name: "team", Value: string(rune('A' + (i % 26)))}}, 1, "sample-out-of-order", time.Unix(i, 0))
		}(i)
	}
	wg.Wait()

	// Verify no data races or inconsistencies, since after 5 all the samples will be counted into the overflow, so the count should be 95
	assert.True(t, len(st.observed) > 0, "Observed set should not be empty after concurrent updates")
	assert.LessOrEqual(t, len(st.observed), st.maxCardinality, "Observed count should not exceed max cardinality")
	assert.NotEqual(t, st.overflowSince.IsZero(), "Expected state to be Overflow")

	expectedMetrics := `
	# HELP cortex_discarded_attributed_samples_total The total number of samples that were discarded per attribution.
    # TYPE cortex_discarded_attributed_samples_total counter
    cortex_discarded_attributed_samples_total{reason="__overflow__",team="__overflow__",tenant="user1",tracker="cost-attribution"} 95
    # HELP cortex_distributor_received_attributed_samples_total The total number of samples that were received per attribution.
    # TYPE cortex_distributor_received_attributed_samples_total counter
	cortex_distributor_received_attributed_samples_total{team="__overflow__",tenant="user1",tracker="cost-attribution"} 95

`
	assert.NoError(t, testutil.GatherAndCompare(m.reg, strings.NewReader(expectedMetrics), "cortex_distributor_received_attributed_samples_total", "cortex_discarded_attributed_samples_total"))
}
