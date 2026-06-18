// SPDX-License-Identifier: AGPL-3.0-only

package compartments

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestRouter(numCompartments int) *Router {
	return NewRouter(numCompartments, "mimir-rc-<read-compartment-id>")
}

func TestRouter_TopicForCompartment(t *testing.T) {
	r := newTestRouter(3)
	require.Equal(t, 3, r.NumCompartments())
	assert.Equal(t, "mimir-rc-0", r.TopicForCompartment(0))
	assert.Equal(t, "mimir-rc-1", r.TopicForCompartment(1))
	assert.Equal(t, "mimir-rc-2", r.TopicForCompartment(2))
}

func TestRouter_CompartmentForMetric(t *testing.T) {
	r := newTestRouter(4)

	t.Run("deterministic", func(t *testing.T) {
		first := r.CompartmentForMetric("user-1", "metric_name")
		for i := 0; i < 100; i++ {
			assert.Equal(t, first, r.CompartmentForMetric("user-1", "metric_name"))
		}
	})

	t.Run("always within bounds", func(t *testing.T) {
		for i := 0; i < 1000; i++ {
			id := r.CompartmentForMetric("user-1", "metric_"+strconv.Itoa(i))
			assert.GreaterOrEqual(t, id, 0)
			assert.Less(t, id, r.NumCompartments())
		}
	})

	t.Run("consistent with TopicForMetric", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			name := "metric_" + strconv.Itoa(i)
			assert.Equal(t, r.TopicForCompartment(r.CompartmentForMetric("user-1", name)), r.TopicForMetric("user-1", name))
		}
	})
}

func TestRouter_CompartmentsForMatchers(t *testing.T) {
	const (
		numCompartments = 4
		userID          = "user-1"
	)
	r := newTestRouter(numCompartments)

	eq := func(name, value string) *labels.Matcher { return labels.MustNewMatcher(labels.MatchEqual, name, value) }
	re := func(name, value string) *labels.Matcher {
		return labels.MustNewMatcher(labels.MatchRegexp, name, value)
	}
	neq := func(name, value string) *labels.Matcher {
		return labels.MustNewMatcher(labels.MatchNotEqual, name, value)
	}

	t.Run("exact __name__ matcher pins to the routing compartment", func(t *testing.T) {
		want := r.CompartmentForMetric(userID, "my_metric")
		got := r.CompartmentsForMatchers(userID, []*labels.Matcher{eq(model.MetricNameLabel, "my_metric"), eq("job", "x")})
		assert.Equal(t, []int{want}, got)
	})

	t.Run("enumerable regexp __name__ targets the union of the matched names' compartments", func(t *testing.T) {
		names := []string{"metric_a", "metric_b", "metric_c", "metric_d", "metric_e"}
		want := map[int]struct{}{}
		for _, n := range names {
			want[r.CompartmentForMetric(userID, n)] = struct{}{}
		}

		got := r.CompartmentsForMatchers(userID, []*labels.Matcher{re(model.MetricNameLabel, strings.Join(names, "|"))})
		assert.Len(t, got, len(want), "should return one entry per distinct compartment")
		gotSet := map[int]struct{}{}
		for _, c := range got {
			gotSet[c] = struct{}{}
		}
		assert.Equal(t, want, gotSet)
	})

	t.Run("exact matcher wins over a regexp set", func(t *testing.T) {
		want := r.CompartmentForMetric(userID, "metric_a")
		got := r.CompartmentsForMatchers(userID, []*labels.Matcher{eq(model.MetricNameLabel, "metric_a"), re(model.MetricNameLabel, "metric_a|metric_b|metric_c")})
		assert.Equal(t, []int{want}, got)
	})

	t.Run("conflicting exact names pin to the first one (the query matches no series anyway)", func(t *testing.T) {
		want := r.CompartmentForMetric(userID, "metric_a")
		got := r.CompartmentsForMatchers(userID, []*labels.Matcher{eq(model.MetricNameLabel, "metric_a"), eq(model.MetricNameLabel, "metric_b")})
		assert.Equal(t, []int{want}, got)
	})

	t.Run("queries that cannot be restricted return all compartments", func(t *testing.T) {
		allCompartments := []int{0, 1, 2, 3}
		cases := map[string][]*labels.Matcher{
			"no matchers":         nil,
			"no __name__ matcher": {eq("job", "x")},
			"unbounded regex":     {re(model.MetricNameLabel, "foo.*")},
			"negative __name__":   {neq(model.MetricNameLabel, "x")},
		}
		for name, matchers := range cases {
			t.Run(name, func(t *testing.T) {
				assert.Equal(t, allCompartments, r.CompartmentsForMatchers(userID, matchers))
			})
		}
	})
}

func TestRouter_TopicForMetric(t *testing.T) {
	t.Run("the same metric may map to different compartments for different tenants", func(t *testing.T) {
		r := newTestRouter(16)
		differ := false
		for i := 0; i < 100; i++ {
			a := r.CompartmentForMetric("user-a-"+strconv.Itoa(i), "the_metric")
			b := r.CompartmentForMetric("user-b-"+strconv.Itoa(i), "the_metric")
			if a != b {
				differ = true
				break
			}
		}
		assert.True(t, differ, "expected the tenant to affect the compartment assignment")
	})

	t.Run("all compartments are used given enough distinct metrics", func(t *testing.T) {
		const numCompartments = 8
		r := newTestRouter(numCompartments)
		seen := make(map[int]struct{})
		for i := 0; i < 10000 && len(seen) < numCompartments; i++ {
			seen[r.CompartmentForMetric("user-1", fmt.Sprintf("metric_%d", i))] = struct{}{}
		}
		assert.Len(t, seen, numCompartments)
	})
}
