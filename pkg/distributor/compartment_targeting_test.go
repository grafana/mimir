// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"testing"

	"github.com/grafana/dskit/ring"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/ingest"
)

func TestCompartmentForMatchers(t *testing.T) {
	const (
		numCompartments = 4
		userID          = "user-1"
	)

	withCompartments := &Distributor{
		compartmentRouter: ingest.NewCompartmentRouter(ingest.CompartmentsConfig{
			Enabled:         true,
			NumCompartments: numCompartments,
			TopicFormat:     "ingest-comp-<compartment-id>",
		}),
		partitionRings: make([]*ring.PartitionInstanceRing, numCompartments),
	}

	eq := func(name, value string) *labels.Matcher { return labels.MustNewMatcher(labels.MatchEqual, name, value) }
	re := func(name, value string) *labels.Matcher {
		return labels.MustNewMatcher(labels.MatchRegexp, name, value)
	}
	neq := func(name, value string) *labels.Matcher {
		return labels.MustNewMatcher(labels.MatchNotEqual, name, value)
	}

	t.Run("exact __name__ matcher pins to the routing compartment", func(t *testing.T) {
		want := withCompartments.compartmentRouter.CompartmentForMetric(userID, "my_metric")
		got := withCompartments.compartmentForMatchers(userID, []*labels.Matcher{eq(labels.MetricName, "my_metric"), eq("job", "x")})
		require.Equal(t, want, got)
		require.GreaterOrEqual(t, got, 0)
		require.Less(t, got, numCompartments)
	})

	t.Run("queries that cannot be pinned fan out (-1)", func(t *testing.T) {
		cases := map[string][]*labels.Matcher{
			"no matchers":          nil,
			"no __name__ matcher":  {eq("job", "x")},
			"regex __name__":       {re(labels.MetricName, "a|b")},
			"negative __name__":    {neq(labels.MetricName, "x")},
			"conflicting __name__": {eq(labels.MetricName, "a"), eq(labels.MetricName, "b")},
		}
		for name, matchers := range cases {
			t.Run(name, func(t *testing.T) {
				require.Equal(t, -1, withCompartments.compartmentForMatchers(userID, matchers))
			})
		}
	})

	t.Run("compartments disabled (nil router) fans out", func(t *testing.T) {
		noCompartments := &Distributor{partitionRings: make([]*ring.PartitionInstanceRing, 1)}
		require.Equal(t, -1, noCompartments.compartmentForMatchers(userID, []*labels.Matcher{eq(labels.MetricName, "my_metric")}))
	})

	t.Run("distinct metric names spread across compartments", func(t *testing.T) {
		seen := map[int]struct{}{}
		for _, name := range []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"} {
			seen[withCompartments.compartmentForMatchers(userID, []*labels.Matcher{eq(labels.MetricName, name)})] = struct{}{}
		}
		require.Greater(t, len(seen), 1, "metric names should not all map to the same compartment")
	})
}
