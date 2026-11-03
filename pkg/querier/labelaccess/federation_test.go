// SPDX-License-Identifier: AGPL-3.0-only

package labelaccess

import (
	"context"
	"sort"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/require"

	shared "github.com/grafana/mimir/pkg/labelaccess"
	"github.com/grafana/mimir/pkg/querier"
	"github.com/grafana/mimir/pkg/querier/tenantfederation"
)

// TestLabelAccessQueryableWithTenantFederation ensures that label access policies are enforced
// per tenant when the label access queryable is wrapped by the tenant federation merge queryable,
// which is the composition set up by Mimir's modules when both features are enabled.
func TestLabelAccessQueryableWithTenantFederation(t *testing.T) {
	const (
		tenantA = "tenant-a"
		tenantB = "tenant-b"
	)

	upstream := &tenantAwareQueryable{seriesByTenant: map[string][]labels.Labels{
		tenantA: {
			labels.FromStrings("__name__", "metric", "service", "authorization-service"),
			labels.FromStrings("__name__", "metric", "service", "checkout-service"),
		},
		tenantB: {
			labels.FromStrings("__name__", "metric", "service", "authorization-service"),
			labels.FromStrings("__name__", "metric", "service", "checkout-service"),
		},
	}}

	// Only tenant-a is restricted by a policy; tenant-b has no entry and must be unrestricted.
	policySet := shared.LabelPolicySet{
		tenantA: {newSingleLabelPolicy(labels.MatchEqual, "service", "authorization-service")},
	}

	// The label access queryable must be upstream of the merge queryable: the merge queryable
	// is the one injecting each tenant ID into the context of the sub-queries it issues.
	federated := querier.NewSampleAndChunkQueryable(tenantfederation.NewQueryable(
		WrapQueryable(querier.NewSampleAndChunkQueryable(upstream), log.NewNopLogger()),
		true,
		10,
		prometheus.NewPedanticRegistry(),
		log.NewNopLogger(),
	))

	runQuery := func(t *testing.T, orgID string, policies shared.LabelPolicySet) []string {
		t.Helper()

		ctx := shared.InjectLabelMatchersContext(user.InjectOrgID(context.Background(), orgID), policies)
		q, err := federated.Querier(0, 10)
		require.NoError(t, err)
		require.NoError(t, q.Close())

		ss := q.Select(ctx, true, nil, labels.MustNewMatcher(labels.MatchEqual, "__name__", "metric"))
		require.NoError(t, ss.Err())

		var got []string
		for ss.Next() {
			got = append(got, ss.At().Labels().String())
		}
		require.NoError(t, ss.Err())
		sort.Strings(got)

		return got
	}

	t.Run("federated query enforces the policy of each tenant", func(t *testing.T) {
		got := runQuery(t, tenantA+"|"+tenantB, policySet)

		// tenant-a only contributes the series allowed by its policy, while tenant-b
		// contributes all of its series. The merge queryable adds the __tenant_id__ label.
		require.Equal(t, []string{
			labels.FromStrings("__name__", "metric", "service", "authorization-service", "__tenant_id__", tenantA).String(),
			labels.FromStrings("__name__", "metric", "service", "authorization-service", "__tenant_id__", tenantB).String(),
			labels.FromStrings("__name__", "metric", "service", "checkout-service", "__tenant_id__", tenantB).String(),
		}, got)
	})

	t.Run("single tenant query enforces the policy", func(t *testing.T) {
		// The merge queryable is bypassed for single tenant queries, so no __tenant_id__ label is added.
		require.Equal(t, []string{
			labels.FromStrings("__name__", "metric", "service", "authorization-service").String(),
		}, runQuery(t, tenantA, policySet))
	})

	t.Run("tenant without policy is unrestricted", func(t *testing.T) {
		require.Equal(t, []string{
			labels.FromStrings("__name__", "metric", "service", "authorization-service").String(),
			labels.FromStrings("__name__", "metric", "service", "checkout-service").String(),
		}, runQuery(t, tenantB, policySet))
	})

	t.Run("multiple policies for a tenant are unioned", func(t *testing.T) {
		policies := shared.LabelPolicySet{
			tenantA: {
				newSingleLabelPolicy(labels.MatchEqual, "service", "authorization-service"),
				newSingleLabelPolicy(labels.MatchEqual, "service", "checkout-service"),
			},
		}

		require.Equal(t, []string{
			labels.FromStrings("__name__", "metric", "service", "authorization-service", "__tenant_id__", tenantA).String(),
			labels.FromStrings("__name__", "metric", "service", "checkout-service", "__tenant_id__", tenantA).String(),
			labels.FromStrings("__name__", "metric", "service", "authorization-service", "__tenant_id__", tenantB).String(),
			labels.FromStrings("__name__", "metric", "service", "checkout-service", "__tenant_id__", tenantB).String(),
		}, runQuery(t, tenantA+"|"+tenantB, policies))
	})
}

// tenantAwareQueryable is a storage.Queryable that serves different series depending on the
// tenant ID in the query context, like the ingester and store-gateway queryables do.
type tenantAwareQueryable struct {
	seriesByTenant map[string][]labels.Labels
}

func (q *tenantAwareQueryable) Querier(_, _ int64) (storage.Querier, error) {
	return &tenantAwareQuerier{upstream: q}, nil
}

type tenantAwareQuerier struct {
	upstream *tenantAwareQueryable
}

func (q *tenantAwareQuerier) Select(ctx context.Context, _ bool, _ *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	tenantID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}

	series := make([]storage.Series, 0, len(q.upstream.seriesByTenant[tenantID]))
	for _, lbls := range q.upstream.seriesByTenant[tenantID] {
		matched := true
		for _, m := range matchers {
			if !m.Matches(lbls.Get(m.Name)) {
				matched = false
				break
			}
		}
		if matched {
			series = append(series, storage.NewListSeries(lbls, nil))
		}
	}

	return newListSeriesSet(series)
}

func (q *tenantAwareQuerier) LabelValues(context.Context, string, *storage.LabelHints, ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}

func (q *tenantAwareQuerier) LabelNames(context.Context, *storage.LabelHints, ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}

func (q *tenantAwareQuerier) Close() error {
	return nil
}

// listSeriesSet is a storage.SeriesSet backed by a list of series.
type listSeriesSet struct {
	idx    int
	series []storage.Series
}

func newListSeriesSet(series []storage.Series) *listSeriesSet {
	return &listSeriesSet{idx: -1, series: series}
}

func (s *listSeriesSet) Next() bool {
	s.idx++
	return s.idx < len(s.series)
}

func (s *listSeriesSet) At() storage.Series {
	return s.series[s.idx]
}

func (s *listSeriesSet) Err() error {
	return nil
}

func (s *listSeriesSet) Warnings() annotations.Annotations {
	return nil
}
