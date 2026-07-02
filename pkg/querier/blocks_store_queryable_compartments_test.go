// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/user"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/compartments"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/storage/bucket"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util/limiter"
)

const compartmentsTestTenant = "user-1"

func TestBlocksStoreQuerier_Compartments_LabelNames(t *testing.T) {
	const minT, maxT = int64(10), int64(20)
	ctx := user.InjectOrgID(context.Background(), compartmentsTestTenant)

	t.Run("should route the query to the owning compartment when pinned to a metric name", func(t *testing.T) {
		router := compartments.NewRouter(2)

		for compartment := 0; compartment < 2; compartment++ {
			t.Run(fmt.Sprintf("compartment %d", compartment), func(t *testing.T) {
				reg := prometheus.NewPedanticRegistry()
				finders := []*blocksFinderMock{{}, {}}
				mockNoBlocks(finders[compartment], minT, maxT)

				stores := []BlocksStoreSet{&blocksStoreSetMock{}, &blocksStoreSetMock{}}
				q := newCompartmentsTestQuerier(finders, stores, reg, minT, maxT)

				matcher := labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, metricNameForCompartment(t, router, compartment))
				_, _, err := q.LabelNames(ctx, nil, matcher)
				require.NoError(t, err)

				// Only the owning compartment's finder is queried.
				finders[compartment].AssertCalled(t, "GetBlocks", mock.Anything, compartmentsTestTenant, minT, maxT)
				finders[1-compartment].AssertNotCalled(t, "GetBlocks", mock.Anything, mock.Anything, mock.Anything, mock.Anything)

				assertCompartmentsHit(t, reg, 1)
				assertStoreGatewayInstancesHit(t, reg, 1, 0)
			})
		}
	})

	t.Run("should fan out to all compartments without a metric name matcher", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		finders := []*blocksFinderMock{{}, {}}
		mockNoBlocks(finders[0], minT, maxT)
		mockNoBlocks(finders[1], minT, maxT)

		stores := []BlocksStoreSet{&blocksStoreSetMock{}, &blocksStoreSetMock{}}
		q := newCompartmentsTestQuerier(finders, stores, reg, minT, maxT)

		_, _, err := q.LabelNames(ctx, nil, labels.MustNewMatcher(labels.MatchEqual, "job", "test"))
		require.NoError(t, err)

		finders[0].AssertCalled(t, "GetBlocks", mock.Anything, compartmentsTestTenant, minT, maxT)
		finders[1].AssertCalled(t, "GetBlocks", mock.Anything, compartmentsTestTenant, minT, maxT)

		assertCompartmentsHit(t, reg, 2)
		assertStoreGatewayInstancesHit(t, reg, 1, 0)
	})

	t.Run("should restrict the compartments queried to the matched metric names", func(t *testing.T) {
		router := compartments.NewRouter(2)
		nameC0 := metricNameForCompartment(t, router, 0)
		nameC1 := metricNameForCompartment(t, router, 1)

		t.Run("names in different compartments query both", func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			finders := []*blocksFinderMock{{}, {}}
			mockNoBlocks(finders[0], minT, maxT)
			mockNoBlocks(finders[1], minT, maxT)

			stores := []BlocksStoreSet{&blocksStoreSetMock{}, &blocksStoreSetMock{}}
			q := newCompartmentsTestQuerier(finders, stores, reg, minT, maxT)

			matcher := labels.MustNewMatcher(labels.MatchRegexp, model.MetricNameLabel, nameC0+"|"+nameC1)
			_, _, err := q.LabelNames(ctx, nil, matcher)
			require.NoError(t, err)

			finders[0].AssertCalled(t, "GetBlocks", mock.Anything, compartmentsTestTenant, minT, maxT)
			finders[1].AssertCalled(t, "GetBlocks", mock.Anything, compartmentsTestTenant, minT, maxT)
			assertCompartmentsHit(t, reg, 2)
			assertStoreGatewayInstancesHit(t, reg, 1, 0)
		})

		t.Run("names in the same compartment query only that compartment", func(t *testing.T) {
			// Find a second name mapping to compartment 0, distinct from nameC0.
			var nameC0b string
			for i := 0; i < 100000 && nameC0b == ""; i++ {
				n := fmt.Sprintf("other_%d", i)
				if router.CompartmentForMetric(compartmentsTestTenant, n) == 0 {
					nameC0b = n
				}
			}
			require.NotEmpty(t, nameC0b)

			reg := prometheus.NewPedanticRegistry()
			finders := []*blocksFinderMock{{}, {}}
			mockNoBlocks(finders[0], minT, maxT)

			stores := []BlocksStoreSet{&blocksStoreSetMock{}, &blocksStoreSetMock{}}
			q := newCompartmentsTestQuerier(finders, stores, reg, minT, maxT)

			matcher := labels.MustNewMatcher(labels.MatchRegexp, model.MetricNameLabel, nameC0+"|"+nameC0b)
			_, _, err := q.LabelNames(ctx, nil, matcher)
			require.NoError(t, err)

			finders[0].AssertCalled(t, "GetBlocks", mock.Anything, compartmentsTestTenant, minT, maxT)
			finders[1].AssertNotCalled(t, "GetBlocks", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
			assertCompartmentsHit(t, reg, 1)
			assertStoreGatewayInstancesHit(t, reg, 1, 0)
		})
	})

	t.Run("should merge label names across compartments", func(t *testing.T) {
		block0 := ulid.MustNew(1, nil)
		block1 := ulid.MustNew(2, nil)

		finders := []*blocksFinderMock{{}, {}}
		finders[0].On("GetBlocks", mock.Anything, compartmentsTestTenant, minT, maxT).Return(bucketindex.Blocks{{ID: block0}}, &bucketindex.Metadata{}, nil)
		finders[1].On("GetBlocks", mock.Anything, compartmentsTestTenant, minT, maxT).Return(bucketindex.Blocks{{ID: block1}}, &bucketindex.Metadata{}, nil)

		// "shared_label" is returned by both compartments, so the merge must deduplicate it. Names are sorted
		// within each response, as the store-gateways return them.
		stores := []BlocksStoreSet{
			&blocksStoreSetMock{mockedResponses: []interface{}{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{
						remoteAddr: "1.1.1.1",
						mockedLabelNamesResponse: &storepb.LabelNamesResponse{
							Names:         []string{"label_from_compartment_0", "shared_label"},
							ResponseHints: mockNamesResponseHints(block0),
						},
					}: {block0},
				},
			}},
			&blocksStoreSetMock{mockedResponses: []interface{}{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{
						remoteAddr: "2.2.2.2",
						mockedLabelNamesResponse: &storepb.LabelNamesResponse{
							Names:         []string{"label_from_compartment_1", "shared_label"},
							ResponseHints: mockNamesResponseHints(block1),
						},
					}: {block1},
				},
			}},
		}

		reg := prometheus.NewPedanticRegistry()
		q := newCompartmentsTestQuerier(finders, stores, reg, minT, maxT)

		// No metric-name matcher fans out to both compartments; their label names are merged and deduplicated.
		names, _, err := q.LabelNames(ctx, nil)
		require.NoError(t, err)
		require.Equal(t, []string{"label_from_compartment_0", "label_from_compartment_1", "shared_label"}, names)
		assertCompartmentsHit(t, reg, 2)
		assertStoreGatewayInstancesHit(t, reg, 1, 2)
	})

	t.Run("should query a single compartment when compartments are disabled", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		finder := &blocksFinderMock{}
		mockNoBlocks(finder, minT, maxT)

		// router nil mirrors the compartments-disabled path: a single compartment and no hit metric.
		q := &blocksStoreQuerier{
			minT:               minT,
			maxT:               maxT,
			compartments:       []blocksStoreCompartment{{finder: finder, stores: &blocksStoreSetMock{}}},
			dynamicReplication: newDynamicReplication(),
			consistency:        NewBlocksConsistency(0, nil),
			logger:             log.NewNopLogger(),
			metrics:            newBlocksStoreQueryableMetrics(compartments.Config{}, reg),
			limits:             &blocksStoreLimitsMock{},
		}

		_, _, err := q.LabelNames(ctx, nil, labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "anything"))
		require.NoError(t, err)

		finder.AssertCalled(t, "GetBlocks", mock.Anything, compartmentsTestTenant, minT, maxT)
		// The hit metric is not registered when compartments are disabled.
		require.Equal(t, 0, testutil.CollectAndCount(reg, "cortex_querier_compartments_hit_per_query"))
		assertStoreGatewayInstancesHit(t, reg, 1, 0)
	})
}

func TestBlocksStoreQuerier_Compartments_LabelValues(t *testing.T) {
	const minT, maxT = int64(10), int64(20)
	ctx := user.InjectOrgID(context.Background(), compartmentsTestTenant)

	t.Run("should route the query to the owning compartment when pinned to a metric name", func(t *testing.T) {
		router := compartments.NewRouter(2)

		for compartment := 0; compartment < 2; compartment++ {
			t.Run(fmt.Sprintf("compartment %d", compartment), func(t *testing.T) {
				reg := prometheus.NewPedanticRegistry()
				finders := []*blocksFinderMock{{}, {}}
				mockNoBlocks(finders[compartment], minT, maxT)

				stores := []BlocksStoreSet{&blocksStoreSetMock{}, &blocksStoreSetMock{}}
				q := newCompartmentsTestQuerier(finders, stores, reg, minT, maxT)

				matcher := labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, metricNameForCompartment(t, router, compartment))
				_, _, err := q.LabelValues(ctx, "some_label", nil, matcher)
				require.NoError(t, err)

				// Only the owning compartment's finder is queried.
				finders[compartment].AssertCalled(t, "GetBlocks", mock.Anything, compartmentsTestTenant, minT, maxT)
				finders[1-compartment].AssertNotCalled(t, "GetBlocks", mock.Anything, mock.Anything, mock.Anything, mock.Anything)

				assertCompartmentsHit(t, reg, 1)
				assertStoreGatewayInstancesHit(t, reg, 1, 0)
			})
		}
	})

	t.Run("should fan out to all compartments without a metric name matcher", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		finders := []*blocksFinderMock{{}, {}}
		mockNoBlocks(finders[0], minT, maxT)
		mockNoBlocks(finders[1], minT, maxT)

		stores := []BlocksStoreSet{&blocksStoreSetMock{}, &blocksStoreSetMock{}}
		q := newCompartmentsTestQuerier(finders, stores, reg, minT, maxT)

		_, _, err := q.LabelValues(ctx, "some_label", nil, labels.MustNewMatcher(labels.MatchEqual, "job", "test"))
		require.NoError(t, err)

		finders[0].AssertCalled(t, "GetBlocks", mock.Anything, compartmentsTestTenant, minT, maxT)
		finders[1].AssertCalled(t, "GetBlocks", mock.Anything, compartmentsTestTenant, minT, maxT)

		assertCompartmentsHit(t, reg, 2)
		assertStoreGatewayInstancesHit(t, reg, 1, 0)
	})

	t.Run("should restrict the compartments queried to the matched metric names", func(t *testing.T) {
		router := compartments.NewRouter(2)
		nameC0 := metricNameForCompartment(t, router, 0)
		nameC1 := metricNameForCompartment(t, router, 1)

		t.Run("names in different compartments query both", func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			finders := []*blocksFinderMock{{}, {}}
			mockNoBlocks(finders[0], minT, maxT)
			mockNoBlocks(finders[1], minT, maxT)

			stores := []BlocksStoreSet{&blocksStoreSetMock{}, &blocksStoreSetMock{}}
			q := newCompartmentsTestQuerier(finders, stores, reg, minT, maxT)

			matcher := labels.MustNewMatcher(labels.MatchRegexp, model.MetricNameLabel, nameC0+"|"+nameC1)
			_, _, err := q.LabelValues(ctx, "some_label", nil, matcher)
			require.NoError(t, err)

			finders[0].AssertCalled(t, "GetBlocks", mock.Anything, compartmentsTestTenant, minT, maxT)
			finders[1].AssertCalled(t, "GetBlocks", mock.Anything, compartmentsTestTenant, minT, maxT)
			assertCompartmentsHit(t, reg, 2)
			assertStoreGatewayInstancesHit(t, reg, 1, 0)
		})

		t.Run("names in the same compartment query only that compartment", func(t *testing.T) {
			// Find a second name mapping to compartment 0, distinct from nameC0.
			var nameC0b string
			for i := 0; i < 100000 && nameC0b == ""; i++ {
				n := fmt.Sprintf("other_%d", i)
				if router.CompartmentForMetric(compartmentsTestTenant, n) == 0 {
					nameC0b = n
				}
			}
			require.NotEmpty(t, nameC0b)

			reg := prometheus.NewPedanticRegistry()
			finders := []*blocksFinderMock{{}, {}}
			mockNoBlocks(finders[0], minT, maxT)

			stores := []BlocksStoreSet{&blocksStoreSetMock{}, &blocksStoreSetMock{}}
			q := newCompartmentsTestQuerier(finders, stores, reg, minT, maxT)

			matcher := labels.MustNewMatcher(labels.MatchRegexp, model.MetricNameLabel, nameC0+"|"+nameC0b)
			_, _, err := q.LabelValues(ctx, "some_label", nil, matcher)
			require.NoError(t, err)

			finders[0].AssertCalled(t, "GetBlocks", mock.Anything, compartmentsTestTenant, minT, maxT)
			finders[1].AssertNotCalled(t, "GetBlocks", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
			assertCompartmentsHit(t, reg, 1)
			assertStoreGatewayInstancesHit(t, reg, 1, 0)
		})
	})

	t.Run("should merge label values across compartments", func(t *testing.T) {
		block0 := ulid.MustNew(1, nil)
		block1 := ulid.MustNew(2, nil)

		finders := []*blocksFinderMock{{}, {}}
		finders[0].On("GetBlocks", mock.Anything, compartmentsTestTenant, minT, maxT).Return(bucketindex.Blocks{{ID: block0}}, &bucketindex.Metadata{}, nil)
		finders[1].On("GetBlocks", mock.Anything, compartmentsTestTenant, minT, maxT).Return(bucketindex.Blocks{{ID: block1}}, &bucketindex.Metadata{}, nil)

		// "shared_value" is returned by both compartments, so the merge must deduplicate it. Values are sorted
		// within each response, as the store-gateways return them.
		stores := []BlocksStoreSet{
			&blocksStoreSetMock{mockedResponses: []interface{}{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{
						remoteAddr: "1.1.1.1",
						mockedLabelValuesResponse: &storepb.LabelValuesResponse{
							Values:        []string{"shared_value", "value_from_compartment_0"},
							ResponseHints: mockValuesResponseHints(block0),
						},
					}: {block0},
				},
			}},
			&blocksStoreSetMock{mockedResponses: []interface{}{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{
						remoteAddr: "2.2.2.2",
						mockedLabelValuesResponse: &storepb.LabelValuesResponse{
							Values:        []string{"shared_value", "value_from_compartment_1"},
							ResponseHints: mockValuesResponseHints(block1),
						},
					}: {block1},
				},
			}},
		}

		reg := prometheus.NewPedanticRegistry()
		q := newCompartmentsTestQuerier(finders, stores, reg, minT, maxT)

		values, _, err := q.LabelValues(ctx, "some_label", nil)
		require.NoError(t, err)
		require.Equal(t, []string{"shared_value", "value_from_compartment_0", "value_from_compartment_1"}, values)
		assertCompartmentsHit(t, reg, 2)
		assertStoreGatewayInstancesHit(t, reg, 1, 2)
	})

	t.Run("should query a single compartment when compartments are disabled", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		finder := &blocksFinderMock{}
		mockNoBlocks(finder, minT, maxT)

		// router nil mirrors the compartments-disabled path: a single compartment and no hit metric.
		q := &blocksStoreQuerier{
			minT:               minT,
			maxT:               maxT,
			compartments:       []blocksStoreCompartment{{finder: finder, stores: &blocksStoreSetMock{}}},
			dynamicReplication: newDynamicReplication(),
			consistency:        NewBlocksConsistency(0, nil),
			logger:             log.NewNopLogger(),
			metrics:            newBlocksStoreQueryableMetrics(compartments.Config{}, reg),
			limits:             &blocksStoreLimitsMock{},
		}

		_, _, err := q.LabelValues(ctx, "some_label", nil, labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "anything"))
		require.NoError(t, err)

		finder.AssertCalled(t, "GetBlocks", mock.Anything, compartmentsTestTenant, minT, maxT)
		// The hit metric is not registered when compartments are disabled.
		require.Equal(t, 0, testutil.CollectAndCount(reg, "cortex_querier_compartments_hit_per_query"))
		assertStoreGatewayInstancesHit(t, reg, 1, 0)
	})
}

func TestBlocksStoreQuerier_Compartments_Select(t *testing.T) {
	const minT, maxT = int64(10), int64(20)

	t.Run("should merge series across compartments", func(t *testing.T) {
		ctx := user.InjectOrgID(context.Background(), compartmentsTestTenant)
		ctx = limiter.AddQueryLimiterToContext(ctx, limiter.NewQueryLimiter(0, 0, 0, 0, nil))
		ctx = limiter.ContextWithNewUnlimitedMemoryConsumptionTracker(ctx)
		ctx = limiter.ContextWithNewSeriesLabelsDeduplicator(ctx, limiter.NewSeriesDeduplicatorMetrics(prometheus.NewPedanticRegistry()))
		_, ctx = stats.ContextWithEmptyStats(ctx)

		block0 := ulid.MustNew(1, nil)
		block1 := ulid.MustNew(2, nil)
		seriesA := labels.FromStrings(model.MetricNameLabel, "metric_a")
		seriesB := labels.FromStrings(model.MetricNameLabel, "metric_b")

		// Non-testify finders: both compartments query concurrently and a testify finder would race on the
		// context it stringifies for argument matching (see stubBlocksFinder).
		finders := []BlocksFinder{
			&stubBlocksFinder{blocks: bucketindex.Blocks{{ID: block0}}, meta: &bucketindex.Metadata{}},
			&stubBlocksFinder{blocks: bucketindex.Blocks{{ID: block1}}, meta: &bucketindex.Metadata{}},
		}

		stores := []BlocksStoreSet{
			&blocksStoreSetMock{mockedResponses: []interface{}{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesResponses: newSeriesResponseBuilder().
						addValue(seriesA, minT, 1).
						addBlocks(block0).
						build(),
					}: {block0},
				},
			}},
			&blocksStoreSetMock{mockedResponses: []interface{}{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "2.2.2.2", mockedSeriesResponses: newSeriesResponseBuilder().
						addValue(seriesB, minT, 2).
						addBlocks(block1).
						build(),
					}: {block1},
				},
			}},
		}

		reg := prometheus.NewPedanticRegistry()
		q := newCompartmentsTestQuerierWithFinders(finders, stores, reg, minT, maxT)

		// A matcher that doesn't pin the metric name fans out to both compartments; their series are merged.
		set := q.Select(ctx, true, &storage.SelectHints{Start: minT, End: maxT}, labels.MustNewMatcher(labels.MatchEqual, "job", "test"))
		require.NoError(t, set.Err())

		var got []string
		for set.Next() {
			got = append(got, set.At().Labels().String())
		}
		require.NoError(t, set.Err())
		require.Equal(t, []string{seriesA.String(), seriesB.String()}, got)
		assertCompartmentsHit(t, reg, 2)
		// The per-query store-gateway histogram is observed once for the whole fanned-out query, summed
		// across the two compartments (one store-gateway each), not once per compartment.
		assertStoreGatewayInstancesHit(t, reg, 1, 2)
	})

	t.Run("should stop on the first compartment error", func(t *testing.T) {
		ctx := user.InjectOrgID(context.Background(), compartmentsTestTenant)

		// One compartment's finder fails; the fan-out query must fail fast and return that error. The sibling
		// compartment may or may not have been reached before the shared context was cancelled.
		newFinders := func() []*blocksFinderMock {
			finders := []*blocksFinderMock{{}, {}}
			finders[0].On("GetBlocks", mock.Anything, compartmentsTestTenant, minT, maxT).Return(bucketindex.Blocks(nil), (*bucketindex.Metadata)(nil), errors.New("finder failed"))
			finders[1].On("GetBlocks", mock.Anything, compartmentsTestTenant, minT, maxT).Return(bucketindex.Blocks(nil), &bucketindex.Metadata{}, nil).Maybe()
			return finders
		}
		stores := func() []BlocksStoreSet { return []BlocksStoreSet{&blocksStoreSetMock{}, &blocksStoreSetMock{}} }
		fanOut := labels.MustNewMatcher(labels.MatchEqual, "job", "test")

		t.Run("via LabelNames", func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			q := newCompartmentsTestQuerier(newFinders(), stores(), reg, minT, maxT)
			_, _, err := q.LabelNames(ctx, nil, fanOut)
			require.ErrorContains(t, err, "finder failed")
			// A failed query observes none of the per-query metrics.
			assertStoreGatewayInstancesHit(t, reg, 0, 0)
		})

		t.Run("via Select", func(t *testing.T) {
			sctx := limiter.AddQueryLimiterToContext(ctx, limiter.NewQueryLimiter(0, 0, 0, 0, nil))
			sctx = limiter.ContextWithNewUnlimitedMemoryConsumptionTracker(sctx)
			sctx = limiter.ContextWithNewSeriesLabelsDeduplicator(sctx, limiter.NewSeriesDeduplicatorMetrics(prometheus.NewPedanticRegistry()))
			_, sctx = stats.ContextWithEmptyStats(sctx)

			reg := prometheus.NewPedanticRegistry()
			q := newCompartmentsTestQuerier(newFinders(), stores(), reg, minT, maxT)
			set := q.Select(sctx, true, &storage.SelectHints{Start: minT, End: maxT}, fanOut)
			require.ErrorContains(t, set.Err(), "finder failed")
			assertStoreGatewayInstancesHit(t, reg, 0, 0)
		})
	})
}

func TestBlocksStoreQuerier_Compartments_SearchLabelNames(t *testing.T) {
	const minT, maxT = int64(10), int64(20)
	ctx := user.InjectOrgID(context.Background(), compartmentsTestTenant)

	t.Run("should merge label names across compartments", func(t *testing.T) {
		block0 := ulid.MustNew(1, nil)
		block1 := ulid.MustNew(2, nil)

		// Non-testify finders: both compartments query concurrently (see stubBlocksFinder).
		finders := []BlocksFinder{
			&stubBlocksFinder{blocks: bucketindex.Blocks{{ID: block0}}, meta: &bucketindex.Metadata{}},
			&stubBlocksFinder{blocks: bucketindex.Blocks{{ID: block1}}, meta: &bucketindex.Metadata{}},
		}

		// "instance" and "job" are returned by both compartments, so the merge must deduplicate them. Each
		// store-gateway returns its values sorted by value ascending (the default search ordering).
		stores := []BlocksStoreSet{
			&blocksStoreSetMock{mockedResponses: []interface{}{
				map[BlocksStoreClient][]ulid.ULID{
					&searchStoreGatewayClientMock{
						storeGatewayClientMock: storeGatewayClientMock{remoteAddr: "1.1.1.1"},
						searchLabelNamesBatches: []*storepb.SearchResultBatch{{
							Results: []storepb.SearchResultBatch_Result{
								{Value: "__name__", Score: 1.0},
								{Value: "instance", Score: 0.9},
								{Value: "job", Score: 0.8},
							},
						}},
						queriedBlockIDs: []ulid.ULID{block0},
					}: {block0},
				},
			}},
			&blocksStoreSetMock{mockedResponses: []interface{}{
				map[BlocksStoreClient][]ulid.ULID{
					&searchStoreGatewayClientMock{
						storeGatewayClientMock: storeGatewayClientMock{remoteAddr: "2.2.2.2"},
						searchLabelNamesBatches: []*storepb.SearchResultBatch{{
							Results: []storepb.SearchResultBatch_Result{
								{Value: "instance", Score: 0.9},
								{Value: "job", Score: 0.8},
								{Value: "namespace", Score: 0.7},
							},
						}},
						queriedBlockIDs: []ulid.ULID{block1},
					}: {block1},
				},
			}},
		}

		reg := prometheus.NewPedanticRegistry()
		q := newCompartmentsTestQuerierWithFinders(finders, stores, reg, minT, maxT)

		// No metric-name matcher fans out to both compartments; their label names are merged and deduplicated.
		rs := q.SearchLabelNames(ctx, nil, nil)
		defer rs.Close()

		var got []string
		for _, r := range drainSearchResults(t, rs) {
			got = append(got, r.Value)
		}
		require.Equal(t, []string{"__name__", "instance", "job", "namespace"}, got)
		assertCompartmentsHit(t, reg, 2)
		assertStoreGatewayInstancesHit(t, reg, 1, 2)
	})
}

func TestBlocksStoreQuerier_Compartments_SearchLabelValues(t *testing.T) {
	const minT, maxT = int64(10), int64(20)
	ctx := user.InjectOrgID(context.Background(), compartmentsTestTenant)

	t.Run("should merge label values across compartments", func(t *testing.T) {
		block0 := ulid.MustNew(1, nil)
		block1 := ulid.MustNew(2, nil)

		// Non-testify finders: both compartments query concurrently (see stubBlocksFinder).
		finders := []BlocksFinder{
			&stubBlocksFinder{blocks: bucketindex.Blocks{{ID: block0}}, meta: &bucketindex.Metadata{}},
			&stubBlocksFinder{blocks: bucketindex.Blocks{{ID: block1}}, meta: &bucketindex.Metadata{}},
		}

		// "prod" and "staging" are returned by both compartments, so the merge must deduplicate them. Each
		// store-gateway returns its values sorted by value ascending (the default search ordering).
		stores := []BlocksStoreSet{
			&blocksStoreSetMock{mockedResponses: []interface{}{
				map[BlocksStoreClient][]ulid.ULID{
					&searchStoreGatewayClientMock{
						storeGatewayClientMock: storeGatewayClientMock{remoteAddr: "1.1.1.1"},
						searchLabelValuesBatches: []*storepb.SearchResultBatch{{
							Results: []storepb.SearchResultBatch_Result{
								{Value: "dev", Score: 1.0},
								{Value: "prod", Score: 0.9},
								{Value: "staging", Score: 0.8},
							},
						}},
						queriedBlockIDs: []ulid.ULID{block0},
					}: {block0},
				},
			}},
			&blocksStoreSetMock{mockedResponses: []interface{}{
				map[BlocksStoreClient][]ulid.ULID{
					&searchStoreGatewayClientMock{
						storeGatewayClientMock: storeGatewayClientMock{remoteAddr: "2.2.2.2"},
						searchLabelValuesBatches: []*storepb.SearchResultBatch{{
							Results: []storepb.SearchResultBatch_Result{
								{Value: "prod", Score: 0.9},
								{Value: "staging", Score: 0.8},
								{Value: "test", Score: 0.7},
							},
						}},
						queriedBlockIDs: []ulid.ULID{block1},
					}: {block1},
				},
			}},
		}

		reg := prometheus.NewPedanticRegistry()
		q := newCompartmentsTestQuerierWithFinders(finders, stores, reg, minT, maxT)

		// No metric-name matcher fans out to both compartments; their label values are merged and deduplicated.
		rs := q.SearchLabelValues(ctx, "env", nil, nil)
		defer rs.Close()

		var got []string
		for _, r := range drainSearchResults(t, rs) {
			got = append(got, r.Value)
		}
		require.Equal(t, []string{"dev", "prod", "staging", "test"}, got)
		assertCompartmentsHit(t, reg, 2)
		assertStoreGatewayInstancesHit(t, reg, 1, 2)
	})
}

func TestNewBlocksStoreQueryableFinder_MetricsAreScopedToComponent(t *testing.T) {
	// Every metric the finder registers (bucket client, metadata cache, bucket-index loader) must carry
	// the given component label, so per-read-compartment finders sharing a registry don't collide.
	const component = "querier-rc-7"

	var storageCfg mimir_tsdb.BlocksStorageConfig
	flagext.DefaultValues(&storageCfg)
	storageCfg.Bucket.Backend = bucket.Filesystem
	storageCfg.Bucket.Filesystem.Directory = t.TempDir()

	reg := prometheus.NewPedanticRegistry()
	_, err := newBlocksStoreQueryableFinder(component, "blocks-rc-7", storageCfg.Bucket, storageCfg, &blocksStoreLimitsMock{}, log.NewNopLogger(), reg)
	require.NoError(t, err)

	mfs, err := reg.Gather()
	require.NoError(t, err)
	require.NotEmpty(t, mfs, "the finder should register at least one metric")

	for _, mf := range mfs {
		for _, m := range mf.GetMetric() {
			value, found := "", false
			for _, l := range m.GetLabel() {
				if l.GetName() == "component" {
					value, found = l.GetValue(), true
					break
				}
			}
			require.Truef(t, found, "metric %q is missing the component label", mf.GetName())
			require.Equalf(t, component, value, "metric %q has an unexpected component label", mf.GetName())
		}
	}
}

// metricNameForCompartment returns a metric name that the router maps to the given read compartment.
func metricNameForCompartment(t *testing.T, router *compartments.Router, compartment int) string {
	t.Helper()
	for i := 0; i < 100000; i++ {
		name := fmt.Sprintf("metric_%d", i)
		if router.CompartmentForMetric(compartmentsTestTenant, name) == compartment {
			return name
		}
	}
	t.Fatalf("no metric name maps to compartment %d", compartment)
	return ""
}

// newCompartmentsTestQuerier builds a compartment-aware blocksStoreQuerier over the given per-compartment
// finders and stores, wiring the read-compartment router and registering the hit metric like
// NewBlocksStoreQueryable does.
func newCompartmentsTestQuerier(finders []*blocksFinderMock, stores []BlocksStoreSet, reg prometheus.Registerer, minT, maxT int64) *blocksStoreQuerier {
	bf := make([]BlocksFinder, len(finders))
	for i, f := range finders {
		bf[i] = f
	}
	return newCompartmentsTestQuerierWithFinders(bf, stores, reg, minT, maxT)
}

func newCompartmentsTestQuerierWithFinders(finders []BlocksFinder, stores []BlocksStoreSet, reg prometheus.Registerer, minT, maxT int64) *blocksStoreQuerier {
	compartmentsCfg := compartments.Config{Enabled: true, Read: compartments.ReadConfig{NumCompartments: len(finders)}}
	router := compartments.NewRouter(len(finders))

	metrics := newBlocksStoreQueryableMetrics(compartmentsCfg, reg)

	comps := make([]blocksStoreCompartment, len(finders))
	for i := range comps {
		comps[i] = blocksStoreCompartment{finder: finders[i], stores: stores[i]}
	}

	return &blocksStoreQuerier{
		minT:               minT,
		maxT:               maxT,
		compartments:       comps,
		router:             router,
		dynamicReplication: newDynamicReplication(),
		consistency:        NewBlocksConsistency(0, nil),
		logger:             log.NewNopLogger(),
		metrics:            metrics,
		limits:             &blocksStoreLimitsMock{},
	}
}

// stubBlocksFinder is a non-testify BlocksFinder. It is used where the finder is invoked from several
// compartment goroutines concurrently: testify's mock argument matching stringifies the context, which
// races with the concurrent query-stats updates carried in that context (the stats are mutated
// atomically, so this is a test-only race, not a production one).
type stubBlocksFinder struct {
	services.Service
	blocks bucketindex.Blocks
	meta   *bucketindex.Metadata
}

func (f *stubBlocksFinder) GetBlocks(context.Context, string, int64, int64) (bucketindex.Blocks, *bucketindex.Metadata, error) {
	return f.blocks, f.meta, nil
}

func mockNoBlocks(finder *blocksFinderMock, minT, maxT int64) {
	finder.On("GetBlocks", mock.Anything, compartmentsTestTenant, minT, maxT).Return(bucketindex.Blocks(nil), &bucketindex.Metadata{}, nil)
}

func assertCompartmentsHit(t *testing.T, reg *prometheus.Registry, expected int) {
	t.Helper()

	var buckets strings.Builder
	for le := 1; le <= 2; le++ {
		count := 0
		if le >= expected {
			count = 1
		}
		fmt.Fprintf(&buckets, "cortex_querier_compartments_hit_per_query_bucket{storage=\"store-gateway\",le=\"%d\"} %d\n", le, count)
	}

	expectedText := fmt.Sprintf(`
		# HELP cortex_querier_compartments_hit_per_query Number of read compartments queried for a single query.
		# TYPE cortex_querier_compartments_hit_per_query histogram
		%scortex_querier_compartments_hit_per_query_bucket{storage="store-gateway",le="+Inf"} 1
		cortex_querier_compartments_hit_per_query_sum{storage="store-gateway"} %d
		cortex_querier_compartments_hit_per_query_count{storage="store-gateway"} 1
`, buckets.String(), expected)

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expectedText), "cortex_querier_compartments_hit_per_query"))
}

// getStoreGatewayInstancesHit returns the observation count and summed value of the per-query
// cortex_querier_storegateway_instances_hit_per_query histogram. It is used to assert the histogram is
// observed once per query (summed across compartments), not once per compartment.
func getStoreGatewayInstancesHit(t *testing.T, reg *prometheus.Registry) (count uint64, sum float64) {
	t.Helper()

	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != "cortex_querier_storegateway_instances_hit_per_query" {
			continue
		}
		require.Len(t, mf.GetMetric(), 1)
		h := mf.GetMetric()[0].GetHistogram()
		return h.GetSampleCount(), h.GetSampleSum()
	}

	t.Fatal("cortex_querier_storegateway_instances_hit_per_query not found")
	return 0, 0
}

// assertStoreGatewayInstancesHit asserts the per-query store-gateway instances histogram was observed
// expectedCount times with the given summed instance count.
func assertStoreGatewayInstancesHit(t *testing.T, reg *prometheus.Registry, expectedCount, expectedSum int) {
	t.Helper()
	count, sum := getStoreGatewayInstancesHit(t, reg)
	require.Equal(t, uint64(expectedCount), count)
	require.Equal(t, float64(expectedSum), sum)
}
