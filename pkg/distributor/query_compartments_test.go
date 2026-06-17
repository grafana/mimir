// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/test"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/compartments"
	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// TestDistributor_QueryStream_ShouldSupportCompartments verifies the end-to-end query path with read
// compartments enabled: a query is served only from the compartments that can hold the selected metric
// names, and from all compartments when it can't be restricted.
func TestDistributor_QueryStream_ShouldSupportCompartments(t *testing.T) {
	const (
		tenantID        = "user"
		numCompartments = 2
	)

	// Pick a metric name that routes to each compartment, and place its series in the ingester owning that
	// compartment's partition (compartment c owns active partition c, owned by ingester-zone-a-c).
	router := compartmentsTestRouter(numCompartments)
	metricForCompartment := metricNamesByCompartment(t, router, tenantID, numCompartments)

	compartmentData := make([]*mimirpb.WriteRequest, numCompartments)
	activePartitions := make(map[int][]int32, numCompartments)
	for c := 0; c < numCompartments; c++ {
		compartmentData[c] = makeWriteRequest(0, 1, 0, false, false, metricForCompartment[c])
		activePartitions[c] = []int32{int32(c)}
	}

	eqName := func(value string) *labels.Matcher {
		return labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, value)
	}
	reName := func(value string) *labels.Matcher {
		return labels.MustNewMatcher(labels.MatchRegexp, model.MetricNameLabel, value)
	}

	tests := map[string]struct {
		matchers                 []*labels.Matcher
		expectedResponse         model.Matrix
		expectedCompartmentsHit  int // Number of compartments queried, as recorded by the compartments-hit metric.
		expectedQueriedIngesters int // Equal to expectedCompartmentsHit here, since one ingester owns one compartment's partition.
	}{
		"a query pinned to compartment 0's metric is served only from compartment 0": {
			matchers:                 []*labels.Matcher{eqName(metricForCompartment[0])},
			expectedResponse:         expectedResponse(0, 1, false, metricForCompartment[0]),
			expectedCompartmentsHit:  1,
			expectedQueriedIngesters: 1,
		},
		"a query pinned to compartment 1's metric is served only from compartment 1": {
			matchers:                 []*labels.Matcher{eqName(metricForCompartment[1])},
			expectedResponse:         expectedResponse(0, 1, false, metricForCompartment[1]),
			expectedCompartmentsHit:  1,
			expectedQueriedIngesters: 1,
		},
		"a regexp metric-name set spanning both compartments is served from both": {
			matchers:                 []*labels.Matcher{reName(strings.Join([]string{metricForCompartment[0], metricForCompartment[1]}, "|"))},
			expectedResponse:         expectedResponse(0, 1, false, metricForCompartment[0], metricForCompartment[1]),
			expectedCompartmentsHit:  2,
			expectedQueriedIngesters: 2,
		},
		"a query without a metric-name matcher fans out to all compartments": {
			matchers:                 []*labels.Matcher{mustEqualMatcher("bar", "baz")},
			expectedResponse:         expectedResponse(0, 1, false, metricForCompartment[0], metricForCompartment[1]),
			expectedCompartmentsHit:  2,
			expectedQueriedIngesters: 2,
		},
		"a query pinned to a compartment is still targeted there even if it matches no series": {
			matchers:                 []*labels.Matcher{eqName(metricForCompartment[0]), mustEqualMatcher("bar", "no-match")},
			expectedResponse:         expectedResponse(0, 0, false),
			expectedCompartmentsHit:  1,
			expectedQueriedIngesters: 1,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			ctx := user.InjectOrgID(context.Background(), tenantID)
			ctx = limiter.ContextWithNewUnlimitedMemoryConsumptionTracker(ctx)
			ctx = limiter.ContextWithNewSeriesLabelsDeduplicator(ctx, limiter.NewSeriesDeduplicatorMetrics(prometheus.NewPedanticRegistry()))

			distributors, ingesters, distributorRegistries, _ := prepare(t, prepConfig{
				numDistributors:         1,
				ingestStorageEnabled:    true,
				ingestStoragePartitions: numCompartments,
				ingesterStateByZone: map[string]ingesterZoneState{
					"zone-a": {numIngesters: numCompartments, happyIngesters: numCompartments},
				},
				ingesterDataByZone:          map[string][]*mimirpb.WriteRequest{"zone-a": compartmentData},
				ingesterDataTenantID:        tenantID,
				replicationFactor:           1,
				numCompartments:             numCompartments,
				compartmentActivePartitions: activePartitions,
				limits:                      prepareDefaultLimits(),
				configure: func(cfg *Config) {
					cfg.Compartments.Enabled = true
					cfg.Compartments.Read.NumCompartments = numCompartments
					cfg.IngestStorageConfig.KafkaConfig.Topic = compartmentsTestTopicFormat
				},
			})
			require.Len(t, distributors, 1)
			d := distributors[0]

			// Wait until each compartment's partition ring has discovered its active partition.
			for c := 0; c < numCompartments; c++ {
				test.Poll(t, 5*time.Second, 1, func() interface{} {
					return d.partitionInstanceRings.Get(c).PartitionRing().ActivePartitionsCount()
				})
			}

			queryMetrics := stats.NewQueryMetrics(distributorRegistries[0])
			resp, err := d.QueryStream(ctx, queryMetrics, 0, 10, testData.matchers...)
			require.NoError(t, err)

			responseMatrix, err := ingester_client.StreamingSeriesToMatrixForTests(0, 5, resp.StreamingSeries)
			require.NoError(t, err)
			assert.Equal(t, testData.expectedResponse.String(), responseMatrix.String())

			// Only the targeted compartments' ingesters should have been queried.
			test.Poll(t, time.Second, testData.expectedQueriedIngesters, func() any {
				return countMockIngestersCalls(ingesters, "QueryStream")
			})

			// The query recorded a single observation of the number of compartments it queried.
			le1 := 0
			if testData.expectedCompartmentsHit <= 1 {
				le1 = 1
			}
			expectedMetrics := fmt.Sprintf(`
				# HELP cortex_querier_compartments_hit_per_query Number of read compartments queried for a single query.
				# TYPE cortex_querier_compartments_hit_per_query histogram
				cortex_querier_compartments_hit_per_query_bucket{storage="ingester",le="1"} %d
				cortex_querier_compartments_hit_per_query_bucket{storage="ingester",le="2"} 1
				cortex_querier_compartments_hit_per_query_bucket{storage="ingester",le="+Inf"} 1
				cortex_querier_compartments_hit_per_query_sum{storage="ingester"} %d
				cortex_querier_compartments_hit_per_query_count{storage="ingester"} 1
			`, le1, testData.expectedCompartmentsHit)
			require.NoError(t, testutil.GatherAndCompare(distributorRegistries[0], strings.NewReader(expectedMetrics), "cortex_querier_compartments_hit_per_query"))
		})
	}
}

// TestDistributor_getIngesterReplicationSetsForQuery_Compartments verifies that, with read
// compartments enabled, a query pinned to a single metric name targets only the owning compartment's
// partition ring, while a non-pinnable query fans out to every compartment.
func TestDistributor_getIngesterReplicationSetsForQuery_Compartments(t *testing.T) {
	const (
		tenantID        = "user"
		numCompartments = 2
	)

	ctx := user.InjectOrgID(context.Background(), tenantID)
	d, reg, metricForCompartment := prepareCompartmentsQueryTestDistributor(t, tenantID, numCompartments)

	queriedPartitions := func(replicationSets []ring.ReplicationSet) []int {
		var ids []int
		for _, rs := range replicationSets {
			require.NotEmpty(t, rs.Instances)
			partitionID, err := ingest.IngesterPartitionID(rs.Instances[0].Addr)
			require.NoError(t, err)
			ids = append(ids, int(partitionID))
		}
		sort.Ints(ids)
		return ids
	}

	// A query pinned to a metric name targets only the owning compartment.
	for c := 0; c < numCompartments; c++ {
		matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, metricForCompartment[c])}
		replicationSets, err := d.getIngesterReplicationSetsForQuery(ctx, matchers)
		require.NoError(t, err)
		assert.Equal(t, []int{c}, queriedPartitions(replicationSets), "metric %q should be queried from compartment %d only", metricForCompartment[c], c)
	}

	// A query that can't be pinned fans out to all compartments.
	replicationSets, err := d.getIngesterReplicationSetsForQuery(ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, []int{0, 1}, queriedPartitions(replicationSets))

	// The compartments-hit metric records 1 for each targeted query and numCompartments for the fan-out.
	expectedMetrics := `
		# HELP cortex_querier_compartments_hit_per_query Number of read compartments queried for a single query.
		# TYPE cortex_querier_compartments_hit_per_query histogram
		cortex_querier_compartments_hit_per_query_bucket{storage="ingester",le="1"} 2
		cortex_querier_compartments_hit_per_query_bucket{storage="ingester",le="2"} 3
		cortex_querier_compartments_hit_per_query_bucket{storage="ingester",le="+Inf"} 3
		cortex_querier_compartments_hit_per_query_sum{storage="ingester"} 4
		cortex_querier_compartments_hit_per_query_count{storage="ingester"} 3
	`
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expectedMetrics), "cortex_querier_compartments_hit_per_query"))
}

// TestDistributor_QueryExemplars_Compartments verifies that the exemplars API, which accepts multiple
// OR-ed matcher sets, targets a single compartment only when there's one set, and fans out to all
// compartments for multiple sets (rather than collapsing distinct __name__ matchers to one compartment).
func TestDistributor_QueryExemplars_Compartments(t *testing.T) {
	const (
		tenantID        = "user"
		numCompartments = 2
	)

	eqName := func(value string) *labels.Matcher {
		return labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, value)
	}

	t.Run("a single matcher set pinning a metric name targets only the owning compartment", func(t *testing.T) {
		ctx := user.InjectOrgID(context.Background(), tenantID)
		d, reg, metricForCompartment := prepareCompartmentsQueryTestDistributor(t, tenantID, numCompartments)

		_, err := d.QueryExemplars(ctx, 0, 10, []*labels.Matcher{eqName(metricForCompartment[0])})
		require.NoError(t, err)

		// A single observation of 1 compartment hit.
		expectedMetrics := `
			# HELP cortex_querier_compartments_hit_per_query Number of read compartments queried for a single query.
			# TYPE cortex_querier_compartments_hit_per_query histogram
			cortex_querier_compartments_hit_per_query_bucket{storage="ingester",le="1"} 1
			cortex_querier_compartments_hit_per_query_bucket{storage="ingester",le="2"} 1
			cortex_querier_compartments_hit_per_query_bucket{storage="ingester",le="+Inf"} 1
			cortex_querier_compartments_hit_per_query_sum{storage="ingester"} 1
			cortex_querier_compartments_hit_per_query_count{storage="ingester"} 1
		`
		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expectedMetrics), "cortex_querier_compartments_hit_per_query"))
	})

	t.Run("multiple OR-ed sets spanning compartments must not collapse to one and fan out to all", func(t *testing.T) {
		ctx := user.InjectOrgID(context.Background(), tenantID)
		d, reg, metricForCompartment := prepareCompartmentsQueryTestDistributor(t, tenantID, numCompartments)

		_, err := d.QueryExemplars(ctx, 0, 10, []*labels.Matcher{eqName(metricForCompartment[0])}, []*labels.Matcher{eqName(metricForCompartment[1])})
		require.NoError(t, err)

		// A single observation of all compartments hit (here, both).
		expectedMetrics := `
			# HELP cortex_querier_compartments_hit_per_query Number of read compartments queried for a single query.
			# TYPE cortex_querier_compartments_hit_per_query histogram
			cortex_querier_compartments_hit_per_query_bucket{storage="ingester",le="1"} 0
			cortex_querier_compartments_hit_per_query_bucket{storage="ingester",le="2"} 1
			cortex_querier_compartments_hit_per_query_bucket{storage="ingester",le="+Inf"} 1
			cortex_querier_compartments_hit_per_query_sum{storage="ingester"} 2
			cortex_querier_compartments_hit_per_query_count{storage="ingester"} 1
		`
		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expectedMetrics), "cortex_querier_compartments_hit_per_query"))
	})
}

// TestDistributor_MetricsMetadata_Compartments verifies that a metadata query for a single metric is
// served only from the owning compartment (metadata is sharded by metric family name), while a query
// without a metric filter fans out to all compartments.
func TestDistributor_MetricsMetadata_Compartments(t *testing.T) {
	const (
		tenantID        = "user"
		numCompartments = 2
	)

	t.Run("a request for a single metric targets its compartment", func(t *testing.T) {
		ctx := user.InjectOrgID(context.Background(), tenantID)
		d, reg, metricForCompartment := prepareCompartmentsQueryTestDistributor(t, tenantID, numCompartments)

		_, err := d.MetricsMetadata(ctx, &ingester_client.MetricsMetadataRequest{Limit: -1, LimitPerMetric: -1, Metric: metricForCompartment[0]})
		require.NoError(t, err)
		assertCompartmentsHitObservation(t, reg, numCompartments, 1)
	})

	t.Run("a request without a metric fans out to all compartments", func(t *testing.T) {
		ctx := user.InjectOrgID(context.Background(), tenantID)
		d, reg, _ := prepareCompartmentsQueryTestDistributor(t, tenantID, numCompartments)

		_, err := d.MetricsMetadata(ctx, &ingester_client.MetricsMetadataRequest{Limit: -1, LimitPerMetric: -1})
		require.NoError(t, err)
		assertCompartmentsHitObservation(t, reg, numCompartments, numCompartments)
	})
}

// TestDistributor_LabelNames_Compartments covers a representative label query caller: matchers are
// threaded into the compartment targeting just like the streaming query path.
func TestDistributor_LabelNames_Compartments(t *testing.T) {
	const (
		tenantID        = "user"
		numCompartments = 2
	)

	t.Run("a query pinned to a metric name targets its compartment", func(t *testing.T) {
		ctx := user.InjectOrgID(context.Background(), tenantID)
		d, reg, metricForCompartment := prepareCompartmentsQueryTestDistributor(t, tenantID, numCompartments)

		_, err := d.LabelNames(ctx, 0, 10, nil, labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, metricForCompartment[0]))
		require.NoError(t, err)
		assertCompartmentsHitObservation(t, reg, numCompartments, 1)
	})

	t.Run("a query without a metric-name matcher fans out to all compartments", func(t *testing.T) {
		ctx := user.InjectOrgID(context.Background(), tenantID)
		d, reg, _ := prepareCompartmentsQueryTestDistributor(t, tenantID, numCompartments)

		_, err := d.LabelNames(ctx, 0, 10, nil, labels.MustNewMatcher(labels.MatchEqual, "bar", "baz"))
		require.NoError(t, err)
		assertCompartmentsHitObservation(t, reg, numCompartments, numCompartments)
	})
}

// TestDistributor_adjustQueryRequestLimit_Compartments verifies the per-query limit is divided by the
// active partition count of only the compartments the query targets, not the whole cluster.
func TestDistributor_adjustQueryRequestLimit_Compartments(t *testing.T) {
	const (
		tenantID        = "user"
		numCompartments = 2
	)

	ctx := user.InjectOrgID(context.Background(), tenantID)
	d, _, metricForCompartment := prepareCompartmentsQueryTestDistributor(t, tenantID, numCompartments)

	// A fan-out query (no metric-name matcher) targets all compartments; with one active partition each,
	// the shard size sums to numCompartments, so a larger limit is divided by it (rounded up).
	assert.Equal(t, 5, d.adjustQueryRequestLimit(ctx, tenantID, nil, 10))
	// A limit not larger than the shard size is returned unchanged.
	assert.Equal(t, numCompartments, d.adjustQueryRequestLimit(ctx, tenantID, nil, numCompartments))
	// A zero limit (no limit) is returned unchanged.
	assert.Equal(t, 0, d.adjustQueryRequestLimit(ctx, tenantID, nil, 0))

	// A query pinned to a single compartment is divided by only that compartment's shard size (1 active
	// partition here), so the limit is left intact rather than over-divided by the whole cluster.
	pinned := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, metricForCompartment[0])}
	assert.Equal(t, 10, d.adjustQueryRequestLimit(ctx, tenantID, pinned, 10))
}

// assertCompartmentsHitObservation asserts the compartments-hit histogram recorded a single observation
// of the given number of compartments.
func assertCompartmentsHitObservation(t *testing.T, reg *prometheus.Registry, numCompartments, hit int) {
	t.Helper()

	var b strings.Builder
	b.WriteString("# HELP cortex_querier_compartments_hit_per_query Number of read compartments queried for a single query.\n")
	b.WriteString("# TYPE cortex_querier_compartments_hit_per_query histogram\n")
	for le := 1; le <= numCompartments; le++ {
		count := 0
		if hit <= le {
			count = 1
		}
		fmt.Fprintf(&b, "cortex_querier_compartments_hit_per_query_bucket{storage=\"ingester\",le=\"%d\"} %d\n", le, count)
	}
	b.WriteString("cortex_querier_compartments_hit_per_query_bucket{storage=\"ingester\",le=\"+Inf\"} 1\n")
	fmt.Fprintf(&b, "cortex_querier_compartments_hit_per_query_sum{storage=\"ingester\"} %d\n", hit)
	b.WriteString("cortex_querier_compartments_hit_per_query_count{storage=\"ingester\"} 1\n")

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(b.String()), "cortex_querier_compartments_hit_per_query"))
}

// prepareCompartmentsQueryTestDistributor builds a queryable distributor with read compartments enabled.
// Each compartment c owns a single active partition (== c) owned by ingester-zone-a-c, so a returned
// replication set's partition ID identifies the compartment it targets. It returns the distributor, its
// metrics registry, and, indexed by compartment, a metric name that routes to it.
func prepareCompartmentsQueryTestDistributor(t *testing.T, tenantID string, numCompartments int) (*Distributor, *prometheus.Registry, []string) {
	t.Helper()

	activePartitions := make(map[int][]int32, numCompartments)
	for c := 0; c < numCompartments; c++ {
		activePartitions[c] = []int32{int32(c)}
	}

	distributors, _, regs, _ := prepare(t, prepConfig{
		numDistributors:         1,
		ingestStorageEnabled:    true,
		ingestStoragePartitions: int32(numCompartments),
		ingesterStateByZone: map[string]ingesterZoneState{
			"zone-a": {numIngesters: numCompartments, happyIngesters: numCompartments},
		},
		ingesterDataTenantID:        tenantID,
		replicationFactor:           1,
		numCompartments:             numCompartments,
		compartmentActivePartitions: activePartitions,
		limits:                      prepareDefaultLimits(),
		configure: func(cfg *Config) {
			cfg.Compartments.Enabled = true
			cfg.Compartments.Read.NumCompartments = numCompartments
			cfg.IngestStorageConfig.KafkaConfig.Topic = compartmentsTestTopicFormat
		},
	})
	require.Len(t, distributors, 1)
	d := distributors[0]

	// Wait until each compartment's partition ring has discovered its active partition.
	for c := 0; c < numCompartments; c++ {
		test.Poll(t, 5*time.Second, 1, func() interface{} {
			return d.partitionInstanceRings.Get(c).PartitionRing().ActivePartitionsCount()
		})
	}

	return d, regs[0], metricNamesByCompartment(t, compartmentsTestRouter(numCompartments), tenantID, numCompartments)
}

// metricNamesByCompartment returns, indexed by compartment, a metric name that the router assigns to that
// compartment for the given tenant.
func metricNamesByCompartment(t *testing.T, router *compartments.Router, tenantID string, numCompartments int) []string {
	t.Helper()

	metricForCompartment := make([]string, numCompartments)
	remaining := numCompartments
	for i := 0; remaining > 0; i++ {
		require.Less(t, i, 1000, "could not find a metric name for every compartment")
		name := fmt.Sprintf("metric_%d", i)
		if c := router.CompartmentForMetric(tenantID, name); metricForCompartment[c] == "" {
			metricForCompartment[c] = name
			remaining--
		}
	}
	return metricForCompartment
}
