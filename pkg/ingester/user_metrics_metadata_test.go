// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestUserMetricsMetadata(t *testing.T) {
	type input struct {
		meta        mimirpb.MetricMetadata
		errContains string
	}

	errorSamplers := newIngesterErrSamplers(0)

	tests := map[string]struct {
		maxMetadataPerUser   int
		maxMetadataPerMetric int
		inputMetadata        []input
		expectedMetadata     []*mimirpb.MetricMetadata
	}{
		"should succeed for multiple metadata per metric": {
			inputMetadata: []input{
				{meta: mimirpb.MetricMetadata{Type: mimirpb.COUNTER, MetricFamilyName: "test_metric_1", Help: "foo"}},
				{meta: mimirpb.MetricMetadata{Type: mimirpb.COUNTER, MetricFamilyName: "test_metric_1", Help: "bar"}},
				{meta: mimirpb.MetricMetadata{Type: mimirpb.COUNTER, MetricFamilyName: "test_metric_2", Help: "baz"}},
				{meta: mimirpb.MetricMetadata{Type: mimirpb.COUNTER, MetricFamilyName: "test_metric_2", Help: "qux"}},
			},
			expectedMetadata: []*mimirpb.MetricMetadata{
				{Type: mimirpb.COUNTER, MetricFamilyName: "test_metric_1", Help: "foo"},
				{Type: mimirpb.COUNTER, MetricFamilyName: "test_metric_1", Help: "bar"},
				{Type: mimirpb.COUNTER, MetricFamilyName: "test_metric_2", Help: "baz"},
				{Type: mimirpb.COUNTER, MetricFamilyName: "test_metric_2", Help: "qux"},
			},
		},
		"should fail when metadata per user limit reached": {
			maxMetadataPerUser: 1,
			inputMetadata: []input{
				{meta: mimirpb.MetricMetadata{Type: mimirpb.COUNTER, MetricFamilyName: "test_metric_1", Help: "foo"}},
				{meta: mimirpb.MetricMetadata{Type: mimirpb.COUNTER, MetricFamilyName: "test_metric_1", Help: "bar"}},
				{meta: mimirpb.MetricMetadata{Type: mimirpb.COUNTER, MetricFamilyName: "test_metric_2", Help: "baz"}, errContains: "err-mimir-max-metadata-per-user"},
				{meta: mimirpb.MetricMetadata{Type: mimirpb.COUNTER, MetricFamilyName: "test_metric_2", Help: "qux"}, errContains: "err-mimir-max-metadata-per-user"},
			},
			expectedMetadata: []*mimirpb.MetricMetadata{
				{Type: mimirpb.COUNTER, MetricFamilyName: "test_metric_1", Help: "foo"},
				{Type: mimirpb.COUNTER, MetricFamilyName: "test_metric_1", Help: "bar"},
			},
		},
		"should fail when metadata per metric limit reached": {
			maxMetadataPerMetric: 1,
			inputMetadata: []input{
				{meta: mimirpb.MetricMetadata{Type: mimirpb.COUNTER, MetricFamilyName: "test_metric_1", Help: "foo"}},
				{meta: mimirpb.MetricMetadata{Type: mimirpb.COUNTER, MetricFamilyName: "test_metric_1", Help: "bar"}, errContains: "err-mimir-max-metadata-per-metric"},
				{meta: mimirpb.MetricMetadata{Type: mimirpb.COUNTER, MetricFamilyName: "test_metric_2", Help: "baz"}},
				{meta: mimirpb.MetricMetadata{Type: mimirpb.COUNTER, MetricFamilyName: "test_metric_2", Help: "qux"}, errContains: "err-mimir-max-metadata-per-metric"},
			},
			expectedMetadata: []*mimirpb.MetricMetadata{
				{Type: mimirpb.COUNTER, MetricFamilyName: "test_metric_1", Help: "foo"},
				{Type: mimirpb.COUNTER, MetricFamilyName: "test_metric_2", Help: "baz"},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			// Mock the ring
			ring := &ringCountMock{instancesCount: 1, zonesCount: 1}

			limits, err := validation.NewOverrides(validation.Limits{
				MaxGlobalMetricsWithMetadataPerUser: testData.maxMetadataPerUser,
				MaxGlobalMetadataPerMetric:          testData.maxMetadataPerMetric,
			}, nil)
			require.NoError(t, err)

			strategy := newIngesterRingLimiterStrategy(ring, 1, false, "", limits.IngestionTenantShardSize)
			limiter := NewLimiter(limits, strategy)

			metrics := newIngesterMetrics(
				prometheus.NewPedanticRegistry(),
				true,
				func() *InstanceLimits { return nil },
				nil, nil, nil,
			)

			mm := newMetadataMap(limiter, metrics, errorSamplers, "test")

			// Attempt to add all metadata
			for _, i := range testData.inputMetadata {
				err := mm.add(i.meta.MetricFamilyName, &i.meta)

				if i.errContains != "" {
					require.Error(t, err)
					require.Contains(t, err.Error(), i.errContains)
				} else {
					require.NoError(t, err)
				}
			}

			// Verify expected elements are stored
			req := client.DefaultMetricsMetadataRequest()
			clientMeta := mm.toClientMetadata(req)
			assert.ElementsMatch(t, testData.expectedMetadata, clientMeta)

			// Purge all metadata
			mm.purge(time.Time{})

			// Verify all metadata purged
			clientMeta = mm.toClientMetadata(req)
			assert.Empty(t, clientMeta)
		})
	}
}

// noopTestingT is used so we can run assert.ElementsMatch without failing the test.
// Since the underlying structure is a map, the order of the metadata is non-deterministic
// so we need to use ElementsMatch. However, for the same reason the combination of the
// metadata is also non-deterministic, so we need to go through a set of possible
// combinations, and only fail the test if none of them match.
type noopTestingT struct{}

func (t noopTestingT) Errorf(string, ...interface{}) {}

func TestUserMetricsMetadataRequest(t *testing.T) {
	// Mock the ring
	ring := &ringCountMock{instancesCount: 1, zonesCount: 1}

	limits, err := validation.NewOverrides(validation.Limits{}, nil)
	require.NoError(t, err)

	strategy := newIngesterRingLimiterStrategy(ring, 1, false, "", limits.IngestionTenantShardSize)
	limiter := NewLimiter(limits, strategy)

	metrics := newIngesterMetrics(
		prometheus.NewPedanticRegistry(),
		true,
		func() *InstanceLimits { return nil },
		nil, nil, nil,
	)

	mm := newMetadataMap(limiter, metrics, newIngesterErrSamplers(0), "test")

	inputMetadata := []mimirpb.MetricMetadata{
		{Type: mimirpb.COUNTER, MetricFamilyName: "test_metric_1", Help: "foo"},
		{Type: mimirpb.COUNTER, MetricFamilyName: "test_metric_1", Help: "bar"},
		{Type: mimirpb.COUNTER, MetricFamilyName: "test_metric_2", Help: "baz"},
		{Type: mimirpb.COUNTER, MetricFamilyName: "test_metric_2", Help: "qux"},
	}

	// Attempt to add all metadata
	for _, i := range inputMetadata {
		err := mm.add(i.MetricFamilyName, &i)
		require.NoError(t, err)
	}

	fullMetadata := [][]*mimirpb.MetricMetadata{
		{
			{Type: mimirpb.COUNTER, MetricFamilyName: "test_metric_1", Help: "foo"},
			{Type: mimirpb.COUNTER, MetricFamilyName: "test_metric_1", Help: "bar"},
			{Type: mimirpb.COUNTER, MetricFamilyName: "test_metric_2", Help: "baz"},
			{Type: mimirpb.COUNTER, MetricFamilyName: "test_metric_2", Help: "qux"},
		},
	}

	tests := map[string]struct {
		request                  *client.MetricsMetadataRequest
		possibleExpectedMetadata [][]*mimirpb.MetricMetadata // since it uses a map, what it includes is non-deterministic
	}{
		"no config": {
			request:                  client.DefaultMetricsMetadataRequest(),
			possibleExpectedMetadata: fullMetadata,
		},
		"limit=-1": {
			request:                  &client.MetricsMetadataRequest{Limit: -1, LimitPerMetric: -1, Metric: ""},
			possibleExpectedMetadata: fullMetadata,
		},
		"limit=0": {
			request: &client.MetricsMetadataRequest{Limit: 0, LimitPerMetric: -1, Metric: ""},
			possibleExpectedMetadata: [][]*mimirpb.MetricMetadata{
				{},
			},
		},
		"limit=1": {
			request: &client.MetricsMetadataRequest{Limit: 1, LimitPerMetric: -1, Metric: ""},
			possibleExpectedMetadata: [][]*mimirpb.MetricMetadata{
				{
					{Type: mimirpb.COUNTER, MetricFamilyName: "test_metric_1", Help: "foo"},
					{Type: mimirpb.COUNTER, MetricFamilyName: "test_metric_1", Help: "bar"},
				},
				{
					{Type: mimirpb.COUNTER, MetricFamilyName: "test_metric_2", Help: "baz"},
					{Type: mimirpb.COUNTER, MetricFamilyName: "test_metric_2", Help: "qux"},
				},
			},
		},
		"limit_per_metric=-1": {
			request:                  &client.MetricsMetadataRequest{Limit: -1, LimitPerMetric: -1, Metric: ""},
			possibleExpectedMetadata: fullMetadata,
		},
		"limit_per_metric=0": {
			request:                  &client.MetricsMetadataRequest{Limit: -1, LimitPerMetric: 0, Metric: ""},
			possibleExpectedMetadata: fullMetadata,
		},
		"limit_per_metric=1": {
			request: &client.MetricsMetadataRequest{Limit: -1, LimitPerMetric: 1, Metric: ""},
			possibleExpectedMetadata: [][]*mimirpb.MetricMetadata{
				{
					{Type: mimirpb.COUNTER, MetricFamilyName: "test_metric_1", Help: "foo"},
					{Type: mimirpb.COUNTER, MetricFamilyName: "test_metric_2", Help: "baz"},
				},
				{
					{Type: mimirpb.COUNTER, MetricFamilyName: "test_metric_1", Help: "foo"},
					{Type: mimirpb.COUNTER, MetricFamilyName: "test_metric_2", Help: "qux"},
				},
				{
					{Type: mimirpb.COUNTER, MetricFamilyName: "test_metric_1", Help: "bar"},
					{Type: mimirpb.COUNTER, MetricFamilyName: "test_metric_2", Help: "baz"},
				},
				{
					{Type: mimirpb.COUNTER, MetricFamilyName: "test_metric_1", Help: "bar"},
					{Type: mimirpb.COUNTER, MetricFamilyName: "test_metric_2", Help: "qux"},
				},
			},
		},
		"metric": {
			request: &client.MetricsMetadataRequest{Limit: -1, LimitPerMetric: -1, Metric: "test_metric_2"},
			possibleExpectedMetadata: [][]*mimirpb.MetricMetadata{
				{
					{Type: mimirpb.COUNTER, MetricFamilyName: "test_metric_2", Help: "baz"},
					{Type: mimirpb.COUNTER, MetricFamilyName: "test_metric_2", Help: "qux"},
				},
			},
		},
	}

	dummyT := noopTestingT{}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			// Verify expected elements are stored
			clientMeta := mm.toClientMetadata(testData.request)
			var pass bool
			for _, expectedMetadata := range testData.possibleExpectedMetadata {
				if assert.ElementsMatch(dummyT, expectedMetadata, clientMeta) {
					pass = true
					break
				}
			}
			assert.True(t, pass)
		})
	}
}
