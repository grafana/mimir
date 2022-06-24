// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestUserMetricsMetadata(t *testing.T) {
	type input struct {
		meta        mimirpb.MetricMetadata
		errContains string
	}

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
			ring := &ringCountMock{}
			ring.On("HealthyInstancesCount").Return(1)
			ring.On("ZonesCount").Return(1)

			// Mock limiter
			limits, err := validation.NewOverrides(validation.Limits{
				MaxGlobalMetricsWithMetadataPerUser: testData.maxMetadataPerUser,
				MaxGlobalMetadataPerMetric:          testData.maxMetadataPerMetric,
			}, nil)
			require.NoError(t, err)
			limiter := NewLimiter(limits, ring, 1, false)

			// Mock metrics
			metrics := newIngesterMetrics(
				prometheus.NewPedanticRegistry(),
				true,
				func() *InstanceLimits { return defaultInstanceLimits },
				nil,
				nil,
			)

			mm := newMetadataMap(limiter, metrics, "test")

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
			clientMeta := mm.toClientMetadata()
			assert.ElementsMatch(t, testData.expectedMetadata, clientMeta)

			// Purge all metadata
			mm.purge(time.Time{})

			// Verify all metadata purged
			clientMeta = mm.toClientMetadata()
			assert.Empty(t, clientMeta)
		})
	}
}
