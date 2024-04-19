// SPDX-License-Identifier: AGPL-3.0-only

package tenantfederation

import (
	"context"
	"fmt"
	"testing"

	"github.com/grafana/dskit/tenant"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/scrape"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/util/test"
)

type mockMetadataSupplier struct {
	results map[string][]scrape.MetricMetadata
}

func (m *mockMetadataSupplier) MetricsMetadata(ctx context.Context, _ *client.MetricsMetadataRequest) ([]scrape.MetricMetadata, error) {
	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to parse single tenant ID from context: %w", err)
	}

	res, ok := m.results[tenantID]
	if !ok {
		return nil, fmt.Errorf("no mock results for tenant ID %s available", tenantID)
	}

	return res, nil
}

func TestMergeMetadataSupplier_MetricsMetadata(t *testing.T) {
	fixtureMetadata1 := scrape.MetricMetadata{
		Metric: "up",
		Type:   model.MetricTypeGauge,
	}

	fixtureMetadata2 := scrape.MetricMetadata{
		Metric: "requests",
		Type:   model.MetricTypeCounter,
	}

	t.Run("invalid tenant IDs", func(t *testing.T) {
		upstream := &mockMetadataSupplier{}
		supplier := NewMetadataSupplier(upstream, defaultConcurrency, test.NewTestingLogger(t))
		_, err := supplier.MetricsMetadata(context.Background(), client.DefaultMetricsMetadataRequest())

		assert.ErrorIs(t, err, user.ErrNoOrgID)
	})

	t.Run("single tenant bypass", func(t *testing.T) {
		upstream := &mockMetadataSupplier{
			results: map[string][]scrape.MetricMetadata{
				"team-a": {fixtureMetadata1},
			},
		}

		supplier := NewMetadataSupplier(upstream, defaultConcurrency, test.NewTestingLogger(t))
		res, err := supplier.MetricsMetadata(user.InjectOrgID(context.Background(), "team-a"), client.DefaultMetricsMetadataRequest())

		require.NoError(t, err)
		require.Len(t, res, 1)
		assert.Contains(t, res, fixtureMetadata1)
	})

	t.Run("multiple tenants no duplicates", func(t *testing.T) {
		upstream := &mockMetadataSupplier{
			results: map[string][]scrape.MetricMetadata{
				"team-a": {fixtureMetadata1},
				"team-b": {fixtureMetadata2},
			},
		}

		supplier := NewMetadataSupplier(upstream, defaultConcurrency, test.NewTestingLogger(t))
		res, err := supplier.MetricsMetadata(user.InjectOrgID(context.Background(), "team-a|team-b"), client.DefaultMetricsMetadataRequest())

		require.NoError(t, err)
		require.Len(t, res, 2)
		assert.Contains(t, res, fixtureMetadata1)
		assert.Contains(t, res, fixtureMetadata2)
	})

	t.Run("multiple tenants with duplicates", func(t *testing.T) {
		upstream := &mockMetadataSupplier{
			results: map[string][]scrape.MetricMetadata{
				"team-a": {fixtureMetadata1},
				"team-b": {fixtureMetadata1, fixtureMetadata2},
			},
		}

		supplier := NewMetadataSupplier(upstream, defaultConcurrency, test.NewTestingLogger(t))
		res, err := supplier.MetricsMetadata(user.InjectOrgID(context.Background(), "team-a|team-b"), client.DefaultMetricsMetadataRequest())

		require.NoError(t, err)
		require.Len(t, res, 2)
		assert.Contains(t, res, fixtureMetadata1)
		assert.Contains(t, res, fixtureMetadata2)
	})
}
