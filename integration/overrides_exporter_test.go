// SPDX-License-Identifier: AGPL-3.0-only
//go:build requires_docker
// +build requires_docker

package integration

import (
	"path/filepath"
	"testing"

	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/e2emimir"
)

const (
	overridesFileName            = "overrides.yaml"
	overridesFileContentTemplate = `
overrides:
  tenant-a:
    ingestion_rate: {{.MaxIngestionRate}}
`
)

func TestOverridesExporterTenantSharding(t *testing.T) {
	limit := 20
	tests := []struct {
		name              string
		flags             map[string]string
		expectedMetricSum int
	}{
		{
			name: "tenant sharding enabled",
			flags: map[string]string{
				"-overrides-exporter.ring.enabled": "true",
			},
			// the limit should only be exported once, i.e. the expected sum of metrics should equal the limit value
			expectedMetricSum: limit,
		},
		{
			name: "tenant sharding disabled",
			flags: map[string]string{
				"-overrides-exporter.ring.enabled": "false",
			},
			// the limit should be exported from both exporters, i.e. the expected sum is twice the limit value
			expectedMetricSum: 2 * limit,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := e2e.NewScenario(networkName)
			require.NoError(t, err)
			defer s.Close()

			require.NoError(t, writeFileToSharedDir(
				s,
				overridesFileName,
				[]byte(buildConfigFromTemplate(overridesFileContentTemplate, map[string]int{"MaxIngestionRate": limit})),
			))

			flags := map[string]string{
				"-runtime-config.file": filepath.Join(e2e.ContainerSharedDir, overridesFileName),
			}

			services, err := newOverridesExporterServices(mergeFlags(flags, tt.flags), s)
			require.NoError(t, err)

			value1, err := getOverrideMetricForTenantFromService("tenant-a", "ingestion_rate", services.e1)
			require.NoError(t, err)
			value2, err := getOverrideMetricForTenantFromService("tenant-a", "ingestion_rate", services.e2)
			require.NoError(t, err)

			require.Equal(t, tt.expectedMetricSum, int(value1+value2))
		})
	}
}

func getOverrideMetricForTenantFromService(tenantName string, limitName string, service *e2emimir.MimirService) (float64, error) {
	sums, err := service.SumMetrics(
		[]string{"cortex_limits_overrides"},
		e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "user", tenantName)),
		e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "limit_name", limitName)),
		e2e.SkipMissingMetrics,
	)
	return sums[0], err
}

type overridesExporterServices struct {
	e1 *e2emimir.MimirService
	e2 *e2emimir.MimirService

	// Dependencies
	consul *e2e.HTTPService
}

func newOverridesExporterServices(flags map[string]string, s *e2e.Scenario) (*overridesExporterServices, error) {
	consul := e2edb.NewConsul()
	err := s.StartAndWaitReady(consul)
	if err != nil {
		return nil, err
	}

	exporter1 := e2emimir.NewOverridesExporter("overrides-exporter-1", consul.NetworkHTTPEndpoint(), flags)
	exporter2 := e2emimir.NewOverridesExporter("overrides-exporter-2", consul.NetworkHTTPEndpoint(), flags)
	err = s.StartAndWaitReady(exporter1, exporter2)
	if err != nil {
		return nil, err
	}

	return &overridesExporterServices{
		e1:     exporter1,
		e2:     exporter2,
		consul: consul,
	}, nil
}
