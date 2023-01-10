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
	overridesFileName    = "overrides.yaml"
	overridesFileContent = `
overrides:
  tenant-a:
    ingestion_rate: 20
`
)

func TestOverridesExporterTenantSharding(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	require.NoError(t, writeFileToSharedDir(s, overridesFileName, []byte(overridesFileContent)))

	consul := e2edb.NewConsul()
	require.NoError(t, s.StartAndWaitReady(consul))
	flags := map[string]string{
		"-overrides-exporter.ring.enabled": "true",
		"-runtime-config.file":             filepath.Join(e2e.ContainerSharedDir, overridesFileName),
	}

	exporter1 := e2emimir.NewOverridesExporter("overrides-exporter-1", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.StartAndWaitReady(exporter1))
	exporter2 := e2emimir.NewOverridesExporter("overrides-exporter-2", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.StartAndWaitReady(exporter2))

	metrics := []string{"cortex_limits_overrides"}
	opts := []e2e.MetricsOption{
		e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "user", "tenant-a")),
		e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "limit_name", "ingestion_rate")),
		e2e.SkipMissingMetrics,
	}

	valueExporter1, err := exporter1.SumMetrics(
		metrics,
		opts...,
	)
	require.NoError(t, err)
	require.Equal(t, 1, len(valueExporter1))

	valueExporter2, err := exporter2.SumMetrics(
		metrics,
		opts...,
	)
	require.NoError(t, err)
	require.Equal(t, 1, len(valueExporter2))
	require.Equal(t, float64(20), valueExporter1[0]+valueExporter2[0])
}
