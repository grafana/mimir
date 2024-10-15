// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/integration/asserts.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.
//go:build requires_docker

package integration

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsRawMetricsContainingMetricName(t *testing.T) {
	rawMetrics := `
# HELP metric_1 Test
# TYPE metric_1 counter
metric_1{a="b"} 0
metric_1{c="d"} 0

# HELP metric_10 Test
# TYPE metric_10 counter
metric_10 0

# HELP metric_20 Test
# TYPE metric_20 counter
metric_20 0
`

	assert.True(t, isRawMetricsContainingMetricName("metric_1", rawMetrics))
	assert.True(t, isRawMetricsContainingMetricName("metric_10", rawMetrics))
	assert.True(t, isRawMetricsContainingMetricName("metric_20", rawMetrics))
	assert.False(t, isRawMetricsContainingMetricName("metric_2", rawMetrics))
	assert.False(t, isRawMetricsContainingMetricName("metric_200", rawMetrics))
}
