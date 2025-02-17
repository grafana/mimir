// SPDX-License-Identifier: AGPL-3.0-only

package minisdk_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/grafana/mimir/pkg/mimirtool/minisdk"
)

func TestPanel_SupportsTargets(t *testing.T) {
	tests := map[string]bool{
		"graph":                  true,
		"table":                  true,
		"text":                   false,
		"singlestat":             true,
		"stat":                   true,
		"dashlist":               false,
		"bargauge":               true,
		"heatmap":                true,
		"timeseries":             true,
		"row":                    false,
		"gauge":                  true,
		"grafana-polystat-panel": true,
		"barchart":               true,
		"piechart":               true,
		"state-timeline":         true,
		"status-history":         true,
		"histogram":              true,
		"candlestick":            true,
		"canvas":                 true,
		"flamegraph":             true,
		"geomap":                 true,
		"nodeGraph":              true,
		"trend":                  true,
		"xychart":                true,
	}

	for give, want := range tests {
		t.Run(give, func(t *testing.T) {
			t.Parallel()
			p := minisdk.Panel{
				Type: give,
			}
			assert.Equal(t, want, p.SupportsTargets())
		})
	}
}
