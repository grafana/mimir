// SPDX-License-Identifier: AGPL-3.0-only

package alertmanager

import (
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/alertmanager/matchers/compat"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/alertmanager/alertspb"
)

func TestValidateMatchersInConfigDesc(t *testing.T) {
	tests := []struct {
		name              string
		config            alertspb.AlertConfigDesc
		total             float64
		disagreeTotal     float64
		incompatibleTotal float64
		invalidTotal      float64
	}{{
		name: "config is accepted",
		config: alertspb.AlertConfigDesc{
			RawConfig: `
route:
  routes:
   - matchers:
      - foo=bar
inhibit_rules:
  - source_matchers:
    - bar=baz
  - target_matchers:
    - baz=qux
`,
		},
		total: 3,
	}, {
		name: "config contains invalid input",
		config: alertspb.AlertConfigDesc{
			RawConfig: `
route:
  routes:
   - matchers:
      - foo!bar
inhibit_rules:
  - source_matchers:
    - bar!baz
  - target_matchers:
    - baz!qux
`,
		},
		total:        3,
		invalidTotal: 3,
	}, {
		name: "config is accepted in matchers/parse but not pkg/labels",
		config: alertspb.AlertConfigDesc{
			RawConfig: `
route:
  routes:
   - matchers:
      - foo🙂=bar
inhibit_rules:
  - source_matchers:
    - bar🙂=baz
  - target_matchers:
    - baz🙂=qux
`,
		},
		total: 3,
	}, {
		name: "config is accepted in pkg/labels but not matchers/parse",
		config: alertspb.AlertConfigDesc{
			RawConfig: `
route:
  routes:
   - matchers:
      - foo=
inhibit_rules:
  - source_matchers:
    - bar=
  - target_matchers:
    - baz=
`,
		},
		total:             3,
		incompatibleTotal: 3,
	}, {
		name: "config contains disagreement",
		config: alertspb.AlertConfigDesc{
			RawConfig: `
route:
  routes:
   - matchers:
      - foo="\xf0\x9f\x99\x82"
inhibit_rules:
  - source_matchers:
    - bar="\xf0\x9f\x99\x82"
  - target_matchers:
    - baz="\xf0\x9f\x99\x82"
`,
		},
		total:         3,
		disagreeTotal: 3,
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			m := compat.NewMetrics(prometheus.NewRegistry())
			validateMatchersInConfigDesc(log.NewNopLogger(), m, "test", test.config)
			requireMetric(t, test.total, m.Total)
			requireMetric(t, test.disagreeTotal, m.DisagreeTotal)
			requireMetric(t, test.incompatibleTotal, m.IncompatibleTotal)
			requireMetric(t, test.invalidTotal, m.InvalidTotal)
		})
	}
}

func requireMetric(t *testing.T, expected float64, m *prometheus.CounterVec) {
	if expected == 0 {
		require.Equal(t, 0, testutil.CollectAndCount(m))
	} else {
		require.Equal(t, 1, testutil.CollectAndCount(m))
		require.Equal(t, expected, testutil.ToFloat64(m))
	}
}
