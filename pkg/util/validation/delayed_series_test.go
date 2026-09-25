// SPDX-License-Identifier: AGPL-3.0-only

package validation

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestDelayedSeriesConfig_IsDelayed(t *testing.T) {
	cfg := DelayedSeriesConfig{
		{Match: `{__name__="http_request_duration_seconds_bucket"}`, Except: []string{`{cluster="hot"}`, `{cluster=~"prod-us-.*", namespace="api"}`}},
		{Match: `{job="batch"}`},
	}
	require.NoError(t, cfg.Validate())

	lbls := func(kv ...string) []mimirpb.LabelAdapter {
		out := make([]mimirpb.LabelAdapter, 0, len(kv)/2)
		for i := 0; i < len(kv); i += 2 {
			out = append(out, mimirpb.LabelAdapter{Name: kv[i], Value: kv[i+1]})
		}
		return out
	}

	tests := map[string]struct {
		series   []mimirpb.LabelAdapter
		expected bool
	}{
		"matches rule":                    {lbls("__name__", "http_request_duration_seconds_bucket", "cluster", "cold"), true},
		"matches single exception":        {lbls("__name__", "http_request_duration_seconds_bucket", "cluster", "hot"), false},
		"matches multi-matcher exception": {lbls("__name__", "http_request_duration_seconds_bucket", "cluster", "prod-us-east-0", "namespace", "api"), false},
		"partially matches exception":     {lbls("__name__", "http_request_duration_seconds_bucket", "cluster", "prod-us-east-0", "namespace", "web"), true},
		"matches second rule":             {lbls("__name__", "anything", "job", "batch"), true},
		"matches no rule":                 {lbls("__name__", "other", "cluster", "cold"), false},
		"missing label compares as empty": {lbls("__name__", "other"), false},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.expected, cfg.IsDelayed(tc.series))
		})
	}
}

func TestDelayedSeriesConfig_UnmarshalYAML(t *testing.T) {
	SetDefaultLimitsForYAMLUnmarshalling(getDefaultLimits())

	t.Run("valid", func(t *testing.T) {
		var l Limits
		require.NoError(t, yaml.Unmarshal([]byte(`
delayed_series:
  - match: '{__name__="m"}'
    except: ['{cluster="hot"}']
`), &l))
		require.True(t, l.DelayedSeries.IsDelayed([]mimirpb.LabelAdapter{{Name: "__name__", Value: "m"}, {Name: "cluster", Value: "cold"}}))
		require.False(t, l.DelayedSeries.IsDelayed([]mimirpb.LabelAdapter{{Name: "__name__", Value: "m"}, {Name: "cluster", Value: "hot"}}))
	})

	t.Run("invalid match", func(t *testing.T) {
		var l Limits
		err := yaml.Unmarshal([]byte(`
delayed_series:
  - match: 'not a selector{'
`), &l)
		require.ErrorContains(t, err, `delayed_series[0]: invalid match selector "not a selector{"`)
	})

	t.Run("invalid except", func(t *testing.T) {
		var l Limits
		err := yaml.Unmarshal([]byte(`
delayed_series:
  - match: '{__name__="m"}'
    except: ['{cluster=}']
`), &l)
		require.ErrorContains(t, err, `delayed_series[0].except[0]: invalid selector "{cluster=}"`)
	})
}
