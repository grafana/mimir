// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeScenarios(t *testing.T, content string) string {
	path := filepath.Join(t.TempDir(), "scenarios.yaml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

func TestLoadScenarios(t *testing.T) {
	path := writeScenarios(t, `
- name: api_by_pod
  selector: '{__name__="up", job="api"}'
  group_by: pod
  windows: [24h]
  kinds: [recent]
- name: names_under_job
  selector: '{job="db"}'
  limit: 10
`)
	got, err := loadScenarios(path)
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, "pod", got[0].GroupBy)
	assert.Equal(t, []time.Duration{24 * time.Hour}, got[0].lengths)
	assert.Equal(t, `{__name__="up", job="api"}`, selector(got[0].matchers...))
	assert.Equal(t, "__name__", got[1].GroupBy, "group_by defaults to __name__")
	assert.Nil(t, got[1].lengths, "no windows takes -windows")

	for name, tc := range map[string]struct{ content, err string }{
		"unknown field":  {"- {name: a, selector: '{job=\"a\"}', group: x}", "field group not found"},
		"bad selector":   {"- {name: a, selector: '{job=}'}", "selector"},
		"unknown kind":   {"- {name: a, selector: '{job=\"a\"}', kinds: [head]}", "unknown window kind"},
		"bad window":     {"- {name: a, selector: '{job=\"a\"}', windows: [1x]}", "not a positive duration"},
		"duplicate name": {"- {name: a, selector: '{job=\"a\"}'}\n- {name: a, selector: '{job=\"b\"}'}", "used twice"},
		"missing name":   {"- {selector: '{job=\"a\"}'}", "name is required"},
		"negative limit": {"- {name: a, selector: '{job=\"a\"}', limit: -1}", "must not be negative"},
		"empty file":     {"[]", "no scenarios"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := loadScenarios(writeScenarios(t, tc.content))
			require.ErrorContains(t, err, tc.err)
		})
	}
}

func TestScenarioPairs(t *testing.T) {
	path := writeScenarios(t, `
- {name: last_day, selector: '{job="api"}', windows: [24h], kinds: [recent]}
- {name: any, selector: '{job="api"}', group_by: pod, limit: 3}
- {name: no_fit, selector: '{job="api"}', windows: [24h], kinds: [ingesters]}
`)
	scs, err := loadScenarios(path)
	require.NoError(t, err)
	now := time.Date(2026, 1, 2, 12, 0, 30, 0, time.UTC)
	wcfg := windowConfig{lengths: []time.Duration{time.Hour}, queryIngestersWithin: 6 * time.Hour, lag: 5 * time.Minute, blocksMargin: time.Hour}
	ps, unplaced := scenarioPairs(scs, wcfg, now, rand.New(rand.NewPCG(1, 1)))
	assert.Equal(t, []string{"no_fit"}, unplaced, "24h fits no ingester window")
	require.Len(t, ps, 2)

	at := func(h, m int) int64 { return time.Date(2026, 1, 2, h, m, 0, 0, time.UTC).UnixMilli() }
	assert.Equal(t, "scenario:last_day", ps[0].query)
	assert.Equal(t, window{windowRecent, at(11, 55) - 24*time.Hour.Milliseconds(), at(11, 55)}, ps[0].window)
	assert.Equal(t, "scenario:any", ps[1].query)
	assert.Equal(t, "pod", ps[1].groupBy)
	assert.Equal(t, 3, ps[1].limit)
	assert.Equal(t, time.Hour, ps[1].window.length(), "without windows, the -windows lengths")
}

// TestRunScenarios: every scenario runs each round on top of -pairs-per-round drawn pairs.
func TestRunScenarios(t *testing.T) {
	_, srv := newFakeFrontend(t, testSeries()...)
	m := newMetrics(prometheus.NewRegistry(), time.Minute)
	cfg := config{
		address: srv.URL, tenant: "test-tenant", pairsPerRound: 1, timeout: time.Minute,
		queries:       []string{queryMetricTotal},
		windowLengths: []string{"1h"},
		windows:       windowConfig{queryIngestersWithin: 6 * time.Hour, lag: 5 * time.Minute, blocksMargin: time.Hour},
		targets:       targetConfig{names: 3, labels: 3, matcherLabel: "job"},
		seed:          1, fallbackNames: 5,
		scenarioPath: writeScenarios(t, `
- {name: api_by_pod, selector: '{__name__="up", job="api"}', group_by: pod}
- {name: db_names, selector: '{job="db"}', kinds: [blocks]}
`),
	}
	require.NoError(t, cfg.validate())
	now := func() time.Time { return time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC) }
	sum, err := run(context.Background(), cfg, m, log.NewNopLogger(), now)
	require.NoError(t, err)
	assert.False(t, sum.mismatched)
	assert.Equal(t, 1.0, pairsCount(m, queryMetricTotal, outcomePass, classExact))
	assert.Equal(t, 1.0, pairsCount(m, "scenario:api_by_pod", outcomePass, classExact))
	assert.Equal(t, 1.0, counterSum(m.pairs, map[string]string{"query": "scenario:db_names", "window": windowBlocks, "outcome": outcomePass, "class": classExact}))
}
