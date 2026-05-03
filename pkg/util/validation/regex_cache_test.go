// SPDX-License-Identifier: AGPL-3.0-only

package validation

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// TestRelabelRegexCacheReusesCompiledRegexp asserts the global regex cache in
// vendor/.../prometheus/model/relabel/relabel.go is engaged by Limits.UnmarshalYAML —
// i.e. two unmarshals of the same metric_relabel_configs pattern produce the
// same *regexp.Regexp pointer instead of recompiling.
//
// This is the externally-observable contract of M2: "no recompile of the same
// pattern across runtime-config reloads". If this test fails, callers that
// poll runtime-config (every 10 s by default) are paying the regex-compile
// cost on every poll cycle.
func TestRelabelRegexCacheReusesCompiledRegexp(t *testing.T) {
	const inp = `
metric_relabel_configs:
- action: drop
  source_labels: [le]
  regex: super-unique-pattern-for-test-[a-z0-9]+
`
	var l1, l2 Limits

	dec := yaml.NewDecoder(strings.NewReader(inp))
	dec.KnownFields(true)
	require.NoError(t, dec.Decode(&l1))

	dec = yaml.NewDecoder(strings.NewReader(inp))
	dec.KnownFields(true)
	require.NoError(t, dec.Decode(&l2))

	require.Len(t, l1.MetricRelabelConfigs, 1)
	require.Len(t, l2.MetricRelabelConfigs, 1)

	// Same compiled regex instance across two independent unmarshal calls.
	// Comparing the embedded *regexp.Regexp pointer is the cache evidence:
	// without the cache the second unmarshal would produce a freshly-compiled
	// distinct pointer.
	r1 := l1.MetricRelabelConfigs[0].Regex.Regexp
	r2 := l2.MetricRelabelConfigs[0].Regex.Regexp
	require.NotNil(t, r1)
	require.NotNil(t, r2)
	assert.Same(t, r1, r2, "second unmarshal should re-use the cached compiled regex")
}

// TestRelabelRegexCacheDistinctPatternsCompileSeparately is the negative
// guard: distinct patterns must still produce distinct compiled regexes (no
// false unification). This catches the failure mode where a too-aggressive
// cache key collapses different patterns onto the same compiled object.
func TestRelabelRegexCacheDistinctPatternsCompileSeparately(t *testing.T) {
	const inp = `
metric_relabel_configs:
- action: drop
  source_labels: [le]
  regex: alpha-pattern-[a-z]+
- action: drop
  source_labels: [le]
  regex: beta-pattern-[a-z]+
`
	var l Limits

	dec := yaml.NewDecoder(strings.NewReader(inp))
	dec.KnownFields(true)
	require.NoError(t, dec.Decode(&l))

	require.Len(t, l.MetricRelabelConfigs, 2)
	r1 := l.MetricRelabelConfigs[0].Regex.Regexp
	r2 := l.MetricRelabelConfigs[1].Regex.Regexp
	require.NotNil(t, r1)
	require.NotNil(t, r2)
	assert.NotSame(t, r1, r2, "different patterns must produce different compiled regexes")
}
