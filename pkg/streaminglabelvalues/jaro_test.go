// SPDX-License-Identifier: AGPL-3.0-only

package streaminglabelvalues

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestJaroMatcher_Similarity(t *testing.T) {
	tests := []struct {
		name      string
		reference string
		candidate string
		expected  float64
	}{
		// Metric name pairs that exercise different similarity levels.
		{
			// Near-identical: adjacent characters transposed (common typo).
			name:      "http_requsets_total / http_requests_total",
			reference: "http_requsets_total",
			candidate: "http_requests_total",
			expected:  0.9825,
		},
		{
			// Partial match: same family of metric, different subsystem.
			name:      "cortex_ingester_active_series / cortex_querier_active_series",
			reference: "cortex_ingester_active_series",
			candidate: "cortex_querier_active_series",
			expected:  0.8383,
		},
		{
			// Low similarity: unrelated metrics from different subsystems.
			name:      "go_goroutines / cortex_distributor_samples_in_total",
			reference: "go_goroutines",
			candidate: "cortex_distributor_samples_in_total",
			expected:  0.5017,
		},
		// Edge cases.
		{
			name:      "identical strings",
			reference: "http_requests_total",
			candidate: "http_requests_total",
			expected:  1.0,
		},
		{
			name:      "completely different",
			reference: "abc",
			candidate: "xyz",
			expected:  0.0,
		},
		{
			name:      "both empty",
			reference: "",
			candidate: "",
			expected:  1.0,
		},
		{
			name:      "reference empty",
			reference: "",
			candidate: "http_requests_total",
			expected:  0.0,
		},
		{
			name:      "candidate empty",
			reference: "http_requests_total",
			candidate: "",
			expected:  0.0,
		},
		{
			name:      "single character match",
			reference: "a",
			candidate: "a",
			expected:  1.0,
		},
		{
			name:      "single character no match",
			reference: "a",
			candidate: "b",
			expected:  0.0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := NewJaroMatcher(tc.reference)
			got := m.Similarity(tc.candidate)
			assert.InDelta(t, tc.expected, got, 0.001)
		})
	}
}

func TestJaroMatcher_ReuseAcrossCandidates(t *testing.T) {
	// Constructing once and calling Similarity multiple times should give the
	// same results as constructing a fresh matcher each time.
	reference := "http_requests_total"
	candidates := []string{
		"http_requests_total",
		"http_request_duration_seconds",
		"go_goroutines",
		"process_cpu_seconds_total",
		"cortex_ingester_active_series",
		"",
	}

	m := NewJaroMatcher(reference)
	for _, c := range candidates {
		got := m.Similarity(c)
		want := NewJaroMatcher(reference).Similarity(c)
		assert.Equal(t, want, got, "mismatch for candidate %q", c)
	}
}

func TestWinklerBoost(t *testing.T) {
	tests := []struct {
		name     string
		s1       string
		s2       string
		p        float64
		expected float64
	}{
		{
			// Near-identical metric names with a transposition typo: jaro ≈ 0.9825,
			// common prefix "http" (l=4, capped), boosted ≈ 0.9825 + 4*0.1*0.0175 ≈ 0.9895.
			name:     "http_requsets_total / http_requests_total with p=0.1",
			s1:       "http_requsets_total",
			s2:       "http_requests_total",
			p:        0.1,
			expected: 0.9895,
		},
		{
			// No common prefix: boost is zero, score unchanged.
			name:     "no common prefix",
			s1:       "go_goroutines",
			s2:       "process_cpu_seconds_total",
			p:        0.1,
			expected: 0.4785,
		},
		{
			// Identical strings: jaro=1, boost adds nothing (1-1=0).
			name:     "identical strings",
			s1:       "http_requests_total",
			s2:       "http_requests_total",
			p:        0.1,
			expected: 1.0,
		},
		{
			// Prefix longer than 4 is capped at 4.
			// cortex_ingester_memory_series / cortex_ingester_memory_users share
			// a prefix far longer than 4, so the cap kicks in.
			name:     "long common prefix capped at 4",
			s1:       "cortex_ingester_memory_series",
			s2:       "cortex_ingester_memory_users",
			p:        0.1,
			expected: 0.0, // placeholder; checked below via formula
		},
		{
			// p=0 means no boost regardless of prefix.
			name:     "p=0 no boost",
			s1:       "http_requsets_total",
			s2:       "http_requests_total",
			p:        0.0,
			expected: 0.9825,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			jaro := NewJaroMatcher(tc.s1).Similarity(tc.s2)
			got := WinklerBoost(jaro, tc.s1, tc.s2, tc.p)

			switch tc.name {
			case "long common prefix capped at 4":
				// Verify the cap: prefix "cort" (l=4), then compute expected manually.
				want := jaro + 4*tc.p*(1-jaro)
				assert.InDelta(t, want, got, 0.001)
			default:
				assert.InDelta(t, tc.expected, got, 0.001)
			}
		})
	}
}

func TestWinklerBoost_NeverExceedsOne(t *testing.T) {
	// For any reasonable inputs with p≤0.25 (the standard upper bound),
	// the boosted score should remain ≤ 1.
	pairs := [][2]string{
		{"cortex_ingester_memory_series", "cortex_ingester_memory_users"},
		{"http_requests_total", "http_requests_total"},
		{"go_goroutines", "go_goroutines_count"},
		{"cortex_ingester_active_series", "cortex_querier_active_series"},
	}
	for _, pair := range pairs {
		jaro := NewJaroMatcher(pair[0]).Similarity(pair[1])
		boosted := WinklerBoost(jaro, pair[0], pair[1], 0.1)
		assert.LessOrEqual(t, boosted, 1.0+1e-9, "boosted score exceeded 1 for %v/%v", pair[0], pair[1])
		assert.False(t, math.IsNaN(boosted), "NaN for %v/%v", pair[0], pair[1])
	}
}
