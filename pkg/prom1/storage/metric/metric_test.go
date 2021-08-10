// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package metric

import (
	"testing"

	"github.com/prometheus/common/model"
)

func TestMetric(t *testing.T) {
	testMetric := model.Metric{
		"to_delete": "test1",
		"to_change": "test2",
	}

	scenarios := []struct {
		fn  func(*Metric)
		out model.Metric
	}{
		{
			fn: func(cm *Metric) {
				cm.Del("to_delete")
			},
			out: model.Metric{
				"to_change": "test2",
			},
		},
		{
			fn: func(cm *Metric) {
				cm.Set("to_change", "changed")
			},
			out: model.Metric{
				"to_delete": "test1",
				"to_change": "changed",
			},
		},
	}

	for i, s := range scenarios {
		orig := testMetric.Clone()
		cm := &Metric{
			Metric: orig,
			Copied: false,
		}

		s.fn(cm)

		// Test that the original metric was not modified.
		if !orig.Equal(testMetric) {
			t.Fatalf("%d. original metric changed; expected %v, got %v", i, testMetric, orig)
		}

		// Test that the new metric has the right changes.
		if !cm.Metric.Equal(s.out) {
			t.Fatalf("%d. copied metric doesn't contain expected changes; expected %v, got %v", i, s.out, cm.Metric)
		}
	}
}
