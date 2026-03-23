// SPDX-License-Identifier: AGPL-3.0-only

package series

import (
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
)

func TestProjectionLabels_Reduce(t *testing.T) {
	tests := map[string]struct {
		retain   []string
		input    []labels.Labels
		expected []labels.Labels
	}{
		"retain __name__ and __series_hash__": {
			retain: []string{"__name__", "__series_hash__"},
			input: []labels.Labels{
				labels.FromMap(map[string]string{
					"__name__":        "http_requests_total",
					"__series_hash__": "abc123",
					"cluster":         "prod-west-1",
					"namespace":       "mimir",
					"pod":             "mimir-1",
				}),
				labels.FromMap(map[string]string{
					"__name__":        "http_requests_total",
					"__series_hash__": "abc456",
					"cluster":         "prod-west-1",
					"namespace":       "mimir",
					"pod":             "mimir-2",
				}),
			},
			expected: []labels.Labels{
				labels.FromMap(map[string]string{
					"__name__":        "http_requests_total",
					"__series_hash__": "abc123",
				}),
				labels.FromMap(map[string]string{
					"__name__":        "http_requests_total",
					"__series_hash__": "abc456",
				}),
			},
		},

		"retain only __name__": {
			retain: []string{"__name__"},
			input: []labels.Labels{
				labels.FromMap(map[string]string{
					"__name__":        "http_requests_total",
					"__series_hash__": "abc123",
					"cluster":         "prod-west-1",
					"namespace":       "mimir",
					"pod":             "mimir-1",
				}),
				labels.FromMap(map[string]string{
					"__name__":        "http_requests_total",
					"__series_hash__": "abc456",
					"cluster":         "prod-west-1",
					"namespace":       "mimir",
					"pod":             "mimir-2",
				}),
			},
			expected: []labels.Labels{
				labels.FromMap(map[string]string{
					"__name__":        "http_requests_total",
					"__series_hash__": "abc123",
				}),
				labels.FromMap(map[string]string{
					"__name__":        "http_requests_total",
					"__series_hash__": "abc456",
				}),
			},
		},

		"retain only __series_hash__": {
			retain: []string{"__series_hash__"},
			input: []labels.Labels{
				labels.FromMap(map[string]string{
					"__name__":        "http_requests_total",
					"__series_hash__": "abc123",
					"cluster":         "prod-west-1",
					"namespace":       "mimir",
					"pod":             "mimir-1",
				}),
				labels.FromMap(map[string]string{
					"__name__":        "http_requests_total",
					"__series_hash__": "abc456",
					"cluster":         "prod-west-1",
					"namespace":       "mimir",
					"pod":             "mimir-2",
				}),
			},
			expected: []labels.Labels{
				labels.FromMap(map[string]string{
					"__name__":        "http_requests_total",
					"__series_hash__": "abc123",
				}),
				labels.FromMap(map[string]string{
					"__name__":        "http_requests_total",
					"__series_hash__": "abc456",
				}),
			},
		},

		"retain normal labels": {
			retain: []string{"__name__", "__series_hash__", "cluster"},
			input: []labels.Labels{
				labels.FromMap(map[string]string{
					"__name__":        "http_requests_total",
					"__series_hash__": "abc123",
					"cluster":         "prod-west-1",
					"namespace":       "mimir",
					"pod":             "mimir-1",
				}),
				labels.FromMap(map[string]string{
					"__name__":        "http_requests_total",
					"__series_hash__": "abc456",
					"cluster":         "prod-west-1",
					"namespace":       "mimir",
					"pod":             "mimir-2",
				}),
			},
			expected: []labels.Labels{
				labels.FromMap(map[string]string{
					"__name__":        "http_requests_total",
					"__series_hash__": "abc123",
					"cluster":         "prod-west-1",
				}),
				labels.FromMap(map[string]string{
					"__name__":        "http_requests_total",
					"__series_hash__": "abc456",
					"cluster":         "prod-west-1",
				}),
			},
		},

		"retain labels before hash": {
			retain: []string{"__name__", "__series_hash__", "cluster"},
			input: []labels.Labels{
				labels.FromMap(map[string]string{
					"\x00\x07__tricky__": "tricky",
					"__name__":           "http_requests_total",
					"__series_hash__":    "abc123",
					"cluster":            "prod-west-1",
					"namespace":          "mimir",
					"pod":                "mimir-1",
				}),
				labels.FromMap(map[string]string{
					"\x00\x07__tricky__": "tricky2",
					"__name__":           "http_requests_total",
					"__series_hash__":    "abc456",
					"cluster":            "prod-west-1",
					"namespace":          "mimir",
					"pod":                "mimir-2",
				}),
			},
			expected: []labels.Labels{
				labels.FromMap(map[string]string{
					"\x00\x07__tricky__": "tricky",
					"__name__":           "http_requests_total",
					"__series_hash__":    "abc123",
					"cluster":            "prod-west-1",
				}),
				labels.FromMap(map[string]string{
					"\x00\x07__tricky__": "tricky2",
					"__name__":           "http_requests_total",
					"__series_hash__":    "abc456",
					"cluster":            "prod-west-1",
				}),
			},
		},
	}

	for testName, testCase := range tests {
		t.Run(testName, func(t *testing.T) {
			proj := NewProjectionLabels(testCase.retain)
			for idx, inp := range testCase.input {
				res := proj.Reduce(inp)
				assert.Equal(t, testCase.expected[idx], res)
			}
		})
	}
}
