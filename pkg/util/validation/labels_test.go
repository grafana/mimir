// SPDX-License-Identifier: AGPL-3.0-only

package validation

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsValidLabelName(t *testing.T) {
	testCases := []struct {
		name  string
		input string
		want  bool
	}{
		{
			name:  "invalid empty string",
			input: "",
			want:  false,
		},
		{
			name:  "invalid character",
			input: "invalid.character",
			want:  false,
		},
		{
			name:  "invalid UTF-8",
			input: "2H₂ + O₂ ⇌ 2H₂O",
			want:  false,
		},
		{
			name:  "valid label name",
			input: "label_name",
			want:  true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, IsValidLabelName(tc.input))
		})
	}
}

func TestIsValidMetricName(t *testing.T) {
	testCases := []struct {
		name  string
		input string
		want  bool
	}{
		{
			name:  "invalid empty string",
			input: "",
			want:  false,
		},
		{
			name:  "invalid start with number",
			input: "123test",
			want:  false,
		},
		{
			name:  "invalid UTF-8",
			input: "2H₂ + O₂ ⇌ 2H₂O",
			want:  false,
		},
		{
			name:  "valid underscore",
			input: "metric_name",
			want:  true,
		},
		{
			name:  "valid colon",
			input: "metric:name",
			want:  true,
		},
		{
			name:  "valid numbers",
			input: "metric_name_123",
			want:  true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, IsValidMetricName(tc.input))
		})
	}
}
