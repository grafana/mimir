// SPDX-License-Identifier: AGPL-3.0-only

package validation

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestValidationSchemeFlag_UnmarshalYAML(t *testing.T) {
	testCases := []struct {
		name    string
		input   string
		want    model.ValidationScheme
		wantErr bool
	}{
		{
			name:    "invalid",
			input:   `invalid`,
			wantErr: true,
		},
		{
			name:  "empty",
			input: `""`,
			want:  model.UnsetValidation,
		},
		{
			name:  "legacy validation",
			input: `legacy`,
			want:  model.LegacyValidation,
		},
		{
			name:  "utf8 validation",
			input: `utf8`,
			want:  model.UTF8Validation,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var got ValidationSchemeValue
			err := yaml.Unmarshal([]byte(tc.input), &got)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, model.ValidationScheme(got))

			output, err := yaml.Marshal(got)
			require.NoError(t, err)
			require.Equal(t, tc.input, strings.TrimSpace(string(output)))
		})
	}
}

func TestValidationSchemeValue_UnmarshalJSON(t *testing.T) {
	testCases := []struct {
		name    string
		input   string
		want    model.ValidationScheme
		wantErr bool
	}{
		{
			name:    "invalid",
			input:   `invalid`,
			wantErr: true,
		},
		{
			name:  "empty",
			input: `""`,
			want:  model.UnsetValidation,
		},
		{
			name:  "legacy validation",
			input: `"legacy"`,
			want:  model.LegacyValidation,
		},
		{
			name:  "utf8 validation",
			input: `"utf8"`,
			want:  model.UTF8Validation,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var got ValidationSchemeValue
			err := json.Unmarshal([]byte(tc.input), &got)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, model.ValidationScheme(got))

			output, err := json.Marshal(got)
			require.NoError(t, err)
			require.Equal(t, tc.input, string(output))
		})
	}
}

func TestValidationSchemeFlag_Set(t *testing.T) {
	testCases := []struct {
		name    string
		input   string
		want    model.ValidationScheme
		wantErr bool
	}{
		{
			name:    "invalid",
			input:   `invalid`,
			wantErr: true,
		},
		{
			name:  "empty",
			input: ``,
			want:  model.UnsetValidation,
		},
		{
			name:  "legacy validation",
			input: `legacy`,
			want:  model.LegacyValidation,
		},
		{
			name:  "utf8 validation",
			input: `utf8`,
			want:  model.UTF8Validation,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var got ValidationSchemeValue
			err := got.Set(tc.input)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, model.ValidationScheme(got))
		})
	}
}
