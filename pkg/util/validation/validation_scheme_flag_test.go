// SPDX-License-Identifier: AGPL-3.0-only

package validation

import (
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
			err := yaml.Unmarshal([]byte(tc.input), &got)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, model.ValidationScheme(got))
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
