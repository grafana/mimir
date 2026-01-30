// SPDX-License-Identifier: AGPL-3.0-only

package validation

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/prometheus/otlptranslator"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

func TestOTelTranslationStrategyValue_UnmarshalYAML(t *testing.T) {
	testCases := []struct {
		name    string
		input   string
		want    otlptranslator.TranslationStrategyOption
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
			want:  otlptranslator.TranslationStrategyOption(""),
		},
		{
			name:  "underscore escaping without suffixes",
			input: "UnderscoreEscapingWithoutSuffixes",
			want:  otlptranslator.UnderscoreEscapingWithoutSuffixes,
		},
		{
			name:  "underscore escaping with suffixes",
			input: "UnderscoreEscapingWithSuffixes",
			want:  otlptranslator.UnderscoreEscapingWithSuffixes,
		},
		{
			name:  "No UTF-8 escaping with suffixes",
			input: "NoUTF8EscapingWithSuffixes",
			want:  otlptranslator.NoUTF8EscapingWithSuffixes,
		},
		{
			name:  "No translation",
			input: "NoTranslation",
			want:  otlptranslator.NoTranslation,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var got OTelTranslationStrategyValue
			err := yaml.Unmarshal([]byte(tc.input), &got)
			if tc.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.want, otlptranslator.TranslationStrategyOption(got))

			output, err := yaml.Marshal(got)
			require.NoError(t, err)
			require.Equal(t, tc.input, strings.TrimSpace(string(output)))
		})
	}
}

func TestOTelTranslationStrategyValue_UnmarshalJSON(t *testing.T) {
	testCases := []struct {
		name    string
		input   string
		want    otlptranslator.TranslationStrategyOption
		wantErr bool
	}{
		{
			name:    "invalid",
			input:   `"invalid"`,
			wantErr: true,
		},
		{
			name:  "empty",
			input: `""`,
			want:  otlptranslator.TranslationStrategyOption(""),
		},
		{
			name:  "underscore escaping without suffixes",
			input: `"UnderscoreEscapingWithoutSuffixes"`,
			want:  otlptranslator.UnderscoreEscapingWithoutSuffixes,
		},
		{
			name:  "underscore escaping with suffixes",
			input: `"UnderscoreEscapingWithSuffixes"`,
			want:  otlptranslator.UnderscoreEscapingWithSuffixes,
		},
		{
			name:  "No UTF-8 escaping with suffixes",
			input: `"NoUTF8EscapingWithSuffixes"`,
			want:  otlptranslator.NoUTF8EscapingWithSuffixes,
		},
		{
			name:  "No translation",
			input: `"NoTranslation"`,
			want:  otlptranslator.NoTranslation,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var got OTelTranslationStrategyValue
			err := json.Unmarshal([]byte(tc.input), &got)
			if tc.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.want, otlptranslator.TranslationStrategyOption(got))

			output, err := json.Marshal(got)
			require.NoError(t, err)
			require.Equal(t, tc.input, string(output))
		})
	}
}

func TestOTelTranslationStrategyValue_Set(t *testing.T) {
	testCases := []struct {
		name    string
		input   string
		want    otlptranslator.TranslationStrategyOption
		wantErr bool
	}{
		{
			name:    "invalid",
			input:   `invalid`,
			wantErr: true,
		},
		{
			name:  "empty",
			input: "",
			want:  otlptranslator.TranslationStrategyOption(""),
		},
		{
			name:  "Underscore escaping without suffixes",
			input: "UnderscoreEscapingWithoutSuffixes",
			want:  otlptranslator.UnderscoreEscapingWithoutSuffixes,
		},
		{
			name:  "Underscore escaping with suffixes",
			input: "UnderscoreEscapingWithSuffixes",
			want:  otlptranslator.UnderscoreEscapingWithSuffixes,
		},
		{
			name:  "No UTF-8 escaping with suffixes",
			input: "NoUTF8EscapingWithSuffixes",
			want:  otlptranslator.NoUTF8EscapingWithSuffixes,
		},
		{
			name:  "No translation",
			input: "NoTranslation",
			want:  otlptranslator.NoTranslation,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var got OTelTranslationStrategyValue
			err := got.Set(tc.input)
			if tc.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.want, otlptranslator.TranslationStrategyOption(got))
		})
	}
}
