// SPDX-License-Identifier: AGPL-3.0-only

package streaminglabelvalues

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewParamsAcceptsValid(t *testing.T) {
	tests := []struct {
		name string
		p    *Params
	}{
		{name: "empty terms accepted", p: &Params{}},
		{name: "single term", p: &Params{Terms: []string{"foo"}}},
		{name: "multi term", p: &Params{Terms: []string{"foo", "bar"}}},
		{name: "case sensitive", p: &Params{Terms: []string{"FOO"}, CaseSensitive: true}},
		{name: "jaro winkler", p: &Params{Terms: []string{"foo"}, FuzzAlg: FuzzAlgJaroWinkler}},
		{name: "threshold low edge", p: &Params{Terms: []string{"foo"}, FuzzThreshold: 0}},
		{name: "threshold high edge", p: &Params{Terms: []string{"foo"}, FuzzThreshold: 100}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NewParams(tc.p.Terms, tc.p.CaseSensitive, tc.p.FuzzAlg, tc.p.FuzzThreshold)
			require.NoError(t, err)
			require.NotNil(t, got)
			assert.Equal(t, tc.p, got)
		})
	}
}

func TestNewParamsRejectsInvalid(t *testing.T) {
	tests := []struct {
		name      string
		terms     []string
		caseSens  bool
		alg       FuzzAlg
		threshold int
		wantMsg   string
	}{
		{name: "empty term in middle", terms: []string{"a", "", "b"}, wantMsg: "search term 1 is empty"},
		{name: "empty single term", terms: []string{""}, wantMsg: "search term 0 is empty"},
		{name: "threshold above 100", terms: []string{"a"}, threshold: 150, wantMsg: "fuzz threshold 150 out of [0,100]"},
		{name: "threshold below 0", terms: []string{"a"}, threshold: -1, wantMsg: "fuzz threshold -1 out of [0,100]"},
		{name: "unknown fuzz alg", terms: []string{"a"}, alg: FuzzAlg(99), wantMsg: "unknown fuzz algorithm 99"},
		// Even with no terms (which would otherwise yield a nil filter at
		// BuildFilter), out-of-range fields are caught here so wire input
		// cannot reach BuildFilter in an invalid state.
		{name: "bad threshold no terms", threshold: 150, wantMsg: "fuzz threshold 150 out of [0,100]"},
		{name: "bad alg no terms", alg: FuzzAlg(99), wantMsg: "unknown fuzz algorithm 99"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NewParams(tc.terms, tc.caseSens, tc.alg, tc.threshold)
			require.Error(t, err)
			assert.Nil(t, got)
			assert.Contains(t, err.Error(), tc.wantMsg)
		})
	}
}
