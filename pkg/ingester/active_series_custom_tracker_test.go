// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"flag"
	"testing"

	"github.com/stretchr/testify/require"

	amlabels "github.com/prometheus/alertmanager/pkg/labels"
	"github.com/prometheus/prometheus/pkg/labels"

	"github.com/stretchr/testify/assert"
)

func TestActiveSeriesCustomTrackersConfigs(t *testing.T) {
	for _, tc := range []struct {
		name     string
		flags    []string
		expected ActiveSeriesCustomTrackersConfig
		error    bool
	}{
		{
			name:     "empty flag value produces empty config",
			flags:    []string{`-ingester.active-series-custom-trackers=`},
			expected: nil,
		},
		{
			name:  "empty matcher fails",
			flags: []string{`-ingester.active-series-custom-trackers=foo:`},
			error: true,
		},
		{
			name:  "empty whitespace-only matcher fails",
			flags: []string{`-ingester.active-series-custom-trackers=foo: `},
			error: true,
		},
		{
			name:  "second empty whitespace-only matcher fails",
			flags: []string{`-ingester.active-series-custom-trackers=foo: ;bar:{}`},
			error: true,
		},
		{
			name:  "empty name fails",
			flags: []string{`-ingester.active-series-custom-trackers=:{}`},
			error: true,
		},
		{
			name:  "empty whitespace-only name fails",
			flags: []string{`-ingester.active-series-custom-trackers= :{}`},
			error: true,
		},
		{
			name:     "one matcher",
			flags:    []string{`-ingester.active-series-custom-trackers=foo:{foo="bar"}`},
			expected: ActiveSeriesCustomTrackersConfig{`foo`: `{foo="bar"}`},
		},
		{
			name: "whitespaces are trimmed from name and matcher",
			flags: []string{`-ingester.active-series-custom-trackers= foo :	{foo="bar"}` + "\n "},
			expected: ActiveSeriesCustomTrackersConfig{`foo`: `{foo="bar"}`},
		},
		{
			name:     "two matchers in one flag value",
			flags:    []string{`-ingester.active-series-custom-trackers=foo:{foo="bar"};baz:{baz="bar"}`},
			expected: ActiveSeriesCustomTrackersConfig{`foo`: `{foo="bar"}`, `baz`: `{baz="bar"}`},
		},
		{
			name:     "two matchers in two flag values",
			flags:    []string{`-ingester.active-series-custom-trackers=foo:{foo="bar"}`, `-ingester.active-series-custom-trackers=baz:{baz="bar"}`},
			expected: ActiveSeriesCustomTrackersConfig{`foo`: `{foo="bar"}`, `baz`: `{baz="bar"}`},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			flagSet := flag.NewFlagSet("test", flag.ContinueOnError)

			config := Config{}
			config.RegisterFlags(flagSet)
			err := flagSet.Parse(tc.flags)

			if tc.error {
				assert.Error(t, err)
			} else {
				assert.Equal(t, tc.expected, config.ActiveSeriesCustomTrackers)
			}
		})
	}
}

func TestAmlabelMatchersToProm(t *testing.T) {
	t.Run("MatchType values are the same so we can convert the types", func(t *testing.T) {
		lastType := amlabels.MatchNotRegexp
		// just checking that our assumption on that MatchType enums are the same is correct
		for mt := amlabels.MatchEqual; mt <= lastType; mt++ {
			assert.Equal(t, mt.String(), labels.MatchType(mt).String())
		}
		// and that nobody just added more match types in amlabels,
		assert.Panics(t, func() {
			_ = (lastType + 1).String()
		}, "amlabels.MatchNotRegexp is expected to be the last enum value, update the test and check mapping")
	})

	t.Run("happy case", func(t *testing.T) {
		amMatcher, err := amlabels.NewMatcher(amlabels.MatchRegexp, "foo", "bar.*")
		require.NoError(t, err)

		expected := labels.MustNewMatcher(labels.MatchRegexp, "foo", "bar.*")
		assert.Equal(t, expected, amlabelMatcherToProm(amMatcher))
	})
}
