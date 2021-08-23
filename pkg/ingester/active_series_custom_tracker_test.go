package ingester

import (
	"flag"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestActiveSeriesCustomTrackersConfigs(t *testing.T) {
	for _, tc := range []struct {
		name     string
		flags    []string
		expected ActiveSeriesCustomTrackersConfigs
		error    bool
	}{
		{
			name:  "empty flag value fails",
			flags: []string{`-ingester.active-series-custom-trackers=`},
			error: true,
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
			expected: ActiveSeriesCustomTrackersConfigs{{Name: `foo`, Matcher: `{foo="bar"}`}},
		},
		{
			name: "whitespaces are trimmed from name and matcher",
			flags: []string{`-ingester.active-series-custom-trackers= foo :	{foo="bar"}` + "\n "},
			expected: ActiveSeriesCustomTrackersConfigs{{Name: `foo`, Matcher: `{foo="bar"}`}},
		},
		{
			name:     "two matchers in one flag value",
			flags:    []string{`-ingester.active-series-custom-trackers=foo:{foo="bar"};baz:{baz="bar"}`},
			expected: ActiveSeriesCustomTrackersConfigs{{Name: `foo`, Matcher: `{foo="bar"}`}, {Name: `baz`, Matcher: `{baz="bar"}`}},
		},
		{
			name:     "two matchers in two flag values",
			flags:    []string{`-ingester.active-series-custom-trackers=foo:{foo="bar"}`, `-ingester.active-series-custom-trackers=baz:{baz="bar"}`},
			expected: ActiveSeriesCustomTrackersConfigs{{Name: `foo`, Matcher: `{foo="bar"}`}, {Name: `baz`, Matcher: `{baz="bar"}`}},
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
