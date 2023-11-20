package alertmanager

import (
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/alertmanager/pkg/labels"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestParseMatcher(t *testing.T) {
	tests := []struct {
		name                      string
		input                     string
		expected                  *labels.Matcher
		expectedTotal             float64
		expectedDisagreeTotal     float64
		expectedIncompatibleTotal float64
		expectedInvalidTotal      float64
		err                       string
	}{{
		name:          "is accepted",
		input:         "foo=bar",
		expected:      mustNewMatcher(t, labels.MatchEqual, "foo", "bar"),
		expectedTotal: 1,
	}, {
		name:          "is regex accepted",
		input:         "foo=~bar",
		expected:      mustNewMatcher(t, labels.MatchRegexp, "foo", "bar"),
		expectedTotal: 1,
	}, {
		name:                      "is accepted, but incompatible",
		input:                     "foo=!bar\\n",
		expected:                  mustNewMatcher(t, labels.MatchEqual, "foo", "!bar\n"),
		expectedTotal:             1,
		expectedIncompatibleTotal: 1,
	}, {
		name:                  "is accepted, but with disagreement",
		input:                 "foo=\"\\xf0\\x9f\\x99\\x82\"",
		expected:              mustNewMatcher(t, labels.MatchEqual, "foo", "\\xf0\\x9f\\x99\\x82"),
		expectedTotal:         1,
		expectedDisagreeTotal: 1,
	}, {
		name:                 "is not accepted",
		input:                "foo🙂=bar",
		err:                  "bad matcher format: foo🙂=bar",
		expectedInvalidTotal: 1,
	}, {
		name:                 "is also not accepted",
		input:                "foo!bar",
		err:                  "bad matcher format: foo!bar",
		expectedInvalidTotal: 1,
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			m := NewParseMetrics(prometheus.NewRegistry())
			f := ParseMatcher(log.NewNopLogger(), m)
			matcher, err := f(test.input)
			if test.err != "" {
				require.EqualError(t, err, test.err)
			} else {
				require.Nil(t, err)
				require.EqualValues(t, test.expected, matcher)
			}
			testutil.CollectAndCount(m.Total)
			require.Equal(t, test.expectedTotal, testutil.ToFloat64(m.Total))
			testutil.CollectAndCount(m.DisagreeTotal)
			require.Equal(t, test.expectedDisagreeTotal, testutil.ToFloat64(m.DisagreeTotal))
			testutil.CollectAndCount(m.IncompatibleTotal)
			require.Equal(t, test.expectedIncompatibleTotal, testutil.ToFloat64(m.IncompatibleTotal))
			testutil.CollectAndCount(m.InvalidTotal)
			require.Equal(t, test.expectedInvalidTotal, testutil.ToFloat64(m.InvalidTotal))
		})
	}
}

func TestParseMatchers(t *testing.T) {
	tests := []struct {
		name                      string
		input                     string
		expected                  labels.Matchers
		expectedTotal             float64
		expectedDisagreeTotal     float64
		expectedIncompatibleTotal float64
		expectedInvalidTotal      float64
		err                       string
	}{{
		name:  "is accepted",
		input: "{foo=bar,bar=baz}",
		expected: labels.Matchers{
			mustNewMatcher(t, labels.MatchEqual, "foo", "bar"),
			mustNewMatcher(t, labels.MatchEqual, "bar", "baz"),
		},
		expectedTotal: 1,
	}, {
		name:  "is regeexaccepted",
		input: "{foo=bar,bar=~baz}",
		expected: labels.Matchers{
			mustNewMatcher(t, labels.MatchEqual, "foo", "bar"),
			mustNewMatcher(t, labels.MatchRegexp, "bar", "baz"),
		},
		expectedTotal: 1,
	}, {
		name:  "is accepted, but incompatible",
		input: "{foo=!bar,bar=$baz\\n}",
		expected: labels.Matchers{
			mustNewMatcher(t, labels.MatchEqual, "foo", "!bar"),
			mustNewMatcher(t, labels.MatchEqual, "bar", "$baz\n"),
		},
		expectedTotal:             1,
		expectedIncompatibleTotal: 1,
	}, {
		name:  "is accepted, but with disagreement",
		input: "{foo=\"\\xf0\\x9f\\x99\\x82\"}",
		expected: labels.Matchers{
			mustNewMatcher(t, labels.MatchEqual, "foo", "\\xf0\\x9f\\x99\\x82"),
		},
		expectedTotal:         1,
		expectedDisagreeTotal: 1,
	}, {
		name:                 "is not accepted",
		input:                "{foo🙂=bar,bar=baz🙂}",
		err:                  "bad matcher format: foo🙂=bar",
		expectedInvalidTotal: 1,
	}, {
		name:                 "is also not accepted",
		input:                "{foo!bar}",
		err:                  "bad matcher format: foo!bar",
		expectedInvalidTotal: 1,
	}}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			m := NewParseMetrics(prometheus.NewRegistry())
			f := ParseMatchers(log.NewNopLogger(), m)
			matchers, err := f(test.input)
			if test.err != "" {
				require.EqualError(t, err, test.err)
			} else {
				require.Nil(t, err)
				require.EqualValues(t, test.expected, matchers)
			}
			testutil.CollectAndCount(m.Total)
			require.Equal(t, test.expectedTotal, testutil.ToFloat64(m.Total))
			testutil.CollectAndCount(m.DisagreeTotal)
			require.Equal(t, test.expectedDisagreeTotal, testutil.ToFloat64(m.DisagreeTotal))
			testutil.CollectAndCount(m.IncompatibleTotal)
			require.Equal(t, test.expectedIncompatibleTotal, testutil.ToFloat64(m.IncompatibleTotal))
			testutil.CollectAndCount(m.InvalidTotal)
			require.Equal(t, test.expectedInvalidTotal, testutil.ToFloat64(m.InvalidTotal))
		})
	}
}

func mustNewMatcher(t *testing.T, op labels.MatchType, name, value string) *labels.Matcher {
	m, err := labels.NewMatcher(op, name, value)
	require.NoError(t, err)
	return m
}
