// SPDX-License-Identifier: AGPL-3.0-only

package trackers

import (
	"encoding/json"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/seriesstore/labels"
)

func pairs(kv ...string) labels.Pairs {
	var p labels.Pairs
	for i := 0; i < len(kv); i += 2 {
		p = append(p, [2]string{kv[i], kv[i+1]})
	}
	sort.Slice(p, func(i, j int) bool { return p[i][0] < p[j][0] })
	return p
}

func TestParsesTrackerMatchers(t *testing.T) {
	matchers, err := ParseMatchers(`{telemetry_sdk_name="beyla", __name__=~"a|b", job!~"x.*",z!="",}`)
	require.NoError(t, err)
	require.Len(t, matchers, 4)
	require.True(t, matchers[1].Matches("a"))
	require.False(t, matchers[1].Matches("ab"))
	require.True(t, matchers[2].Matches("y"))
	require.False(t, matchers[2].Matches("xy"))
	_, err = ParseMatchers(`{a="b`)
	require.Error(t, err)
	_, err = ParseMatchers(`{a<"b"}`)
	require.Error(t, err)
	unquoted, err := ParseMatchers("a=b, c=~d.*")
	require.NoError(t, err)
	require.True(t, unquoted[0].Matches("b"))
	require.True(t, unquoted[1].Matches("dz"))
	escaped, err := ParseMatchers(`{a="x\"y"}`)
	require.NoError(t, err)
	require.True(t, escaped[0].Matches(`x"y`))
}

func TestParsesTheCustomTrackersFlag(t *testing.T) {
	got, err := ParseFlag(`a/b:{x="1"};c:{__name__=~"y|z"}`)
	require.NoError(t, err)
	require.Equal(t, [2]string{"a/b", `{x="1"}`}, got[0])
	require.Len(t, got, 2)
	_, err = ParseFlag("nocolon")
	require.Error(t, err)
}

func TestIndexedAndUnindexedTrackersMatchLikeAFullScan(t *testing.T) {
	trackers, err := NewCustomTrackers(map[string]string{
		"eq":          `{job="api"}`,
		"alternation": `{__name__=~"up|down"}`,
		"regex":       `{__name__=~"up.*"}`,
		"negative":    `{job!="api"}`,
		"empty":       `{missing=""}`,
		"two":         `{job="api", __name__="up"}`,
	})
	require.NoError(t, err)
	for _, series := range []labels.Pairs{
		pairs("__name__", "up", "job", "api"),
		pairs("__name__", "down", "job", "web"),
		pairs("__name__", "upper", "missing", "x"),
	} {
		var expected []uint16
		for index := range trackers.matchers {
			all := true
			for i := range trackers.matchers[index] {
				m := &trackers.matchers[index][i]
				all = all && m.Matches(series.Get(m.Name))
			}
			if all {
				expected = append(expected, uint16(index))
			}
		}
		require.Equal(t, expected, trackers.Matching(series), "%v", series)
	}
	var names []string
	for _, index := range trackers.Matching(pairs("__name__", "up", "job", "api")) {
		names = append(names, trackers.Names()[index])
	}
	require.Equal(t, []string{"alternation", "empty", "eq", "regex", "two"}, names)
	// Stored labels match like pairs.
	require.Equal(t, trackers.Matching(pairs("__name__", "up", "job", "api")),
		trackers.Matching(labels.FromStrings("__name__", "up", "job", "api")))
}

func TestMergesAdditionalTrackersOverBase(t *testing.T) {
	base, err := NewCustomTrackers(map[string]string{"a": `{x="1"}`, "b": `{x="2"}`})
	require.NoError(t, err)
	extra, err := NewCustomTrackers(map[string]string{"b": `{x="3"}`})
	require.NoError(t, err)
	merged := base.Merged(extra)
	require.Equal(t, []string{"a", "b"}, merged.Names())
	require.Equal(t, []uint16{1}, merged.Matching(pairs("x", "3")))
}

func TestParsesCostAttributionTrackers(t *testing.T) {
	var value any
	require.NoError(t, json.Unmarshal([]byte(`{"source-reservation": {"internal": true, "labels": [
		{"input": "__grafana_meta_source__", "output": "source"},
		{"input": "team"}]}}`), &value))
	trackers, err := CostAttributionTrackersFromValue(value)
	require.NoError(t, err)
	tracker := trackers.Trackers[0]
	require.True(t, tracker.Internal)
	require.Equal(t, "team", tracker.Labels[1].Output)
	require.Equal(t, []string{"k6", MissingValue}, tracker.Key(pairs("__grafana_meta_source__", "k6")))
}
