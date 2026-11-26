// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"strings"
	"testing"

	"github.com/prometheus/common/model"
	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
)

// A native info() lookup of every target, as the run's prober makes: info()'s alternation of every target's instance,
// past maxAcceptedValues and past the 64 KiB a compiled regex or a set is cached for, so that each lookup compiles its
// pattern, over targets of several series each, and as many series without a resource, which the lookup scans too. With
// each of its optional paths, and all of them.
func BenchmarkNativeResourcesCapped(b *testing.B) {
	const targets, seriesPerTarget = 5_000, 4
	var series, withResources []promlabels.Labels
	instances := make([]string, 0, targets)
	for target := range targets {
		instance := fmt.Sprintf("10.0.%d.%d:9100", target/256, target%256)
		instances = append(instances, regexp.QuoteMeta(instance))
		for n := range seriesPerTarget {
			l := promlabels.FromStrings(model.MetricNameLabel, fmt.Sprintf("metric_%d", n), "instance", instance, "job", "node")
			series, withResources = append(series, l), append(withResources, l)
		}
		series = append(series, promlabels.FromStrings(model.MetricNameLabel, "prw_metric", "instance", instance, "job", "node"))
	}
	pattern := "(" + strings.Join(instances, "|") + ")"
	require.Greater(b, len(pattern), maxSizedRegexPattern, "the pattern isn't cached")
	e, err := OpenEngine(b.TempDir(), "tenant", EngineOptions{Shards: 4, OutOfOrderTimeWindowMs: 3_600_000, SecondaryHashFunction: secondaryHash})
	require.NoError(b, err)
	b.Cleanup(func() { _ = e.Close() })
	e.SetNativeResources(true)
	app := e.Appender(context.Background())
	resource := asResourceContext(testResourceAttributes("node-exporter", "node-1"))
	for _, l := range series {
		_, err := app.Append(0, l, 0, 1_000, 1, nil, nil, storage.AppendV2Options{})
		require.NoError(b, err)
	}
	for _, l := range withResources {
		_, err := app.Append(0, l, 0, 2_000, 1, nil, nil, storage.AppendV2Options{Resource: resource})
		require.NoError(b, err)
	}
	require.NoError(b, app.Commit())
	matchers := [][]*promlabels.Matcher{{
		promlabels.MustNewMatcher(promlabels.MatchEqual, "job", "node"),
		promlabels.MustNewMatcher(promlabels.MatchRegexp, "instance", pattern),
	}}

	for name, opts := range map[string]NativeResourcesOptions{
		"regex":    {},
		"set":      {SetMatching: true},
		"unsorted": {Unsorted: true},
		"memo":     {Memo: true},
		"all":      {SetMatching: true, Unsorted: true, Memo: true},
	} {
		b.Run(name, func(b *testing.B) {
			setBefore, _, _ := NativeResourcesPaths()
			for b.Loop() {
				found, _, _, err := e.NativeResourcesCapped(context.Background(), matchers, math.MinInt64, math.MaxInt64, 0, 0, opts)
				if err != nil || len(found) != targets*seriesPerTarget {
					b.Fatalf("found %d series: %v", len(found), err)
				}
			}
			setAfter, _, _ := NativeResourcesPaths()
			// The lookup matched a set exactly when asked to.
			require.Equal(b, opts.SetMatching, setAfter > setBefore)
		})
	}
}
