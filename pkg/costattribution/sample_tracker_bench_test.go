// SPDX-License-Identifier: AGPL-3.0-only

package costattribution

import (
	"testing"
	"time"

	"github.com/go-kit/log"

	"github.com/grafana/mimir/pkg/costattribution/costattributionmodel"
	"github.com/grafana/mimir/pkg/costattribution/testutils"
	"github.com/grafana/mimir/pkg/mimirpb"
)

func BenchmarkIncrementReceivedSamples(b *testing.B) {
	// Create a sample tracker with a single label
	st, err := newSampleTracker("user1",
		costattributionmodel.Labels{{Input: "team", Output: "team"}},
		100, 5*time.Minute, log.NewNopLogger())
	if err != nil {
		b.Fatal(err)
	}

	// Pre-populate some observations so we hit the fast path (known keys under RLock)
	now := time.Unix(1000, 0)
	for _, team := range []string{"alpha", "beta", "gamma"} {
		st.updateObservations(team, now, 1, 0, nil)
	}

	// Create a write request with 100 timeseries, each belonging to one of 3 teams
	teams := []string{"alpha", "beta", "gamma"}
	series := make([]testutils.Series, 100)
	for i := range series {
		team := teams[i%len(teams)]
		series[i] = testutils.Series{
			LabelValues:  []string{"__name__", "http_requests_total", "team", team, "instance", "localhost:9090"},
			SamplesCount: 1,
		}
	}
	req := testutils.CreateRequest(series)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		st.IncrementReceivedSamples(req, now)
	}
}

func BenchmarkIncrementReceivedSamples_SingleKey(b *testing.B) {
	// All timeseries map to the same attribution key
	st, err := newSampleTracker("user1",
		costattributionmodel.Labels{{Input: "team", Output: "team"}},
		100, 5*time.Minute, log.NewNopLogger())
	if err != nil {
		b.Fatal(err)
	}

	now := time.Unix(1000, 0)
	st.updateObservations("alpha", now, 1, 0, nil)

	series := make([]testutils.Series, 100)
	for i := range series {
		series[i] = testutils.Series{
			LabelValues:  []string{"__name__", "http_requests_total", "team", "alpha", "instance", "localhost:9090"},
			SamplesCount: 1,
		}
	}
	req := testutils.CreateRequest(series)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		st.IncrementReceivedSamples(req, now)
	}
}

func BenchmarkIncrementReceivedSamples_ManyKeys(b *testing.B) {
	// Many unique attribution keys
	st, err := newSampleTracker("user1",
		costattributionmodel.Labels{{Input: "team", Output: "team"}},
		1000, 5*time.Minute, log.NewNopLogger())
	if err != nil {
		b.Fatal(err)
	}

	now := time.Unix(1000, 0)
	teamNames := make([]string, 50)
	for i := range teamNames {
		teamNames[i] = "team-" + string(rune('A'+i))
		st.updateObservations(teamNames[i], now, 1, 0, nil)
	}

	series := make([]testutils.Series, 100)
	for i := range series {
		team := teamNames[i%len(teamNames)]
		series[i] = testutils.Series{
			LabelValues:  []string{"__name__", "http_requests_total", "team", team, "instance", "localhost:9090"},
			SamplesCount: 1,
		}
	}
	req := testutils.CreateRequest(series)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		st.IncrementReceivedSamples(req, now)
	}
}

func BenchmarkIncrementReceivedSamples_TwoLabels(b *testing.B) {
	st, err := newSampleTracker("user1",
		costattributionmodel.Labels{
			{Input: "department", Output: "department"},
			{Input: "service", Output: "service"},
		},
		100, 5*time.Minute, log.NewNopLogger())
	if err != nil {
		b.Fatal(err)
	}

	now := time.Unix(1000, 0)

	// Build some known observation keys
	departments := []string{"eng", "ops", "sales"}
	services := []string{"api", "web"}
	for _, d := range departments {
		for _, s := range services {
			key := d + string(sep) + s
			st.updateObservations(key, now, 1, 0, nil)
		}
	}

	series := make([]testutils.Series, 100)
	for i := range series {
		dept := departments[i%len(departments)]
		svc := services[i%len(services)]
		series[i] = testutils.Series{
			LabelValues:  []string{"__name__", "http_requests_total", "department", dept, "service", svc},
			SamplesCount: 1,
		}
	}
	req := testutils.CreateRequest(series)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		st.IncrementReceivedSamples(req, now)
	}
}

// BenchmarkIncrementDiscardedSamples benchmarks the discard path
func BenchmarkIncrementDiscardedSamples(b *testing.B) {
	st, err := newSampleTracker("user1",
		costattributionmodel.Labels{{Input: "team", Output: "team"}},
		100, 5*time.Minute, log.NewNopLogger())
	if err != nil {
		b.Fatal(err)
	}

	now := time.Unix(1000, 0)
	st.updateObservations("alpha", now, 1, 0, nil)

	lbls := []mimirpb.LabelAdapter{
		{Name: "__name__", Value: "http_requests_total"},
		{Name: "team", Value: "alpha"},
		{Name: "instance", Value: "localhost:9090"},
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		st.IncrementDiscardedSamples(lbls, 1, "invalid-metrics-name", now)
	}
}
