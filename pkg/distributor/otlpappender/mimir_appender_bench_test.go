// SPDX-License-Identifier: AGPL-3.0-only

package otlpappender

import (
	"fmt"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/storage"
)

// makeLabels creates a labels.Labels with the given number of labels.
// It always includes __name__ and adds extra labels named "label_N".
func makeLabels(numLabels int) labels.Labels {
	pairs := make([]string, 0, numLabels*2)
	pairs = append(pairs, model.MetricNameLabel, "test_metric")
	for i := 1; i < numLabels; i++ {
		pairs = append(pairs, fmt.Sprintf("label_%03d", i), fmt.Sprintf("value_%03d", i))
	}
	return labels.FromStrings(pairs...)
}

// BenchmarkCreateNewSeries benchmarks the createNewSeries code path via Append,
// covering different label counts.
// In production, typical OTLP series have 5-20 labels (resource + scope + metric attributes + __name__).
func BenchmarkCreateNewSeries(b *testing.B) {
	for _, numLabels := range []int{5, 10, 15, 20, 30, 50} {
		b.Run(fmt.Sprintf("labels=%d", numLabels), func(b *testing.B) {
			ls := makeLabels(numLabels)
			meta := metadata.Metadata{Type: model.MetricTypeGauge, Help: "bench"}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				appender := NewCombinedAppender()
				_, _ = appender.Append(0, ls, 0, 1000, 42.0, nil, nil, storage.AppendV2Options{
					Metadata:         meta,
					MetricFamilyName: "test_metric",
				})
			}
		})
	}
}

// BenchmarkCreateNewSeriesMultiple benchmarks appending many distinct series to
// a single appender, which is the typical production pattern (one appender per
// OTLP write request, many series per request).
func BenchmarkCreateNewSeriesMultiple(b *testing.B) {
	for _, numSeries := range []int{10, 50, 100, 500} {
		for _, numLabels := range []int{10, 20} {
			b.Run(fmt.Sprintf("series=%d/labels=%d", numSeries, numLabels), func(b *testing.B) {
				// Pre-create distinct label sets.
				allLabels := make([]labels.Labels, numSeries)
				for i := 0; i < numSeries; i++ {
					pairs := make([]string, 0, numLabels*2)
					pairs = append(pairs, model.MetricNameLabel, fmt.Sprintf("metric_%d", i))
					for j := 1; j < numLabels; j++ {
						pairs = append(pairs, fmt.Sprintf("label_%03d", j), fmt.Sprintf("value_%03d", j))
					}
					allLabels[i] = labels.FromStrings(pairs...)
				}
				meta := metadata.Metadata{Type: model.MetricTypeGauge, Help: "bench"}

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					appender := NewCombinedAppender()
					for s := 0; s < numSeries; s++ {
						_, _ = appender.Append(0, allLabels[s], 0, 1000, 42.0, nil, nil, storage.AppendV2Options{
							Metadata:         meta,
							MetricFamilyName: allLabels[s].Get(model.MetricNameLabel),
						})
					}
				}
			})
		}
	}
}

// BenchmarkCreateNewSeriesExistingSeries benchmarks the case where most series
// already exist (common case: only a few new series per request).
func BenchmarkCreateNewSeriesExistingSeries(b *testing.B) {
	numLabels := 15
	numExisting := 100
	numNew := 5

	// Pre-create label sets.
	existingLabels := make([]labels.Labels, numExisting)
	for i := 0; i < numExisting; i++ {
		pairs := make([]string, 0, numLabels*2)
		pairs = append(pairs, model.MetricNameLabel, fmt.Sprintf("existing_%d", i))
		for j := 1; j < numLabels; j++ {
			pairs = append(pairs, fmt.Sprintf("label_%03d", j), fmt.Sprintf("value_%03d", j))
		}
		existingLabels[i] = labels.FromStrings(pairs...)
	}
	newLabels := make([]labels.Labels, numNew)
	for i := 0; i < numNew; i++ {
		pairs := make([]string, 0, numLabels*2)
		pairs = append(pairs, model.MetricNameLabel, fmt.Sprintf("new_%d", i))
		for j := 1; j < numLabels; j++ {
			pairs = append(pairs, fmt.Sprintf("label_%03d", j), fmt.Sprintf("value_%03d", j))
		}
		newLabels[i] = labels.FromStrings(pairs...)
	}
	meta := metadata.Metadata{Type: model.MetricTypeGauge, Help: "bench"}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		appender := NewCombinedAppender()
		// Append existing series first (creates them).
		for s := 0; s < numExisting; s++ {
			_, _ = appender.Append(0, existingLabels[s], 0, 1000, 42.0, nil, nil, storage.AppendV2Options{
				Metadata:         meta,
				MetricFamilyName: existingLabels[s].Get(model.MetricNameLabel),
			})
		}
		// Append same existing series again (no new series creation).
		for s := 0; s < numExisting; s++ {
			_, _ = appender.Append(0, existingLabels[s], 0, 2000, 43.0, nil, nil, storage.AppendV2Options{
				Metadata:         meta,
				MetricFamilyName: existingLabels[s].Get(model.MetricNameLabel),
			})
		}
		// Append a few new series.
		for s := 0; s < numNew; s++ {
			_, _ = appender.Append(0, newLabels[s], 0, 1000, 44.0, nil, nil, storage.AppendV2Options{
				Metadata:         meta,
				MetricFamilyName: newLabels[s].Get(model.MetricNameLabel),
			})
		}
	}
}
