// SPDX-License-Identifier: AGPL-3.0-only

package explore

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/prompb"
)

const (
	counterMetricName = "mimir_continuous_test_explore_native_histogram_counter"
	gaugeMetricName   = "mimir_continuous_test_explore_native_histogram_gauge"

	labelSampleType   = "sample_type"
	labelKind         = "kind"
	labelSchema       = "schema"
	labelScenario     = "scenario"
	labelScenarioSeed = "scenario_seed"
	labelSeriesID     = "series_id"
	labelSeed         = "seed"
	labelPace         = "pace"
	labelProtocol     = "protocol"

	zeroThreshold = 0x1p-128
)

// Scenario is one combination of dimension values, written to its own series for its lifetime.
type Scenario struct {
	Slot  int64
	Start time.Time
	// Seed determines the bucket layout and values of every series in the scenario.
	Seed uint64

	// Dimensions holds the dimension values of the scenario as labels, in the order of dimensions.
	Dimensions []prompb.Label

	SampleType SampleType
	Kind       Kind
	Schema     int32
}

func newScenario(slot int64, start time.Time, seed uint64, dims []dimension, values []string) (Scenario, error) {
	s := Scenario{Slot: slot, Start: start, Seed: seed}
	for i, d := range dims {
		v := values[i]
		s.Dimensions = append(s.Dimensions, prompb.Label{Name: d.name, Value: v})
		switch d.name {
		case labelSampleType:
			s.SampleType = SampleType(v)
		case labelKind:
			s.Kind = Kind(v)
		case labelSchema:
			schema, err := strconv.ParseInt(v, 10, 32)
			if err != nil {
				return Scenario{}, fmt.Errorf("parsing schema %q: %w", v, err)
			}
			s.Schema = int32(schema)
		default:
			return Scenario{}, fmt.Errorf("unknown dimension %q", d.name)
		}
	}
	return s, nil
}

func (s Scenario) String() string {
	parts := make([]string, 0, len(s.Dimensions))
	for _, l := range s.Dimensions {
		parts = append(parts, l.Name+"="+l.Value)
	}
	return fmt.Sprintf("slot=%d seed=%d %s", s.Slot, s.Seed, strings.Join(parts, " "))
}

func (s Scenario) MetricName() string {
	if s.Kind == KindGauge {
		return gaugeMetricName
	}
	return counterMetricName
}

func (s Scenario) Metadata() prompb.MetricMetadata {
	if s.Kind == KindGauge {
		return prompb.MetricMetadata{
			Type:             prompb.MetricMetadata_GAUGEHISTOGRAM,
			MetricFamilyName: gaugeMetricName,
			Help:             "A native histogram gauge written by the continuous-test explore test.",
		}
	}
	return prompb.MetricMetadata{
		Type:             prompb.MetricMetadata_HISTOGRAM,
		MetricFamilyName: counterMetricName,
		Help:             "A native histogram counter written by the continuous-test explore test.",
	}
}

// SeriesLabels returns the sorted labels of the given series. commonLabels identify the test
// instance writing the scenario.
func (s Scenario) SeriesLabels(series int, commonLabels []prompb.Label) []prompb.Label {
	lbls := make([]prompb.Label, 0, len(s.Dimensions)+len(commonLabels)+4)
	lbls = append(lbls,
		prompb.Label{Name: "__name__", Value: s.MetricName()},
		prompb.Label{Name: labelScenario, Value: strconv.FormatInt(s.Slot, 10)},
		prompb.Label{Name: labelScenarioSeed, Value: strconv.FormatUint(s.Seed, 10)},
		prompb.Label{Name: labelSeriesID, Value: strconv.Itoa(series)},
	)
	lbls = append(lbls, s.Dimensions...)
	lbls = append(lbls, commonLabels...)
	slices.SortFunc(lbls, func(a, b prompb.Label) int { return strings.Compare(a.Name, b.Name) })
	return lbls
}

// Histogram returns the sample of the given series at the given step, where step 0 is the
// scenario start. Exactly one of the returned histograms is non-nil, depending on the sample type.
func (s Scenario) Histogram(series int, step int64) (*histogram.Histogram, *histogram.FloatHistogram) {
	seriesSeed := mix(s.Seed, uint64(series))
	l := newLayout(seriesSeed)

	counts := make([]int64, len(l.rates))
	var sum int64
	switch s.Kind {
	case KindGauge:
		for b := range counts {
			counts[b] = 1 + int64(mix(seriesSeed, uint64(step), uint64(b))%100)
		}
		sum = int64(mix(seriesSeed, uint64(step)) % 10000)
	default:
		for b, rate := range l.rates {
			counts[b] = rate * (step + 1)
		}
		sum = l.sumRate * (step + 1)
	}
	var total int64
	for _, c := range counts {
		total += c
	}

	hint := histogram.UnknownCounterReset
	if s.Kind == KindGauge {
		hint = histogram.GaugeType
	}

	if s.SampleType == SampleTypeFloat {
		// Quarters are exactly representable, so float values survive the round trip unchanged.
		buckets := make([]float64, len(counts))
		for i, c := range counts {
			buckets[i] = float64(c) / 4
		}
		return nil, &histogram.FloatHistogram{
			CounterResetHint: hint,
			Schema:           s.Schema,
			ZeroThreshold:    zeroThreshold,
			Count:            float64(total) / 4,
			Sum:              float64(sum) / 4,
			PositiveSpans:    l.spans,
			PositiveBuckets:  buckets,
		}
	}

	deltas := make([]int64, len(counts))
	prev := int64(0)
	for i, c := range counts {
		deltas[i] = c - prev
		prev = c
	}
	return &histogram.Histogram{
		CounterResetHint: hint,
		Schema:           s.Schema,
		ZeroThreshold:    zeroThreshold,
		Count:            uint64(total),
		Sum:              float64(sum),
		PositiveSpans:    l.spans,
		PositiveBuckets:  deltas,
	}, nil
}

// ExpectedHistogram returns the sample of the given series at the given step as a float histogram,
// which is how it is expected to be read back.
func (s Scenario) ExpectedHistogram(series int, step int64) *histogram.FloatHistogram {
	h, fh := s.Histogram(series, step)
	if h != nil {
		return h.ToFloat(nil)
	}
	return fh
}

// ExpectedHistograms returns the samples of the given series from step from to step to, as they
// are expected to be read back.
func (s Scenario) ExpectedHistograms(series int, from, to int64) []*histogram.FloatHistogram {
	out := make([]*histogram.FloatHistogram, 0, max(0, to-from+1))
	for step := from; step <= to; step++ {
		out = append(out, s.ExpectedHistogram(series, step))
	}
	return out
}

// layout is the bucket layout of a series and the rate at which its counter values grow.
type layout struct {
	spans   []histogram.Span
	rates   []int64
	sumRate int64
}

func newLayout(seed uint64) layout {
	rng := newRand(seed, 0)
	var l layout
	numBuckets := 0
	for i := range 1 + rng.IntN(3) {
		// The first span starts near bucket 0, later spans leave a gap of 1 to 3 buckets.
		offset := int32(1 + rng.IntN(3))
		if i == 0 {
			offset = int32(rng.IntN(9) - 3)
		}
		length := uint32(1 + rng.IntN(3))
		l.spans = append(l.spans, histogram.Span{Offset: offset, Length: length})
		numBuckets += int(length)
	}
	for range numBuckets {
		l.rates = append(l.rates, int64(1+rng.IntN(10)))
	}
	l.sumRate = int64(1 + rng.IntN(100))
	return l
}
