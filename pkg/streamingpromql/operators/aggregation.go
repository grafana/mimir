// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package operators

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/util/zeropool"

	"github.com/grafana/mimir/pkg/streamingpromql/pooling"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type Aggregation struct {
	Inner    types.InstantVectorOperator
	Start    time.Time
	End      time.Time
	Interval time.Duration
	Grouping []string
	Pool     *pooling.LimitingPool

	remainingInnerSeriesToGroup []*group // One entry per series produced by Inner, value is the group for that series
	remainingGroups             []*group // One entry per group, in the order we want to return them
}

type groupWithLabels struct {
	labels labels.Labels
	group  *group
}

type group struct {
	// The number of input series that belong to this group that we haven't yet seen.
	remainingSeriesCount uint

	// The index of the last series that contributes to this group.
	// Used to sort groups in the order that they'll be completed in.
	lastSeriesIndex int

	// Sum, presence, and histograms for each step.
	floatSums           []float64
	floatPresent        []bool
	histogramSums       []*histogram.FloatHistogram
	histogramPointCount int
}

var _ types.InstantVectorOperator = &Aggregation{}

var groupPool = zeropool.New(func() *group {
	return &group{}
})

func (a *Aggregation) SeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
	// Fetch the source series
	innerSeries, err := a.Inner.SeriesMetadata(ctx)
	if err != nil {
		return nil, err
	}

	defer pooling.PutSeriesMetadataSlice(innerSeries)

	if len(innerSeries) == 0 {
		// No input series == no output series.
		return nil, nil
	}

	// Determine the groups we'll return
	groups := map[uint64]groupWithLabels{}
	buf := make([]byte, 0, 1024)
	lb := labels.NewBuilder(labels.EmptyLabels())
	a.remainingInnerSeriesToGroup = make([]*group, 0, len(innerSeries))

	for seriesIdx, series := range innerSeries {
		// Note that this doesn't handle potential hash collisions between groups.
		// This is something we should likely fix, but at present, Prometheus' PromQL engine doesn't handle collisions either,
		// so at least both engines will be incorrect in the same way.
		var groupingKey uint64
		groupingKey, buf = series.Labels.HashForLabels(buf, a.Grouping...)
		g, groupExists := groups[groupingKey]

		if !groupExists {
			g.labels = a.labelsForGroup(series.Labels, lb)
			g.group = groupPool.Get()
			g.group.remainingSeriesCount = 0

			groups[groupingKey] = g
		}

		g.group.remainingSeriesCount++
		g.group.lastSeriesIndex = seriesIdx
		a.remainingInnerSeriesToGroup = append(a.remainingInnerSeriesToGroup, g.group)
	}

	// Sort the list of series we'll return, and maintain the order of the corresponding groups at the same time
	seriesMetadata := pooling.GetSeriesMetadataSlice(len(groups))
	a.remainingGroups = make([]*group, 0, len(groups))

	for _, g := range groups {
		seriesMetadata = append(seriesMetadata, types.SeriesMetadata{Labels: g.labels})
		a.remainingGroups = append(a.remainingGroups, g.group)
	}

	sort.Sort(groupSorter{seriesMetadata, a.remainingGroups})

	return seriesMetadata, nil
}

func (a *Aggregation) labelsForGroup(m labels.Labels, lb *labels.Builder) labels.Labels {
	if len(a.Grouping) == 0 {
		return labels.EmptyLabels()
	}

	lb.Reset(m)
	lb.Keep(a.Grouping...)
	return lb.Labels()
}

func (a *Aggregation) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	if len(a.remainingGroups) == 0 {
		// No more groups left.
		return types.InstantVectorSeriesData{}, types.EOS
	}

	start := timestamp.FromTime(a.Start)
	end := timestamp.FromTime(a.End)
	interval := a.Interval.Milliseconds()
	steps := stepCount(start, end, interval)

	// Determine next group to return
	thisGroup := a.remainingGroups[0]
	a.remainingGroups = a.remainingGroups[1:]

	// Iterate through inner series until the desired group is complete
	for thisGroup.remainingSeriesCount > 0 {
		s, err := a.Inner.NextSeries(ctx)

		if err != nil {
			if errors.Is(err, types.EOS) {
				return types.InstantVectorSeriesData{}, fmt.Errorf("exhausted series before all groups were completed: %w", err)
			}

			return types.InstantVectorSeriesData{}, err
		}

		thisSeriesGroup := a.remainingInnerSeriesToGroup[0]
		a.remainingInnerSeriesToGroup = a.remainingInnerSeriesToGroup[1:]

		if len(s.Floats) > 0 && thisSeriesGroup.floatSums == nil {
			// First series for this group, populate it.

			thisSeriesGroup.floatSums, err = a.Pool.GetFloatSlice(steps)
			if err != nil {
				return types.InstantVectorSeriesData{}, err
			}

			thisSeriesGroup.floatPresent, err = a.Pool.GetBoolSlice(steps)
			if err != nil {
				return types.InstantVectorSeriesData{}, err
			}
			thisSeriesGroup.floatSums = thisSeriesGroup.floatSums[:steps]
			thisSeriesGroup.floatPresent = thisSeriesGroup.floatPresent[:steps]
		}

		if len(s.Histograms) > 0 && thisSeriesGroup.histogramSums == nil {
			// First series for this group, populate it.
			thisSeriesGroup.histogramSums, err = a.Pool.GetHistogramPointerSlice(steps)
			if err != nil {
				return types.InstantVectorSeriesData{}, err
			}
			thisSeriesGroup.histogramSums = thisSeriesGroup.histogramSums[:steps]
		}

		for _, p := range s.Floats {
			idx := (p.T - start) / interval
			thisSeriesGroup.floatSums[idx] += p.F
			thisSeriesGroup.floatPresent[idx] = true
		}

		for _, p := range s.Histograms {
			idx := (p.T - start) / interval
			if thisSeriesGroup.histogramSums[idx] == nil {
				// We copy here because we modify the histogram through Add later on.
				// It is necessary to preserve the original Histogram in case of any range-queries using lookback.
				thisSeriesGroup.histogramSums[idx] = p.H.Copy()
				// We already have to do the check if the histogram exists at this idx,
				// so we can count the histogram points present at this point instead
				// of needing to loop again later like we do for floats.
				thisSeriesGroup.histogramPointCount++
			} else {
				thisSeriesGroup.histogramSums[idx] = thisSeriesGroup.histogramSums[idx].Add(p.H)
			}
		}

		a.Pool.PutInstantVectorSeriesData(s)
		thisSeriesGroup.remainingSeriesCount--
	}

	// Construct the group and return it
	// It would be possible to calculate the number of points when constructing
	// the series groups. However, it requires checking each point at each input
	// series which is more costly than looping again here and just checking each
	// point of the already grouped series.
	// See: https://github.com/grafana/mimir/pull/8442
	// We also take two different approaches here. One with extra checks if we
	// have both Floats and Histograms present, and one without these checks
	// so we don't have to do it at every point.
	floatPointCount := 0
	if len(thisGroup.floatPresent) > 0 && len(thisGroup.histogramSums) > 0 {
		for idx, present := range thisGroup.floatPresent {
			if present {
				if thisGroup.histogramSums[idx] != nil {
					// If a mix of histogram samples and float samples, the corresponding vector element is removed from the output vector entirely.
					thisGroup.floatPresent[idx] = false
					thisGroup.histogramSums[idx] = nil
					thisGroup.histogramPointCount--
				} else {
					floatPointCount++
				}
			}
		}
	} else {
		for _, p := range thisGroup.floatPresent {
			if p {
				floatPointCount++
			}
		}
	}

	var floatPoints []promql.FPoint
	var err error
	if floatPointCount > 0 {
		floatPoints, err = a.Pool.GetFPointSlice(floatPointCount)
		if err != nil {
			return types.InstantVectorSeriesData{}, err
		}

		for i, havePoint := range thisGroup.floatPresent {
			if havePoint {
				t := start + int64(i)*interval
				floatPoints = append(floatPoints, promql.FPoint{T: t, F: thisGroup.floatSums[i]})
			}
		}
	}

	a.Pool.PutFloatSlice(thisGroup.floatSums)
	a.Pool.PutBoolSlice(thisGroup.floatPresent)
	thisGroup.floatSums = nil
	thisGroup.floatPresent = nil

	var histogramPoints []promql.HPoint
	if thisGroup.histogramPointCount > 0 {
		histogramPoints, err = a.Pool.GetHPointSlice(thisGroup.histogramPointCount)
		if err != nil {
			return types.InstantVectorSeriesData{}, err
		}

		for i, h := range thisGroup.histogramSums {
			if h != nil {
				t := start + int64(i)*interval
				histogramPoints = append(histogramPoints, promql.HPoint{T: t, H: thisGroup.histogramSums[i]})
			}
		}
	}

	a.Pool.PutHistogramPointerSlice(thisGroup.histogramSums)
	thisGroup.histogramSums = nil
	thisGroup.histogramPointCount = 0

	groupPool.Put(thisGroup)
	return types.InstantVectorSeriesData{Floats: floatPoints, Histograms: histogramPoints}, nil
}

func (a *Aggregation) Close() {
	a.Inner.Close()
}

type groupSorter struct {
	metadata []types.SeriesMetadata
	groups   []*group
}

func (g groupSorter) Len() int {
	return len(g.metadata)
}

func (g groupSorter) Less(i, j int) bool {
	return g.groups[i].lastSeriesIndex < g.groups[j].lastSeriesIndex
}

func (g groupSorter) Swap(i, j int) {
	g.metadata[i], g.metadata[j] = g.metadata[j], g.metadata[i]
	g.groups[i], g.groups[j] = g.groups[j], g.groups[i]
}
