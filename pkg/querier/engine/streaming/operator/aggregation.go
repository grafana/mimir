// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package operator

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/util/zeropool"
)

type Aggregation struct {
	Inner    InstantVectorOperator
	Start    time.Time
	End      time.Time
	Interval time.Duration
	Grouping []string

	remainingInnerSeriesToGroup []*group // One entry per series produced by Inner, value is the group for that series
	remainingGroups             []*group // One entry per group, in the order we want to return them
}

type group struct {
	labels               labels.Labels
	remainingSeriesCount uint

	// Sum and presence for each step.
	sums    []float64
	present []bool
}

var _ InstantVectorOperator = &Aggregation{}

var groupPool = zeropool.New(func() *group {
	return &group{}
})

func (a *Aggregation) Series(ctx context.Context) ([]SeriesMetadata, error) {
	// Fetch the source series
	innerSeries, err := a.Inner.Series(ctx)
	if err != nil {
		return nil, err
	}

	defer PutSeriesMetadataSlice(innerSeries)

	if len(innerSeries) == 0 {
		// No input series == no output series.
		return nil, nil
	}

	// Determine the groups we'll return
	groups := map[uint64]*group{}
	buf := make([]byte, 0, 1024)
	lb := labels.NewBuilder(labels.EmptyLabels())
	a.remainingInnerSeriesToGroup = make([]*group, 0, len(innerSeries))

	for _, series := range innerSeries {
		var groupingKey uint64
		groupingKey, buf = series.Labels.HashForLabels(buf, a.Grouping...)
		g, groupExists := groups[groupingKey]

		if !groupExists {
			g = groupPool.Get()
			g.labels = a.labelsForGroup(series.Labels, lb)
			g.remainingSeriesCount = 0

			groups[groupingKey] = g
		}

		g.remainingSeriesCount++
		a.remainingInnerSeriesToGroup = append(a.remainingInnerSeriesToGroup, g)
	}

	// Sort the list of series we'll return, and maintain the order of the corresponding groups at the same time
	seriesMetadata := GetSeriesMetadataSlice(len(groups))
	a.remainingGroups = make([]*group, 0, len(groups))

	for _, g := range groups {
		seriesMetadata = append(seriesMetadata, SeriesMetadata{Labels: g.labels})
		a.remainingGroups = append(a.remainingGroups, g)
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

func (a *Aggregation) Next(ctx context.Context) (InstantVectorSeriesData, error) {
	if len(a.remainingGroups) == 0 {
		// No more groups left.
		return InstantVectorSeriesData{}, EOS
	}

	start := timestamp.FromTime(a.Start)
	end := timestamp.FromTime(a.End)
	interval := DurationMilliseconds(a.Interval)
	steps := stepCount(start, end, interval)

	// Determine next group to return
	thisGroup := a.remainingGroups[0]
	a.remainingGroups = a.remainingGroups[1:]

	// Iterate through inner series until the desired group is complete
	for thisGroup.remainingSeriesCount > 0 {
		s, err := a.Inner.Next(ctx)

		if err != nil {
			if errors.Is(err, EOS) {
				return InstantVectorSeriesData{}, fmt.Errorf("exhausted series before all groups were completed: %w", err)
			}

			return InstantVectorSeriesData{}, err
		}

		thisSeriesGroup := a.remainingInnerSeriesToGroup[0]
		a.remainingInnerSeriesToGroup = a.remainingInnerSeriesToGroup[1:]

		if thisSeriesGroup.sums == nil {
			// First series for this group, populate it
			thisSeriesGroup.sums = GetFloatSlice(steps)[:steps]
			thisSeriesGroup.present = GetBoolSlice(steps)[:steps]
		}

		for _, p := range s.Floats {
			idx := (p.T - start) / interval
			thisSeriesGroup.sums[idx] += p.F
			thisSeriesGroup.present[idx] = true
		}

		PutFPointSlice(s.Floats)
		thisSeriesGroup.remainingSeriesCount--
	}

	// Construct the group and return it
	pointCount := 0
	for _, p := range thisGroup.present {
		if p {
			pointCount++
		}
	}

	points := GetFPointSlice(pointCount)

	for i, havePoint := range thisGroup.present {
		if havePoint {
			t := start + int64(i)*interval
			points = append(points, promql.FPoint{T: t, F: thisGroup.sums[i]})
		}
	}

	PutFloatSlice(thisGroup.sums)
	PutBoolSlice(thisGroup.present)

	thisGroup.sums = nil
	thisGroup.present = nil
	groupPool.Put(thisGroup)

	return InstantVectorSeriesData{Floats: points}, nil
}

func (a *Aggregation) Close() {
	a.Inner.Close()
}

type groupSorter struct {
	metadata []SeriesMetadata
	groups   []*group
}

func (g groupSorter) Len() int {
	return len(g.metadata)
}

func (g groupSorter) Less(i, j int) bool {
	return labels.Compare(g.metadata[i].Labels, g.metadata[j].Labels) < 0
}

func (g groupSorter) Swap(i, j int) {
	g.metadata[i], g.metadata[j] = g.metadata[j], g.metadata[i]
	g.groups[i], g.groups[j] = g.groups[j], g.groups[i]
}
