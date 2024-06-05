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

	// Sum and presence for each step.
	sums    []float64
	present []bool
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

		if thisSeriesGroup.sums == nil {
			// First series for this group, populate it.

			thisSeriesGroup.sums, err = a.Pool.GetFloatSlice(steps)
			if err != nil {
				return types.InstantVectorSeriesData{}, err
			}

			thisSeriesGroup.present, err = a.Pool.GetBoolSlice(steps)
			if err != nil {
				return types.InstantVectorSeriesData{}, err
			}

			thisSeriesGroup.sums = thisSeriesGroup.sums[:steps]
			thisSeriesGroup.present = thisSeriesGroup.present[:steps]
		}

		for _, p := range s.Floats {
			idx := (p.T - start) / interval
			thisSeriesGroup.sums[idx] += p.F
			thisSeriesGroup.present[idx] = true
		}

		a.Pool.PutFPointSlice(s.Floats)
		thisSeriesGroup.remainingSeriesCount--
	}

	// Construct the group and return it
	pointCount := 0
	for _, p := range thisGroup.present {
		if p {
			pointCount++
		}
	}

	points, err := a.Pool.GetFPointSlice(pointCount)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	for i, havePoint := range thisGroup.present {
		if havePoint {
			t := start + int64(i)*interval
			points = append(points, promql.FPoint{T: t, F: thisGroup.sums[i]})
		}
	}

	a.Pool.PutFloatSlice(thisGroup.sums)
	a.Pool.PutBoolSlice(thisGroup.present)

	thisGroup.sums = nil
	thisGroup.present = nil
	groupPool.Put(thisGroup)

	return types.InstantVectorSeriesData{Floats: points}, nil
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
