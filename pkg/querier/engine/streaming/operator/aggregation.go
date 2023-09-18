package operator

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
)

// TODO: support aggregations other than 'sum'
type Aggregation struct {
	Inner    Operator
	Start    time.Time
	End      time.Time
	Interval time.Duration
	Grouping []string
	Pool     *Pool

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

var _ Operator = &Aggregation{}

// TODO: test case for grouping by multiple labels
// TODO: add test for case where Inner returns no results
// TODO: special case for aggregation to single series? (query without 'by' or 'without', eg. sum(metric{}))
func (a *Aggregation) Series(ctx context.Context) ([]SeriesMetadata, error) {
	// Fetch the source series
	innerSeries, err := a.Inner.Series(ctx)
	if err != nil {
		return nil, err
	}

	defer a.Pool.PutSeriesMetadataSlice(innerSeries)

	if len(innerSeries) == 0 {
		// No input series == no output series.
		return nil, nil
	}

	// Determine the groups we'll return
	groups := map[uint64]*group{} // TODO: pool this?
	buf := make([]byte, 0, 1024)
	lb := labels.NewBuilder(labels.EmptyLabels())                       // TODO: pool this?
	a.remainingInnerSeriesToGroup = make([]*group, 0, len(innerSeries)) // TODO: pool this?

	for _, series := range innerSeries {
		var groupingKey uint64
		groupingKey, buf = series.Labels.HashForLabels(buf, a.Grouping...)
		g, groupExists := groups[groupingKey]

		if !groupExists {
			g = &group{ // TODO: pool these?
				labels: a.labelsForGroup(series.Labels, lb),
			}

			groups[groupingKey] = g
		}

		g.remainingSeriesCount++
		a.remainingInnerSeriesToGroup = append(a.remainingInnerSeriesToGroup, g)
	}

	// Sort the list of series we'll return, and maintain the order of the corresponding groups at the same time
	seriesMetadata := a.Pool.GetSeriesMetadataSlice(len(groups))
	a.remainingGroups = make([]*group, 0, len(groups)) // TODO: pool this?

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

// TODO: add test for case where series does not contain a point at every step
func (a *Aggregation) Next(ctx context.Context) (bool, SeriesData, error) {
	if len(a.remainingGroups) == 0 {
		// No more groups left.
		return false, SeriesData{}, nil
	}

	start := timestamp.FromTime(a.Start)
	end := timestamp.FromTime(a.End)
	interval := durationMilliseconds(a.Interval)
	steps := stepCount(start, end, interval)

	// Determine next group to return
	thisGroup := a.remainingGroups[0]
	a.remainingGroups = a.remainingGroups[1:]

	// Iterate through inner series until the desired group is complete
	for thisGroup.remainingSeriesCount > 0 {
		ok, s, err := a.Inner.Next(ctx)

		if err != nil {
			return false, SeriesData{}, err
		}

		if !ok {
			return false, SeriesData{}, errors.New("exhausted series before all groups were completed")
		}

		thisSeriesGroup := a.remainingInnerSeriesToGroup[0]
		a.remainingInnerSeriesToGroup = a.remainingInnerSeriesToGroup[1:]

		if thisSeriesGroup.sums == nil {
			// First series for this group, populate it
			thisSeriesGroup.sums = a.Pool.GetFloatSlice(steps)[:steps]
			thisSeriesGroup.present = a.Pool.GetBoolSlice(steps)[:steps]
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

	points := a.Pool.GetFPointSlice(pointCount)

	for i, havePoint := range thisGroup.present {
		if havePoint {
			t := start + int64(i)*interval
			points = append(points, promql.FPoint{T: t, F: thisGroup.sums[i]})
		}
	}

	a.Pool.PutFloatSlice(thisGroup.sums)
	a.Pool.PutBoolSlice(thisGroup.present)

	// TODO: return thisGroup to pool (zero-out slices)

	return true, SeriesData{Floats: points}, nil
}

func (a *Aggregation) Close() {
	a.Inner.Close()

	// TODO: return remaining groups and their slices to the pool
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
