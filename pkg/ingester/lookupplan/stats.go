// SPDX-License-Identifier: AGPL-3.0-only

package lookupplan

import (
	"context"
	"fmt"
	"time"
	"unsafe"

	"github.com/DmitriyVTitov/size"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"
	boom "github.com/tylertreat/BoomFilters"
)

// countPostings counts the number of series in the given postings
func countPostings(postings index.Postings) (uint64, error) {
	var count uint64
	for postings.Next() {
		count++
	}
	return count, postings.Err()
}

type StatisticsGenerator struct {
	logger log.Logger
}

func NewStatisticsGenerator(l log.Logger) *StatisticsGenerator {
	return &StatisticsGenerator{
		logger: l,
	}
}

func setCountMinEpsilon(smallLabelCardinalityThreshold, largeLabelCardinalityThreshold uint64) func(numValues uint64) float64 {
	// The more label values for a label name, the larger the sketch, ideally the more accurate the count per label value.
	return func(numSeries uint64) float64 {
		switch {
		case numSeries >= largeLabelCardinalityThreshold:
			return 0.005
		case numSeries < smallLabelCardinalityThreshold:
			return 0.05
		default:
			return 0.01
		}
	}
}

// Stats creates statistics using count-min sketches
func (g StatisticsGenerator) Stats(meta tsdb.BlockMeta, r tsdb.IndexReader, smallLabelCardinalityThreshold, largeLabelCardinalityThreshold uint64) (retStats Statistics, retErr error) {
	ctx := context.Background()

	defer func(startTime time.Time) {
		l := g.logger
		if retErr != nil {
			l = log.With(level.Error(l), "err", retErr)
		} else {
			l = log.With(level.Info(l), "total_series", retStats.TotalSeries())
		}
		l.Log("msg", "generated statistics for block", "block", meta.ULID.String(), "duration", time.Since(startTime).String(), "total_size_bytes", size.Of(retStats))
	}(time.Now())

	// Use the "all series" postings to count total series
	allPostingsName, allPostingsValue := index.AllPostingsKey()
	allPostings, err := r.Postings(ctx, allPostingsName, allPostingsValue)
	if err != nil {
		return nil, fmt.Errorf("failed to get all postings: %w", err)
	}

	// Count total series by expanding the postings
	seriesCount, err := countPostings(allPostings)
	if err != nil {
		return nil, fmt.Errorf("error iterating postings: %w", err)
	}

	// Get all label names
	labelNames, err := r.LabelNames(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get label names: %w", err)
	}

	// Build count-min sketches for each label
	labelSketches := make(map[string]*LabelValuesSketch)
	selectEpsilon := setCountMinEpsilon(smallLabelCardinalityThreshold, largeLabelCardinalityThreshold)

	for _, labelName := range labelNames {
		// Get all values for this label
		values, err := r.LabelValues(ctx, labelName, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to get label values for label %s: %w", labelName, err)
		}

		labelCardinality, err := countPostings(r.PostingsForAllLabelValues(ctx, labelName))
		if err != nil {
			return nil, fmt.Errorf("error counting postings for label %s: %w", labelName, err)
		}

		epsilon := selectEpsilon(labelCardinality)
		// Create count-min sketch for this label
		sketch := &LabelValuesSketch{
			s:              boom.NewCountMinSketch(epsilon, 0.01),
			distinctValues: uint64(len(values)),
		}

		// Add each value to the sketch
		// For each value, we need to count how many series have that value
		for _, value := range values {
			// Get postings for this label name/value pair
			postings, err := r.Postings(ctx, labelName, value)
			if err != nil {
				return nil, fmt.Errorf("failed to get postings for label %s=%s: %w", labelName, value, err)
			}

			// Count the number of series for this value
			seriesCountForValue, err := countPostings(postings)
			if err != nil {
				return nil, fmt.Errorf("error counting postings for label %s=%s: %w", labelName, value, err)
			}

			// Add to the sketch
			valBytes := yoloBytes(value)
			sketch.s.AddN(valBytes, seriesCountForValue)
		}

		labelSketches[labelName] = sketch
	}

	// Create and return the statistics
	return &BlockStatistics{
		totalSeries: seriesCount,
		labelNames:  labelSketches,
	}, nil
}

// BlockStatistics contains count-min sketches of the values for each label name in a TSDB block.
// It implements Statistics, which can be used to inform query plan generation.
type BlockStatistics struct {
	totalSeries uint64
	labelNames  map[string]*LabelValuesSketch
}

// LabelValuesSketch contains a count-min sketch for a specific label name.
type LabelValuesSketch struct {
	s              *boom.CountMinSketch
	distinctValues uint64
}

// TotalSeries returns the number of series in the TSDB block.
func (s *BlockStatistics) TotalSeries() uint64 {
	return s.totalSeries
}

// LabelValuesCount returns the number of values for a label name. If the given label name does not exist,
// it returns 0.
func (s *BlockStatistics) LabelValuesCount(_ context.Context, name string) uint64 {
	sketch, ok := s.labelNames[name]
	if !ok {
		// If we don't find a sketch for a label name, we return 0 but no error, since we assume that the nonexistence
		// of a sketch is equivalent to the nonexistence of values for the label name.
		return 0
	}
	return sketch.distinctValues
}

// LabelValuesCardinality returns the cardinality of a given label name (i.e., the number of series which
// contain that label name). If values are provided, it returns the combined cardinality of all given values;
// otherwise, it returns the total cardinality across all values for the label name. If the label name does not exist,
// it returns 0.
func (s *BlockStatistics) LabelValuesCardinality(_ context.Context, name string, values ...string) uint64 {
	sketch, ok := s.labelNames[name]
	if !ok {
		// If we don't find a sketch for a label name, we return 0 but no error, since we assume that the nonexistence
		// of a label name is equivalent to 0 cardinality
		return 0
	}

	if len(values) == 0 {
		return sketch.s.TotalCount()
	}
	totalCount := uint64(0)
	for _, val := range values {
		valBytes := yoloBytes(val)
		totalCount += sketch.s.Count(valBytes)
	}
	return totalCount
}

// yoloBytes converts a string to a byte slice without allocation.
func yoloBytes(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}
