// SPDX-License-Identifier: AGPL-3.0-only

package lookupplan

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/assert"
)

// errorIndexReader is a mock IndexReader that always returns errors
type errorIndexReader struct{}

func (e *errorIndexReader) Close() error              { return nil }
func (e *errorIndexReader) Symbols() index.StringIter { return nil }
func (e *errorIndexReader) SortedLabelValues(context.Context, string, *storage.LabelHints, ...*labels.Matcher) ([]string, error) {
	return nil, errors.New("mock error from SortedLabelValues")
}
func (e *errorIndexReader) LabelValues(context.Context, string, *storage.LabelHints, ...*labels.Matcher) ([]string, error) {
	return nil, errors.New("mock error from LabelValues")
}
func (e *errorIndexReader) Postings(context.Context, string, ...string) (index.Postings, error) {
	return nil, errors.New("mock error from Postings")
}
func (e *errorIndexReader) PostingsForLabelMatching(context.Context, string, func(value string) bool) index.Postings {
	return index.EmptyPostings()
}
func (e *errorIndexReader) PostingsForAllLabelValues(context.Context, string) index.Postings {
	return index.EmptyPostings()
}
func (e *errorIndexReader) PostingsForMatchers(context.Context, bool, ...*labels.Matcher) (index.Postings, error) {
	return nil, errors.New("mock error from PostingsForMatchers")
}
func (e *errorIndexReader) SortedPostings(index.Postings) index.Postings { return nil }
func (e *errorIndexReader) ShardedPostings(index.Postings, uint64, uint64) index.Postings {
	return nil
}
func (e *errorIndexReader) Series(storage.SeriesRef, *labels.ScratchBuilder, *[]chunks.Meta) error {
	return errors.New("mock error from Series")
}
func (e *errorIndexReader) LabelNames(context.Context, ...*labels.Matcher) ([]string, error) {
	return nil, errors.New("mock error from LabelNames")
}
func (e *errorIndexReader) LabelValueFor(context.Context, storage.SeriesRef, string) (string, error) {
	return "", errors.New("mock error from LabelValueFor")
}
func (e *errorIndexReader) LabelValuesExcluding(index.Postings, string) storage.LabelValues {
	return nil // Return empty label values
}
func (e *errorIndexReader) LabelValuesFor(index.Postings, string) storage.LabelValues {
	return nil // Return empty label values
}
func (e *errorIndexReader) LabelNamesFor(context.Context, index.Postings) ([]string, error) {
	return nil, errors.New("mock error from LabelNamesFor")
}
func (e *errorIndexReader) IndexLookupPlanner() index.LookupPlanner {
	return NoopPlanner{} // Return a basic planner for the interface
}

// createMockIndexReaderWithSeries creates a mockIndexReader with the specified number of series
func createMockIndexReaderWithSeries(numSeries int) tsdb.IndexReader {
	p := newMockIndexReader()
	for i := 0; i < numSeries; i++ {
		ls := labels.FromStrings("__name__", "test_metric", "instance", strconv.Itoa(i))
		p.add(storage.SeriesRef(i), ls)
	}
	return p
}

func TestPlannerFactory_CreatePlanner(t *testing.T) {
	logger := log.NewNopLogger()
	metrics := NewMetrics(nil)
	statsGenerator := NewStatisticsGenerator(logger)

	factory := NewPlannerFactory(metrics, logger, statsGenerator, defaultCostConfig)

	tests := []struct {
		name            string
		blockMeta       tsdb.BlockMeta
		indexReader     tsdb.IndexReader
		expectedPlanner reflect.Type
	}{
		{
			name: "small block returns NoopPlanner",
			blockMeta: tsdb.BlockMeta{
				Stats: tsdb.BlockStats{
					NumSeries: 5000,
				},
			},
			indexReader:     createMockIndexReaderWithSeries(5000),
			expectedPlanner: reflect.TypeOf(NoopPlanner{}),
		},
		{
			name: "very small block returns NoopPlanner",
			blockMeta: tsdb.BlockMeta{
				Stats: tsdb.BlockStats{
					NumSeries: 1,
				},
			},
			indexReader:     createMockIndexReaderWithSeries(1),
			expectedPlanner: reflect.TypeOf(NoopPlanner{}),
		},
		{
			name: "empty block returns NoopPlanner",
			blockMeta: tsdb.BlockMeta{
				Stats: tsdb.BlockStats{
					NumSeries: 0,
				},
			},
			indexReader:     createMockIndexReaderWithSeries(0),
			expectedPlanner: reflect.TypeOf(NoopPlanner{}),
		},
		{
			name: "boundary case just below threshold returns NoopPlanner",
			blockMeta: tsdb.BlockMeta{
				Stats: tsdb.BlockStats{
					NumSeries: defaultCostConfig.MinSeriesPerBlockForQueryPlanning - 1,
				},
			},
			indexReader:     createMockIndexReaderWithSeries(int(defaultCostConfig.MinSeriesPerBlockForQueryPlanning - 1)),
			expectedPlanner: reflect.TypeOf(NoopPlanner{}),
		},
		{
			name:            "large block with working IndexReader returns CostBasedPlanner",
			blockMeta:       tsdb.BlockMeta{Stats: tsdb.BlockStats{NumSeries: 15000}},
			indexReader:     createMockIndexReaderWithSeries(15000),
			expectedPlanner: reflect.TypeOf(&CostBasedPlanner{}),
		},
		{
			name: "exactly at threshold with working IndexReader returns CostBasedPlanner",
			blockMeta: tsdb.BlockMeta{
				Stats: tsdb.BlockStats{
					NumSeries: defaultCostConfig.MinSeriesPerBlockForQueryPlanning,
				},
			},
			indexReader:     createMockIndexReaderWithSeries(int(defaultCostConfig.MinSeriesPerBlockForQueryPlanning)),
			expectedPlanner: reflect.TypeOf(&CostBasedPlanner{}),
		},
		{
			name: "very large block with working IndexReader returns CostBasedPlanner",
			blockMeta: tsdb.BlockMeta{
				Stats: tsdb.BlockStats{
					NumSeries: 100000,
				},
			},
			indexReader:     createMockIndexReaderWithSeries(100000),
			expectedPlanner: reflect.TypeOf(&CostBasedPlanner{}),
		},
		{
			name: "large block with error IndexReader fallbacks to NoopPlanner",
			blockMeta: tsdb.BlockMeta{
				Stats: tsdb.BlockStats{
					NumSeries: 50000,
				},
			},
			indexReader:     &errorIndexReader{},
			expectedPlanner: reflect.TypeOf(NoopPlanner{}),
		},
		{
			name: "exactly at threshold with error IndexReader fallbacks to NoopPlanner",
			blockMeta: tsdb.BlockMeta{
				Stats: tsdb.BlockStats{
					NumSeries: defaultCostConfig.MinSeriesPerBlockForQueryPlanning,
				},
			},
			indexReader:     &errorIndexReader{},
			expectedPlanner: reflect.TypeOf(NoopPlanner{}),
		},
		{
			name: "very large block with working IndexReader returns CostBasedPlanner",
			blockMeta: tsdb.BlockMeta{
				Stats: tsdb.BlockStats{
					NumSeries: 1000000,
				},
			},
			indexReader:     createMockIndexReaderWithSeries(1000000),
			expectedPlanner: reflect.TypeOf(&CostBasedPlanner{}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			planner := factory.CreatePlanner(tt.blockMeta, tt.indexReader)
			assert.NotNil(t, planner, "CreatePlanner should never return nil")
			assert.Equal(t, tt.expectedPlanner.String(), reflect.ValueOf(planner).Type().String())
		})
	}
}
