// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"fmt"
	"testing"

	"github.com/grafana/dskit/multierror"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// listActiveSeries is used for testing purposes, builds the whole array of active series in memory.
func listActiveSeries(ctx context.Context, db *userTSDB, matchers []*labels.Matcher) (series *Series, err error) {
	idx, err := db.Head().Index()
	if err != nil {
		return nil, fmt.Errorf("error getting index: %w", err)
	}
	postings, err := getPostings(ctx, db, idx, matchers, false)
	if err != nil {
		return nil, err
	}
	return newSeries(postings, idx), nil
}

// Series is a wrapper around index.Postings and tsdb.IndexReader. It implements
// the generic iterator interface to list all series in the index that are
// contained in the given postings.
type Series struct {
	postings index.Postings
	idx      tsdb.IndexReader
	buf      labels.ScratchBuilder
	err      error
}

func newSeries(postings index.Postings, index tsdb.IndexReader) *Series {
	return &Series{
		postings: postings,
		idx:      index,
		buf:      labels.NewScratchBuilder(10),
	}
}

func (s *Series) Next() bool {
	return s.postings.Next()
}

func (s *Series) At() labels.Labels {
	s.buf.Reset()
	err := s.idx.Series(s.postings.At(), &s.buf, nil)
	if err != nil {
		s.err = fmt.Errorf("error getting series: %w", err)
		return labels.Labels{}
	}
	return s.buf.Labels()
}

func (s *Series) Err() error {
	if s.err != nil {
		return fmt.Errorf("error listing series: %w", multierror.MultiError{s.err, s.postings.Err()}.Err())
	}
	return s.postings.Err()
}

func TestSeries(t *testing.T) {
	seriesInIndex := []labels.Labels{

		labels.FromStrings("a", "0"),
		labels.FromStrings("a", "1"),
		labels.FromStrings("a", "2"),
		labels.FromStrings("a", "3"),
		labels.FromStrings("a", "4"),
	}

	tests := []struct {
		name          string
		storageRefs   []storage.SeriesRef
		expectOnIndex func(*mockIndex)
		expectLabels  []labels.Labels
		expectErr     bool
	}{
		{
			name:        "fetches expected series",
			storageRefs: []storage.SeriesRef{1, 2, 4},
			expectOnIndex: func(idx *mockIndex) {
				idx.On(
					"Series",
					mock.AnythingOfType("storage.SeriesRef"),
					mock.AnythingOfType("*labels.ScratchBuilder"),
					mock.Anything,
				).Run(func(args mock.Arguments) {
					args.Get(1).(*labels.ScratchBuilder).Assign(seriesInIndex[args.Get(0).(storage.SeriesRef)])
				}).Return(
					nil,
				)
			},
			expectLabels: []labels.Labels{
				labels.FromStrings("a", "1"),
				labels.FromStrings("a", "2"),
				labels.FromStrings("a", "4"),
			},
		}, {
			name:        "returns error if index fails",
			storageRefs: []storage.SeriesRef{0, 3},
			expectOnIndex: func(idx *mockIndex) {
				idx.On("Series", mock.Anything, mock.Anything, mock.Anything).Return(errors.New("error in Series()"))
			},
			expectErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			idx := &mockIndex{}
			test.expectOnIndex(idx)

			series := newSeries(index.NewListPostings(test.storageRefs), idx)

			var seriesSet []labels.Labels
			for series.Next() {
				seriesSet = append(seriesSet, series.At())
			}
			err := series.Err()
			if test.expectErr {
				assert.Error(t, err)
				return
			}

			assert.EqualValues(t, test.expectLabels, seriesSet)
			assert.NoError(t, err)
			idx.AssertExpectations(t)
		})
	}
}
