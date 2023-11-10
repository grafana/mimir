// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

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

			series := NewSeries(index.NewListPostings(test.storageRefs), idx)

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
