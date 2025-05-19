// Copyright The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package schema

import (
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func Test_DataCols(t *testing.T) {
	testCases := []struct {
		mint, maxt, dataColDuration int64
		expected                    int
	}{
		{
			mint:            0,
			maxt:            10*time.Hour.Milliseconds() - 1,
			dataColDuration: time.Hour.Milliseconds(),
			expected:        10,
		},
		{
			mint:            0,
			maxt:            10*time.Hour.Milliseconds() - 1,
			dataColDuration: 30 * time.Minute.Milliseconds(),
			expected:        20,
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%d,%d,%d", tc.mint, tc.maxt, tc.dataColDuration), func(t *testing.T) {
			b := NewBuilder(tc.mint, tc.maxt, tc.dataColDuration)
			s, err := b.Build()
			require.NoError(t, err)
			require.Len(t, s.DataColsIndexes, tc.expected)
		})
	}
}

func Test_LabelCols(t *testing.T) {
	b := NewBuilder(0, time.Hour.Milliseconds(), time.Hour.Milliseconds())
	b.AddLabelNameColumn("test", labels.MetricName)
	s, err := b.Build()
	require.NoError(t, err)
	_, ok := s.Schema.Lookup(LabelToColumn("test"))
	require.True(t, ok)
	_, ok = s.Schema.Lookup(LabelToColumn(labels.MetricName))
	require.True(t, ok)
	require.Equal(t, b.metadata[DataColSizeMd], fmt.Sprintf("%v", time.Hour.Milliseconds()))
}
