// SPDX-License-Identifier: AGPL-3.0-only

package astmapper

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProm2RangeCompat_Cancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	query, _ := CreateParser().ParseExpr(`up{foo="bar"}`)
	mapper := NewProm2RangeCompat()
	_, err := mapper.Map(ctx, query)

	require.ErrorIs(t, err, context.Canceled)
}

func TestProm2RangeCompat_Queries(t *testing.T) {
	type testCase struct {
		query         string
		expectedQuery string
	}

	testCases := []testCase{
		{
			query:         `sum(rate(some_series{job="foo"}[1m]))`,
			expectedQuery: `sum(rate(some_series{job="foo"}[1m]))`,
		},
		{
			query:         `sum(rate(some_series{job="foo"}[1m:1m]))`,
			expectedQuery: `sum(rate(some_series{job="foo"}[1m1ms:1m]))`,
		},
		{
			query:         `sum(rate(some_series{job="foo"}[1h]))`,
			expectedQuery: `sum(rate(some_series{job="foo"}[1h]))`,
		},
		{
			query:         `sum(rate(some_series{job="foo"}[1h:1h]))`,
			expectedQuery: `sum(rate(some_series{job="foo"}[1h1ms:1h]))`,
		},
		{
			query:         `sum(rate(some_series{job="foo"}[1h:1h])) / sum(rate(other_series{job="foo"}[1m:1m]))`,
			expectedQuery: `sum(rate(some_series{job="foo"}[1h1ms:1h])) / sum(rate(other_series{job="foo"}[1m1ms:1m]))`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.query, func(t *testing.T) {
			query, err := CreateParser().ParseExpr(tc.query)
			require.NoError(t, err)

			mapper := NewProm2RangeCompat()
			ctx := context.Background()
			mapped, err := mapper.Map(ctx, query)
			require.NoError(t, err)
			require.Equal(t, tc.expectedQuery, mapped.String())
		})
	}
}
