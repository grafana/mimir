// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestErrorTypeForError(t *testing.T) {
	testCases := map[string]struct {
		err      error
		expected mimirpb.QueryErrorType
	}{
		"generic error": {
			err:      errors.New("something went wrong"),
			expected: mimirpb.QUERY_ERROR_TYPE_EXECUTION,
		},
		"context canceled": {
			err:      context.Canceled,
			expected: mimirpb.QUERY_ERROR_TYPE_CANCELED,
		},
		"context deadline exceeded": {
			err:      context.DeadlineExceeded,
			expected: mimirpb.QUERY_ERROR_TYPE_TIMEOUT,
		},
		"storage error": {
			err:      promql.ErrStorage{Err: errors.New("could not load data")},
			expected: mimirpb.QUERY_ERROR_TYPE_INTERNAL,
		},
		// These types shouldn't be emitted by MQE, but we support them for consistency.
		"query canceled error": {
			err:      promql.ErrQueryCanceled("canceled"),
			expected: mimirpb.QUERY_ERROR_TYPE_CANCELED,
		},
		"query timeout error": {
			err:      promql.ErrQueryTimeout("timed out"),
			expected: mimirpb.QUERY_ERROR_TYPE_TIMEOUT,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, testCase.expected, errorTypeForError(testCase.err))
		})

		t.Run(name+" (wrapped)", func(t *testing.T) {
			err := fmt.Errorf("something went wrong one level down: %w", testCase.err)
			require.Equal(t, testCase.expected, errorTypeForError(err))
		})
	}
}
