// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/stretchr/testify/require"

	apierror "github.com/grafana/mimir/pkg/api/error"
)

func TestFrontendRunningMiddleware_Enabled(t *testing.T) {
	maxRetries := 5
	backoffDuration := 100 * time.Millisecond

	testCases := map[string]struct {
		stateFunc              func(callCount int) services.State
		expectedStateFuncCalls int
		expectedError          string
	}{
		"frontend always in 'new' state": {
			stateFunc:              func(int) services.State { return services.New },
			expectedStateFuncCalls: maxRetries,
			expectedError:          "frontend not running after 400ms: New",
		},
		"frontend always in 'starting' state": {
			stateFunc:              func(int) services.State { return services.Starting },
			expectedStateFuncCalls: maxRetries,
			expectedError:          "frontend not running after 400ms: Starting",
		},
		"frontend transitions from 'new' to 'running' after retry": {
			stateFunc: func(callCount int) services.State {
				if callCount == 3 {
					return services.Running
				}

				return services.New
			},
			expectedStateFuncCalls: 3,
		},
		"frontend in 'running' state": {
			stateFunc:              func(int) services.State { return services.Running },
			expectedStateFuncCalls: 1,
		},
		"frontend in 'stopping' state": {
			stateFunc:              func(int) services.State { return services.Stopping },
			expectedStateFuncCalls: 1,
			expectedError:          "frontend shutting down: Stopping",
		},
		"frontend in 'terminated' state": {
			stateFunc:              func(int) services.State { return services.Terminated },
			expectedStateFuncCalls: 1,
			expectedError:          "frontend shutting down: Terminated",
		},
		"frontend in 'failed' state": {
			stateFunc:              func(int) services.State { return services.Failed },
			expectedStateFuncCalls: 1,
			expectedError:          "frontend shutting down: Failed",
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			downstreamCalled := false
			successResponse := &PrometheusResponse{Status: statusSuccess}

			downstream := HandlerFunc(func(ctx context.Context, r Request) (Response, error) {
				downstreamCalled = true
				return successResponse, nil
			})

			stateFuncCalls := 0
			lastStateFuncCall := time.Time{}
			stateFunc := func() services.State {
				stateFuncCalls++
				require.Greater(t, time.Since(lastStateFuncCall), backoffDuration)
				lastStateFuncCall = time.Now()
				return testCase.stateFunc(stateFuncCalls)
			}

			handler := newFrontendRunningMiddleware(stateFunc, backoffDuration, maxRetries, log.NewNopLogger()).Wrap(downstream)
			startTime := time.Now()
			resp, err := handler.Do(context.Background(), nil)
			duration := time.Since(startTime)

			if testCase.expectedError == "" {
				require.NoError(t, err)
				require.True(t, downstreamCalled, "expected downstream handler to be invoked")
				require.Equal(t, successResponse, resp)
			} else {
				require.False(t, downstreamCalled, "expected downstream handler to not be invoked")
				require.Equal(t, apierror.New(apierror.TypeUnavailable, testCase.expectedError), err)
				require.Nil(t, resp)
			}

			require.Equal(t, testCase.expectedStateFuncCalls, stateFuncCalls)
			require.Less(t, duration, time.Duration(testCase.expectedStateFuncCalls)*backoffDuration, "should only wait between calls to the state function, not before or after calls")
		})
	}
}

func TestFrontendRunningMiddleware_Disabled(t *testing.T) {
	downstream := HandlerFunc(func(ctx context.Context, r Request) (Response, error) {
		panic("should not be called")
	})

	stateFuncCalls := 0
	stateFunc := func() services.State {
		stateFuncCalls++
		return services.New
	}

	handler := newFrontendRunningMiddleware(stateFunc, 0, 10, log.NewNopLogger()).Wrap(downstream)
	resp, err := handler.Do(context.Background(), nil)

	require.Equal(t, apierror.New(apierror.TypeUnavailable, "frontend not running: New"), err)
	require.Nil(t, resp)
	require.Equal(t, 1, stateFuncCalls)
}
