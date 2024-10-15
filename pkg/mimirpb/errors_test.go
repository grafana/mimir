// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

import (
	"errors"
	"net/http"
	"testing"

	"github.com/gogo/status"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

func TestIsClientError(t *testing.T) {
	testCases := map[string]struct {
		err      error
		expected bool
	}{
		"a gRPC error with status code 4xx built by httpgrpc package is a client error": {
			err:      httpgrpc.Errorf(http.StatusBadRequest, "this is an error"),
			expected: true,
		},
		"a gRPC error with status code 4xx built by grpc's status package is a client error": {
			err:      grpcstatus.Error(http.StatusTooManyRequests, "this is an error"),
			expected: true,
		},
		"a gRPC error with status code 4xx built by gogo's status package is a client error": {
			err:      status.Error(http.StatusTooManyRequests, "this is an error"),
			expected: true,
		},
		"a gRPC error with status code different from 4xx built by httpgrpc package is a not client error": {
			err:      httpgrpc.Errorf(http.StatusInternalServerError, "this is an error"),
			expected: false,
		},
		"a gRPC error with status code different from 4xx built by grpc's status package is not a client error": {
			err:      grpcstatus.Error(http.StatusServiceUnavailable, "this is an error"),
			expected: false,
		},
		"a gRPC error with status code different from 4xx built by gogo's status package is not a client error": {
			err:      status.Error(codes.Internal, "this is an error"),
			expected: false,
		},
		"a gRPC error with BAD_DATA in details is a client error": {
			err:      mustStatusWithDetails(codes.InvalidArgument, BAD_DATA).Err(),
			expected: true,
		},
		"a gRPC error with TENANT_LIMIT in details is a client error": {
			err:      mustStatusWithDetails(codes.FailedPrecondition, TENANT_LIMIT).Err(),
			expected: true,
		},
		"a gRPC error with TSDB_UNAVAILABLE in details is not a client error": {
			err:      mustStatusWithDetails(codes.FailedPrecondition, TSDB_UNAVAILABLE).Err(),
			expected: false,
		},
		"a random non-gRPC error is not a client error": {
			err:      errors.New("this is a random error"),
			expected: false,
		},
	}
	for testName, testData := range testCases {
		t.Run(testName, func(t *testing.T) {
			require.Equal(t, testData.expected, IsClientError(testData.err))
		})
	}
}

func mustStatusWithDetails(code codes.Code, cause ErrorCause) *status.Status {
	s, err := status.New(code, "").WithDetails(&ErrorDetails{Cause: cause})
	if err != nil {
		panic(err)
	}
	return s
}
