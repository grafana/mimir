// SPDX-License-Identifier: AGPL-3.0-only

package error

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/gogo/status"
	"github.com/grafana/regexp"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	googlestatus "google.golang.org/grpc/status"
)

func TestAllPrometheusErrorTypeValues(t *testing.T) {
	prometheusErrorTypeStrings := extractPrometheusErrorTypeStrings(t)

	for _, prometheusErrorTypeString := range prometheusErrorTypeStrings {
		errorType := Type(prometheusErrorTypeString)
		apiError := New(errorType, "")

		switch errorType {
		case TypeUnavailable:
			require.Equal(t, http.StatusServiceUnavailable, apiError.StatusCode())
		case TypeInternal, TypeNone:
			require.Equal(t, http.StatusInternalServerError, apiError.StatusCode())
		default:
			// If this assertion fails, it probably means a new error type has been added to Prometheus' API.
			require.NotEqual(t, http.StatusInternalServerError, apiError.StatusCode(), "unrecognised Prometheus error type constant '%s'", prometheusErrorTypeString)
		}
	}
}

// HACK: this is a very fragile way of checking if there have been any additional error type values added to Prometheus
// It won't catch any values that are created that aren't defined as constants, and will break if the values are moved to a new file, defined in a different way etc.
func extractPrometheusErrorTypeStrings(t *testing.T) []string {
	// This regexp is intended to match lines like: errorTimeout = errorType{ErrorTimeout, "timeout"}, capturing the value between the quotes
	return extractPrometheusStrings(t, `(?m)^\s*[^ ]+\s+=\s+errorType\{[^,]+,\s+"([^"]*)"\}`)
}

func extractPrometheusStrings(t *testing.T, rgx string) []string {
	sourceFile, err := filepath.Abs(filepath.Join("..", "..", "..", "vendor", "github.com", "prometheus", "prometheus", "web", "api", "v1", "api.go"))
	require.NoError(t, err)

	sourceFileContents, err := os.ReadFile(sourceFile)
	require.NoError(t, err)

	matchRegex := regexp.MustCompile(rgx)
	matches := matchRegex.FindAllSubmatch(sourceFileContents, -1)
	strings := make([]string, 0, len(matches))

	for _, match := range matches {
		strings = append(strings, string(match[1]))
	}

	require.NotEmpty(t, strings)

	return strings
}

func TestIsRetryableAPIError(t *testing.T) {
	retryable := []error{
		New(TypeInternal, string(TypeInternal)),
		context.Canceled,
		context.DeadlineExceeded,
		errors.New("something bad"),
	}

	nonRetryable := []error{
		New(TypeNone, "none"),
		New(TypeNotFound, string(TypeNotFound)),
		New(TypeTimeout, string(TypeTimeout)),
		New(TypeTooManyRequests, string(TypeTooManyRequests)),
		New(TypeNotAcceptable, string(TypeNotAcceptable)),
		New(TypeUnavailable, string(TypeUnavailable)),
		New(TypeCanceled, string(TypeCanceled)),
		New(TypeExec, string(TypeExec)),
		New(TypeBadData, string(TypeBadData)),
		New(TypeTooLargeEntry, string(TypeTooLargeEntry)),
	}

	t.Run("retryable", func(t *testing.T) {
		for _, err := range retryable {
			t.Run(err.Error(), func(t *testing.T) {
				assert.True(t, IsRetryableAPIError(err))
			})
		}
	})

	t.Run("non-retryable", func(t *testing.T) {
		for _, err := range nonRetryable {
			t.Run(err.Error(), func(t *testing.T) {
				assert.False(t, IsRetryableAPIError(err))
			})
		}
	})
}

func TestTypeForError(t *testing.T) {
	fallback := TypeNotAcceptable

	testCases := map[string]struct {
		err      error
		expected Type
	}{
		"generic error": {
			err:      errors.New("something went wrong"),
			expected: fallback,
		},
		"context canceled": {
			err:      context.Canceled,
			expected: TypeCanceled,
		},
		"context deadline exceeded": {
			err:      context.DeadlineExceeded,
			expected: TypeTimeout,
		},
		"storage error": {
			err:      promql.ErrStorage{Err: errors.New("could not load data")},
			expected: TypeInternal,
		},
		"query canceled error": {
			err:      promql.ErrQueryCanceled("canceled"),
			expected: TypeCanceled,
		},
		"query timeout error": {
			err:      promql.ErrQueryTimeout("timed out"),
			expected: TypeTimeout,
		},
		"APIError": {
			err:      New(TypeNotAcceptable, "request is not acceptable"),
			expected: TypeNotAcceptable,
		},
		"gRPC error with canceled code": {
			err:      status.Error(codes.Canceled, "request was canceled"),
			expected: TypeCanceled,
		},
		"gRPC error with unknown code": {
			err:      status.Error(codes.Unknown, "something went wrong"),
			expected: fallback,
		},
		"gRPC error with invalid argument code": {
			err:      status.Error(codes.InvalidArgument, "request has invalid argument"),
			expected: TypeBadData,
		},
		"gRPC error with deadline exceeded code": {
			err:      status.Error(codes.DeadlineExceeded, "request timed out"),
			expected: TypeTimeout,
		},
		"gRPC error with not found code": {
			err:      status.Error(codes.NotFound, "resource not found"),
			expected: TypeNotFound,
		},
		"gRPC error with internal code": {
			err:      status.Error(codes.Internal, "something went wrong"),
			expected: TypeInternal,
		},
		"gRPC error with unavailable code": {
			err:      status.Error(codes.Unavailable, "resource not available"),
			expected: TypeUnavailable,
		},
		"gRPC error from Google's gRPC library": {
			err:      googlestatus.Error(codes.Canceled, "request was canceled"),
			expected: TypeCanceled,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, testCase.expected, TypeForError(testCase.err, fallback))
		})

		t.Run(name+" (wrapped)", func(t *testing.T) {
			err := fmt.Errorf("something went wrong one level down: %w", testCase.err)
			require.Equal(t, testCase.expected, TypeForError(err, fallback))
		})
	}
}
