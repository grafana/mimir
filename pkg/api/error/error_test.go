// SPDX-License-Identifier: AGPL-3.0-only

package error

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/regexp"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestAllPrometheusErrorTypeValues(t *testing.T) {
	prometheusErrorTypeStrings := extractPrometheusErrorTypeStrings(t)

	for _, prometheusErrorTypeString := range prometheusErrorTypeStrings {
		errorType := Type(prometheusErrorTypeString)
		apiError := New(errorType, "").(*apiError)

		if errorType == TypeUnavailable {
			require.Equal(t, http.StatusServiceUnavailable, apiError.statusCode())
		} else if errorType == TypeInternal || errorType == TypeNone {
			require.Equal(t, http.StatusInternalServerError, apiError.statusCode())
		} else {
			// If this assertion fails, it probably means a new error type has been added to Prometheus' API.
			require.NotEqual(t, http.StatusInternalServerError, apiError.statusCode(), "unrecognised Prometheus error type constant '%s'", prometheusErrorTypeString)
		}
	}
}

// HACK: this is a very fragile way of checking if there have been any additional error type values added to Prometheus
// It won't catch any values that are created that aren't defined as constants, and will break if the values are moved to a new file, defined in a different way etc.
func extractPrometheusErrorTypeStrings(t *testing.T) []string {
	return extractPrometheusStrings(t, "errorType")
}

func extractPrometheusStrings(t *testing.T, constantType string) []string {
	sourceFile, err := filepath.Abs(filepath.Join("..", "..", "..", "vendor", "github.com", "prometheus", "prometheus", "web", "api", "v1", "api.go"))
	require.NoError(t, err)

	sourceFileContents, err := os.ReadFile(sourceFile)
	require.NoError(t, err)

	// This regexp is intended to match lines like: errorTimeout  errorType = "timeout"
	matchRegex := regexp.MustCompile(`(?m)^\s*[^ ]+\s+` + regexp.QuoteMeta(constantType) + `\s+=\s+"(.*)"$`)
	matches := matchRegex.FindAllSubmatch(sourceFileContents, -1)
	strings := make([]string, 0, len(matches))

	for _, match := range matches {
		strings = append(strings, string(match[1]))
	}

	require.NotEmpty(t, strings)

	return strings
}

func TestHTTPResponseFromError(t *testing.T) {
	testCases := map[string]struct {
		err                error
		expectOk           bool
		expectedStatusCode int
		expectedError      string
		expectedErrorType  Type
	}{
		"not an api error": {
			err:      errors.New("not an api error"),
			expectOk: false,
		},
		"bad data": {
			err:                New(TypeBadData, "bad data"),
			expectOk:           true,
			expectedStatusCode: http.StatusBadRequest,
			expectedError:      "bad data",
			expectedErrorType:  TypeBadData,
		},
		"execution": {
			err:                New(TypeExec, "execution"),
			expectOk:           true,
			expectedStatusCode: http.StatusUnprocessableEntity,
			expectedError:      "execution",
			expectedErrorType:  TypeExec,
		},
		"cancelled": {
			err:                New(TypeCanceled, "cancelled"),
			expectOk:           true,
			expectedStatusCode: 499,
			expectedError:      "cancelled",
			expectedErrorType:  TypeCanceled,
		},
		"timeout": {
			err:                New(TypeTimeout, "timeout"),
			expectOk:           true,
			expectedStatusCode: http.StatusServiceUnavailable,
			expectedError:      "timeout",
			expectedErrorType:  TypeTimeout,
		},
		"internal": {
			err:                New(TypeInternal, "internal"),
			expectOk:           true,
			expectedStatusCode: http.StatusInternalServerError,
			expectedError:      "internal",
			expectedErrorType:  TypeInternal,
		},
		"not found": {
			err:                New(TypeNotFound, "not found"),
			expectOk:           true,
			expectedStatusCode: http.StatusNotFound,
			expectedError:      "not found",
			expectedErrorType:  TypeNotFound,
		},
		"too many requests": {
			err:                New(TypeTooManyRequests, "too many requests"),
			expectOk:           true,
			expectedStatusCode: http.StatusTooManyRequests,
			expectedError:      "too many requests",
			expectedErrorType:  TypeTooManyRequests,
		},
		"too large entry": {
			err:                New(TypeTooLargeEntry, "too large entry"),
			expectOk:           true,
			expectedStatusCode: http.StatusRequestEntityTooLarge,
			expectedError:      "too large entry",
			expectedErrorType:  TypeTooLargeEntry,
		},
		"not acceptable": {
			err:                New(TypeNotAcceptable, "not acceptable"),
			expectOk:           true,
			expectedStatusCode: http.StatusNotAcceptable,
			expectedError:      "not acceptable",
			expectedErrorType:  TypeNotAcceptable,
		},
		"wrapped error": {
			err:                errors.Wrap(New(TypeBadData, "bad data"), "wrapped"),
			expectOk:           true,
			expectedStatusCode: http.StatusBadRequest,
			expectedError:      "wrapped: bad data",
			expectedErrorType:  TypeBadData,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			response, ok := HTTPResponseFromError(tc.err)
			require.Equal(t, tc.expectOk, ok)
			if !tc.expectOk {
				return
			}
			require.Contains(t, response.Headers, &httpgrpc.Header{Key: "Content-Type", Values: []string{"application/json"}})
			require.Equal(t, tc.expectedStatusCode, int(response.Code))
			apiErr := apiResponse{}
			require.NoError(t, json.Unmarshal(response.Body, &apiErr))
			require.Equal(t, "error", apiErr.Status)
			require.Equal(t, tc.expectedErrorType, apiErr.ErrorType)
			require.Equal(t, tc.expectedError, apiErr.Error)
		})
	}
}

type apiResponse struct {
	Status    string `json:"status"`
	ErrorType Type   `json:"errorType,omitempty"`
	Error     string `json:"error,omitempty"`
}
