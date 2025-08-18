// SPDX-License-Identifier: AGPL-3.0-only

package error

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/grafana/regexp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
