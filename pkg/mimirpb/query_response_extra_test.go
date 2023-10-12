// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/grafana/regexp"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/test"
)

func TestAllPrometheusStatusValues(t *testing.T) {
	prometheusStatusStrings := extractPrometheusStatusStrings(t)

	for _, prometheusStatusString := range prometheusStatusStrings {
		status, err := StatusFromPrometheusString(prometheusStatusString)
		require.NoError(t, err)

		actualStatusString, err := status.ToPrometheusString()
		require.NoError(t, err)
		require.Equal(t, prometheusStatusString, actualStatusString)
	}
}

func TestAllPrometheusErrorTypeValues(t *testing.T) {
	prometheusErrorTypeStrings := extractPrometheusErrorTypeStrings(t)

	for _, prometheusErrorTypeString := range prometheusErrorTypeStrings {
		errorType, err := ErrorTypeFromPrometheusString(prometheusErrorTypeString)
		require.NoError(t, err)

		actualErrorTypeString, err := errorType.ToPrometheusString()
		require.NoError(t, err)
		require.Equal(t, prometheusErrorTypeString, actualErrorTypeString)
	}
}

// HACK: this is a very fragile way of checking if there have been any additional status values added to Prometheus
// It won't catch any values that are created that aren't defined as constants, and will break if the values are moved to a new file, defined in a different way etc.
func extractPrometheusStatusStrings(t *testing.T) []string {
	return extractPrometheusStrings(t, "status")
}

// HACK: this is a very fragile way of checking if there have been any additional error type values added to Prometheus
// It won't catch any values that are created that aren't defined as constants, and will break if the values are moved to a new file, defined in a different way etc.
func extractPrometheusErrorTypeStrings(t *testing.T) []string {
	return extractPrometheusStrings(t, "errorType")
}

func extractPrometheusStrings(t *testing.T, constantType string) []string {
	sourceFile, err := filepath.Abs(filepath.Join("..", "..", "vendor", "github.com", "prometheus", "prometheus", "web", "api", "v1", "api.go"))
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

// Check that the Prometheus histogram.FloatHistogram and MimirPb
// FloatHistogram types converted into each other with unsafe.Pointer
// are compatible
func TestFloatHistogramProtobufTypeRemainsInSyncWithPrometheus(t *testing.T) {
	test.RequireSameShape(t, histogram.FloatHistogram{}, FloatHistogram{}, false)
}
