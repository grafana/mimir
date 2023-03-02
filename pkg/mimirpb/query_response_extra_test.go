// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/grafana/regexp"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/stretchr/testify/require"
	"github.com/xlab/treeprint"
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

func TestFloatHistogramProtobufTypeRemainsInSyncWithPrometheus(t *testing.T) {
	// Why does this test exist?
	// We use unsafe casting to convert between mimirpb.FloatHistogram and Prometheus' histogram.FloatHistogram in
	// FloatHistogramFromPrometheusModel and FloatHistogram.ToPrometheusModel.
	// For this to be safe, the two types need to have the same shape (same fields in the same order).
	// This test ensures that this property is maintained.
	// The fields do not need to have the same names to make the conversion safe, but we also check the names are
	// the same here to ensure there's no confusion (eg. two bool fields swapped).

	protoType := reflect.TypeOf(FloatHistogram{})
	prometheusType := reflect.TypeOf(histogram.FloatHistogram{})

	requireSameShape(t, prometheusType, protoType)
}

func requireSameShape(t *testing.T, expectedType reflect.Type, actualType reflect.Type) {
	expectedFormatted := prettyPrintType(expectedType)
	actualFormatted := prettyPrintType(actualType)

	require.Equal(t, expectedFormatted, actualFormatted)
}

func prettyPrintType(t reflect.Type) string {
	if t.Kind() != reflect.Struct {
		panic(fmt.Sprintf("expected %s to be a struct but is %s", t.Name(), t.Kind()))
	}

	tree := treeprint.NewWithRoot("<root>")
	addTypeToTree(t, tree)

	return tree.String()
}

func addTypeToTree(t reflect.Type, tree treeprint.Tree) {
	if t.Kind() != reflect.Struct {
		panic(fmt.Sprintf("unexpected kind %s", t.Kind()))
	}

	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)

		switch f.Type.Kind() {
		case reflect.Slice:
			name := fmt.Sprintf("+%v %s: []%s", f.Offset, f.Name, f.Type.Elem().Kind())

			if isPrimitive(f.Type.Elem().Kind()) {
				tree.AddNode(name)
			} else {
				addTypeToTree(f.Type.Elem(), tree.AddBranch(name))
			}
		default:
			name := fmt.Sprintf("+%v %s: %s", f.Offset, f.Name, f.Type.Kind())
			tree.AddNode(name)
		}
	}
}

func isPrimitive(k reflect.Kind) bool {
	switch k {
	case reflect.Bool,
		reflect.Int,
		reflect.Int8,
		reflect.Int16,
		reflect.Int32,
		reflect.Int64,
		reflect.Uint,
		reflect.Uint8,
		reflect.Uint16,
		reflect.Uint32,
		reflect.Uint64,
		reflect.Uintptr,
		reflect.Float32,
		reflect.Float64,
		reflect.Complex64,
		reflect.Complex128,
		reflect.String,
		reflect.UnsafePointer:
		return true
	default:
		return false
	}
}
