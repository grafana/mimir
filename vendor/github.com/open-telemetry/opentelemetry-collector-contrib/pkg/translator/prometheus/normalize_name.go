// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package prometheus // import "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/prometheus"

import (
	"strings"
	"unicode"

	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/service/featuregate"
)

// The map to translate OTLP units to Prometheus units
// OTLP metrics use the c/s notation as specified at https://ucum.org/ucum.html
// (See also https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/semantic_conventions/README.md#instrument-units)
// Prometheus best practices for units: https://prometheus.io/docs/practices/naming/#base-units
// OpenMetrics specification for units: https://github.com/OpenObservability/OpenMetrics/blob/main/specification/OpenMetrics.md#units-and-base-units
var unitMap = map[string]string{

	// Time
	"d":   "days",
	"h":   "hours",
	"min": "minutes",
	"s":   "seconds",
	"ms":  "milliseconds",
	"us":  "microseconds",
	"ns":  "nanoseconds",

	// Bytes
	"By":   "bytes",
	"KiBy": "kibibytes",
	"MiBy": "mebibytes",
	"GiBy": "gibibytes",
	"TiBy": "tibibytes",
	"KBy":  "kilobytes",
	"MBy":  "megabytes",
	"GBy":  "gigabytes",
	"TBy":  "terabytes",
	"B":    "bytes",
	"KB":   "kilobytes",
	"MB":   "megabytes",
	"GB":   "gigabytes",
	"TB":   "terabytes",

	// SI
	"m": "meters",
	"V": "volts",
	"A": "amperes",
	"J": "joules",
	"W": "watts",
	"g": "grams",

	// Misc
	"Cel": "celsius",
	"Hz":  "hertz",
	"1":   "",
	"%":   "percent",
	"$":   "dollars",
}

// The map that translates the "per" unit
// Example: s => per second (singular)
var perUnitMap = map[string]string{
	"s":  "second",
	"m":  "minute",
	"h":  "hour",
	"d":  "day",
	"w":  "week",
	"mo": "month",
	"y":  "year",
}

var normalizeNameGate = featuregate.Gate{
	ID:          "pkg.translator.prometheus.NormalizeName",
	Enabled:     false,
	Description: "Controls whether metrics names are automatically normalized to follow Prometheus naming convention",
}

func init() {
	// Register the feature gates
	featuregate.GetRegistry().MustRegister(normalizeNameGate)
}

// Build a Prometheus-compliant metric name for the specified metric
//
// Metric name is prefixed with specified namespace and underscore (if any).
// Namespace is not cleaned up. Make sure specified namespace follows Prometheus
// naming convention.
//
// See rules at https://prometheus.io/docs/concepts/data_model/#metric-names-and-labels
// and https://prometheus.io/docs/practices/naming/#metric-and-label-naming
func BuildPromCompliantName(metric pmetric.Metric, namespace string) string {
	var metricName string

	// Full normalization following standard Prometheus naming conventions
	if featuregate.GetRegistry().IsEnabled(normalizeNameGate.ID) {
		return normalizeName(metric, namespace)
	}

	// Simple case (no full normalization, no units, etc.), we simply trim out forbidden chars
	metricName = CleanUpString(metric.Name())

	// Namespace?
	if namespace != "" {
		return namespace + "_" + metricName
	}

	// Metric name starts with a digit? Prefix it with an underscore
	if metricName != "" && unicode.IsDigit(rune(metricName[0])) {
		metricName = "_" + metricName
	}

	return metricName
}

// Build a normalized name for the specified metric
func normalizeName(metric pmetric.Metric, namespace string) string {

	// Split metric name in "tokens" (remove all non-alphanumeric)
	nameTokens := strings.FieldsFunc(
		metric.Name(),
		func(r rune) bool { return !unicode.IsLetter(r) && !unicode.IsDigit(r) },
	)

	// Split unit at the '/' if any
	unitTokens := strings.SplitN(metric.Unit(), "/", 2)

	// Main unit
	// Append if not blank, doesn't contain '{}', and is not present in metric name already
	if len(unitTokens) > 0 {
		mainUnitOtel := strings.TrimSpace(unitTokens[0])
		if mainUnitOtel != "" && !strings.ContainsAny(mainUnitOtel, "{}") {
			mainUnitProm := CleanUpString(unitMapGetOrDefault(mainUnitOtel))
			if mainUnitProm != "" && !contains(nameTokens, mainUnitProm) {
				nameTokens = append(nameTokens, mainUnitProm)
			}
		}

		// Per unit
		// Append if not blank, doesn't contain '{}', and is not present in metric name already
		if len(unitTokens) > 1 && unitTokens[1] != "" {
			perUnitOtel := strings.TrimSpace(unitTokens[1])
			if perUnitOtel != "" && !strings.ContainsAny(perUnitOtel, "{}") {
				perUnitProm := CleanUpString(perUnitMapGetOrDefault(perUnitOtel))
				if perUnitProm != "" && !contains(nameTokens, perUnitProm) {
					nameTokens = append(append(nameTokens, "per"), perUnitProm)
				}
			}
		}

	}

	// Append _total for Counters
	if metric.DataType() == pmetric.MetricDataTypeSum && metric.Sum().IsMonotonic() {
		nameTokens = append(removeItem(nameTokens, "total"), "total")
	}

	// Append _ratio for metrics with unit "1"
	// Some Otel receivers improperly use unit "1" for counters of objects
	// See https://github.com/open-telemetry/opentelemetry-collector-contrib/issues?q=is%3Aissue+some+metric+units+don%27t+follow+otel+semantic+conventions
	// Until these issues have been fixed, we're appending `_ratio` for gauges ONLY
	// Theoretically, counters could be ratios as well, but it's absurd (for mathematical reasons)
	if metric.Unit() == "1" && metric.DataType() == pmetric.MetricDataTypeGauge {
		nameTokens = append(removeItem(nameTokens, "ratio"), "ratio")
	}

	// Namespace?
	if namespace != "" {
		nameTokens = append([]string{namespace}, nameTokens...)
	}

	// Build the string from the tokens, separated with underscores
	normalizedName := strings.Join(nameTokens, "_")

	// Metric name cannot start with a digit, so prefix it with "_" in this case
	if normalizedName != "" && unicode.IsDigit(rune(normalizedName[0])) {
		normalizedName = "_" + normalizedName
	}

	return normalizedName
}

// Clean up specified string so it's Prometheus compliant
func CleanUpString(s string) string {
	return strings.Join(strings.FieldsFunc(s, func(r rune) bool { return !unicode.IsLetter(r) && !unicode.IsDigit(r) }), "_")
}

// Retrieve the Prometheus "basic" unit corresponding to the specified "basic" unit
// Returns the specified unit if not found in unitMap
func unitMapGetOrDefault(unit string) string {
	if promUnit, ok := unitMap[unit]; ok {
		return promUnit
	}
	return unit
}

// Retrieve the Prometheus "per" unit corresponding to the specified "per" unit
// Returns the specified unit if not found in perUnitMap
func perUnitMapGetOrDefault(perUnit string) string {
	if promPerUnit, ok := perUnitMap[perUnit]; ok {
		return promPerUnit
	}
	return perUnit
}

// Returns whether the slice contains the specified value
func contains(slice []string, value string) bool {
	for _, sliceEntry := range slice {
		if sliceEntry == value {
			return true
		}
	}
	return false
}

// Remove the specified value from the slice
func removeItem(slice []string, value string) []string {
	newSlice := make([]string, 0, len(slice))
	for _, sliceEntry := range slice {
		if sliceEntry != value {
			newSlice = append(newSlice, sliceEntry)
		}
	}
	return newSlice
}
