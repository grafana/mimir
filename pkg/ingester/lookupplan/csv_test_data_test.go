// SPDX-License-Identifier: AGPL-3.0-only

package lookupplan

import (
	"encoding/csv"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/prometheus/alertmanager/matchers/parse"
	amlabels "github.com/prometheus/alertmanager/pkg/labels"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/promqlext"
)

// csvTestData provides generic CSV parsing and writing functionality for test data.
//
// Expected data can be read from filePath for tests, and test results can be written
// back to filePath. Any unit tests are expected to handle assertions on results against
// data from the CSV themselves.
//
// The CSV format includes:
// - Header row with column names (automatically written on WriteTestCases and skipped during ParseTestCases)
// - Data rows with values corresponding to each column
// - Semicolon (;) as the delimiter
//
// Usage example:
//
//	type testCase struct {
//		input    string
//		expected float64
//	}
//
//	data := newCSVTestData(
//		[]string{"input", "expected"},
//		"testdata/example.csv",
//		func(record []string) testCase {
//			return testCase{input: record[0], expected: parseFloat(t, record[1])}
//		},
//		func(tc testCase) []string {
//			return []string{tc.input, fmt.Sprintf("%.2f", tc.expected)}
//		},
//	)
//
//	testCases := data.ParseTestCases(t)
//	for _, tc := range testCases {
//		result := myFunction(tc.input)
//		assert.Equal(t, tc.expected, result)
//	}
//
//	// Optionally update CSV with new expected values:
//	const writeNewResults = true
//
//	if writeNewResults {
//	    t.Cleanup(func() { data.WriteTestCases(t, testCases) })
//	}
type csvTestData[T any] struct {
	comma         rune
	columnNames   []string
	filePath      string
	fromCSVRecord func([]string) T
	toCSVRecord   func(T) []string
}

// newCSVTestData creates a new CSV test data loader for type T
func newCSVTestData[T any](columnNames []string, filePath string, fromCSVRecord func([]string) T, toCSVRecord func(T) []string) *csvTestData[T] {
	return &csvTestData[T]{
		comma:         ';',
		columnNames:   columnNames,
		filePath:      filePath,
		fromCSVRecord: fromCSVRecord,
		toCSVRecord:   toCSVRecord,
	}
}

// ParseTestCases reads and parses test cases from the CSV file
func (d *csvTestData[T]) ParseTestCases(t testing.TB) []T {
	file, err := os.Open(d.filePath)
	require.NoError(t, err)
	defer file.Close()

	reader := csv.NewReader(file)
	reader.LazyQuotes = true
	reader.Comma = d.comma
	records, err := reader.ReadAll()
	require.NoError(t, err)

	assert.Equal(t, d.columnNames, records[0])

	testCases := make([]T, 0, len(records)-1)
	for i := 1; i < len(records); i++ {
		record := records[i]
		if !assert.Len(t, record, len(d.columnNames)) {
			continue
		}
		testCase := d.fromCSVRecord(record)
		testCases = append(testCases, testCase)
	}

	return testCases
}

// WriteTestCases writes test cases back to the CSV file
func (d *csvTestData[T]) WriteTestCases(t *testing.T, testCases []T) {
	tmpFile, err := os.CreateTemp("", "")
	require.NoError(t, err)

	// Write header row
	_, err = tmpFile.WriteString(strings.Join(d.columnNames, string(d.comma)) + "\n")
	assert.NoError(t, err)

	for _, tc := range testCases {
		record := d.toCSVRecord(tc)
		// we write out our own CSV so that we don't get double quotes
		_, err = tmpFile.WriteString(strings.Join(record, string(d.comma)) + "\n")
		assert.NoError(t, err)
	}
	assert.NoError(t, tmpFile.Sync())
	assert.NoError(t, tmpFile.Close())
	assert.NoError(t, os.Rename(tmpFile.Name(), d.filePath))
}

func parseMatcher(t *testing.T, m string) *labels.Matcher {
	amMatcher, err := parse.Matcher(m)
	require.NoErrorf(t, err, "Failed to parse matcher: %s", m)

	// Convert alertmanager matcher to prometheus matcher
	var promMatcher *labels.Matcher
	switch amMatcher.Type {
	case amlabels.MatchEqual:
		promMatcher = labels.MustNewMatcher(labels.MatchEqual, amMatcher.Name, amMatcher.Value)
	case amlabels.MatchNotEqual:
		promMatcher = labels.MustNewMatcher(labels.MatchNotEqual, amMatcher.Name, amMatcher.Value)
	case amlabels.MatchRegexp:
		promMatcher = labels.MustNewMatcher(labels.MatchRegexp, amMatcher.Name, amMatcher.Value)
	case amlabels.MatchNotRegexp:
		promMatcher = labels.MustNewMatcher(labels.MatchNotRegexp, amMatcher.Name, amMatcher.Value)
	default:
		t.Fatalf("Unexpected matcher type: %s", amMatcher.Type)
	}
	return promMatcher
}

func formatFloat(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}

func parseFloat(t *testing.T, str string) float64 {
	f, err := strconv.ParseFloat(str, 64)
	require.NoErrorf(t, err, "Failed to parse float: %s", str)
	return f
}

func parseUint(t *testing.T, str string) uint64 {
	u, err := strconv.ParseUint(str, 10, 64)
	require.NoErrorf(t, err, "Failed to parse uint: %s", str)
	return u
}

func parseVectorSelector(t testing.TB, str string) []*labels.Matcher {
	p := promqlext.NewDefaultParser()
	matchers, err := p.ParseMetricSelector(str)
	require.NoError(t, err)
	return matchers
}
