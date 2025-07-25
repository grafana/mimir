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
)

// csvMapper provides generic CSV parsing and writing functionality for test data
type csvMapper[T any] struct {
	comma         rune
	numColumns    int
	filePath      string
	fromCSVRecord func([]string) T
	toCSVRecord   func(T) []string
}

// newCSVMapper creates a new CSV mapper for type T
func newCSVMapper[T any](numColumns int, filePath string, fromCSVRecord func([]string) T, toCSVRecord func(T) []string) *csvMapper[T] {
	return &csvMapper[T]{
		comma:         ';',
		numColumns:    numColumns,
		filePath:      filePath,
		fromCSVRecord: fromCSVRecord,
		toCSVRecord:   toCSVRecord,
	}
}

// ParseTestCases reads and parses test cases from the CSV file
func (m *csvMapper[T]) ParseTestCases(t *testing.T) []T {
	file, err := os.Open(m.filePath)
	require.NoError(t, err)
	defer file.Close()

	reader := csv.NewReader(file)
	reader.LazyQuotes = true
	reader.Comma = m.comma
	records, err := reader.ReadAll()
	require.NoError(t, err)

	testCases := make([]T, 0, len(records))
	for _, record := range records {
		if !assert.Len(t, record, m.numColumns) {
			continue
		}
		testCase := m.fromCSVRecord(record)
		testCases = append(testCases, testCase)
	}

	return testCases
}

// WriteTestCases writes test cases back to the CSV file
func (m *csvMapper[T]) WriteTestCases(t *testing.T, testCases []T) {
	tmpFile, err := os.CreateTemp("", "")
	require.NoError(t, err)

	for _, tc := range testCases {
		record := m.toCSVRecord(tc)
		// we write out our own CSV so that we don't get double quotes
		_, err = tmpFile.WriteString(strings.Join(record, string(m.comma)) + "\n")
		assert.NoError(t, err)
	}
	assert.NoError(t, tmpFile.Sync())
	assert.NoError(t, tmpFile.Close())
	assert.NoError(t, os.Rename(tmpFile.Name(), m.filePath))
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
