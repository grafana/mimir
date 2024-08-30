// SPDX-License-Identifier: AGPL-3.0-only

package test

import (
	"bufio"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

type ExpectedMetricsContext struct {
	help  map[string]string
	types map[string]string
}

func NewExpectedMetricsContext() ExpectedMetricsContext {
	return ExpectedMetricsContext{
		help:  make(map[string]string),
		types: make(map[string]string),
	}
}

func (m *ExpectedMetricsContext) Add(name, help, types string) {
	m.help[name] = help
	m.types[name] = types
}

type ExpectedMetrics struct {
	Context ExpectedMetricsContext
	Output  []string
	Names   []string
}

func (m *ExpectedMetrics) AddMultiple(name string, counts map[string]int) {
	outputs := []string{fmt.Sprintf(`# HELP %s %s
# TYPE %s %s`, name, m.Context.help[name], name, m.Context.types[name])}
	for labelSet, count := range counts {
		if labelSet == "" {
			outputs = append(outputs, fmt.Sprintf(`%s %d`, name, count))
			continue
		}
		outputs = append(outputs, fmt.Sprintf(`%s{%s} %d`, name, labelSet, count))
	}
	m.Output = append(m.Output, strings.Join(outputs, "\n"))
	m.Names = append(m.Names, name)
}

func (m *ExpectedMetrics) Add(name, labelSet string, count int) {
	m.AddMultiple(name, map[string]int{labelSet: count})
}

func (m *ExpectedMetrics) AddEmpty(name string) {
	m.AddMultiple(name, map[string]int{})
}

func (m *ExpectedMetrics) GetOutput() io.Reader {
	return strings.NewReader(strings.Join(m.Output, "\n\n") + "\n")
}

func (m *ExpectedMetrics) GetNames() []string {
	return m.Names
}

func AssertGatherAndCompare(t *testing.T, g prometheus.Gatherer, expectedText string, metrics ...string) {
	sc := bufio.NewScanner(strings.NewReader(expectedText))
	absent := make([]string, len(metrics))
	copy(absent, metrics)
	required := make([]string, 0, len(metrics))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if len(line) == 0 || strings.HasPrefix(line, "#") {
			continue
		}
		for i, metric := range absent {
			if strings.HasPrefix(line, metric) && (line[len(metric)] == ' ' || line[len(metric)] == '{') {
				absent = append(absent[:i], absent[i+1:]...)
				required = append(required, metric)
				break
			}
		}
	}
	require.Equal(t, len(metrics), len(required)+len(absent)) // Sanity check.
	if len(required) > 0 {
		require.NoError(t, testutil.GatherAndCompare(g, strings.NewReader(expectedText), required...), "should be present: metrics=%s", strings.Join(required, ", "))
	}
	notAbsent := []string{}
	for _, metric := range absent {
		count, err := testutil.GatherAndCount(g, metric)
		require.NoError(t, err)
		if count > 0 {
			notAbsent = append(notAbsent, metric)
		}
	}
	require.Empty(t, notAbsent, "should be absent: metrics=%s", strings.Join(notAbsent, ", "))
}
