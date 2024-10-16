// SPDX-License-Identifier: AGPL-3.0-only

package test

import (
	"fmt"
	"io"
	"strings"
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
