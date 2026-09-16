// SPDX-License-Identifier: AGPL-3.0-only

package inflight

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

const (
	countName = countMetricName
	countHelp = countMetricHelp
	ageName   = ageMetricName
	ageHelp   = ageMetricHelp

	testType = "test"
)

type fakeClock struct {
	t time.Time
}

func (c *fakeClock) advance(d time.Duration) {
	c.t = c.t.Add(d)
}

// newTestCollector returns a collector driven by a clock the test controls, plus a registry
// it is registered with.
func newTestCollector(t *testing.T) (*MaxInflightCollector, *fakeClock, *prometheus.Registry) {
	t.Helper()

	clock := &fakeClock{t: time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)}
	c := NewMaxInflightCollector(testType)
	c.now = func() time.Time { return clock.t }

	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(c)

	return c, clock, reg
}

func expected(series string) string {
	return "# HELP " + countName + " " + countHelp + "\n" +
		"# TYPE " + countName + " gauge\n" +
		series
}

func TestMaxInflightCollector_ReportsPeakNotInstantaneousCount(t *testing.T) {
	c, _, reg := newTestCollector(t)

	a, b := c.Add("tenant-1"), c.Add("tenant-1")
	c.Add("tenant-1")
	c.Remove(a)
	c.Remove(b)

	// One query is left in flight, but the peak during the window was three.
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expected(
		countName+`{type="test",user="tenant-1"} 3`+"\n",
	)), countName))
}

func TestMaxInflightCollector_ResetsToCurrentNotZero(t *testing.T) {
	c, _, reg := newTestCollector(t)

	c.Add("tenant-1")
	id := c.Add("tenant-1")
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expected(
		countName+`{type="test",user="tenant-1"} 2`+"\n",
	)), countName))

	// The second window starts from the standing concurrency of two, not from zero.
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expected(
		countName+`{type="test",user="tenant-1"} 2`+"\n",
	)), countName))

	c.Remove(id)
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expected(
		countName+`{type="test",user="tenant-1"} 2`+"\n",
	)), countName))

	// Only now, with no drop during the window, does it fall to the standing one.
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expected(
		countName+`{type="test",user="tenant-1"} 1`+"\n",
	)), countName))
}

func TestMaxInflightCollector_AgeOfQueryThatFinishedInWindow(t *testing.T) {
	c, clock, reg := newTestCollector(t)

	// A query that starts and finishes entirely between two collections is still reported.
	id := c.Add("tenant-1")
	clock.advance(7 * time.Second)
	c.Remove(id)
	clock.advance(3 * time.Second)

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(
		"# HELP "+ageName+" "+ageHelp+"\n"+
			"# TYPE "+ageName+" gauge\n"+
			ageName+`{type="test",user="tenant-1"} 7`+"\n",
	), ageName))
}

func TestMaxInflightCollector_AgeOfQueryStillRunningKeepsRising(t *testing.T) {
	c, clock, reg := newTestCollector(t)

	c.Add("tenant-1")

	clock.advance(5 * time.Second)
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(
		"# HELP "+ageName+" "+ageHelp+"\n"+
			"# TYPE "+ageName+" gauge\n"+
			ageName+`{type="test",user="tenant-1"} 5`+"\n",
	), ageName))

	clock.advance(11 * time.Second)
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(
		"# HELP "+ageName+" "+ageHelp+"\n"+
			"# TYPE "+ageName+" gauge\n"+
			ageName+`{type="test",user="tenant-1"} 16`+"\n",
	), ageName))
}

func TestMaxInflightCollector_LongestOfFinishedAndRunningWins(t *testing.T) {
	c, clock, reg := newTestCollector(t)

	slow := c.Add("tenant-1")
	clock.advance(20 * time.Second)
	c.Remove(slow)

	// A younger query is still in flight, so the finished query's age is the one reported.
	c.Add("tenant-1")
	clock.advance(2 * time.Second)

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(
		"# HELP "+ageName+" "+ageHelp+"\n"+
			"# TYPE "+ageName+" gauge\n"+
			ageName+`{type="test",user="tenant-1"} 20`+"\n",
	), ageName))

	// The finished query belongs to the previous window; only the running one remains.
	clock.advance(1 * time.Second)
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(
		"# HELP "+ageName+" "+ageHelp+"\n"+
			"# TYPE "+ageName+" gauge\n"+
			ageName+`{type="test",user="tenant-1"} 3`+"\n",
	), ageName))
}

func TestMaxInflightCollector_RemoveIsIdempotent(t *testing.T) {
	c, clock, reg := newTestCollector(t)

	id := c.Add("tenant-1")
	clock.advance(4 * time.Second)
	c.Remove(id)
	// A cleanup that runs twice, and an ID that was never issued, must both be no-ops.
	c.Remove(id)
	c.Remove(99999)

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expected(
		countName+`{type="test",user="tenant-1"} 1`+"\n",
	)), countName))

	// Nothing is left in flight, so the tenant reports nothing at all from here on.
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(""), countName, ageName))
}

func TestMaxInflightCollector_PrunesTenantWithNothingInFlight(t *testing.T) {
	c, _, reg := newTestCollector(t)

	id := c.Add("tenant-1")
	c.Remove(id)

	// The window in which the query ran still reports it.
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expected(
		countName+`{type="test",user="tenant-1"} 1`+"\n",
	)), countName))
	require.Empty(t, c.tenants, "tenant should be dropped once it has nothing in flight")

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(""), countName, ageName))

	// A later query recreates the tenant.
	c.Add("tenant-1")
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expected(
		countName+`{type="test",user="tenant-1"} 1`+"\n",
	)), countName))
}

func TestMaxInflightCollector_TenantsAreIndependent(t *testing.T) {
	c, clock, reg := newTestCollector(t)

	c.Add("tenant-1")
	clock.advance(9 * time.Second)
	c.Add("tenant-2")
	c.Add("tenant-2")
	clock.advance(1 * time.Second)

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expected(
		countName+`{type="test",user="tenant-1"} 1`+"\n"+
			countName+`{type="test",user="tenant-2"} 2`+"\n",
	)), countName))

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(
		"# HELP "+ageName+" "+ageHelp+"\n"+
			"# TYPE "+ageName+" gauge\n"+
			ageName+`{type="test",user="tenant-1"} 10`+"\n"+
			ageName+`{type="test",user="tenant-2"} 1`+"\n",
	), ageName))
}

func TestMaxInflightCollector_NoQueriesReportsNothing(t *testing.T) {
	_, _, reg := newTestCollector(t)

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(""), countName, ageName))
}

// The two request types share one metric name and are told apart by the "type" const label,
// so both collectors have to coexist in a single registry.
func TestMaxInflightCollector_RequestTypesShareOneMetricName(t *testing.T) {
	http, dispatched := NewMaxInflightCollector("http"), NewMaxInflightCollector("dispatched")

	reg := prometheus.NewPedanticRegistry()
	require.NoError(t, reg.Register(http))
	require.NoError(t, reg.Register(dispatched), "both request types must register the same metric name")

	http.Add("tenant-1")
	dispatched.Add("tenant-1")
	dispatched.Add("tenant-1")

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(
		"# HELP "+countName+" "+countHelp+"\n"+
			"# TYPE "+countName+" gauge\n"+
			countName+`{type="dispatched",user="tenant-1"} 2`+"\n"+
			countName+`{type="http",user="tenant-1"} 1`+"\n",
	), countName))
}
