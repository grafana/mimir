// SPDX-License-Identifier: AGPL-3.0-only

package inflight

import (
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

const (
	countName = "cortex_query_frontend_max_inflight_requests"
	countHelp = "Peak number of concurrent in-flight requests for a tenant since the last metric collection (reset on each scrape). " + typeHelp
	ageName   = "cortex_query_frontend_max_inflight_request_age_seconds"
	ageHelp   = "Greatest age reached by an in-flight request for a tenant since the last metric collection (reset on each scrape). Requests that finished within the window are included. " + typeHelp

	testType = "test"
)

// newTestCollector returns a collector and the registry it is registered with. Tests that
// need to control time run inside synctest.Test, whose fake clock reaches time.Now.
func newTestCollector(t *testing.T) (*MaxInflightCollector, *prometheus.Registry) {
	t.Helper()

	c := NewMaxInflightCollector(testType)

	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(c)

	return c, reg
}

func expected(series string) string {
	return "# HELP " + countName + " " + countHelp + "\n" +
		"# TYPE " + countName + " gauge\n" +
		series
}

func TestMaxInflightCollector_ReportsPeakNotInstantaneousCount(t *testing.T) {
	c, reg := newTestCollector(t)

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
	c, reg := newTestCollector(t)

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
	synctest.Test(t, func(t *testing.T) {
		c, reg := newTestCollector(t)

		// A query that starts and finishes entirely between two collections is still reported.
		id := c.Add("tenant-1")
		time.Sleep(7 * time.Second)
		c.Remove(id)
		time.Sleep(3 * time.Second)

		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(
			"# HELP "+ageName+" "+ageHelp+"\n"+
				"# TYPE "+ageName+" gauge\n"+
				ageName+`{type="test",user="tenant-1"} 7`+"\n",
		), ageName))
	})
}

func TestMaxInflightCollector_AgeOfQueryStillRunningKeepsRising(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, reg := newTestCollector(t)

		c.Add("tenant-1")

		time.Sleep(5 * time.Second)
		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(
			"# HELP "+ageName+" "+ageHelp+"\n"+
				"# TYPE "+ageName+" gauge\n"+
				ageName+`{type="test",user="tenant-1"} 5`+"\n",
		), ageName))

		time.Sleep(11 * time.Second)
		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(
			"# HELP "+ageName+" "+ageHelp+"\n"+
				"# TYPE "+ageName+" gauge\n"+
				ageName+`{type="test",user="tenant-1"} 16`+"\n",
		), ageName))
	})
}

func TestMaxInflightCollector_LongestOfFinishedAndRunningWins(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, reg := newTestCollector(t)

		slow := c.Add("tenant-1")
		time.Sleep(20 * time.Second)
		c.Remove(slow)

		// A younger query is still in flight, so the finished query's age is the one reported.
		c.Add("tenant-1")
		time.Sleep(2 * time.Second)

		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(
			"# HELP "+ageName+" "+ageHelp+"\n"+
				"# TYPE "+ageName+" gauge\n"+
				ageName+`{type="test",user="tenant-1"} 20`+"\n",
		), ageName))

		// The finished query belongs to the previous window; only the running one remains.
		time.Sleep(1 * time.Second)
		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(
			"# HELP "+ageName+" "+ageHelp+"\n"+
				"# TYPE "+ageName+" gauge\n"+
				ageName+`{type="test",user="tenant-1"} 3`+"\n",
		), ageName))
	})
}

func TestMaxInflightCollector_RemoveIsIdempotent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, reg := newTestCollector(t)

		id := c.Add("tenant-1")
		time.Sleep(4 * time.Second)
		c.Remove(id)
		// A cleanup that runs twice, and an ID that was never issued, must both be no-ops.
		c.Remove(id)
		c.Remove(99999)

		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expected(
			countName+`{type="test",user="tenant-1"} 1`+"\n",
		)), countName))

		// Nothing is left in flight, so the tenant reports nothing at all from here on.
		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(""), countName, ageName))
	})
}

func TestMaxInflightCollector_PrunesTenantWithNothingInFlight(t *testing.T) {
	c, reg := newTestCollector(t)

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
	synctest.Test(t, func(t *testing.T) {
		c, reg := newTestCollector(t)

		c.Add("tenant-1")
		time.Sleep(9 * time.Second)
		c.Add("tenant-2")
		c.Add("tenant-2")
		time.Sleep(1 * time.Second)

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
	})
}

func TestMaxInflightCollector_NoQueriesReportsNothing(t *testing.T) {
	_, reg := newTestCollector(t)

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

// Collect returns pruned tenant counters to a pool, so a later tenant can be handed a
// recycled object. It must not inherit the previous tenant's peak or age.
func TestMaxInflightCollector_RecycledTenantStartsClean(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, reg := newTestCollector(t)

		// tenant-1 runs two queries, the longer for 50s, then goes idle.
		a, b := c.Add("tenant-1"), c.Add("tenant-1")
		time.Sleep(50 * time.Second)
		c.Remove(a)
		c.Remove(b)

		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expected(
			countName+`{type="test",user="tenant-1"} 2`+"\n",
		)), countName))

		// That collection pruned tenant-1 and pooled its counters.
		require.Empty(t, c.tenants)

		// tenant-2 now takes the recycled object.
		c.Add("tenant-2")
		time.Sleep(1 * time.Second)

		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expected(
			countName+`{type="test",user="tenant-2"} 1`+"\n",
		)), countName))
		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(
			"# HELP "+ageName+" "+ageHelp+"\n"+
				"# TYPE "+ageName+" gauge\n"+
				ageName+`{type="test",user="tenant-2"} 1`+"\n",
		), ageName))
	})
}
