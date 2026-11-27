// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"sync"
	"testing"

	"github.com/grafana/dskit/httpgrpc"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/queue"
)

func newTestQCU(t *testing.T) (*QueryComponentUtilization, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewRegistry()
	metric := promauto.With(reg).NewSummaryVec(prometheus.SummaryOpts{
		Name: "cortex_query_scheduler_querier_inflight_requests_test",
	}, []string{"query_component"})
	qcu, err := NewQueryComponentUtilization(metric)
	require.NoError(t, err)
	return qcu, reg
}

func newTestSchedulerRequest(queryID uint64, queryComponent string) *SchedulerRequest {
	var dims []string
	if queryComponent != "" {
		dims = []string{queryComponent}
	}
	return &SchedulerRequest{
		FrontendAddr:              "frontend",
		QueryID:                   queryID,
		HttpRequest:               &httpgrpc.HTTPRequest{Method: "GET", Url: "/x"},
		AdditionalQueueDimensions: dims,
	}
}

func TestQueryComponentUtilization_MarkRequestSent_ComponentRouting(t *testing.T) {
	cases := []struct {
		name           string
		queryComponent string
		wantIngester   int
		wantStoreGW    int
	}{
		{"ingester increments only ingester", ingesterQueueDimension, 1, 0},
		{"store-gateway increments only store-gateway", storeGatewayQueueDimension, 0, 1},
		{"ingester-and-store-gateway increments both", ingesterAndStoreGatewayQueueDimension, 1, 1},
		{"empty (unknown) increments both", "", 1, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			qcu, _ := newTestQCU(t)
			qcu.MarkRequestSent(newTestSchedulerRequest(1, tc.queryComponent))
			require.Equal(t, tc.wantIngester, qcu.GetForComponent(Ingester))
			require.Equal(t, tc.wantStoreGW, qcu.GetForComponent(StoreGateway))
			require.Zero(t, qcu.GetForComponent(StrongReadConsistency))
		})
	}
}

func TestQueryComponentUtilization_StrongReadConsistencyRouting(t *testing.T) {
	cases := map[string]struct {
		dims         []string
		wantIngester int
		wantStoreGW  int
	}{
		"strong ingester request": {
			dims: []string{ingesterQueueDimension, strongReadConsistencyQueueDimension}, wantIngester: 1, wantStoreGW: 0,
		},
		"strong ingester-and-store-gateway request": {
			dims: []string{ingesterAndStoreGatewayQueueDimension, strongReadConsistencyQueueDimension}, wantIngester: 1, wantStoreGW: 1,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			qcu, _ := newTestQCU(t)
			req := newTestSchedulerRequest(1, "")
			req.AdditionalQueueDimensions = tc.dims
			qcu.MarkRequestSent(req)
			require.Equal(t, tc.wantIngester, qcu.GetForComponent(Ingester))
			require.Equal(t, tc.wantStoreGW, qcu.GetForComponent(StoreGateway))
			require.Equal(t, 1, qcu.GetForComponent(StrongReadConsistency))

			qcu.MarkRequestCompleted(req)
			require.Zero(t, qcu.GetForComponent(Ingester))
			require.Zero(t, qcu.GetForComponent(StoreGateway))
			require.Zero(t, qcu.GetForComponent(StrongReadConsistency))
		})
	}
}

func TestQueryComponentUtilization_MarkRequestCompleted_Balanced(t *testing.T) {
	qcu, _ := newTestQCU(t)
	req := newTestSchedulerRequest(1, ingesterQueueDimension)
	qcu.MarkRequestSent(req)
	require.Equal(t, 1, qcu.GetForComponent(Ingester))
	qcu.MarkRequestCompleted(req)
	require.Equal(t, 0, qcu.GetForComponent(Ingester))
}

func TestQueryComponentUtilization_MarkRequestCompleted_UnknownKeyDoesNotDecrement(t *testing.T) {
	// MarkRequestCompleted for a request the QCU never saw must not decrement
	// the counters, otherwise a stale or duplicate completion could drive them
	// negative.
	qcu, _ := newTestQCU(t)
	qcu.MarkRequestSent(newTestSchedulerRequest(1, ingesterQueueDimension))
	qcu.MarkRequestCompleted(newTestSchedulerRequest(99, ingesterQueueDimension))
	require.Equal(t, 1, qcu.GetForComponent(Ingester))
}

func TestQueryComponentUtilization_NilRequestIsNoop(t *testing.T) {
	qcu, _ := newTestQCU(t)
	qcu.MarkRequestSent(nil)
	require.Equal(t, 0, qcu.GetForComponent(Ingester))
	require.Equal(t, 0, qcu.GetForComponent(StoreGateway))

	qcu.MarkRequestSent(newTestSchedulerRequest(1, ingesterQueueDimension))
	qcu.MarkRequestCompleted(nil)
	require.Equal(t, 1, qcu.GetForComponent(Ingester))
}

func TestQueryComponentUtilization_GetForComponent_UnknownComponentReturnsZero(t *testing.T) {
	qcu, _ := newTestQCU(t)
	qcu.MarkRequestSent(newTestSchedulerRequest(1, ingesterAndStoreGatewayQueueDimension))
	require.Equal(t, 0, qcu.GetForComponent(QueryComponent("not-a-real-component")))
}

func TestQueryComponentUtilization_BalancedUnderConcurrency(t *testing.T) {
	qcu, _ := newTestQCU(t)
	const goroutines = 16
	const reqsPerGoroutine = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			for i := 0; i < reqsPerGoroutine; i++ {
				req := newTestSchedulerRequest(uint64(g*reqsPerGoroutine+i), ingesterAndStoreGatewayQueueDimension)
				qcu.MarkRequestSent(req)
				qcu.MarkRequestCompleted(req)
			}
		}(g)
	}
	wg.Wait()
	require.Equal(t, 0, qcu.GetForComponent(Ingester))
	require.Equal(t, 0, qcu.GetForComponent(StoreGateway))
}

func TestQueryComponentUtilization_ObserveInflightRequests(t *testing.T) {
	qcu, reg := newTestQCU(t)
	qcu.MarkRequestSent(newTestSchedulerRequest(1, ingesterQueueDimension))
	qcu.MarkRequestSent(newTestSchedulerRequest(2, ingesterQueueDimension))
	qcu.MarkRequestSent(newTestSchedulerRequest(3, storeGatewayQueueDimension))
	strongReq := newTestSchedulerRequest(4, "")
	strongReq.AdditionalQueueDimensions = []string{ingesterQueueDimension, strongReadConsistencyQueueDimension}
	qcu.MarkRequestSent(strongReq)

	qcu.ObserveInflightRequests()

	families, err := reg.Gather()
	require.NoError(t, err)
	require.Len(t, families, 1)

	// Expect one Summary observation per component (ingester + store-gateway + strong-read-consistency),
	// keyed on the query_component label. The strong request also counts for its ingester query component.
	samplesByComponent := map[string]uint64{}
	sumByComponent := map[string]float64{}
	for _, m := range families[0].Metric {
		for _, l := range m.Label {
			if l.GetName() == "query_component" {
				samplesByComponent[l.GetValue()] = m.GetSummary().GetSampleCount()
				sumByComponent[l.GetValue()] = m.GetSummary().GetSampleSum()
			}
		}
	}
	require.Equal(t, map[string]uint64{
		string(Ingester):              1,
		string(StoreGateway):          1,
		string(StrongReadConsistency): 1,
	}, samplesByComponent)
	require.Equal(t, map[string]float64{
		string(Ingester):              3,
		string(StoreGateway):          1,
		string(StrongReadConsistency): 1,
	}, sumByComponent)

	// A second observation accumulates onto the same per-component summary.
	qcu.ObserveInflightRequests()
	families, err = reg.Gather()
	require.NoError(t, err)
	for _, m := range families[0].Metric {
		require.Equal(t, uint64(2), m.GetSummary().GetSampleCount())
	}
}

func TestQueryComponentFlags(t *testing.T) {
	cases := []struct {
		name         string
		input        string
		wantIngester bool
		wantStoreGW  bool
	}{
		{"ingester", ingesterQueueDimension, true, false},
		{"store-gateway", storeGatewayQueueDimension, false, true},
		{"ingester-and-store-gateway falls through to default (both)", ingesterAndStoreGatewayQueueDimension, true, true},
		{"empty falls through to default (both)", "", true, true},
		{"unrecognised value falls through to default (both)", "garbage", true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isIng, isSG := queryComponentFlags(tc.input)
			require.Equal(t, tc.wantIngester, isIng)
			require.Equal(t, tc.wantStoreGW, isSG)
		})
	}
}

func TestSchedulerRequest_ExpectedQueryComponentName(t *testing.T) {
	cases := map[string]struct {
		dims                  []string
		wantQueueDimension    string
		wantTargetComponent   string
		wantStrongConsistency bool
	}{
		"no queue dimension": {
			dims: nil, wantQueueDimension: queue.UnknownDimension, wantTargetComponent: "", wantStrongConsistency: false,
		},
		"eventual ingester request": {
			dims:               []string{ingesterQueueDimension},
			wantQueueDimension: ingesterQueueDimension, wantTargetComponent: ingesterQueueDimension, wantStrongConsistency: false,
		},
		"strong ingester request": {
			dims:               []string{ingesterQueueDimension, strongReadConsistencyQueueDimension},
			wantQueueDimension: strongReadConsistencyQueueDimension, wantTargetComponent: ingesterQueueDimension, wantStrongConsistency: true,
		},
		"strong ingester-and-store-gateway request": {
			dims:               []string{ingesterAndStoreGatewayQueueDimension, strongReadConsistencyQueueDimension},
			wantQueueDimension: strongReadConsistencyQueueDimension, wantTargetComponent: ingesterAndStoreGatewayQueueDimension, wantStrongConsistency: true,
		},
		"unrecognised second queue dimension": {
			dims:               []string{ingesterQueueDimension, "garbage"},
			wantQueueDimension: ingesterQueueDimension, wantTargetComponent: ingesterQueueDimension, wantStrongConsistency: false,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			req := &SchedulerRequest{AdditionalQueueDimensions: tc.dims}
			require.Equal(t, tc.wantQueueDimension, req.ExpectedQueryComponentName())
			component, strong := req.ExpectedTargetQueryComponentNameWithConsistency()
			require.Equal(t, tc.wantTargetComponent, component)
			require.Equal(t, tc.wantStrongConsistency, strong)
		})
	}
}
