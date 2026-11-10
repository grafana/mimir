// SPDX-License-Identifier: AGPL-3.0-only

package queue

import (
	"fmt"
	"math/rand"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// simulationEnvPrefix is the prefix of the environment variables that configure TestQueueSimulation_FixedPriorityQueueDimensions.
const simulationEnvPrefix = "MIMIR_QUEUE_SIMULATION"

type simulationComponent struct {
	dim string
	// rate returns the requests per second at time at.
	rate    func(at time.Duration) float64
	service func(r *rand.Rand) time.Duration
	// serviceFactor returns the factor for the request time of an arrival at time at.
	serviceFactor func(at time.Duration) float64
}

func simulationEnvFloat(name string, def float64) float64 {
	if v, err := strconv.ParseFloat(os.Getenv(simulationEnvPrefix+"_"+name), 64); err == nil {
		return v
	}
	return def
}

func simulationExp(mean time.Duration) func(r *rand.Rand) time.Duration {
	return func(r *rand.Rand) time.Duration { return time.Duration(r.ExpFloat64() * float64(mean)) }
}

func simulationUniform(lo, hi time.Duration) func(r *rand.Rand) time.Duration {
	return func(r *rand.Rand) time.Duration { return lo + time.Duration(r.Float64()*float64(hi-lo)) }
}

// TestQueueSimulation_FixedPriorityQueueDimensions compares the queue wait per query component
// with and without fixed priority queue dimensions.
// Requests for each query component arrive as a Poisson process, and the consumers run in fake time.
// The values below are for this simulation only.
// They are not production values.
//
// The test runs only when MIMIR_QUEUE_SIMULATION=true, because each run takes several seconds.
// Environment variables MIMIR_QUEUE_SIMULATION_<NAME> change the values, for example MIMIR_QUEUE_SIMULATION_QUERIERS=64.
// MIMIR_QUEUE_SIMULATION_SCENARIOS selects scenarios by name, separated by commas.
func TestQueueSimulation_FixedPriorityQueueDimensions(t *testing.T) {
	if os.Getenv(simulationEnvPrefix) != "true" {
		t.Skip("set " + simulationEnvPrefix + "=true to run the simulation")
	}
	fixed := []string{ingesterQueueDimension, storeGatewayQueueDimension, ingesterAndStoreGatewayQueueDimension, UnknownDimension}
	queriers := int(simulationEnvFloat("QUERIERS", 32))
	workersPerQuerier := int(simulationEnvFloat("WORKERS_PER_QUERIER", 4))
	// MIMIR_QUEUE_SIMULATION_SCALE_TO_QUERIERS adds queriers at MIMIR_QUEUE_SIMULATION_SCALE_AT_SECONDS, as querier autoscaling does.
	scaleTo := int(simulationEnvFloat("SCALE_TO_QUERIERS", 0))
	scaleAt := time.Duration(simulationEnvFloat("SCALE_AT_SECONDS", 60) * float64(time.Second))
	horizon := time.Duration(simulationEnvFloat("HORIZON_SECONDS", 120) * float64(time.Second))
	warmup := time.Duration(simulationEnvFloat("WARMUP_SECONDS", 30) * float64(time.Second))
	ingesterTime := time.Duration(simulationEnvFloat("INGESTER_SECONDS", 0.05) * float64(time.Second))
	storeGatewayTime := time.Duration(simulationEnvFloat("STORE_GATEWAY_SECONDS", 0.2) * float64(time.Second))
	unknownTime := time.Duration(simulationEnvFloat("UNKNOWN_SECONDS", 0.1) * float64(time.Second))
	ingesterRate := simulationEnvFloat("INGESTER_RATE", 800)
	storeGatewayRate := simulationEnvFloat("STORE_GATEWAY_RATE", 40)
	ingesterAndStoreGatewayRate := simulationEnvFloat("INGESTER_AND_STORE_GATEWAY_RATE", 20)
	unknownRate := simulationEnvFloat("UNKNOWN_RATE", 5)
	seeds := int64(simulationEnvFloat("SEEDS", 2))
	// In the flood scenario, every arrival rate is floodFactor times higher from floodFrom for floodDuration.
	floodFactor := simulationEnvFloat("FLOOD_FACTOR", 3)
	floodFrom := time.Duration(simulationEnvFloat("FLOOD_FROM_SECONDS", 30) * float64(time.Second))
	floodDuration := time.Duration(simulationEnvFloat("FLOOD_SECONDS", 30) * float64(time.Second))
	// MIMIR_QUEUE_SIMULATION_FLOOD_QUEUES limits the flood to some queues, separated by commas.
	// The request times of the queues in MIMIR_QUEUE_SIMULATION_FLOOD_SLOW_QUEUES are floodRequestTimeFactor times longer during the flood.
	floodRequestTimeFactor := simulationEnvFloat("FLOOD_REQUEST_TIME_FACTOR", 1)
	inList := func(name, dim string) bool {
		v := os.Getenv(simulationEnvPrefix + "_" + name)
		return slices.Contains(strings.Split(v, ","), dim)
	}

	// A slow request takes 10 to 40 seconds.
	slow := simulationUniform(10*time.Second, 40*time.Second)
	scenarios := []struct {
		name                                              string
		ingester, storeGateway, ingesterAndStore, unknown func(r *rand.Rand) time.Duration
		flood                                             bool
	}{
		{name: "normal", ingester: simulationExp(ingesterTime), storeGateway: simulationExp(storeGatewayTime), ingesterAndStore: simulationExp(storeGatewayTime), unknown: simulationExp(unknownTime)},
		{name: "store-gateway-slow", ingester: simulationExp(ingesterTime), storeGateway: slow, ingesterAndStore: slow, unknown: simulationExp(unknownTime)},
		{name: "ingester-slow", ingester: slow, storeGateway: simulationExp(storeGatewayTime), ingesterAndStore: slow, unknown: simulationExp(unknownTime)},
		{name: "both-slow", ingester: slow, storeGateway: slow, ingesterAndStore: slow, unknown: simulationExp(unknownTime)},
		{name: "ingester-and-store-gateway-slow", ingester: simulationExp(ingesterTime), storeGateway: simulationExp(storeGatewayTime), ingesterAndStore: slow, unknown: simulationExp(unknownTime)},
		{name: "unknown-slow", ingester: simulationExp(ingesterTime), storeGateway: simulationExp(storeGatewayTime), ingesterAndStore: simulationExp(storeGatewayTime), unknown: slow},
		{name: "flood", ingester: simulationExp(ingesterTime), storeGateway: simulationExp(storeGatewayTime), ingesterAndStore: simulationExp(storeGatewayTime), unknown: simulationExp(unknownTime), flood: true},
	}
	selected := os.Getenv(simulationEnvPrefix + "_SCENARIOS")

	for _, sc := range scenarios {
		if selected != "" && !slices.Contains(strings.Split(selected, ","), sc.name) {
			continue
		}
		inFlood := func(at time.Duration) bool { return sc.flood && at >= floodFrom && at < floodFrom+floodDuration }
		rate := func(dim string, base float64) func(at time.Duration) float64 {
			flooded := os.Getenv(simulationEnvPrefix+"_FLOOD_QUEUES") == "" || inList("FLOOD_QUEUES", dim)
			return func(at time.Duration) float64 {
				if flooded && inFlood(at) {
					return base * floodFactor
				}
				return base
			}
		}
		serviceFactor := func(dim string) func(at time.Duration) float64 {
			slow := inList("FLOOD_SLOW_QUEUES", dim)
			return func(at time.Duration) float64 {
				if slow && inFlood(at) {
					return floodRequestTimeFactor
				}
				return 1
			}
		}
		components := []simulationComponent{
			{dim: ingesterQueueDimension, rate: rate(ingesterQueueDimension, ingesterRate), service: sc.ingester, serviceFactor: serviceFactor(ingesterQueueDimension)},
			{dim: storeGatewayQueueDimension, rate: rate(storeGatewayQueueDimension, storeGatewayRate), service: sc.storeGateway, serviceFactor: serviceFactor(storeGatewayQueueDimension)},
			{dim: ingesterAndStoreGatewayQueueDimension, rate: rate(ingesterAndStoreGatewayQueueDimension, ingesterAndStoreGatewayRate), service: sc.ingesterAndStore, serviceFactor: serviceFactor(ingesterAndStoreGatewayQueueDimension)},
			{dim: UnknownDimension, rate: rate(UnknownDimension, unknownRate), service: sc.unknown, serviceFactor: serviceFactor(UnknownDimension)},
		}
		for _, algo := range []struct {
			name                         string
			fixedPriorityQueueDimensions []string
		}{{"today", nil}, {"fixed priority", fixed}} {
			for seed := int64(1); seed <= seeds; seed++ {
				synctest.Test(t, func(t *testing.T) {
					waits, served, windows := runQueueSimulation(t, algo.fixedPriorityQueueDimensions, components, queriers, workersPerQuerier, scaleTo, scaleAt, horizon, warmup, seed)
					line := fmt.Sprintf("%s, %d workers, %s, seed %d", sc.name, queriers*workersPerQuerier, algo.name, seed)
					if scaleTo > queriers {
						line = fmt.Sprintf("%s, %d to %d workers at %s, %s, seed %d", sc.name, queriers*workersPerQuerier, scaleTo*workersPerQuerier, scaleAt, algo.name, seed)
					}
					windowLine := line + " windows"
					for _, c := range components {
						w := waits[c.dim]
						if len(w) == 0 {
							continue
						}
						slices.Sort(w)
						line += fmt.Sprintf(" | %s: n=%d served=%d mean=%.3fs p99=%.3fs", c.dim, len(w), served[c.dim], mean(w), w[len(w)*99/100])
						windowLine += " | " + c.dim + ":"
						for i := int(warmup / simulationWindow); i < int(horizon/simulationWindow); i++ {
							windowLine += fmt.Sprintf(" %ds=%.3f", i*int(simulationWindow/time.Second), mean(windows[c.dim][i]))
						}
					}
					t.Log(line)
					t.Log(windowLine)
				})
			}
		}
	}
}

// simulationWindow is the width of the arrival time windows for the queue waits.
const simulationWindow = 10 * time.Second

// runQueueSimulation returns the queue waits in seconds of the requests that arrive after warmup, by query component.
// It also returns the number of requests dequeued between warmup and horizon, by query component.
// The third result holds the queue waits of all requests, by query component and by arrival window of simulationWindow.
// Arrivals do not slow down when a queue grows.
// So under overload, only the served counts and the waits of the fast query components are comparable.
func runQueueSimulation(t *testing.T, fixedPriorityQueueDimensions []string, components []simulationComponent, queriers, workersPerQuerier, scaleTo int, scaleAt, horizon, warmup time.Duration, seed int64) (map[string][]float64, map[string]int, map[string]map[int][]float64) {
	const tenantID = "0"
	rng := rand.New(rand.NewSource(seed))

	// Generate the arrivals and service times first, so both algorithms get the same requests for a seed.
	type arrival struct {
		at      time.Duration
		dim     string
		service time.Duration
	}
	var arrivals []arrival
	for _, c := range components {
		for at := time.Duration(rng.ExpFloat64() / c.rate(0) * float64(time.Second)); at < horizon; at += time.Duration(rng.ExpFloat64() / c.rate(at) * float64(time.Second)) {
			arrivals = append(arrivals, arrival{at: at, dim: c.dim, service: time.Duration(float64(c.service(rng)) * c.serviceFactor(at))})
		}
	}
	slices.SortFunc(arrivals, func(a, b arrival) int { return int(a.at - b.at) })

	queue, err := New(
		log.NewNopLogger(),
		len(arrivals)+1,
		0,
		promauto.With(nil).NewGaugeVec(prometheus.GaugeOpts{}, []string{"user"}),
		nil,
		promauto.With(nil).NewCounterVec(prometheus.CounterOpts{}, []string{"user"}),
		promauto.With(nil).NewHistogram(prometheus.HistogramOpts{}),
		nil,
		fixedPriorityQueueDimensions,
	)
	require.NoError(t, err)
	require.NoError(t, queue.starting(t.Context()))

	var mu sync.Mutex
	services := map[*testItem]time.Duration{}
	waits := map[string][]float64{}
	served := map[string]int{}
	windows := map[string]map[int][]float64{}
	start := time.Now()
	consume := func(request Item) error {
		req := request.(*testItem)
		dim := req.ExpectedQueryComponentName()
		mu.Lock()
		d := services[req]
		if now := time.Since(start); now >= warmup && now < horizon {
			served[dim]++
		}
		if req.EnqueueTime.Sub(start) >= warmup {
			waits[dim] = append(waits[dim], time.Since(req.EnqueueTime).Seconds())
		}
		if windows[dim] == nil {
			windows[dim] = map[int][]float64{}
		}
		window := int(req.EnqueueTime.Sub(start) / simulationWindow)
		windows[dim][window] = append(windows[dim][window], time.Since(req.EnqueueTime).Seconds())
		mu.Unlock()
		time.Sleep(d)
		return nil
	}
	// The queriers from queriers to scaleTo start to dequeue at scaleAt.
	consumers, ctx := errgroup.WithContext(t.Context())
	consumed := make(chan struct{}, len(arrivals))
	stop := make(chan struct{})
	startInitial, startScaled := make(chan struct{}), make(chan struct{})
	runInitial := runQueueConsumerUntilEmpty(ctx, len(arrivals), queue, consume, consumed, startInitial, stop)
	runScaled := runQueueConsumerUntilEmpty(ctx, len(arrivals), queue, consume, consumed, startScaled, stop)
	for consumerIdx := range max(queriers, scaleTo) {
		run := runInitial
		if consumerIdx >= queriers {
			run = runScaled
		}
		for range workersPerQuerier {
			consumers.Go(func() error { return run(consumerIdx) })
		}
	}
	close(startInitial)
	go func() {
		time.Sleep(scaleAt)
		close(startScaled)
	}()

	var producers errgroup.Group
	producers.Go(func() error {
		for _, a := range arrivals {
			if d := time.Until(start.Add(a.at)); d > 0 {
				time.Sleep(d)
			}
			req := makeTestItem(tenantID, []string{a.dim})
			mu.Lock()
			services[req] = a.service
			mu.Unlock()
			if err := queue.SubmitItemToEnqueue(tenantID, req, req.ExpectedQueryComponentName(), 0, func() {}); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, producers.Wait())
	require.NoError(t, consumers.Wait())
	require.NoError(t, queue.stop(nil))
	return waits, served, windows
}
