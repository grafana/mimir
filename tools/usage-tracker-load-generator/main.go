// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand/v2"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/dns"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv/memberlist"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/grafana/mimir/pkg/usagetracker"
	"github.com/grafana/mimir/pkg/usagetracker/usagetrackerclient"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/validation"
)

type Config struct {
	Client        usagetrackerclient.Config
	InstanceRing  usagetracker.InstanceRingConfig
	PartitionRing usagetracker.PartitionRingConfig
	MemberlistKV  memberlist.KVConfig

	// Load generator.
	TenantID                       string
	ReplicaName                    string
	SimulatedTotalSeries           int
	SimulatedScrapeInterval        time.Duration
	SimulatedSeriesPerWriteRequest int
	SimulatedSeriesLifetime        time.Duration
}

func (c *Config) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	f.StringVar(&c.TenantID, "tenant-id", "usage-tracker-load-generator", "The tenant ID.")
	f.StringVar(&c.ReplicaName, "replica-name", "usage-tracker-load-generator-0", "The load generator replica name. Different replicas generate different series hashes.")
	f.IntVar(&c.SimulatedTotalSeries, "test.simulated-total-series", 1000, "Total number of series simulated.")
	f.DurationVar(&c.SimulatedScrapeInterval, "test.simulated-scrape-interval", 20*time.Second, "Simulated scrape interval.")
	f.IntVar(&c.SimulatedSeriesPerWriteRequest, "test.simulated-series-per-write-request", 1000, "Simulated number of series per write request.")
	f.DurationVar(&c.SimulatedSeriesLifetime, "test.simulated-series-lifetime", time.Hour, "Simulated series lifetime. Series will churn after this duration.")

	c.Client.RegisterFlagsWithPrefix("usage-tracker-client.", f)
	c.InstanceRing.RegisterFlags(f, logger)
	c.PartitionRing.RegisterFlags(f)
	c.MemberlistKV.RegisterFlags(f)
}

func (c *Config) Validate() error {
	return nil
}

func main() {
	var (
		ctx    = context.Background()
		logger = newLoggerWithoutDebugLevel(log.NewLogfmtLogger(os.Stderr))
	)

	// Clean up all flags registered via init() methods of 3rd-party libraries.
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	cfg := Config{}
	cfg.RegisterFlags(flag.CommandLine, logger)

	// Parse CLI arguments.
	assertNoError(flagext.ParseFlagsWithoutArguments(flag.CommandLine))
	assertNoError(cfg.Validate())

	// Init memberlist.
	cfg.MemberlistKV.Codecs = append(cfg.MemberlistKV.Codecs, ring.GetCodec())
	cfg.MemberlistKV.Codecs = append(cfg.MemberlistKV.Codecs, ring.GetPartitionRingCodec())

	dnsProvider := dns.NewProvider(logger, prometheus.DefaultRegisterer, dns.GolangResolverType)
	memberlistKV := memberlist.NewKVInitService(&cfg.MemberlistKV, logger, dnsProvider, prometheus.DefaultRegisterer)
	assertNoError(services.StartAndAwaitRunning(ctx, memberlistKV))

	// Update the config.
	cfg.InstanceRing.KVStore.MemberlistKV = memberlistKV.GetMemberlistKV
	cfg.PartitionRing.KVStore.MemberlistKV = memberlistKV.GetMemberlistKV

	// Init the instance ring.
	instanceRing, err := usagetracker.NewInstanceRingClient(cfg.InstanceRing, logger, prometheus.DefaultRegisterer)
	assertNoError(err)
	assertNoError(services.StartAndAwaitRunning(ctx, instanceRing))

	// Init the partition ring.
	partitionKVClient, err := usagetracker.NewPartitionRingKVClient(cfg.PartitionRing, "watcher", logger, prometheus.DefaultRegisterer)
	assertNoError(err)

	partitionRingWatcher := usagetracker.NewPartitionRingWatcher(partitionKVClient, logger, prometheus.DefaultRegisterer)
	assertNoError(services.StartAndAwaitRunning(ctx, partitionRingWatcher))

	partitionRing := ring.NewMultiPartitionInstanceRing(partitionRingWatcher, instanceRing, cfg.InstanceRing.HeartbeatTimeout)

	router := http.NewServeMux()
	router.Handle("/metrics", promhttp.Handler())
	router.Handle("/usage-tracker/instance-ring", instanceRing)
	router.Handle("/usage-tracker/partition-ring", ring.NewPartitionRingPageHandler(partitionRingWatcher, ring.NewPartitionRingEditor(usagetracker.PartitionRingKey, partitionKVClient)))
	go func() {
		if err := http.ListenAndServe(":80", router); err != nil { // nolint:gosec
			fmt.Fprintln(os.Stderr, "Failed to start HTTP server:", err)
			os.Exit(1)
		}
	}()

	// Create the usage-tracker client.
	stubLimits := validation.NewOverrides(validation.Limits{}, validation.NewMockTenantLimits(nil))
	client := usagetrackerclient.NewUsageTrackerClient("load-generator", cfg.Client, partitionRing, instanceRing, stubLimits, logger, prometheus.DefaultRegisterer, &noOpUsageTrackerRejectionObserver{})

	// Compute the number of workers assuming each TrackSeries() request 100ms on average (we consider this a worst case scenario).
	numRequestsPerScrapeInterval := cfg.SimulatedTotalSeries / cfg.SimulatedSeriesPerWriteRequest
	numRequestsPerSecond := int(float64(numRequestsPerScrapeInterval) / cfg.SimulatedScrapeInterval.Seconds())
	numWorkers := (numRequestsPerSecond / 10) + 1
	replicaSeed := xxhash.Sum64String(cfg.ReplicaName)
	fmt.Println("ReplicaName:", cfg.ReplicaName, "ReplicaSeed:", replicaSeed, "Num workers:", numWorkers)

	wg := sync.WaitGroup{}
	wg.Add(numWorkers)

	for w := 0; w < numWorkers; w++ {
		go func(workerID int) {
			defer wg.Done()
			runWorker(ctx, replicaSeed, workerID, numWorkers, cfg, client)
		}(w)
	}

	wg.Wait()
}

type noOpUsageTrackerRejectionObserver struct{}

func (n *noOpUsageTrackerRejectionObserver) ObserveUsageTrackerRejection(userID string) {
}

var _ usagetrackerclient.UsageTrackerRejectionObserver = (*noOpUsageTrackerRejectionObserver)(nil)

func runWorker(ctx context.Context, replicaSeed uint64, workerID, numWorkers int, cfg Config, client *usagetrackerclient.UsageTrackerClient) {
	numSeriesPerRequest := cfg.SimulatedSeriesPerWriteRequest
	numSeriesPerWorker := cfg.SimulatedTotalSeries / numWorkers
	numRequestsPerWorker := (numSeriesPerWorker / numSeriesPerRequest) + 1
	targetTimePerRequest := cfg.SimulatedScrapeInterval / time.Duration(numRequestsPerWorker)

	// Inject the user ID in the context, required by the usage-tracker server.
	ctx = user.InjectOrgID(ctx, cfg.TenantID)

	for {
		seriesHashes := generateSeriesHashesWithChurn(replicaSeed, workerID, numSeriesPerWorker, cfg.SimulatedSeriesLifetime, time.Now())
		sendSeriesHashes(ctx, workerID, cfg.TenantID, seriesHashes, numSeriesPerRequest, cfg.SimulatedScrapeInterval, targetTimePerRequest, client)
	}
}

func sendSeriesHashes(ctx context.Context, workerID int, tenantID string, seriesHashes []uint64, numSeriesPerRequest int, simulatedScrapeInterval, targetTimePerRequest time.Duration, client usageTrackerClient) {
	// Start a new simulated scrape interval cycle.
	startTime := time.Now()
	numRequests := 0
	numSeriesRejected := 0

	// Sequentially iterate over all the series that needs be tracked by this worker.
	for startIdx := 0; startIdx < len(seriesHashes); startIdx += numSeriesPerRequest {
		endIdx := startIdx + numSeriesPerRequest
		if endIdx > len(seriesHashes) {
			endIdx = len(seriesHashes)
		}
		reqSeriesHashes := seriesHashes[startIdx:endIdx]

		rejectedHashes, err := client.TrackSeries(ctx, tenantID, reqSeriesHashes)
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
		}
		numSeriesRejected += len(rejectedHashes)

		// Throttle (with jitter) to distribute requests over the scrape interval.
		numRequests++
		elapsedTime := time.Since(startTime)
		targetTime := targetTimePerRequest * time.Duration(numRequests)
		if elapsedTime < targetTime {
			time.Sleep(util.DurationWithJitter(targetTime-elapsedTime, 0.5))
		}
	}

	fmt.Println("Worker", workerID, "has tracked", len(seriesHashes), "series (", numSeriesRejected, "rejected) in", time.Since(startTime), "(", numRequests, "requests,", targetTimePerRequest, "target time per request)")

	// Final throttle before the next cycle.
	elapsedTime := time.Since(startTime)
	targetTime := simulatedScrapeInterval
	if elapsedTime < targetTime {
		time.Sleep(util.DurationWithJitter(targetTime-elapsedTime, 0.5))
	}
}

func generateSeriesHashesWithChurn(replicaSeed uint64, workerID, numSeries int, seriesLifetime time.Duration, now time.Time) []uint64 {
	// We use a random generator. We don't care about collisions between workers.
	// We expect collisions to be a very low %.
	// We want series to churn gradually, so we will select a churning point in the seriesLifetime nanos duration where it should churn.
	// Churning will be achieved by adding time.Now().UnixNano() / seriesLifetime.Nanoseconds() to the generated hash,
	// and additionally the proportional amount of series to the remainder of the division will be churned by 1.
	churnOffset := now.UnixNano() / seriesLifetime.Nanoseconds()
	extraChurnOffsetNanos := now.UnixNano() % seriesLifetime.Nanoseconds()
	extraChurnOffsetIndex := numSeries - int((extraChurnOffsetNanos*int64(numSeries))/seriesLifetime.Nanoseconds())

	random := rand.New(rand.NewPCG(replicaSeed, uint64(workerID)))
	seriesHashes := make([]uint64, 0, numSeries)
	for i := 0; i < extraChurnOffsetIndex; i++ {
		seriesHashes = append(seriesHashes, random.Uint64()+uint64(churnOffset))
	}
	for i := extraChurnOffsetIndex; i < numSeries; i++ {
		seriesHashes = append(seriesHashes, random.Uint64()+uint64(churnOffset)+1)
	}

	return seriesHashes
}

func assertNoError(err error) {
	if err == nil {
		return
	}

	fmt.Fprintln(os.Stderr, err.Error())
	os.Exit(1)
}

type loggerWithoutDebugLevel struct {
	wrapped log.Logger
}

func newLoggerWithoutDebugLevel(wrapped log.Logger) log.Logger {
	return loggerWithoutDebugLevel{
		wrapped: wrapped,
	}
}

func (l loggerWithoutDebugLevel) Log(keyvals ...interface{}) error {
	for i := 0; i < len(keyvals)-1; i++ {
		if keyvals[i] == "level" && keyvals[i+1] == level.DebugValue() {
			return nil
		}
	}

	return l.wrapped.Log(keyvals...)
}

type usageTrackerClient interface {
	TrackSeries(ctx context.Context, userID string, series []uint64) (_ []uint64, returnErr error)
}
