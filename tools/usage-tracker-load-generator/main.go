// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/dns"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv/memberlist"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/user"

	"github.com/grafana/mimir/pkg/usagetracker"
	"github.com/grafana/mimir/pkg/usagetracker/usagetrackerclient"
	"github.com/grafana/mimir/pkg/util"
)

const userID = "usage-tracker-load-generator"

type Config struct {
	Client        usagetrackerclient.Config
	InstanceRing  usagetracker.InstanceRingConfig
	PartitionRing usagetracker.PartitionRingConfig
	MemberlistKV  memberlist.KVConfig

	// Load generator.
	SimulatedTotalSeries           int
	SimulatedScrapeInterval        time.Duration
	SimulatedSeriesPerWriteRequest int
}

func (c *Config) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	f.IntVar(&c.SimulatedTotalSeries, "test.simulated-total-series", 1000, "Total number of series simulated.")
	f.DurationVar(&c.SimulatedScrapeInterval, "test.simulated-scrape-interval", 20*time.Second, "Simulated scrape interval.")
	f.IntVar(&c.SimulatedSeriesPerWriteRequest, "test.simulated-series-per-write-request", 1000, "Simulated number of series per write request.")

	c.Client.RegisterFlags(f)
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

	dnsProvider := dns.NewProvider(logger, nil, dns.GolangResolverType)
	memberlistKV := memberlist.NewKVInitService(&cfg.MemberlistKV, logger, dnsProvider, nil)
	assertNoError(services.StartAndAwaitRunning(ctx, memberlistKV))

	// Update the config.
	cfg.InstanceRing.KVStore.MemberlistKV = memberlistKV.GetMemberlistKV
	cfg.PartitionRing.KVStore.MemberlistKV = memberlistKV.GetMemberlistKV

	// Init the instance ring.
	instanceRing, err := usagetracker.NewInstanceRingClient(cfg.InstanceRing, logger, nil)
	assertNoError(err)
	assertNoError(services.StartAndAwaitRunning(ctx, instanceRing))

	// Init the partition ring.
	partitionKVClient, err := usagetracker.NewPartitionRingKVClient(cfg.PartitionRing, "watcher", logger, nil)
	assertNoError(err)

	partitionRingWatcher := usagetracker.NewPartitionRingWatcher(partitionKVClient, logger, nil)
	assertNoError(services.StartAndAwaitRunning(ctx, partitionRingWatcher))

	partitionRing := ring.NewPartitionInstanceRing(partitionRingWatcher, instanceRing, cfg.InstanceRing.HeartbeatTimeout)

	// Create the usage-tracker client.
	client := usagetrackerclient.NewUsageTrackerClient("load-generator", cfg.Client, partitionRing, instanceRing, logger, nil)

	// Compute the number of workers assuming each TrackSeries() request 100ms on average (we consider this a worst case scenario).
	numRequestsPerScrapeInterval := cfg.SimulatedTotalSeries / cfg.SimulatedSeriesPerWriteRequest
	numRequestsPerSecond := int(float64(numRequestsPerScrapeInterval) / cfg.SimulatedScrapeInterval.Seconds())
	numWorkers := (numRequestsPerSecond / 10) + 1

	wg := sync.WaitGroup{}
	wg.Add(numWorkers)

	for w := 0; w < numWorkers; w++ {
		go func() {
			defer wg.Done()
			runWorker(ctx, w, numWorkers, cfg, client)
		}()
	}

	wg.Wait()
}

func runWorker(ctx context.Context, workerID, numWorkers int, cfg Config, client *usagetrackerclient.UsageTrackerClient) {
	numSeriesPerRequest := cfg.SimulatedSeriesPerWriteRequest
	numSeriesPerWorker := cfg.SimulatedTotalSeries / numWorkers
	numRequestsPerWorker := (numSeriesPerWorker / numSeriesPerRequest) + 1
	targetTimePerRequest := cfg.SimulatedScrapeInterval / time.Duration(numRequestsPerWorker)

	// Inject the user ID in the context, required by the usage-tracker server.
	ctx = user.InjectOrgID(ctx, userID)

	for {
		// Start a new simulated scrape interval cycle.
		nextSeriesID := workerID
		startTime := time.Now()
		numRequests := 0
		numSeries := 0

		// Sequentially iterate over all the series that needs be tracked by this worker.
		for nextSeriesID < cfg.SimulatedTotalSeries {
			seriesHashes := make([]uint64, 0, numSeriesPerRequest)

			// Generate the series to track in this request.
			for len(seriesHashes) < numSeriesPerRequest && nextSeriesID < cfg.SimulatedTotalSeries {
				seriesHashes = append(seriesHashes, uint64(nextSeriesID))
				nextSeriesID += numWorkers
				numSeries++
			}

			_, err := client.TrackSeries(ctx, userID, seriesHashes)
			if err != nil {
				fmt.Fprintln(os.Stderr, err.Error())
			}

			// Throttle (with jitter) to distribute requests over the scrape interval.
			numRequests++
			elapsedTime := time.Since(startTime)
			targetTime := targetTimePerRequest * time.Duration(numRequests)
			if elapsedTime < targetTime {
				time.Sleep(util.DurationWithJitter(targetTime-elapsedTime, 0.5))
			}
		}

		fmt.Println("Worker", workerID, "has tracked", numSeries, "series in", time.Since(startTime), "(", numRequests, "requests,", targetTimePerRequest, "target time per request)")

		// Final throttle before the next cycle.
		elapsedTime := time.Since(startTime)
		targetTime := cfg.SimulatedScrapeInterval
		if elapsedTime < targetTime {
			time.Sleep(util.DurationWithJitter(targetTime-elapsedTime, 0.5))
		}
	}
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
