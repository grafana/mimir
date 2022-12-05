package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/pkg/mimir"
	"github.com/grafana/mimir/pkg/querier"
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/storegateway"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

type Config struct {
	Mimir mimir.Config `yaml:"inline"`

	// Tested config.
	TesterUserID          string
	TesterMinTime         flagext.Time
	TesterMaxTime         flagext.Time
	TesterMinRange        time.Duration
	TesterMaxRange        time.Duration
	TesterMetricNameRegex string
	TesterConcurrency     int
}

func (c *Config) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	c.Mimir.RegisterFlags(f, logger)

	f.StringVar(&c.TesterUserID, "tester.user-id", "anonymous", "The user ID to run queries.")
	f.Var(&c.TesterMinTime, "tester.min-time", fmt.Sprintf("The minimum time to query. The supported time format is %q.", time.RFC3339))
	f.Var(&c.TesterMaxTime, "tester.max-time", fmt.Sprintf("The maximum time to query. The supported time format is %q.", time.RFC3339))
	f.DurationVar(&c.TesterMinRange, "tester.min-range", 24*time.Hour, "The minimum time range to query within the configured min and max time.")
	f.DurationVar(&c.TesterMaxRange, "tester.max-range", 7*24*time.Hour, "The maximum time range to query within the configured min and max time.")
	f.StringVar(&c.TesterMetricNameRegex, "tester.metric-name-regex", "up", "The metric name regex to use in the request to store-gateways.")
	f.IntVar(&c.TesterConcurrency, "tester.concurrency", 1, "The number of concurrent requests to run.")
}

func (c *Config) Validate(logger log.Logger) error {
	return c.Mimir.Validate(logger)
}

func main() {
	ctx := context.Background()
	logger := log.NewLogfmtLogger(os.Stdout)
	reg := prometheus.NewRegistry()

	// IMPORTANT: we assume store-gateway shuffle sharding is disabled (or the tenant blocks are sharded across all store-gateways).
	limits := &noBlocksStoreLimits{}

	// Parse the config.
	cfg := &Config{}
	cfg.RegisterFlags(flag.CommandLine, logger)
	if err := flag.CommandLine.Parse(os.Args[1:]); err != nil {
		fmt.Fprintln(flag.CommandLine.Output(), "Run with -help to get a list of available parameters")
		os.Exit(1)
	}

	if err := cfg.Validate(logger); err != nil {
		panic(err)
	}

	// Init memberlist.
	memberlistKV, err := initMemberlistKV(cfg, reg)
	if err != nil {
		panic(err)
	}

	if err := services.StartAndAwaitRunning(ctx, memberlistKV); err != nil {
		panic(err)
	}

	// Init the bucket client.
	bucketClient, err := bucket.NewClient(context.Background(), cfg.Mimir.BlocksStorage.Bucket, "querier", logger, reg)
	if err != nil {
		panic(errors.Wrap(err, "failed to create bucket client"))
	}

	// Init the finder.
	finder := querier.NewBucketIndexBlocksFinder(querier.BucketIndexBlocksFinderConfig{
		IndexLoader: bucketindex.LoaderConfig{
			CheckInterval:         time.Minute,
			UpdateOnStaleInterval: cfg.Mimir.BlocksStorage.BucketStore.SyncInterval,
			UpdateOnErrorInterval: cfg.Mimir.BlocksStorage.BucketStore.BucketIndex.UpdateOnErrorInterval,
			IdleTimeout:           cfg.Mimir.BlocksStorage.BucketStore.BucketIndex.IdleTimeout,
		},
		MaxStalePeriod:           cfg.Mimir.BlocksStorage.BucketStore.BucketIndex.MaxStalePeriod,
		IgnoreDeletionMarksDelay: cfg.Mimir.BlocksStorage.BucketStore.IgnoreDeletionMarksDelay,
	}, bucketClient, limits, logger, reg)

	if err := services.StartAndAwaitRunning(ctx, finder); err != nil {
		panic(err)
	}

	// Init the selector.
	storesRingCfg := cfg.Mimir.StoreGateway.ShardingRing.ToRingConfig()
	storesRingBackend, err := kv.NewClient(
		storesRingCfg.KVStore,
		ring.GetCodec(),
		kv.RegistererWithKVName(prometheus.WrapRegistererWithPrefix("cortex_", reg), "querier-store-gateway"),
		logger,
	)
	if err != nil {
		panic(errors.Wrap(err, "failed to create store-gateway ring backend"))
	}

	storesRing, err := ring.NewWithStoreClientAndStrategy(storesRingCfg, storegateway.RingNameForClient, storegateway.RingKey, storesRingBackend, ring.NewIgnoreUnhealthyInstancesReplicationStrategy(), prometheus.WrapRegistererWithPrefix("cortex_", reg), logger)
	if err != nil {
		panic(errors.Wrap(err, "failed to create store-gateway ring client"))
	}

	if err := services.StartAndAwaitRunning(ctx, storesRing); err != nil {
		panic(err)
	}

	selector := newStoreGatewaySelector(storesRing, cfg.Mimir.Querier.StoreGatewayClient, limits, logger, reg)

	// Request.
	matchers := []storepb.LabelMatcher{
		{
			Type:  storepb.LabelMatcher_RE,
			Name:  labels.MetricName,
			Value: cfg.TesterMetricNameRegex,
		},
	}

	t := newTester(cfg.TesterUserID, finder, selector, logger)
	g, _ := errgroup.WithContext(ctx)

	for c := 0; c < cfg.TesterConcurrency; c++ {
		// Compare results only on 1 request, to reduce memory utilization.
		compareResults := c == 0

		g.Go(func() error {
			for {
				start, end := getRandomRequestTimeRange(cfg)
				level.Info(logger).Log("msg", "request", "start", time.UnixMilli(start).String(), "end", time.UnixMilli(end).String(), "metric name regexp", cfg.TesterMetricNameRegex)

				if err := t.sendRequestToAllStoreGatewayZonesAndCompareResults(ctx, start, end, matchers, compareResults); err != nil {
					level.Error(logger).Log("msg", "failed to run test", "err", err)
				}
			}
		})
	}

	if err := g.Wait(); err != nil {
		level.Error(logger).Log("msg", "test execution failed", "err", err)
	}
}

func getRandomRequestTimeRange(cfg *Config) (start, end int64) {
	// Get a random time range duration, honoring the configured min and max range.
	timeRangeDurationMillis := (cfg.TesterMinRange + time.Duration(rand.Int63n(int64(cfg.TesterMaxRange-cfg.TesterMinRange)))).Milliseconds()

	// Get a random min timestamp, honoring the configured min and max time.
	minTimeMillis := time.Time(cfg.TesterMinTime).UnixMilli()
	maxTimeMillis := time.Time(cfg.TesterMaxTime).UnixMilli()

	if timeRangeDurationMillis < maxTimeMillis-minTimeMillis {
		start = minTimeMillis + rand.Int63n(maxTimeMillis-minTimeMillis-timeRangeDurationMillis)
	} else {
		start = minTimeMillis
	}

	if start+timeRangeDurationMillis <= maxTimeMillis {
		end = start + timeRangeDurationMillis
	} else {
		end = maxTimeMillis
	}

	return
}
