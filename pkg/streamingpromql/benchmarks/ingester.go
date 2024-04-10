// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/bench_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package benchmarks

import (
	"context"
	"fmt"
	"io"
	"math"
	"net"
	"path/filepath"
	"strconv"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/prometheus/model/labels"
	"google.golang.org/grpc"

	"github.com/grafana/mimir/pkg/ingester"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/validation"
)

const interval = 10 * time.Second

const NumIntervals = 10000 + int(time.Minute/interval) + 1 // The longest-range test we run has 10000 steps with a 1m range selector, so make sure we have slightly more data than that.

const UserID = "benchmark-tenant"

func StartIngesterAndLoadData(rootDataDir string, metricSizes []int) (string, func(), error) {
	ing, addr, cleanup, err := startBenchmarkIngester(rootDataDir)

	if err != nil {
		return "", nil, fmt.Errorf("could not start ingester: %w", err)
	}

	if err := pushTestData(ing, metricSizes); err != nil {
		cleanup()
		return "", nil, fmt.Errorf("could not load test data into ingester: %w", err)
	}

	return addr, cleanup, nil
}

func startBenchmarkIngester(rootDataDir string) (*ingester.Ingester, string, func(), error) {
	var cleanupFuncs []func() error
	cleanup := func() {
		for i := len(cleanupFuncs) - 1; i >= 0; i-- {
			cleanupFuncs[i]()
		}
	}

	limits := defaultLimitsTestConfig()
	limits.NativeHistogramsIngestionEnabled = true

	overrides, err := validation.NewOverrides(limits, nil)
	if err != nil {
		return nil, "", nil, err
	}

	ingesterCfg, closer := defaultIngesterTestConfig()
	cleanupFuncs = append(cleanupFuncs, closer.Close)
	ingesterCfg.BlocksStorageConfig.TSDB.Dir = filepath.Join(rootDataDir, "data")
	ingesterCfg.BlocksStorageConfig.Bucket.Backend = "filesystem"
	ingesterCfg.BlocksStorageConfig.Bucket.Filesystem.Directory = filepath.Join(rootDataDir, "bucket")

	// Disable shipping, and retain blocks and TSDB forever.
	ingesterCfg.BlocksStorageConfig.TSDB.ShipInterval = 0
	ingesterCfg.BlocksStorageConfig.TSDB.Retention = time.Duration(math.MaxInt64)
	ingesterCfg.BlocksStorageConfig.TSDB.CloseIdleTSDBTimeout = 0

	// Disable TSDB head compaction jitter to have predictable tests.
	ingesterCfg.BlocksStorageConfig.TSDB.HeadCompactionIntervalJitterEnabled = false
	ingesterCfg.BlocksStorageConfig.TSDB.HeadCompactionIdleTimeout = 0

	ingestersRing, err := createAndStartRing(ingesterCfg.IngesterRing.ToRingConfig())
	if err != nil {
		cleanup()
		return nil, "", nil, fmt.Errorf("could not create and start ring: %w", err)
	}

	cleanupFuncs = append(cleanupFuncs, func() error {
		return services.StopAndAwaitTerminated(context.Background(), ingestersRing)
	})

	ing, err := ingester.New(ingesterCfg, overrides, ingestersRing, nil, nil, nil, log.NewNopLogger())
	if err != nil {
		cleanup()
		return nil, "", nil, fmt.Errorf("could not create ingester: %w", err)
	}

	if err := services.StartAndAwaitRunning(context.Background(), ing); err != nil {
		cleanup()
		return nil, "", nil, fmt.Errorf("could not stop ingester: %w", err)
	}

	cleanupFuncs = append(cleanupFuncs, func() error {
		return services.StopAndAwaitTerminated(context.Background(), ing)
	})

	serv := grpc.NewServer(grpc.StreamInterceptor(middleware.StreamServerUserHeaderInterceptor))
	client.RegisterIngesterServer(serv, ing)
	cleanupFuncs = append(cleanupFuncs, func() error {
		serv.GracefulStop()
		return nil
	})

	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return nil, "", nil, fmt.Errorf("could not start gRPC listener: %w", err)
	}

	go func() {
		serv.Serve(listener)
	}()

	return ing, listener.Addr().String(), cleanup, nil
}

func defaultIngesterTestConfig() (ingester.Config, io.Closer) {
	consul, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)

	cfg := ingester.Config{}
	flagext.DefaultValues(&cfg)
	flagext.DefaultValues(&cfg.BlocksStorageConfig)
	flagext.DefaultValues(&cfg.IngestStorageConfig)
	cfg.IngesterRing.KVStore.Mock = consul
	cfg.IngesterRing.NumTokens = 1
	cfg.IngesterRing.ListenPort = 0
	cfg.IngesterRing.InstanceAddr = "localhost"
	cfg.IngesterRing.InstanceID = "localhost"
	cfg.IngesterRing.FinalSleep = 0
	cfg.ActiveSeriesMetrics.Enabled = false

	return cfg, closer
}

func defaultLimitsTestConfig() validation.Limits {
	limits := validation.Limits{}
	flagext.DefaultValues(&limits)
	return limits
}

func createAndStartRing(ringConfig ring.Config) (*ring.Ring, error) {
	rng, err := ring.New(ringConfig, "ingester", ingester.IngesterRingKey, log.NewNopLogger(), nil)
	if err != nil {
		return nil, fmt.Errorf("could not create ring: %w", err)
	}

	if err := services.StartAndAwaitRunning(context.Background(), rng); err != nil {
		return nil, fmt.Errorf("could not start ring service: %w", err)
	}

	return rng, nil
}

func pushTestData(ing *ingester.Ingester, metricSizes []int) error {
	const histogramBuckets = 5

	totalMetrics := 0

	for _, size := range metricSizes {
		totalMetrics += (2 + histogramBuckets + 1) * size // 2 non-histogram metrics + 5 metrics for histogram buckets + 1 metric for +Inf histogram bucket
	}

	metrics := make([]labels.Labels, 0, totalMetrics)

	for _, size := range metricSizes {
		aName := "a_" + strconv.Itoa(size)
		bName := "b_" + strconv.Itoa(size)
		histogramName := "h_" + strconv.Itoa(size)

		if size == 1 {
			// We don't want a "l" label on metrics with one series (some test cases rely on this label not being present).
			metrics = append(metrics, labels.FromStrings("__name__", aName))
			metrics = append(metrics, labels.FromStrings("__name__", bName))
			for le := 0; le < histogramBuckets; le++ {
				metrics = append(metrics, labels.FromStrings("__name__", histogramName, "le", strconv.Itoa(le)))
			}
			metrics = append(metrics, labels.FromStrings("__name__", histogramName, "le", "+Inf"))
		} else {
			for i := 0; i < size; i++ {
				metrics = append(metrics, labels.FromStrings("__name__", aName, "l", strconv.Itoa(i)))
				metrics = append(metrics, labels.FromStrings("__name__", bName, "l", strconv.Itoa(i)))
				for le := 0; le < histogramBuckets; le++ {
					metrics = append(metrics, labels.FromStrings("__name__", histogramName, "l", strconv.Itoa(i), "le", strconv.Itoa(le)))
				}
				metrics = append(metrics, labels.FromStrings("__name__", histogramName, "l", strconv.Itoa(i), "le", "+Inf"))
			}
		}
	}

	ctx := user.InjectOrgID(context.Background(), UserID)
	req := &mimirpb.WriteRequest{
		Timeseries: make([]mimirpb.PreallocTimeseries, len(metrics)),
	}

	for i, m := range metrics {
		series := mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{
			Labels:  mimirpb.FromLabelsToLabelAdapters(m),
			Samples: make([]mimirpb.Sample, NumIntervals),
		}}

		for s := 0; s < NumIntervals; s++ {
			series.Samples[s].TimestampMs = int64(s) * interval.Milliseconds()
			series.Samples[s].Value = float64(s) + float64(i)/float64(len(metrics))
		}

		req.Timeseries[i] = series
	}

	if _, err := ing.Push(ctx, req); err != nil {
		return fmt.Errorf("failed to push samples to ingester: %w", err)
	}

	ing.Flush()

	return nil
}
