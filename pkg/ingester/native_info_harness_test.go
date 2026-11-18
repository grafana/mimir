// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/ring"
	ring_client "github.com/grafana/dskit/ring/client"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/grafana/mimir/pkg/distributor"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier"
	querierapi "github.com/grafana/mimir/pkg/querier/api"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/util/testkafka"
	"github.com/grafana/mimir/pkg/util/validation"
)

// nativeInfoHarness runs native info() end to end on ingest storage: a distributor writes OTLP and Remote Write
// requests to an in-process Kafka with several partitions, an ingester of the engine under test consumes each, and
// MQE queries them through the distributor queryable, with native target_info on top for info().
type nativeInfoHarness struct {
	tenant        string
	distributor   *distributor.Distributor
	limits        *validation.Overrides
	tenantLimits  *harnessTenantLimits
	queryable     storage.Queryable
	engine        promql.QueryEngine
	nativeMetrics *querier.NativeTargetInfoMetrics
	reg           *prometheus.Registry
	lookbackDelta time.Duration
	// Each partition's ingester, which restart replaces, and the ring the distributor finds them in.
	ingesters     []*harnessIngester
	ingestersRing *ring.Ring
}

// harnessIngester is a partition's ingester, behind a gRPC server of its own.
type harnessIngester struct {
	cfg      Config
	ingester *Ingester
	server   *grpc.Server
}

// harnessTenantLimits are the tenant's limits, which a fixture may change partway, as a runtime config reload does.
type harnessTenantLimits struct {
	tenant string
	limits atomic.Pointer[validation.Limits]
}

func (l *harnessTenantLimits) ByUserID(userID string) *validation.Limits {
	if userID != l.tenant {
		return nil
	}
	return l.limits.Load()
}

func (l *harnessTenantLimits) AllByUserID() map[string]*validation.Limits {
	return map[string]*validation.Limits{l.tenant: l.limits.Load()}
}

// startIngester starts an ingester of cfg, with the data in cfg's directories, behind a gRPC server on a new port.
func (h *nativeInfoHarness) startIngester(t testing.TB, cfg Config, kafkaAddr string) *harnessIngester {
	ctx := context.Background()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	cfg.IngesterRing.InstancePort = listener.Addr().(*net.TCPAddr).Port
	ing, _, _ := createTestIngesterWithIngestStorage(t, &cfg, h.limits, nil, nil, log.NewNopLogger(), kafkaAddr)
	server := grpc.NewServer(
		grpc.ChainUnaryInterceptor(middleware.ServerUserHeaderInterceptor, querierapi.ReadConsistencyServerUnaryInterceptor),
		grpc.ChainStreamInterceptor(middleware.StreamServerUserHeaderInterceptor, querierapi.ReadConsistencyServerStreamInterceptor),
	)
	client.RegisterIngesterServer(server, ing)
	grpc_health_v1.RegisterHealthServer(server, servingHealth{})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	require.NoError(t, services.StartAndAwaitRunning(ctx, ing))
	t.Cleanup(func() { _ = services.StopAndAwaitTerminated(ctx, ing) })
	return &harnessIngester{cfg: cfg, ingester: ing, server: server}
}

// restart stops the partition's ingester and starts another on its data, which consumes the partition from where the
// first left off, as an ingester's restart does.
func (h *nativeInfoHarness) restart(t testing.TB, partition int) {
	old := h.ingesters[partition]
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), old.ingester))
	old.server.Stop()
	cfg := old.cfg
	cfg.IngestStorageConfig.KafkaConfig.ConsumeFromPositionAtStartup = "last-offset"
	h.ingesters[partition] = h.startIngester(t, cfg, cfg.IngestStorageConfig.KafkaConfig.Address[0])
	// The distributor finds the ingester at its new address in the ring.
	addr := fmt.Sprintf("127.0.0.1:%d", h.ingesters[partition].cfg.IngesterRing.InstancePort)
	require.Eventually(t, func() bool {
		set, err := h.ingestersRing.GetAllHealthy(ring.Read)
		return err == nil && slices.ContainsFunc(set.Instances, func(i ring.InstanceDesc) bool { return i.Addr == addr })
	}, 30*time.Second, 50*time.Millisecond)
}

func newNativeInfoHarness(t testing.TB, engine string, partitions int, limits validation.Limits) *nativeInfoHarness {
	const topic = "mimir"
	ctx := context.Background()
	logger := log.NewNopLogger()
	tenantLimits := &harnessTenantLimits{tenant: "tenant"}
	tenantLimits.limits.Store(&limits)
	h := &nativeInfoHarness{tenant: "tenant", limits: validation.NewOverrides(limits, tenantLimits), tenantLimits: tenantLimits, reg: prometheus.NewRegistry(), lookbackDelta: 5 * time.Minute}

	_, kafkaAddr := testkafka.CreateCluster(t, int32(partitions), topic)
	ringKV, closeRing := consul.NewInMemoryClient(ring.GetCodec(), logger, nil)
	t.Cleanup(func() { _ = closeRing.Close() })
	partitionKV, closePartitions := consul.NewInMemoryClient(ring.GetPartitionRingCodec(), logger, nil)
	t.Cleanup(func() { _ = closePartitions.Close() })

	for partition := range partitions {
		cfg := defaultIngesterTestConfig(t)
		cfg.IngesterRing.KVStore.Mock = ringKV
		// The ingest storage reads the partition from the instance ID.
		cfg.IngesterRing.InstanceID = fmt.Sprintf("ingester-zone-a-%d", partition)
		cfg.IngesterRing.InstanceAddr = "127.0.0.1"
		cfg.IngesterPartitionRing.KVStore.Mock = partitionKV
		cfg.BlocksStorageConfig.TSDB.Engine = engine
		// Fixed, so that a restarted ingester opens the same data.
		cfg.BlocksStorageConfig.TSDB.Dir = t.TempDir()
		cfg.IngestStorageConfig.KafkaConfig.ConsumeFromPositionAtStartup = "start"
		// The sequential pusher appends each record by itself.
		cfg.IngestStorageConfig.KafkaConfig.IngestionConcurrencyMax = 0
		h.ingesters = append(h.ingesters, h.startIngester(t, cfg, kafkaAddr))
	}

	ingestersRing, err := ring.New(ring.Config{KVStore: kv.Config{Mock: ringKV}, HeartbeatTimeout: time.Hour, ReplicationFactor: 1}, IngesterRingKey, IngesterRingKey, logger, nil)
	require.NoError(t, err)
	h.ingestersRing = ingestersRing
	require.NoError(t, services.StartAndAwaitRunning(ctx, ingestersRing))
	t.Cleanup(ingestersRing.StopAsync)
	partitionRings, err := ingest.NewPartitionRingWatchers(false, 0, PartitionRingName, PartitionRingKey, partitionKV, nil, logger, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, partitionRings))
	t.Cleanup(func() { _ = services.StopAndAwaitTerminated(ctx, partitionRings) })
	// Writes and reads need every partition active, with its ingester as owner.
	require.Eventually(t, func() bool {
		partitionRing := partitionRings.PartitionRing(0)
		active := 0
		for _, id := range partitionRing.ActivePartitionIDs() {
			if len(partitionRing.PartitionOwnerIDs(id)) > 0 {
				active++
			}
		}
		return active == partitions && ingestersRing.InstancesCount() == partitions
	}, 30*time.Second, 50*time.Millisecond)

	var (
		distributorCfg distributor.Config
		clientCfg      client.Config
		ingestCfg      ingest.Config
	)
	flagext.DefaultValues(&distributorCfg, &clientCfg, &ingestCfg)
	ingestCfg.Enabled = true
	ingestCfg.KafkaConfig.Topic = topic
	ingestCfg.KafkaConfig.Address = flagext.StringSliceCSV{kafkaAddr}
	ingestCfg.KafkaConfig.LastProducedOffsetPollInterval = 100 * time.Millisecond
	distributorCfg.IngestStorageConfig = ingestCfg
	distributorCfg.IngestersLookbackPeriod = time.Hour
	distributorCfg.DistributorRing.Common.KVStore.Mock = ringKV
	distributorCfg.DistributorRing.Common.InstanceID = "distributor"
	distributorCfg.DistributorRing.Common.InstanceAddr = "127.0.0.1"
	clientMetrics := client.NewMetrics(nil)
	// The distributor's pool doesn't close its clients when it stops, and goleak would find their connections.
	var clientsMtx sync.Mutex
	var clients []client.HealthAndIngesterClient
	t.Cleanup(func() {
		clientsMtx.Lock()
		defer clientsMtx.Unlock()
		for _, c := range clients {
			_ = c.Close()
		}
	})
	distributorCfg.IngesterClientFactory = ring_client.PoolInstFunc(func(inst ring.InstanceDesc) (ring_client.PoolClient, error) {
		c, err := client.MakeIngesterClient(inst, clientCfg, clientMetrics, logger)
		if err == nil {
			clientsMtx.Lock()
			clients = append(clients, c)
			clientsMtx.Unlock()
		}
		return c, err
	})
	partitionInstanceRings := ring.NewPartitionInstanceRings(partitionRings, ingestersRing, time.Hour)
	h.distributor, err = distributor.New(distributorCfg, clientCfg, h.limits, nil, nil, ingestersRing, partitionInstanceRings, partitionRings, false, true, nil, nil, prometheus.NewRegistry(), logger)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, h.distributor))
	t.Cleanup(func() { _ = services.StopAndAwaitTerminated(ctx, h.distributor) })

	// As the querier's, which accounts for the series' labels in the query's memory.
	h.queryable = querier.NewMemoryTrackingQueryable(querier.NewDistributorQueryable(h.distributor, h.limits, stats.NewQueryMetrics(nil), logger), nil)
	h.nativeMetrics = querier.NewNativeTargetInfoMetrics(h.reg)
	opts := streamingpromql.NewTestEngineOpts()
	opts.CommonOpts.LookbackDelta = h.lookbackDelta
	opts.InfoSeriesQueryable = func(q storage.Queryable) storage.Queryable {
		return querier.NewNativeTargetInfoQueryable(q, h.distributor, h.limits, h.lookbackDelta, h.nativeMetrics)
	}
	planner, err := streamingpromql.NewQueryPlanner(opts, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)
	h.engine, err = streamingpromql.NewEngine(opts, stats.NewQueryMetrics(nil), planner)
	require.NoError(t, err)
	return h
}

// setNativeResources turns the tenant's native resources on or off, for the pushes from then on: the distributor gives
// the series their resources only while they're on, and the ingesters keep them only then.
func (h *nativeInfoHarness) setNativeResources(enabled bool) {
	limits := *h.tenantLimits.limits.Load()
	limits.NativeResourcesEnabled = enabled
	h.tenantLimits.limits.Store(&limits)
}

// nativeResourcesCoverageStart is the coverage start of the native resources of the series named name that native
// info() gets, the latest of the ingesters', once they have every push.
func (h *nativeInfoHarness) nativeResourcesCoverageStart(t testing.TB, name string) int64 {
	res, err := h.distributor.NativeResources(h.queryContext(true), [][]*labels.Matcher{{labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, name)}}, math.MinInt64, math.MaxInt64, 0, false)
	require.NoError(t, err)
	return res.CoverageStart
}

// pushOTLP pushes the metrics as an OTLP request, which the distributor converts with the tenant's settings: its
// series carry their resources, and the conversion writes target_info.
func (h *nativeInfoHarness) pushOTLP(t testing.TB, md pmetric.Metrics) {
	body, err := pmetricotlp.NewExportRequestFromMetrics(md).MarshalProto()
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "/otlp/v1/metrics", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/x-protobuf")
	req = req.WithContext(user.InjectOrgID(req.Context(), h.tenant))
	handler := distributor.OTLPHandler(10<<20, nil, nil, false, h.limits, nil, nil, distributor.RetryConfig{}, nil, h.distributor.PushWithMiddlewares, nil, nil, log.NewNopLogger())
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
}

// pushRemoteWrite pushes series without resources, as a Remote Write client does.
func (h *nativeInfoHarness) pushRemoteWrite(t testing.TB, series ...mimirpb.PreallocTimeseries) {
	_, err := h.distributor.Push(user.InjectOrgID(context.Background(), h.tenant), &mimirpb.WriteRequest{Timeseries: series, Source: mimirpb.API})
	require.NoError(t, err)
}

// queryContext is the tenant's, with native target_info or the join, and strong read consistency, so that queries see
// every push.
func (h *nativeInfoHarness) queryContext(native bool) context.Context {
	ctx := querierapi.ContextWithReadConsistencyLevel(h.eventualQueryContext(native), querierapi.ReadConsistencyStrong)
	return ctx
}

// eventualQueryContext is queryContext's without strong read consistency, whose wait for the ingesters to catch up
// would dominate a benchmark's time once they have.
func (h *nativeInfoHarness) eventualQueryContext(native bool) context.Context {
	return querier.ContextWithNativeTargetInfo(user.InjectOrgID(context.Background(), h.tenant), native)
}

// rangeQuery evaluates the query over [start, end] by step, with native target_info or the join.
func (h *nativeInfoHarness) rangeQuery(t testing.TB, native bool, query string, start, end time.Time, step time.Duration) promql.Matrix {
	q, err := h.engine.NewRangeQuery(h.queryContext(native), h.queryable, nil, query, start, end, step)
	require.NoError(t, err)
	defer q.Close()
	res := q.Exec(h.queryContext(native))
	require.NoError(t, res.Err)
	matrix, err := res.Matrix()
	require.NoError(t, err)
	// Closing the query gives its points back to the engine's pools, which the next query reuses.
	copied := make(promql.Matrix, 0, len(matrix))
	for _, s := range matrix {
		copied = append(copied, promql.Series{Metric: s.Metric.Copy(), Floats: slices.Clone(s.Floats), Histograms: slices.Clone(s.Histograms)})
	}
	return copied
}

// fallbacks is how many selects of target_info fell back to the join, for any reason.
func (h *nativeInfoHarness) fallbacks(t testing.TB) float64 {
	return h.counter(t, "cortex_querier_native_target_info_fallbacks_total")
}

// synthesized is how many target_info series with native samples queries got: without any, a native query measured
// the stored target_info, as before its lookup's coverage started.
func (h *nativeInfoHarness) synthesized(t testing.TB) float64 {
	return h.counter(t, "cortex_querier_native_target_info_series_synthesized_total")
}

func (h *nativeInfoHarness) counter(t testing.TB, name string) float64 {
	families, err := h.reg.Gather()
	require.NoError(t, err)
	total := 0.0
	for _, f := range families {
		if f.GetName() == name {
			for _, m := range f.GetMetric() {
				total += m.GetCounter().GetValue()
			}
		}
	}
	return total
}

// servingHealth answers the distributor's ingester client pool's health checks.
type servingHealth struct {
	grpc_health_v1.UnimplementedHealthServer
}

func (servingHealth) Check(context.Context, *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
}
