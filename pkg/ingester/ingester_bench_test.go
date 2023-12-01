package ingester

import (
	"context"
	"fmt"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/util"
)

func BenchmarkIngesterPush(b *testing.B) {
	const (
		series           = 200_000
		samples          = 12
		tenantName       = "t1"
		ingesterHostname = "ingester-0"
		kTopic           = "topic"
	)
	kPartition, err := ingest.IngesterPartition(ingesterHostname)
	require.NoError(b, err)
	allLabels, allSamples := benchmarkData(series)

	for _, concurrency := range []int{1} {
		for _, requestsPerConcurrency := range []int{
			2000,
			200,
			20,
		} {
			b.Run(fmt.Sprintf("kafka=false,series=%d,req=%d,samples=%d,concurrency=%d", series, requestsPerConcurrency*concurrency, samples, concurrency), func(b *testing.B) {
				registry := prometheus.NewRegistry()
				ctx := user.InjectOrgID(context.Background(), userID)

				// Create a mocked ingester
				cfg := defaultIngesterTestConfig(b)
				limitsCfg := defaultLimitsTestConfig()
				limitsCfg.MaxGlobalSeriesPerUser = 100_000_000

				ingester, err := prepareIngesterWithBlocksStorageAndLimits(b, cfg, limitsCfg, "", registry)
				require.NoError(b, err)
				require.NoError(b, services.StartAndAwaitRunning(context.Background(), ingester))
				b.Cleanup(func() {
					services.StopAndAwaitTerminated(context.Background(), ingester) //nolint:errcheck
				})

				// Wait until the ingester is healthy
				test.Poll(b, 100*time.Millisecond, 1, func() interface{} {
					return ingester.lifecycler.HealthyInstancesCount()
				})

				startTime := util.TimeToMillis(time.Now())

				var (
					seriesPerReqPerConcurrency = series / requestsPerConcurrency / concurrency
				)

				pushReqs := make([][][]byte, concurrency)
				wg := &sync.WaitGroup{}
				start := make(chan struct{})
				for i := 0; i < concurrency; i++ {
					pushReqs[i] = make([][]byte, 0, b.N*samples)
					wg.Add(1)
					go func(i int) {
						defer wg.Done()
						<-start
						for _, reqBytes := range pushReqs[i] {
							req := &mimirpb.WriteRequest{}
							require.NoError(b, req.Unmarshal(reqBytes))
							_, err := ingester.Push(ctx, req)
							require.NoError(b, err)
						}
					}(i)
				}

				for iter := 0; iter < b.N; iter++ {
					// Bump the timestamp on each of our test samples each time round the loop
					for j := 0; j < samples; j++ {
						for i := range allSamples {
							allSamples[i].TimestampMs = startTime + int64(iter*samples+j+1)
						}

						for seriesIdx := 0; seriesIdx < len(allLabels); {
							for c := 0; c < concurrency && seriesIdx < len(allLabels); c++ {
								startI, endI := seriesIdx, min(len(allLabels), seriesIdx+seriesPerReqPerConcurrency)
								req := mimirpb.ToWriteRequest(allLabels[startI:endI], allSamples[startI:endI], nil, nil, mimirpb.API)
								reqBytes, err := req.Marshal()
								require.NoError(b, err)
								pushReqs[c] = append(pushReqs[c], reqBytes)
								seriesIdx += seriesPerReqPerConcurrency
							}
						}
					}
				}

				b.ResetTimer()
				close(start)
				go ingester.DoReplay(httptest.NewRecorder(), nil)
				wg.Wait()
			})
			b.Run(fmt.Sprintf("kafka=true,series=%d,req=%d,samples=%d,concurrency=%d", series, requestsPerConcurrency, samples, concurrency), func(b *testing.B) {
				registry := prometheus.NewRegistry()

				ctx := user.InjectOrgID(context.Background(), userID)
				var kafkaAddr string
				{
					kCluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(kPartition+1, kTopic))
					require.NoError(b, err)
					b.Cleanup(kCluster.Close)
					require.Len(b, kCluster.ListenAddrs(), 1)
					kafkaAddr = kCluster.ListenAddrs()[0]
				}

				// Create a mocked ingester
				cfg := defaultIngesterTestConfig(b)
				cfg.IngestStorageConfig.KafkaTopic = kTopic
				cfg.IngestStorageConfig.KafkaAddress = kafkaAddr
				cfg.IngestStorageConfig.Enabled = true
				cfg.IngesterRing.InstanceID = ingesterHostname

				limitsCfg := defaultLimitsTestConfig()
				limitsCfg.MaxGlobalSeriesPerUser = 100_000_000

				ingester, err := prepareIngesterWithBlocksStorageAndLimits(b, cfg, limitsCfg, "", registry)
				require.NoError(b, err)
				require.NoError(b, services.StartAndAwaitRunning(context.Background(), ingester))
				b.Cleanup(func() {
					services.StopAndAwaitTerminated(context.Background(), ingester) //nolint:errcheck
				})

				// Wait until the ingester is healthy
				test.Poll(b, 100*time.Millisecond, 1, func() interface{} {
					return ingester.lifecycler.HealthyInstancesCount()
				})

				var (
					seriesPerReqPerConcurrency = series / requestsPerConcurrency / concurrency
				)

				pushReqs := make([][][]mimirpb.PreallocTimeseries, concurrency)
				// Generate requests
				{
					startTime := util.TimeToMillis(time.Now())
					for iter := 0; iter < b.N; iter++ {
						// Bump the timestamp on each of our test samples each time round the loop
						for j := 0; j < samples; j++ {
							for i := range allSamples {
								allSamples[i].TimestampMs = startTime + int64(iter*samples+j+1)
							}

							for seriesIdx := 0; seriesIdx < len(allLabels); {
								for c := 0; c < concurrency && seriesIdx < len(allLabels); c++ {
									startI, endI := seriesIdx, min(len(allLabels), seriesIdx+seriesPerReqPerConcurrency)
									series, samples := allLabels[startI:endI], allSamples[startI:endI]
									request := make([]mimirpb.PreallocTimeseries, 0, len(series))
									for i := range series {
										request = append(request, mimirpbTimeseries(series[i], samples[i:i+1]))
									}
									pushReqs[c] = append(pushReqs[c], request)
									seriesIdx += seriesPerReqPerConcurrency
								}
							}
						}
					}

					pushReqs[len(pushReqs)-1] = append(pushReqs[len(pushReqs)-1], []mimirpb.PreallocTimeseries{{TimeSeries: &mimirpb.TimeSeries{
						Labels:  []mimirpb.LabelAdapter{{Value: "marker", Name: labels.MetricName}},
						Samples: []mimirpb.Sample{{TimestampMs: time.Now().UnixMilli(), Value: FinalMessageSampleValue}},
					}}})
				}
				// Start producers

				for i := 0; i < concurrency; i++ {
					writer := ingest.NewWriter(kafkaAddr, kTopic, log.NewNopLogger(), prometheus.NewRegistry())
					for _, req := range pushReqs[i] {
						err = writer.WriteSync(ctx, kPartition, tenantName, req, nil, mimirpb.API)
						assert.NoError(b, err)
					}
				}

				b.ResetTimer()
				ingester.DoReplay(httptest.NewRecorder(), nil)
			})
		}
	}
}

func mimirpbTimeseries(labels []mimirpb.LabelAdapter, samples []mimirpb.Sample) mimirpb.PreallocTimeseries {
	return mimirpb.PreallocTimeseries{
		TimeSeries: &mimirpb.TimeSeries{Labels: labels, Samples: samples},
	}
}
