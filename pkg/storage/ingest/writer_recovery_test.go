// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/grafana/mimir/pkg/util/testkafka"
)

func TestKafkaWriterMetadataMinAge(t *testing.T) {
	for _, age := range []time.Duration{0, 10 * time.Millisecond, 100 * time.Millisecond, time.Second, 10 * time.Second, -time.Second, time.Millisecond, 11 * time.Second} {
		t.Run(age.String(), func(t *testing.T) {
			cfg := createTestKafkaConfig("localhost:9092", "test")
			cfg.ProducerMetadataMinAge = age
			reg := prometheus.NewPedanticRegistry()
			client, err := NewKafkaWriterClient(cfg, defaultMaxInflightProduceRequests, log.NewNopLogger(), reg)
			if age < 0 || (age > 0 && age < 10*time.Millisecond) || age > 10*time.Second {
				require.ErrorContains(t, cfg.Validate(), "producer-metadata-min-age")
				require.ErrorContains(t, err, "producer-metadata-min-age")
				metrics, gatherErr := reg.Gather()
				require.NoError(t, gatherErr)
				assert.Empty(t, metrics, "invalid settings must fail before registering metrics")
				return
			}
			require.NoError(t, cfg.Validate())
			require.NoError(t, err)
			t.Cleanup(client.Close)
			want := age
			if want == 0 {
				want = DefaultMetadataRefreshInterval
			}
			assert.Equal(t, want, client.OptValue(kgo.MetadataMinAge))
			assert.Equal(t, DefaultMetadataRefreshInterval, client.OptValue(kgo.MetadataMaxAge))
			reader, err := NewKafkaReaderClient(cfg, nil, log.NewNopLogger())
			require.NoError(t, err)
			t.Cleanup(reader.Close)
			assert.Equal(t, DefaultMetadataRefreshInterval, reader.OptValue(kgo.MetadataMinAge))
			assert.Equal(t, DefaultMetadataRefreshInterval, reader.OptValue(kgo.MetadataMaxAge))
		})
	}

	t.Run("unsupported backend", func(t *testing.T) {
		cfg := createTestKafkaConfigForBackend(KafkaBackendWarpstream, "localhost:9092", "test")
		cfg.ProducerMetadataMinAge = time.Second
		require.ErrorContains(t, cfg.Validate(), "only supported with backend=kafka")
		reg := prometheus.NewPedanticRegistry()
		_, err := newKafkaProducerForBackend(cfg, 1, log.NewNopLogger(), reg)
		require.ErrorContains(t, err, "only supported with backend=kafka")
		metrics, err := reg.Gather()
		require.NoError(t, err)
		assert.Empty(t, metrics)
	})
}

// Observe terminal callbacks without changing the producer's admission or cancellation paths.
type recoveryTrackingClient struct {
	KafkaProducerClient
	mu        sync.Mutex
	completed map[string][]error
}

func (c *recoveryTrackingClient) Produce(ctx context.Context, record *kgo.Record, promise func(*kgo.Record, error)) {
	id := string(record.Value)
	c.KafkaProducerClient.Produce(ctx, record, func(r *kgo.Record, err error) {
		c.mu.Lock()
		c.completed[id] = append(c.completed[id], err)
		c.mu.Unlock()
		promise(r, err)
	})
}

func storageErrorResponse(req *kmsg.ProduceRequest) *kmsg.ProduceResponse {
	resp := kmsg.NewPtrProduceResponse()
	resp.SetVersion(req.GetVersion())
	for _, topic := range req.Topics {
		rt := kmsg.NewProduceResponseTopic()
		rt.Topic = topic.Topic
		rt.TopicID = topic.TopicID
		for _, partition := range topic.Partitions {
			rp := kmsg.NewProduceResponseTopicPartition()
			rp.Partition = partition.Partition
			rp.ErrorCode = kerr.KafkaStorageError.Code
			rt.Partitions = append(rt.Partitions, rp)
		}
		resp.Topics = append(resp.Topics, rt)
	}
	return resp
}

func TestKafkaProducer_RecoveryAfterStorageError(t *testing.T) {
	for _, refreshDuringRequest := range []bool{false, true} {
		for _, minAge := range []time.Duration{0, time.Second, 100 * time.Millisecond} {
			t.Run(fmt.Sprintf("refresh_during_request=%t/min_age=%s", refreshDuringRequest, minAge), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					start := time.Now()
					var vnet kfake.VirtualNetwork
					cluster, addr := testkafka.CreateCluster(t, 2, "test", testkafka.WithVirtualNetwork(&vnet), testkafka.WithNumBrokers(2), func() []kfake.Opt {
						return []kfake.Opt{kfake.Ports(9092, 9093)}
					})
					var metadataTimes []time.Duration
					cluster.ControlKey(kmsg.Metadata.Int16(), func(kmsg.Request) (kmsg.Response, error, bool) {
						metadataTimes = append(metadataTimes, time.Since(start))
						return nil, nil, false
					})
					cfg := createTestKafkaConfig(addr, "test")
					cfg.Dialer = vnet.DialContext
					cfg.WriteTimeout = 10 * time.Second
					cfg.ProducerMetadataMinAge = minAge
					reg := prometheus.NewPedanticRegistry()
					client, err := NewKafkaWriterClient(cfg, defaultMaxInflightProduceRequests, log.NewNopLogger(), reg)
					require.NoError(t, err)
					tracked := &recoveryTrackingClient{KafkaProducerClient: client, completed: map[string][]error{}}
					producer := NewKafkaProducer(tracked, 32, reg)
					t.Cleanup(producer.Close)
					require.NoError(t, producer.ProduceSync(t.Context(), []*kgo.Record{{Partition: 0, Value: []byte("prime")}}).FirstErr())

					// Cross the natural 10s periodic refresh with an in-flight Produce, or enqueue just after it.
					// Do not force a refresh: the relative phase is the behavior under test.
					enqueueAt := 10100 * time.Millisecond
					if refreshDuringRequest {
						enqueueAt = 9800 * time.Millisecond
					}
					time.Sleep(enqueueAt - time.Since(start))
					errorAt := enqueueAt + 250*time.Millisecond
					cluster.ControlKey(kmsg.Produce.Int16(), func(req kmsg.Request) (kmsg.Response, error, bool) {
						cluster.SleepControl(func() { time.Sleep(errorAt - time.Since(start)) })
						return storageErrorResponse(req.(*kmsg.ProduceRequest)), nil, true
					})
					produce := func(id string, partition int32) <-chan error {
						done := make(chan error, 1)
						go func() {
							ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
							defer cancel()
							result := producer.ProduceSync(ctx, []*kgo.Record{{Partition: partition, Value: []byte(id)}})
							done <- result.FirstErr()
						}()
						return done
					}
					old := produce("old", 0)
					time.Sleep(errorAt + 25*time.Millisecond - time.Since(start))
					synctest.Wait()
					young := produce("young", 0)
					healthy := produce("healthy", 1)
					require.NoError(t, <-healthy)
					assert.Less(t, time.Since(start)-errorAt, time.Second, "healthy broker must keep progressing")
					if minAge == 0 {
						require.ErrorIs(t, <-old, context.DeadlineExceeded)
						require.ErrorIs(t, <-young, context.DeadlineExceeded)
					} else {
						require.NoError(t, <-old)
						require.NoError(t, <-young)
						assert.Less(t, time.Since(start)-enqueueAt, 5*time.Second)
					}
					time.Sleep(25*time.Second - time.Since(start))
					synctest.Wait()
					assert.Contains(t, metadataTimes, 10*time.Second, "natural periodic refresh must actually occur")
					tracked.mu.Lock()
					defer tracked.mu.Unlock()
					for _, id := range []string{"prime", "old", "young", "healthy"} {
						require.Len(t, tracked.completed[id], 1, id)
						if minAge == 0 && refreshDuringRequest && (id == "old" || id == "young") {
							assert.ErrorIs(t, tracked.completed[id][0], kgo.ErrRecordTimeout, id)
						} else {
							assert.NoError(t, tracked.completed[id][0], id)
						}
					}
					assert.Zero(t, producer.bufferedBytes.Load())
					assert.Zero(t, producer.BufferedProduceRecords())
				})
			})
		}
	}
}

func TestKafkaProducer_RecoveryFailureLifecycle(t *testing.T) {
	for _, mode := range []string{"repeated storage errors", "metadata failure", "connection loss", "ownership change", "shutdown"} {
		for _, age := range []time.Duration{0, time.Second, 100 * time.Millisecond} {
			t.Run(fmt.Sprintf("%s/%s", mode, age), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					var vnet kfake.VirtualNetwork
					cluster, addr := testkafka.CreateCluster(t, 1, "test", testkafka.WithVirtualNetwork(&vnet), testkafka.WithNumBrokers(2), func() []kfake.Opt {
						return []kfake.Opt{kfake.Ports(9092, 9093)}
					})
					cfg := createTestKafkaConfig(addr, "test")
					cfg.Dialer = vnet.DialContext
					cfg.WriteTimeout = 10 * time.Second
					cfg.ProducerMetadataMinAge = age
					reg := prometheus.NewPedanticRegistry()
					client, err := NewKafkaWriterClient(cfg, defaultMaxInflightProduceRequests, log.NewNopLogger(), reg)
					require.NoError(t, err)
					tracked := &recoveryTrackingClient{KafkaProducerClient: client, completed: map[string][]error{}}
					producer := NewKafkaProducer(tracked, 8, reg)
					t.Cleanup(producer.Close)
					require.NoError(t, producer.ProduceSync(t.Context(), []*kgo.Record{{Value: []byte("prime")}}).FirstErr())
					var produces, metadata atomic.Int64
					cluster.ControlKey(kmsg.Produce.Int16(), func(req kmsg.Request) (kmsg.Response, error, bool) {
						produces.Add(1)
						if mode != "ownership change" {
							cluster.KeepControl()
						}
						if mode == "connection loss" {
							return nil, errors.New("injected connection loss"), true
						}
						return storageErrorResponse(req.(*kmsg.ProduceRequest)), nil, true
					})
					if mode == "metadata failure" {
						cluster.ControlKey(kmsg.Metadata.Int16(), func(kmsg.Request) (kmsg.Response, error, bool) {
							metadata.Add(1)
							cluster.KeepControl()
							return nil, errors.New("injected metadata connection loss"), true
						})
					}
					ctx, cancel := context.WithCancel(t.Context())
					defer cancel()
					done := make(chan error, 1)
					go func() {
						done <- producer.ProduceSync(ctx, []*kgo.Record{{Value: []byte("old")}, {Value: []byte("young")}}).FirstErr()
					}()
					synctest.Wait()
					require.Equal(t, int64(8), producer.bufferedBytes.Load())
					require.ErrorIs(t, producer.ProduceSync(t.Context(), []*kgo.Record{{Value: []byte("overflow")}}).FirstErr(), kgo.ErrMaxBuffered)
					cancel()
					require.ErrorIs(t, <-done, context.Canceled)
					assert.Equal(t, int64(8), producer.bufferedBytes.Load(), "caller cancellation does not release records still owned by the client")
					time.Sleep(200 * time.Millisecond)
					if mode == "ownership change" {
						// Cluster administration must run outside a control callback.
						require.NoError(t, cluster.MoveTopicPartition("test", 0, 1))
					}
					if mode != "shutdown" {
						time.Sleep(25 * time.Second)
					}
					producer.Close()
					synctest.Wait()
					assert.Positive(t, produces.Load())
					if mode == "metadata failure" {
						assert.Positive(t, metadata.Load())
					}
					tracked.mu.Lock()
					defer tracked.mu.Unlock()
					for _, id := range []string{"old", "young"} {
						require.Len(t, tracked.completed[id], 1, id)
						if mode == "ownership change" {
							assert.NoError(t, tracked.completed[id][0])
						} else {
							assert.Error(t, tracked.completed[id][0])
						}
					}
					assert.NotContains(t, tracked.completed, "overflow")
					assert.Zero(t, producer.bufferedBytes.Load())
					assert.Zero(t, producer.BufferedProduceRecords())
				})
			})
		}
	}
}

type recoveryCountedConn struct {
	net.Conn
	closed *atomic.Int64
	once   sync.Once
}

func (c *recoveryCountedConn) Close() error {
	c.once.Do(func() { c.closed.Add(1) })
	return c.Conn.Close()
}

func TestKafkaProducer_MetadataRefreshLoad(t *testing.T) {
	for _, clients := range []int{1, 10, 100} {
		for _, staggered := range []bool{false, true} {
			for _, age := range []time.Duration{0, time.Second, 100 * time.Millisecond} {
				t.Run(fmt.Sprintf("clients=%d/staggered=%t/min_age=%s", clients, staggered, age), func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						var vnet kfake.VirtualNetwork
						cluster, addr := testkafka.CreateCluster(t, 1, "test", testkafka.WithVirtualNetwork(&vnet))
						var metadata, produces, opened, closed atomic.Int64
						cluster.ControlKey(kmsg.Metadata.Int16(), func(kmsg.Request) (kmsg.Response, error, bool) {
							metadata.Add(1)
							return nil, nil, false
						})
						writers := make([]*KafkaProducer, clients)
						var warm sync.WaitGroup
						for i := range writers {
							cfg := createTestKafkaConfig(addr, "test")
							cfg.WriteTimeout = 10 * time.Second
							cfg.ProducerMetadataMinAge = age
							cfg.Dialer = func(ctx context.Context, network, address string) (net.Conn, error) {
								conn, err := vnet.DialContext(ctx, network, address)
								if err != nil {
									return nil, err
								}
								opened.Add(1)
								return &recoveryCountedConn{Conn: conn, closed: &closed}, nil
							}
							writer, err := newKafkaProducerForBackend(cfg, defaultMaxInflightProduceRequests, log.NewNopLogger(), prometheus.NewPedanticRegistry())
							require.NoError(t, err)
							t.Cleanup(writer.Close)
							writers[i] = writer
							warm.Add(1)
							go func() {
								defer warm.Done()
								assert.NoError(t, writer.ProduceSync(t.Context(), []*kgo.Record{{Topic: "test", Value: []byte("prime")}}).FirstErr())
							}()
						}
						warm.Wait()
						start := time.Now()
						baselineMetadata, baselineOpened, baselineClosed := metadata.Load(), opened.Load(), closed.Load()
						cluster.ControlKey(kmsg.Produce.Int16(), func(req kmsg.Request) (kmsg.Response, error, bool) {
							produces.Add(1)
							if time.Since(start) < 2*time.Second {
								cluster.KeepControl()
								return storageErrorResponse(req.(*kmsg.ProduceRequest)), nil, true
							}
							return nil, nil, false
						})
						var done sync.WaitGroup
						var deadlines, elapsedNs atomic.Int64
						for i, writer := range writers {
							done.Add(1)
							go func() {
								defer done.Done()
								if staggered {
									time.Sleep(time.Duration(i) * 10 * time.Millisecond)
								}
								ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
								defer cancel()
								enqueued := time.Now()
								err := writer.ProduceSync(ctx, []*kgo.Record{{Topic: "test", Value: []byte("record")}}).FirstErr()
								elapsedNs.Add(int64(time.Since(enqueued)))
								if errors.Is(err, context.DeadlineExceeded) {
									deadlines.Add(1)
								} else {
									assert.NoError(t, err)
								}
							}()
						}
						done.Wait()
						time.Sleep(25*time.Second - time.Since(start))
						synctest.Wait()
						var recordErrors float64
						for _, writer := range writers {
							assert.Zero(t, writer.BufferedProduceRecords())
							assert.Zero(t, writer.bufferedBytes.Load())
							recordErrors += promtest.ToFloat64(writer.produceRecordsFailedTotal.WithLabelValues(produceErrReason(kgo.ErrRecordTimeout)))
						}
						t.Logf("window=25s metadata=%d metadata_per_second=%.2f produce_attempts=%d connections_opened=%d connections_closed=%d caller_deadlines=%d mean_caller_latency=%s record_timeouts=%.0f", metadata.Load()-baselineMetadata, float64(metadata.Load()-baselineMetadata)/25, produces.Load(), opened.Load()-baselineOpened, closed.Load()-baselineClosed, deadlines.Load(), time.Duration(elapsedNs.Load()/int64(clients)), recordErrors)
						if age != 0 {
							assert.Zero(t, deadlines.Load())
						}
					})
				})
			}
		}
	}
}

func BenchmarkKafkaProducer_RequestThroughput(b *testing.B) {
	_, addr := testkafka.CreateCluster(b, 1, "test")
	cfg := createTestKafkaConfig(addr, "test")
	// Removing linger exposes request overhead instead of measuring intentional batching waits.
	cfg.DisableLinger = true
	producer, err := newKafkaProducerForBackend(cfg, defaultMaxInflightProduceRequests, log.NewNopLogger(), prometheus.NewPedanticRegistry())
	require.NoError(b, err)
	b.Cleanup(producer.Close)
	payload := make([]byte, 1024)
	require.NoError(b, producer.ProduceSync(b.Context(), []*kgo.Record{{Topic: "test", Value: payload}}).FirstErr())
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			result := producer.ProduceSync(b.Context(), []*kgo.Record{{Topic: "test", Value: payload}})
			if err := result.FirstErr(); err != nil {
				b.Error(err)
				return
			}
		}
	})
}
