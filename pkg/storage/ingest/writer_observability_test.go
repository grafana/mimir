// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/grafana/mimir/pkg/util/testkafka"
)

func TestKafkaWriterRequestMetrics(t *testing.T) {
	for _, api := range []struct {
		key   int16
		label string
	}{{0, "produce"}, {3, "metadata"}, {1, "other"}, {-1, "other"}} {
		for _, outcome := range []struct {
			writeErr, readErr error
			label             string
		}{
			{nil, nil, "success"}, {errors.New("write"), nil, "write_error"}, {nil, errors.New("read"), "read_error"}, {errors.New("write"), errors.New("read"), "write_error"},
		} {
			t.Run(fmt.Sprintf("api=%d/%s/write=%t/read=%t", api.key, outcome.label, outcome.writeErr != nil, outcome.readErr != nil), func(t *testing.T) {
				reg := prometheus.NewPedanticRegistry()
				metrics := newKafkaWriterRequestMetrics(prometheus.WrapRegistererWithPrefix(writerMetricsPrefix, reg))
				e2e := kgo.BrokerE2E{WriteWait: 9 * time.Second, TimeToWrite: time.Second, ReadWait: 2 * time.Second, TimeToRead: 3 * time.Second, WriteErr: outcome.writeErr, ReadErr: outcome.readErr}
				metrics.OnBrokerE2E(kgo.BrokerMetadata{NodeID: 42}, api.key, e2e)
				families, err := reg.Gather()
				require.NoError(t, err)
				require.Len(t, families, 2)
				for _, family := range families {
					require.Len(t, family.Metric, 9)
					for _, metric := range family.Metric {
						labels := map[string]string{}
						for _, label := range metric.Label {
							labels[label.GetName()] = label.GetValue()
						}
						require.Len(t, labels, 2)
						if labels["api"] == api.label && labels["transport_outcome"] == outcome.label {
							assert.Equal(t, uint64(1), metric.Histogram.GetSampleCount())
							want := 6.0
							if family.GetName() == writerMetricsPrefix+"kafka_request_write_wait_by_api_seconds" {
								want = 9
							}
							assert.Equal(t, want, metric.Histogram.GetSampleSum())
						} else {
							assert.Zero(t, metric.Histogram.GetSampleCount())
						}
					}
				}
			})
		}
	}

	t.Run("bounded labels and allocation-free observations", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		m := newKafkaWriterRequestMetrics(reg)
		for i := range 1000 {
			m.OnBrokerE2E(kgo.BrokerMetadata{NodeID: int32(i)}, int16(i), kgo.BrokerE2E{})
		}
		assert.Zero(t, testing.AllocsPerRun(1000, func() { m.OnBrokerE2E(kgo.BrokerMetadata{}, 0, kgo.BrokerE2E{}) }))
		families, err := reg.Gather()
		require.NoError(t, err)
		for _, family := range families {
			assert.Len(t, family.Metric, 9)
		}
	})

	t.Run("shared metrics retain their existing schema", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		m := NewKafkaClientExtendedMetrics(reg)
		m.OnBrokerE2E(kgo.BrokerMetadata{}, 0, kgo.BrokerE2E{})
		m.OnBrokerThrottle(kgo.BrokerMetadata{}, time.Second, false)
		families, err := reg.Gather()
		require.NoError(t, err)
		require.Len(t, families, 6)
		var names []string
		for _, family := range families {
			names = append(names, family.GetName())
			require.Len(t, family.Metric, 1)
			assert.Empty(t, family.Metric[0].Label)
			assert.Equal(t, uint64(1), family.Metric[0].Histogram.GetSampleCount())
		}
		assert.ElementsMatch(t, []string{"kafka_write_wait_seconds", "kafka_write_time_seconds", "kafka_read_wait_seconds", "kafka_read_time_seconds", "kafka_request_duration_e2e_seconds", "kafka_request_throttled_seconds"}, names)
	})
}

func TestKafkaWriterDiagnosticConfiguration(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprint(enabled), func(t *testing.T) {
			cfg := createTestKafkaConfig("localhost:9092", "test")
			cfg.ProducerDiagnosticLoggingEnabled = enabled
			cfg.ClientID = "wire-client-id"
			require.NoError(t, cfg.Validate())
			var buf bytes.Buffer
			logger := log.NewJSONLogger(log.NewSyncWriter(&buf))
			for range 2 {
				reg := prometheus.NewPedanticRegistry()
				client, err := NewKafkaWriterClient(cfg, defaultMaxInflightProduceRequests, logger, reg)
				require.NoError(t, err)
				clientLogger := client.OptValue(kgo.WithLogger).(kgo.Logger)
				assert.Equal(t, kgo.LogLevelInfo, clientLogger.Level())
				assert.Equal(t, cfg.ClientID, client.OptValue(kgo.ClientID))
				clientLogger.Log(kgo.LogLevelInfo, "identity test")
				client.Close()
				families, err := reg.Gather()
				require.NoError(t, err)
				found := false
				for _, family := range families {
					if family.GetName() == "kafka_diagnostic_events_dropped_total" {
						found = true
						assert.Len(t, family.Metric, 3)
					}
				}
				assert.Equal(t, enabled, found)
			}
			decoder := json.NewDecoder(&buf)
			var first, second map[string]any
			require.NoError(t, decoder.Decode(&first))
			require.NoError(t, decoder.Decode(&second))
			require.NotNil(t, first["kafka_writer_client_id"])
			assert.NotEqual(t, first["kafka_writer_client_id"], second["kafka_writer_client_id"])

			reader, err := NewKafkaReaderClient(cfg, nil, logger)
			require.NoError(t, err)
			assert.Equal(t, kgo.LogLevelInfo, reader.OptValue(kgo.WithLogger).(kgo.Logger).Level())
			reader.Close()
		})
	}
	t.Run("unsupported backend fails before construction", func(t *testing.T) {
		cfg := createTestKafkaConfigForBackend(KafkaBackendWarpstream, "localhost:9092", "test")
		cfg.ProducerDiagnosticLoggingEnabled = true
		require.ErrorContains(t, cfg.Validate(), "producer-diagnostic-logging-enabled is only supported")
		reg := prometheus.NewPedanticRegistry()
		_, err := newKafkaProducerForBackend(cfg, 1, log.NewNopLogger(), reg)
		require.ErrorContains(t, err, "producer-diagnostic-logging-enabled is only supported")
		families, err := reg.Gather()
		require.NoError(t, err)
		assert.Empty(t, families)
	})
}

type diagnosticCountingLogger struct{ calls atomic.Int64 }

func (l *diagnosticCountingLogger) Log(...any) error { l.calls.Add(1); return nil }

func TestKafkaWriterDiagnostics_RateLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logger := &diagnosticCountingLogger{}
		d := newKafkaWriterDiagnostics(logger, prometheus.NewPedanticRegistry())
		var done sync.WaitGroup
		for range 500 {
			done.Add(1)
			go func() { defer done.Done(); d.OnBrokerE2E(kgo.BrokerMetadata{}, 0, kgo.BrokerE2E{}) }()
		}
		done.Wait()
		assert.Equal(t, int64(100), logger.calls.Load())
		assert.Equal(t, 400.0, promtest.ToFloat64(d.dropped[0]))
		d.OnBrokerE2E(kgo.BrokerMetadata{}, 3, kgo.BrokerE2E{})
		d.OnBrokerConnect(kgo.BrokerMetadata{}, 0, nil, nil)
		assert.Equal(t, int64(102), logger.calls.Load(), "Produce traffic must not exhaust other groups")
		// Write and completion events share the Produce budget.
		d.OnBrokerWrite(kgo.BrokerMetadata{}, 0, 0, 0, 0, nil)
		assert.Equal(t, 401.0, promtest.ToFloat64(d.dropped[0]))
		time.Sleep(time.Second)
		for range 101 {
			d.OnBrokerWrite(kgo.BrokerMetadata{}, 0, 0, 0, 0, nil)
		}
		assert.Equal(t, int64(202), logger.calls.Load())
		assert.Equal(t, 402.0, promtest.ToFloat64(d.dropped[0]))
		d.OnBrokerWrite(kgo.BrokerMetadata{}, kmsg.SASLAuthenticate.Int16(), 0, 0, 0, nil)
		d.OnBrokerE2E(kgo.BrokerMetadata{}, kmsg.SASLAuthenticate.Int16(), kgo.BrokerE2E{})
		assert.Equal(t, int64(202), logger.calls.Load(), "unselected APIs must not emit diagnostics")
	})
}

func TestKafkaWriterDiagnostics_Logging(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		for _, debug := range []bool{false, true} {
			t.Run(fmt.Sprintf("enabled=%t/debug=%t", enabled, debug), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					var vnet kfake.VirtualNetwork
					cluster, addr := testkafka.CreateCluster(t, 1, "test", testkafka.WithVirtualNetwork(&vnet))
					var injected atomic.Int64
					cluster.ControlKey(kmsg.Produce.Int16(), func(req kmsg.Request) (kmsg.Response, error, bool) {
						injected.Add(1)
						return storageErrorResponse(req.(*kmsg.ProduceRequest)), nil, true
					})
					cfg := createTestKafkaConfig(addr, "test")
					cfg.Dialer = vnet.DialContext
					cfg.ProducerMetadataMinAge = 100 * time.Millisecond
					cfg.ProducerDiagnosticLoggingEnabled = enabled
					var buf bytes.Buffer
					filter := level.AllowInfo()
					if debug {
						filter = level.AllowDebug()
					}
					logger := level.NewFilter(log.NewJSONLogger(log.NewSyncWriter(&buf)), filter)
					client, err := NewKafkaWriterClient(cfg, defaultMaxInflightProduceRequests, logger, prometheus.NewPedanticRegistry())
					require.NoError(t, err)
					// Payloads and headers must not appear in diagnostics.
					result := client.ProduceSync(t.Context(), &kgo.Record{Key: []byte("private-key"), Value: []byte("private-payload"), Headers: []kgo.RecordHeader{{Key: "secret-header", Value: []byte("private-header")}}})
					require.NoError(t, result.FirstErr())
					client.Close()
					synctest.Wait()
					require.Equal(t, int64(1), injected.Load())
					for _, forbidden := range []string{"private-key", "private-payload", "private-header", "secret-header"} {
						assert.NotContains(t, buf.String(), forbidden)
					}
					decoder := json.NewDecoder(&buf)
					var clientID any
					var produceCompletions, writes, metadata, connects, disconnects int
					for decoder.More() {
						var event map[string]any
						require.NoError(t, decoder.Decode(&event))
						if clientID == nil {
							clientID = event["kafka_writer_client_id"]
						}
						assert.Equal(t, clientID, event["kafka_writer_client_id"])
						switch event["msg"] {
						case "Kafka writer request transport completed":
							assert.Equal(t, "success", event["transport_outcome"], "the error response still completed its transport successfully")
							assert.NotNil(t, event["observed_at"])
							assert.NotNil(t, event["elapsed"])
							switch event["api"] {
							case "produce":
								produceCompletions++
							case "metadata":
								metadata++
							}
						case "Kafka writer request write completed":
							writes++
						case "Kafka writer connection attempt completed":
							connects++
						case "Kafka writer connection closed":
							disconnects++
						}
						assert.NotEqual(t, "produced", event["msg"], "general franz-go debug logging must remain disabled")
					}
					if enabled && debug {
						assert.Equal(t, 2, produceCompletions)
						assert.Positive(t, metadata)
						assert.Positive(t, writes)
						assert.Positive(t, connects)
						assert.Positive(t, disconnects)
					} else {
						assert.Zero(t, produceCompletions+writes+metadata+connects+disconnects)
					}
				})
			})
		}
	}
}

func BenchmarkKafkaWriterRequestMetrics(b *testing.B) {
	m := newKafkaWriterRequestMetrics(prometheus.NewRegistry())
	event := kgo.BrokerE2E{WriteWait: time.Millisecond, TimeToWrite: time.Millisecond, ReadWait: time.Millisecond, TimeToRead: time.Millisecond}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		m.OnBrokerE2E(kgo.BrokerMetadata{}, 0, event)
	}
}
