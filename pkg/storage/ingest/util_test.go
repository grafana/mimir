// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl"
	awssasl "github.com/twmb/franz-go/pkg/sasl/aws"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestMSKIAMCredentials(t *testing.T) {
	for how, setUp := range map[string]func(t *testing.T, secret awssasl.Auth) kafkaSASLConfig[KafkaMSKIAMStaticConfig]{
		"file-based": func(t *testing.T, secret awssasl.Auth) kafkaSASLConfig[KafkaMSKIAMStaticConfig] {
			filePath := writeSecretToFile(t, secret)

			var cfg KafkaAuthMSKIAMConfig
			cfg.RegisterFlagsWithPrefix("", flag.NewFlagSet("", flag.PanicOnError))
			cfg.FilePath = filePath
			return kafkaSASLConfig[KafkaMSKIAMStaticConfig](cfg)
		},
		"socket-based": func(t *testing.T, secret awssasl.Auth) kafkaSASLConfig[KafkaMSKIAMStaticConfig] {
			socketPath := serveSecretFromSocket(t, secret)

			var cfg KafkaAuthMSKIAMConfig
			cfg.RegisterFlagsWithPrefix("", flag.NewFlagSet("", flag.PanicOnError))
			cfg.HTTPSocketPath = socketPath
			return kafkaSASLConfig[KafkaMSKIAMStaticConfig](cfg)
		},
	} {
		t.Run(how, func(t *testing.T) {
			t.Parallel()

			secret := awssasl.Auth{
				AccessKey:    "AKIDEXAMPLE",
				SecretKey:    "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
				SessionToken: "AQoDYXdzEJr//some/session/token",
			}

			cfg := setUp(t, secret)

			var gotCallback func(context.Context) (awssasl.Auth, error)
			gotMechanism := saslMechanism(cfg, func(callback func(context.Context) (awssasl.Auth, error)) sasl.Mechanism {
				gotCallback = callback
				return awssasl.ManagedStreamingIAM(callback)
			})
			require.NotNil(t, gotCallback)
			require.NotNil(t, gotMechanism)

			gotSecret, err := gotCallback(t.Context())
			require.NoError(t, err)
			require.Equal(t, secret, gotSecret)
		})
	}
}

func TestMSKIAMStaticCredentials(t *testing.T) {
	var secret KafkaMSKIAMStaticConfig
	require.NoError(t, secret.AccessKey.Set("AKIDEXAMPLE"))
	require.NoError(t, secret.SecretKey.Set("wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"))
	require.NoError(t, secret.SessionToken.Set("AQoDYXdzEJr//some/session/token"))

	cfg := kafkaSASLConfig[KafkaMSKIAMStaticConfig]{Secret: secret}

	_ = saslMechanism(cfg, func(_ func(context.Context) (awssasl.Auth, error)) (_ sasl.Mechanism) {
		require.Fail(t, "should have not been called")
		return
	})
}

func TestOauthbearerCredentials(t *testing.T) {
	for how, setUp := range map[string]func(t *testing.T, secret oauth.Auth) kafkaSASLConfig[KafkaOauthbearerStaticConfig]{
		"file-based": func(t *testing.T, secret oauth.Auth) kafkaSASLConfig[KafkaOauthbearerStaticConfig] {
			filePath := writeSecretToFile(t, secret)

			var cfg KafkaAuthOauthbearerConfig
			cfg.RegisterFlagsWithPrefix("", flag.NewFlagSet("", flag.PanicOnError))
			cfg.FilePath = filePath
			return kafkaSASLConfig[KafkaOauthbearerStaticConfig](cfg)
		},
		"socket-based": func(t *testing.T, secret oauth.Auth) kafkaSASLConfig[KafkaOauthbearerStaticConfig] {
			socketPath := serveSecretFromSocket(t, secret)

			var cfg KafkaAuthOauthbearerConfig
			cfg.RegisterFlagsWithPrefix("", flag.NewFlagSet("", flag.PanicOnError))
			cfg.HTTPSocketPath = socketPath
			return kafkaSASLConfig[KafkaOauthbearerStaticConfig](cfg)
		},
	} {
		t.Run(how, func(t *testing.T) {
			t.Parallel()

			secret := oauth.Auth{
				Token: "some-oauth-token",
				Zid:   "some-zid",
			}

			cfg := setUp(t, secret)

			var gotCallback func(context.Context) (oauth.Auth, error)
			gotMechanism := saslMechanism(cfg, func(callback func(context.Context) (oauth.Auth, error)) sasl.Mechanism {
				gotCallback = callback
				return oauth.Oauth(callback)
			})
			require.NotNil(t, gotCallback)
			require.NotNil(t, gotMechanism)

			gotSecret, err := gotCallback(t.Context())
			require.NoError(t, err)
			require.Equal(t, secret, gotSecret)
		})
	}
}

func TestOauthbearerStaticCredentials(t *testing.T) {
	var secret KafkaOauthbearerStaticConfig
	require.NoError(t, secret.Token.Set("some-oauth-token"))
	secret.Zid = "some-zid"

	cfg := kafkaSASLConfig[KafkaOauthbearerStaticConfig]{Secret: secret}

	_ = saslMechanism(cfg, func(_ func(context.Context) (oauth.Auth, error)) (_ sasl.Mechanism) {
		require.Fail(t, "should have not been called")
		return
	})
}

func TestIngesterPartitionID(t *testing.T) {
	t.Run("with zones", func(t *testing.T) {
		actual, err := IngesterPartitionID("ingester-zone-a-0")
		require.NoError(t, err)
		assert.EqualValues(t, 0, actual)

		actual, err = IngesterPartitionID("ingester-zone-b-0")
		require.NoError(t, err)
		assert.EqualValues(t, 0, actual)

		actual, err = IngesterPartitionID("ingester-zone-a-1")
		require.NoError(t, err)
		assert.EqualValues(t, 1, actual)

		actual, err = IngesterPartitionID("ingester-zone-b-1")
		require.NoError(t, err)
		assert.EqualValues(t, 1, actual)

		actual, err = IngesterPartitionID("mimir-write-zone-c-2")
		require.NoError(t, err)
		assert.EqualValues(t, 2, actual)
	})

	t.Run("without zones", func(t *testing.T) {
		actual, err := IngesterPartitionID("ingester-0")
		require.NoError(t, err)
		assert.EqualValues(t, 0, actual)

		actual, err = IngesterPartitionID("ingester-1")
		require.NoError(t, err)
		assert.EqualValues(t, 1, actual)

		actual, err = IngesterPartitionID("mimir-write-2")
		require.NoError(t, err)
		assert.EqualValues(t, 2, actual)
	})

	t.Run("should return error if the ingester ID has a non supported format", func(t *testing.T) {
		_, err := IngesterPartitionID("unknown")
		require.Error(t, err)

		_, err = IngesterPartitionID("ingester-zone-a-")
		require.Error(t, err)

		_, err = IngesterPartitionID("ingester-zone-a")
		require.Error(t, err)
	})
}

func TestResultPromise(t *testing.T) {
	t.Run("wait() should block until a result has been notified", func(t *testing.T) {
		var (
			wg  = sync.WaitGroup{}
			rw  = newResultPromise[int]()
			ctx = context.Background()
		)

		// Spawn few goroutines waiting for the result.
		for i := 0; i < 3; i++ {
			runAsync(&wg, func() {
				actual, err := rw.wait(ctx)
				require.NoError(t, err)
				require.Equal(t, 12345, actual)
			})
		}

		// Notify the result.
		rw.notify(12345, nil)

		// Wait until all goroutines have done.
		wg.Wait()
	})

	t.Run("wait() should block until an error has been notified", func(t *testing.T) {
		var (
			wg        = sync.WaitGroup{}
			rw        = newResultPromise[int]()
			ctx       = context.Background()
			resultErr = errors.New("test error")
		)

		// Spawn few goroutines waiting for the result.
		for i := 0; i < 3; i++ {
			runAsync(&wg, func() {
				actual, err := rw.wait(ctx)
				require.Equal(t, resultErr, err)
				require.Equal(t, 0, actual)
			})
		}

		// Notify the result.
		rw.notify(0, resultErr)

		// Wait until all goroutines have done.
		wg.Wait()
	})

	t.Run("wait() should return when the input context timeout expires", func(t *testing.T) {
		var (
			rw  = newResultPromise[int]()
			ctx = context.Background()
		)

		ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()

		actual, err := rw.wait(ctxWithTimeout)
		require.Equal(t, context.DeadlineExceeded, err)
		require.Equal(t, 0, actual)
	})
}

func TestCreateTopics(t *testing.T) {
	createKafkaCluster := func(t *testing.T) (string, *kfake.Cluster) {
		cluster, err := kfake.NewCluster(kfake.NumBrokers(1))
		require.NoError(t, err)
		t.Cleanup(cluster.Close)

		addrs := cluster.ListenAddrs()
		require.Len(t, addrs, 1)

		return addrs[0], cluster
	}

	t.Run("should create the partitions", func(t *testing.T) {
		var (
			addr, cluster = createKafkaCluster(t)
			cfg           = KafkaConfig{
				Topic:                            "topic-name",
				AutoCreateTopicDefaultPartitions: 100,
			}
		)
		require.NoError(t, cfg.Address.Set(addr))

		cluster.ControlKey(kmsg.AlterConfigs.Int16(), func(request kmsg.Request) (kmsg.Response, error, bool) {
			r := request.(*kmsg.CreateTopicsRequest)

			assert.Len(t, r.Topics, 1)
			res := r.Topics[0]
			assert.Equal(t, cfg.Topic, res.Topic)
			assert.Equal(t, 100, res.NumPartitions)
			assert.Equal(t, -1, res.ReplicationFactor)
			assert.Empty(t, res.Configs)
			assert.Empty(t, res.ReplicaAssignment)

			return &kmsg.CreateTopicsResponse{
				Version: r.Version,
				Topics: []kmsg.CreateTopicsResponseTopic{
					{
						Topic:             cfg.Topic,
						NumPartitions:     res.NumPartitions,
						ReplicationFactor: 3,
					},
				},
			}, nil, true
		})

		logger := log.NewNopLogger()

		require.NoError(t, CreateTopics(cfg, logger, cfg.Topic))
	})

	t.Run("should create all the requested topics", func(t *testing.T) {
		var (
			addr, cluster = createKafkaCluster(t)
			cfg           = KafkaConfig{
				AutoCreateTopicDefaultPartitions: 100,
			}
			topics = []string{"topic-a", "topic-b"}
		)
		require.NoError(t, cfg.Address.Set(addr))

		cluster.ControlKey(kmsg.CreateTopics.Int16(), func(request kmsg.Request) (kmsg.Response, error, bool) {
			r := request.(*kmsg.CreateTopicsRequest)

			require.Len(t, r.Topics, 2)
			respTopics := make([]kmsg.CreateTopicsResponseTopic, 0, len(r.Topics))
			for _, res := range r.Topics {
				assert.Equal(t, int32(100), res.NumPartitions)
				assert.Equal(t, int16(-1), res.ReplicationFactor)
				respTopics = append(respTopics, kmsg.CreateTopicsResponseTopic{
					Topic:             res.Topic,
					NumPartitions:     res.NumPartitions,
					ReplicationFactor: 3,
				})
			}

			return &kmsg.CreateTopicsResponse{Version: r.Version, Topics: respTopics}, nil, true
		})

		require.NoError(t, CreateTopics(cfg, log.NewNopLogger(), topics...))
	})

	t.Run("should return an error if one of multiple topics fails to be created", func(t *testing.T) {
		var (
			addr, cluster = createKafkaCluster(t)
			cfg           = KafkaConfig{
				AutoCreateTopicDefaultPartitions: 100,
			}
			topics = []string{"topic-a", "topic-b"}
		)
		require.NoError(t, cfg.Address.Set(addr))

		// topic-a already exists (tolerated), topic-b genuinely fails: the call must surface topic-b's error.
		cluster.ControlKey(kmsg.CreateTopics.Int16(), func(request kmsg.Request) (kmsg.Response, error, bool) {
			r := request.(*kmsg.CreateTopicsRequest)
			return &kmsg.CreateTopicsResponse{
				Version: r.Version,
				Topics: []kmsg.CreateTopicsResponseTopic{
					{Topic: "topic-a", ErrorCode: kerr.TopicAlreadyExists.Code},
					{Topic: "topic-b", ErrorCode: kerr.NotLeaderForPartition.Code},
				},
			}, nil, true
		})

		err := CreateTopics(cfg, log.NewNopLogger(), topics...)
		require.Error(t, err)
		assert.ErrorContains(t, err, "topic-b")
	})

	t.Run("should return an error if a requested topic is missing from the response", func(t *testing.T) {
		var (
			addr, cluster = createKafkaCluster(t)
			cfg           = KafkaConfig{
				AutoCreateTopicDefaultPartitions: 100,
			}
			topics = []string{"topic-a", "topic-b"}
		)
		require.NoError(t, cfg.Address.Set(addr))

		// The broker omits topic-b from the response: we can't confirm its creation, so it must error.
		cluster.ControlKey(kmsg.CreateTopics.Int16(), func(request kmsg.Request) (kmsg.Response, error, bool) {
			r := request.(*kmsg.CreateTopicsRequest)
			return &kmsg.CreateTopicsResponse{
				Version: r.Version,
				Topics: []kmsg.CreateTopicsResponseTopic{
					{Topic: "topic-a", NumPartitions: 100, ReplicationFactor: 3},
				},
			}, nil, true
		})

		err := CreateTopics(cfg, log.NewNopLogger(), topics...)
		require.Error(t, err)
		assert.ErrorContains(t, err, "topic-b")
	})

	t.Run("should return an error if the request fails", func(t *testing.T) {
		var (
			addr, cluster = createKafkaCluster(t)
			cfg           = KafkaConfig{
				AutoCreateTopicDefaultPartitions: 100,
			}
		)
		require.NoError(t, cfg.Address.Set(addr))

		cluster.ControlKey(kmsg.CreateTopics.Int16(), func(request kmsg.Request) (kmsg.Response, error, bool) {
			r := request.(*kmsg.CreateTopicsRequest)
			return &kmsg.CreateTopicsResponse{
				Version: r.Version,
				Topics: []kmsg.CreateTopicsResponseTopic{
					{
						Topic:     cfg.Topic,
						ErrorCode: kerr.NotLeaderForPartition.Code,
					},
				},
			}, nil, true
		})

		logger := log.NewNopLogger()

		require.Error(t, CreateTopics(cfg, logger, cfg.Topic))
	})

	t.Run("should return an error if the request succeed but the response contains an error", func(t *testing.T) {
		var (
			addr, cluster = createKafkaCluster(t)
			cfg           = KafkaConfig{
				Topic:                            "topic-name",
				AutoCreateTopicDefaultPartitions: 100,
			}
		)
		require.NoError(t, cfg.Address.Set(addr))

		cluster.ControlKey(kmsg.AlterConfigs.Int16(), func(kReq kmsg.Request) (kmsg.Response, error, bool) {
			req := kReq.(*kmsg.CreateTopicsRequest)
			assert.Len(t, req.Topics, 1)
			res := req.Topics[0]
			assert.Equal(t, cfg.Topic, res.Topic)
			assert.Equal(t, 100, res.NumPartitions)
			assert.Equal(t, -1, res.ReplicationFactor)
			assert.Empty(t, res.Configs)
			assert.Empty(t, res.ReplicaAssignment)

			return &kmsg.CreateTopicsResponse{
				Version: req.Version,
				Topics: []kmsg.CreateTopicsResponseTopic{
					{
						Topic:             cfg.Topic,
						NumPartitions:     res.NumPartitions,
						ReplicationFactor: 3,
					},
				},
			}, nil, true
		})

		logger := log.NewNopLogger()

		require.NoError(t, CreateTopics(cfg, logger, cfg.Topic))
	})

	t.Run("should not return error when topic already exists", func(t *testing.T) {
		var (
			addr, _ = createKafkaCluster(t)
			cfg     = KafkaConfig{
				Topic:                            "topic-name",
				AutoCreateTopicDefaultPartitions: 100,
			}
			logger = log.NewNopLogger()
		)
		require.NoError(t, cfg.Address.Set(addr))

		// First call should create the topic
		assert.NoError(t, CreateTopics(cfg, logger, cfg.Topic))

		// Second call should succeed because topic already exists
		assert.NoError(t, CreateTopics(cfg, logger, cfg.Topic))
	})
}

func writeSecretToFile(t *testing.T, secret any) string {
	t.Helper()

	js, err := json.Marshal(secret)
	require.NoError(t, err)
	filePath := filepath.Join(t.TempDir(), "secret.json")
	require.NoError(t, os.WriteFile(filePath, js, 0600))

	return filePath
}

func serveSecretFromSocket(t *testing.T, secret any) string {
	t.Helper()

	js, err := json.Marshal(secret)
	require.NoError(t, err)

	sockDir, err := os.MkdirTemp("/tmp", "mimir")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(sockDir) })

	socketPath := filepath.Join(sockDir, "secret.sock")
	listener, err := net.Listen("unix", socketPath)
	require.NoError(t, err)

	server := &http.Server{
		ReadHeaderTimeout: 10 * time.Second,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(js)
		}),
	}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })

	return socketPath
}

// setupTestTracerProvider installs a global TracerProvider with a parent-based
// sampler and a span recorder, restoring the previous provider on cleanup.
// Tests using it must not run in parallel because the provider is global.
func setupTestTracerProvider(t testing.TB) *tracetest.SpanRecorder {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(recorder),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())),
	)
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTracerProvider(prev)
		_ = tp.Shutdown(t.Context())
	})
	return recorder
}

// fetchRecordWithTraceparent returns a fetched record carrying a traceparent
// header with the given trace ID and sampled flag, as injected by a producer.
func fetchRecordWithTraceparent(traceID string, sampled bool) *kgo.Record {
	flags := "00"
	if sampled {
		flags = "01"
	}
	return &kgo.Record{
		Topic: "test-topic",
		Headers: []kgo.RecordHeader{
			{Key: "traceparent", Value: []byte("00-" + traceID + "-00f067aa0ba902b7-" + flags)},
		},
	}
}

func TestSampledOnlyTracer_FetchHooks(t *testing.T) {
	const traceID = "4bf92f3577b34da6a3ce929d0e0e4736"

	t.Run("unsampled record: context carries the remote span context, no span is created", func(t *testing.T) {
		recorder := setupTestTracerProvider(t)
		tracer := newSampledOnlyTracer()

		rec := fetchRecordWithTraceparent(traceID, false)
		tracer.OnFetchRecordBuffered(rec)

		require.NotNil(t, rec.Context)
		sc := trace.SpanContextFromContext(rec.Context)
		assert.Equal(t, traceID, sc.TraceID().String())
		assert.False(t, sc.IsSampled())
		assert.True(t, sc.IsRemote())
		assert.Empty(t, recorder.Started())

		tracer.OnFetchRecordUnbuffered(rec, true)
		assert.Empty(t, recorder.Ended())
	})

	t.Run("sampled record: a receive span is created and ended", func(t *testing.T) {
		recorder := setupTestTracerProvider(t)
		tracer := newSampledOnlyTracer()

		rec := fetchRecordWithTraceparent(traceID, true)
		tracer.OnFetchRecordBuffered(rec)

		require.Len(t, recorder.Started(), 1)
		span := recorder.Started()[0]
		assert.Equal(t, "test-topic receive", span.Name())
		assert.Equal(t, traceID, span.Parent().TraceID().String())
		assert.True(t, trace.SpanContextFromContext(rec.Context).IsSampled())

		tracer.OnFetchRecordUnbuffered(rec, true)
		require.Len(t, recorder.Ended(), 1)
	})

	t.Run("record without trace headers: context is set, no span is created", func(t *testing.T) {
		recorder := setupTestTracerProvider(t)
		tracer := newSampledOnlyTracer()

		rec := &kgo.Record{Topic: "test-topic"}
		tracer.OnFetchRecordBuffered(rec)

		require.NotNil(t, rec.Context)
		assert.False(t, trace.SpanContextFromContext(rec.Context).IsValid())
		assert.Empty(t, recorder.Started())

		tracer.OnFetchRecordUnbuffered(rec, true)
		assert.Empty(t, recorder.Ended())
	})
}

func TestSampledOnlyTracer_ProduceHooks(t *testing.T) {
	t.Run("unsampled context: no span is created", func(t *testing.T) {
		recorder := setupTestTracerProvider(t)
		tracer := newSampledOnlyTracer()

		rec := &kgo.Record{Topic: "test-topic", Context: trace.ContextWithSpanContext(t.Context(), trace.NewSpanContext(trace.SpanContextConfig{
			TraceID:    trace.TraceID{1},
			SpanID:     trace.SpanID{1},
			TraceFlags: 0,
			Remote:     true,
		}))}
		tracer.OnProduceRecordBuffered(rec)
		assert.Empty(t, recorder.Started())

		tracer.OnProduceRecordUnbuffered(rec, nil)
		assert.Empty(t, recorder.Ended())
	})

	t.Run("sampled context: a publish span is created and ended", func(t *testing.T) {
		recorder := setupTestTracerProvider(t)
		tracer := newSampledOnlyTracer()

		ctx, parent := otel.Tracer("test").Start(t.Context(), "request")
		defer parent.End()

		rec := &kgo.Record{Topic: "test-topic", Context: ctx}
		tracer.OnProduceRecordBuffered(rec)
		require.Len(t, recorder.Started(), 2) // "request" + the publish span.
		assert.Equal(t, "test-topic publish", recorder.Started()[1].Name())

		tracer.OnProduceRecordUnbuffered(rec, nil)
		require.Len(t, recorder.Ended(), 1)
	})
}

// BenchmarkFetchRecordTracing compares the per-record fetch-hook cost for
// unsampled traces (the overwhelmingly common case in production) between the
// gated sampledOnlyTracer and the raw kotel tracer it wraps.
func BenchmarkFetchRecordTracing(b *testing.B) {
	const traceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	setupTestTracerProvider(b)

	b.Run("sampledOnlyTracer unsampled", func(b *testing.B) {
		tracer := newSampledOnlyTracer()
		rec := fetchRecordWithTraceparent(traceID, false)
		b.ReportAllocs()
		for b.Loop() {
			rec.Context = b.Context()
			tracer.OnFetchRecordBuffered(rec)
			tracer.OnFetchRecordUnbuffered(rec, true)
		}
	})

	b.Run("kotel unsampled", func(b *testing.B) {
		tracer := recordsTracer()
		rec := fetchRecordWithTraceparent(traceID, false)
		b.ReportAllocs()
		for b.Loop() {
			rec.Context = b.Context()
			tracer.OnFetchRecordBuffered(rec)
			tracer.OnFetchRecordUnbuffered(rec, true)
		}
	})
}
