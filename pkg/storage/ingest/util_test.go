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
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl"
	awssasl "github.com/twmb/franz-go/pkg/sasl/aws"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
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

func TestCreateTopic(t *testing.T) {
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

		require.NoError(t, CreateTopic(cfg, logger))
	})

	t.Run("should return an error if the request fails", func(t *testing.T) {
		var (
			addr, cluster = createKafkaCluster(t)
			cfg           = KafkaConfig{
				AutoCreateTopicDefaultPartitions: 100,
			}
		)
		require.NoError(t, cfg.Address.Set(addr))

		cluster.ControlKey(kmsg.CreateTopics.Int16(), func(_ kmsg.Request) (kmsg.Response, error, bool) {
			return &kmsg.CreateTopicsResponse{
				Topics: []kmsg.CreateTopicsResponseTopic{
					{
						Topic:     cfg.Topic,
						ErrorCode: kerr.NotLeaderForPartition.Code,
					},
				},
			}, nil, true
		})

		logger := log.NewNopLogger()

		require.Error(t, CreateTopic(cfg, logger))
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

		require.NoError(t, CreateTopic(cfg, logger))
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
		assert.NoError(t, CreateTopic(cfg, logger))

		// Second call should succeed because topic already exists
		assert.NoError(t, CreateTopic(cfg, logger))
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
