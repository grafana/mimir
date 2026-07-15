// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"fmt"
	"testing"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/warpstream-go/pkg/wgo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_Validate(t *testing.T) {
	type testCase struct {
		setup       func(*Config)
		expectedErr error
	}

	tests := map[string]testCase{
		"should pass with the default config": {
			setup: func(_ *Config) {},
		},
		"should fail if ingest storage is enabled and Kafka address is not configured": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Topic = "test"
			},
			expectedErr: ErrMissingKafkaAddress,
		},
		"should fail if ingest storage is enabled and Kafka topic is not configured": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = flagext.StringSliceCSV{"localhost"}
			},
			expectedErr: ErrMissingKafkaTopic,
		},
		"should pass if ingest storage is enabled and required config is set": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = flagext.StringSliceCSV{"localhost"}
				cfg.KafkaConfig.Topic = "test"
			},
		},
		"should pass if backend is explicitly set to warpstream": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = flagext.StringSliceCSV{"localhost"}
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.Backend = KafkaBackendWarpstream
			},
		},
		"should fail if backend is warpstream and write timeout is less than twice the request overhead": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = flagext.StringSliceCSV{"localhost"}
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.Backend = KafkaBackendWarpstream
				cfg.KafkaConfig.WriteTimeout = DefaultKafkaRequestTimeoutOverhead*2 - time.Millisecond
			},
			expectedErr: ErrInvalidWarpstreamWriteTimeout,
		},
		"should pass if backend is warpstream and write timeout equals twice the request overhead": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = flagext.StringSliceCSV{"localhost"}
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.Backend = KafkaBackendWarpstream
				cfg.KafkaConfig.WriteTimeout = DefaultKafkaRequestTimeoutOverhead * 2
			},
		},
		"should pass if backend is warpstream and write timeout is just above twice the request overhead": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = flagext.StringSliceCSV{"localhost"}
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.Backend = KafkaBackendWarpstream
				cfg.KafkaConfig.WriteTimeout = DefaultKafkaRequestTimeoutOverhead*2 + time.Millisecond
			},
		},
		"should honor a custom write timeout overhead when enforcing the warpstream write timeout floor": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = flagext.StringSliceCSV{"localhost"}
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.Backend = KafkaBackendWarpstream
				cfg.KafkaConfig.WriteTimeoutOverhead = 500 * time.Millisecond
				cfg.KafkaConfig.WriteTimeout = 500*time.Millisecond*2 - time.Millisecond
			},
			expectedErr: ErrInvalidWarpstreamWriteTimeout,
		},
		"should fail if the write timeout overhead is below the minimum": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = flagext.StringSliceCSV{"localhost"}
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.WriteTimeoutOverhead = MinKafkaRequestTimeoutOverhead - time.Millisecond
			},
			expectedErr: ErrInvalidWriteTimeoutOverhead,
		},
		"should pass if the write timeout overhead equals the minimum": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = flagext.StringSliceCSV{"localhost"}
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.WriteTimeoutOverhead = MinKafkaRequestTimeoutOverhead
			},
		},
		"should not enforce the warpstream write timeout floor for the kafka backend": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = flagext.StringSliceCSV{"localhost"}
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.WriteTimeout = time.Second
			},
		},
		"should fail if backend is empty": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = flagext.StringSliceCSV{"localhost"}
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.Backend = ""
			},
			expectedErr: ErrInvalidKafkaBackend,
		},
		"should fail if backend is unknown": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = flagext.StringSliceCSV{"localhost"}
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.Backend = "confluent"
			},
			expectedErr: ErrInvalidKafkaBackend,
		},
		"should fail if backend value is not lowercase": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = flagext.StringSliceCSV{"localhost"}
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.Backend = "Kafka"
			},
			expectedErr: ErrInvalidKafkaBackend,
		},
		"should fail if ingest storage is enabled and consume position is invalid": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = flagext.StringSliceCSV{"localhost"}
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.ConsumeFromPositionAtStartup = "middle"
			},
			expectedErr: ErrInvalidConsumePosition,
		},
		"should fail if ingest storage is enabled and consume timestamp is set and consume position is not expected": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = flagext.StringSliceCSV{"localhost"}
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.ConsumeFromPositionAtStartup = consumeFromEnd
				cfg.KafkaConfig.ConsumeFromTimestampAtStartup = time.Now().UnixMilli()
			},
			expectedErr: ErrInvalidConsumePosition,
		},
		"should fail if ingest storage is enabled and consume position is expected but consume timestamp is invalid": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = flagext.StringSliceCSV{"localhost"}
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.ConsumeFromPositionAtStartup = consumeFromTimestamp
				cfg.KafkaConfig.ConsumeFromTimestampAtStartup = 0
			},
			expectedErr: ErrInvalidConsumePosition,
		},
		"should fail if ingest storage is enabled and producer max record size bytes is set too low": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = flagext.StringSliceCSV{"localhost"}
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.ProducerMaxRecordSizeBytes = minProducerRecordDataBytesLimit - 1
			},
			expectedErr: ErrInvalidProducerMaxRecordSizeBytes,
		},
		"should fail if ingest storage is enabled and producer max record size bytes is set too high": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = flagext.StringSliceCSV{"localhost"}
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.ProducerMaxRecordSizeBytes = maxProducerRecordDataBytesLimit + 1
			},
			expectedErr: ErrInvalidProducerMaxRecordSizeBytes,
		},
		"should pass if ingest storage is enabled and producer compression is set to none": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = flagext.StringSliceCSV{"localhost"}
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.ProducerCompression = kafkaCompressionNone
			},
		},
		"should pass if ingest storage is enabled and producer compression is set to zstd": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = flagext.StringSliceCSV{"localhost"}
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.ProducerCompression = kafkaCompressionZstd
			},
		},
		"should fail if ingest storage is enabled and producer compression is set to an unsupported codec": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = flagext.StringSliceCSV{"localhost"}
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.ProducerCompression = "brotli"
			},
			expectedErr: ErrInvalidProducerCompression,
		},
		"should fail if target consumer lag is enabled but max consumer lag is not": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = flagext.StringSliceCSV{"localhost"}
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.TargetConsumerLagAtStartup = 2 * time.Second
				cfg.KafkaConfig.MaxConsumerLagAtStartup = 0
			},
			expectedErr: ErrInconsistentConsumerLagAtStartup,
		},
		"should fail if max consumer lag is enabled but target consumer lag is not": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = flagext.StringSliceCSV{"localhost"}
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.TargetConsumerLagAtStartup = 0
				cfg.KafkaConfig.MaxConsumerLagAtStartup = 2 * time.Second
			},
			expectedErr: ErrInconsistentConsumerLagAtStartup,
		},
		"should fail if target consumer lag is > max consumer lag": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = flagext.StringSliceCSV{"localhost"}
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.TargetConsumerLagAtStartup = 2 * time.Second
				cfg.KafkaConfig.MaxConsumerLagAtStartup = 1 * time.Second
			},
			expectedErr: ErrInvalidMaxConsumerLagAtStartup,
		},
		"should fail if SASL username is configured but password is not": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = flagext.StringSliceCSV{"localhost"}
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.SASL.Username = "mimir"
			},
			expectedErr: ErrInconsistentSASLCredentials,
		},
		"should fail if SASL password is configured but username is not": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = flagext.StringSliceCSV{"localhost"}
				cfg.KafkaConfig.Topic = "test"
				require.NoError(t, cfg.KafkaConfig.SASL.Password.Set("supersecret"))
			},
			expectedErr: ErrInconsistentSASLCredentials,
		},
		"should pass if both SASL username and password are configured": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = flagext.StringSliceCSV{"localhost"}
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.SASL.Username = "mimir"
				require.NoError(t, cfg.KafkaConfig.SASL.Password.Set("supersecret"))
			},
		},
		"should fail if SASL mechanism is AWS_MSK_IAM but no way to get credentials is configured": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = flagext.StringSliceCSV{"localhost"}
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.SASL.Mechanism = SASLMechanismMSKIAM
			},
			expectedErr: ErrSASLMSKIAMBadConfig,
		},
		"should fail if SASL mechanism is AWS_MSK_IAM with only access key in static credentials": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = flagext.StringSliceCSV{"localhost"}
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.SASL.Mechanism = SASLMechanismMSKIAM
				require.NoError(t, cfg.KafkaConfig.SASL.MSKIAM.Secret.AccessKey.Set("AKID"))
			},
			expectedErr: errIncompleteMSKIAMSecret,
		},
		"should fail if SASL mechanism is AWS_MSK_IAM with both file path and HTTP socket path": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = flagext.StringSliceCSV{"localhost"}
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.SASL.Mechanism = SASLMechanismMSKIAM
				cfg.KafkaConfig.SASL.MSKIAM.FilePath = "/path/to/creds.json"
				cfg.KafkaConfig.SASL.MSKIAM.HTTPSocketPath = "/tmp/aws.sock"
			},
			expectedErr: ErrSASLMSKIAMBadConfig,
		},
		"should succeed if SASL mechanism is AWS_MSK_IAM and static credentials are passed": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = flagext.StringSliceCSV{"localhost"}
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.SASL.Mechanism = SASLMechanismMSKIAM
				require.NoError(t, cfg.KafkaConfig.SASL.MSKIAM.Secret.AccessKey.Set("AKID"))
				require.NoError(t, cfg.KafkaConfig.SASL.MSKIAM.Secret.SecretKey.Set("secret"))
			},
		},
		"should succeed if SASL mechanism is AWS_MSK_IAM and a file path is passed": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = flagext.StringSliceCSV{"localhost"}
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.SASL.Mechanism = SASLMechanismMSKIAM
				cfg.KafkaConfig.SASL.MSKIAM.FilePath = "/path/to/creds.json"
			},
		},
		"should succeed if SASL mechanism is AWS_MSK_IAM and an HTTP socket path is passed": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = flagext.StringSliceCSV{"localhost"}
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.SASL.Mechanism = SASLMechanismMSKIAM
				cfg.KafkaConfig.SASL.MSKIAM.HTTPSocketPath = "/tmp/aws.sock"
			},
		},
		"should fail if SASL mechanism is AWS_MSK_IAM with both static credentials and file path": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = flagext.StringSliceCSV{"localhost"}
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.SASL.Mechanism = SASLMechanismMSKIAM
				require.NoError(t, cfg.KafkaConfig.SASL.MSKIAM.Secret.AccessKey.Set("AKID"))
				require.NoError(t, cfg.KafkaConfig.SASL.MSKIAM.Secret.SecretKey.Set("secret"))
				cfg.KafkaConfig.SASL.MSKIAM.FilePath = "/path/to/creds.json"
			},
			expectedErr: ErrSASLMSKIAMBadConfig,
		},
		"should fail if SASL mechanism is OAUTHBEARER but no way to get the token is configured": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = flagext.StringSliceCSV{"localhost"}
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.SASL.Mechanism = SASLMechanismOauthbearer
			},
			expectedErr: ErrSASLOauthbearerBadConfig,
		},
		"should fail if SASL mechanism is OAUTHBEARER but no single way to get the token is configured": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = flagext.StringSliceCSV{"localhost"}
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.SASL.Mechanism = SASLMechanismOauthbearer
				require.NoError(t, cfg.KafkaConfig.SASL.Oauthbearer.Secret.Token.Set("foo"))
				cfg.KafkaConfig.SASL.Oauthbearer.FilePath = "bar"
			},
			expectedErr: ErrSASLOauthbearerBadConfig,
		},
		"should succeed if SASL mechanism is OAUTHBEARER and a token is passed": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = flagext.StringSliceCSV{"localhost"}
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.SASL.Mechanism = SASLMechanismOauthbearer
				require.NoError(t, cfg.KafkaConfig.SASL.Oauthbearer.Secret.Token.Set("foo"))
			},
		},
		"should succeed if SASL mechanism is OAUTHBEARER and a file path to the token is passed": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = flagext.StringSliceCSV{"localhost"}
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.SASL.Mechanism = SASLMechanismOauthbearer
				cfg.KafkaConfig.SASL.Oauthbearer.FilePath = "foo"
			},
		},
		"should succeed if SASL mechanism is OAUTHBEARER and an HTTP socket path is passed": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = flagext.StringSliceCSV{"localhost"}
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.SASL.Mechanism = SASLMechanismOauthbearer
				cfg.KafkaConfig.SASL.Oauthbearer.HTTPSocketPath = "/tmp/oauth.sock"
			},
		},
		"should fail if SASL mechanism is OAUTHBEARER with both file path and HTTP socket path": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = flagext.StringSliceCSV{"localhost"}
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.SASL.Mechanism = SASLMechanismOauthbearer
				cfg.KafkaConfig.SASL.Oauthbearer.FilePath = "foo"
				cfg.KafkaConfig.SASL.Oauthbearer.HTTPSocketPath = "/tmp/oauth.sock"
			},
			expectedErr: ErrSASLOauthbearerBadConfig,
		},
		"should fail if SASL mechanism is OAUTHBEARER with both token and HTTP socket path": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = flagext.StringSliceCSV{"localhost"}
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.SASL.Mechanism = SASLMechanismOauthbearer
				require.NoError(t, cfg.KafkaConfig.SASL.Oauthbearer.Secret.Token.Set("foo"))
				cfg.KafkaConfig.SASL.Oauthbearer.HTTPSocketPath = "/tmp/oauth.sock"
			},
			expectedErr: ErrSASLOauthbearerBadConfig,
		},
		"should fail if max ingestion concurrency is lower than 0": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = flagext.StringSliceCSV{"localhost"}
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.IngestionConcurrencyMax = -1
			},
			expectedErr: ErrInvalidIngestionConcurrencyMax,
		},
		"should pass if max ingestion concurrency is 0": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = flagext.StringSliceCSV{"localhost"}
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.IngestionConcurrencyMax = 0
			},
		},
		"should fail if ingestion concurrency batch size is lower than 0": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = flagext.StringSliceCSV{"localhost"}
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.IngestionConcurrencyMax = 5
				cfg.KafkaConfig.IngestionConcurrencyBatchSize = -1
			},
			expectedErr: ErrInvalidIngestionConcurrencyParams,
		},
		"should fail if ingestion concurrency queue capacity is lower than 0": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = flagext.StringSliceCSV{"localhost"}
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.IngestionConcurrencyMax = 5
				cfg.KafkaConfig.IngestionConcurrencyQueueCapacity = -1
			},
			expectedErr: ErrInvalidIngestionConcurrencyParams,
		},
		"should fail if ingestion concurrency estimates bytes per sample is lower than 0": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = flagext.StringSliceCSV{"localhost"}
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.IngestionConcurrencyMax = 5
				cfg.KafkaConfig.IngestionConcurrencyEstimatedBytesPerSample = -1
			},
			expectedErr: ErrInvalidIngestionConcurrencyParams,
		},
		"should fail if ingestion concurrency target flushes per shard is lower than 0": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = flagext.StringSliceCSV{"localhost"}
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.IngestionConcurrencyMax = 5
				cfg.KafkaConfig.IngestionConcurrencyTargetFlushesPerShard = -1
			},
			expectedErr: ErrInvalidIngestionConcurrencyParams,
		},
		"should fail when auto create topic default partitions is lower than 1": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = flagext.StringSliceCSV{"localhost"}
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.AutoCreateTopicEnabled = true
				cfg.KafkaConfig.AutoCreateTopicDefaultPartitions = -100
			},
			expectedErr: ErrInvalidAutoCreateTopicParams,
		},
		"should pass when auto create topic default partitions is -1 (using Kafka broker's default)": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = flagext.StringSliceCSV{"localhost"}
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.AutoCreateTopicEnabled = true
				cfg.KafkaConfig.AutoCreateTopicDefaultPartitions = -1
			},
		},
		"should fail if Kafka fetch max wait is less than 5s": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = flagext.StringSliceCSV{"localhost"}
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.FetchMaxWait = 2 * time.Second
			},
			expectedErr: ErrInvalidFetchMaxWait,
		},
		"should fail if Kafka fetch max wait is greater than 30s": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = flagext.StringSliceCSV{"localhost"}
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.FetchMaxWait = 32 * time.Second
			},
			expectedErr: ErrInvalidFetchMaxWait,
		},
		"should fail if fsync concurrency is not at least 1": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.WriteLogsFsyncBeforeKafkaCommitConcurrency = 0
			},

			expectedErr: ErrInvalidWriteLogsFsyncConcurrency,
		},
	}

	for _, mechanism := range []SASLMechanism{SASLMechanismScramSHA256, SASLMechanismScramSHA512} {
		for missing, setup := range map[string]func(*Config){
			"username":              func(cfg *Config) { require.NoError(t, cfg.KafkaConfig.SASL.Password.Set("supersecret")) },
			"password":              func(cfg *Config) { cfg.KafkaConfig.SASL.Username = "mimir" },
			"username and password": func(cfg *Config) {},
		} {
			tests[fmt.Sprintf("should fail if SASL %s is missing but mechanism is %s", missing, mechanism)] = testCase{
				setup: func(cfg *Config) {
					cfg.Enabled = true
					cfg.KafkaConfig.Address = flagext.StringSliceCSV{"localhost"}
					cfg.KafkaConfig.Topic = "test"
					cfg.KafkaConfig.SASL.Mechanism = mechanism
					setup(cfg)
				},
				expectedErr: ErrInconsistentSASLCredentials,
			}
		}
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			cfg := Config{}
			flagext.DefaultValues(&cfg)
			testData.setup(&cfg)

			assert.ErrorIs(t, cfg.Validate(), testData.expectedErr)
		})
	}
}

func TestConfig_GetConsumerGroup(t *testing.T) {
	tests := map[string]struct {
		consumerGroup string
		instanceID    string
		partitionID   int32
		expected      string
	}{
		"should return the instance ID if no consumer group is explicitly configured": {
			consumerGroup: "",
			instanceID:    "ingester-zone-a-1",
			partitionID:   1,
			expected:      "ingester-zone-a-1",
		},
		"should return the configured consumer group if set": {
			consumerGroup: "ingester-a",
			instanceID:    "ingester-zone-a-1",
			partitionID:   1,
			expected:      "ingester-a",
		},
		"should support <partition> placeholder in the consumer group": {
			consumerGroup: "ingester-zone-a-partition-<partition>",
			instanceID:    "ingester-zone-a-1",
			partitionID:   1,
			expected:      "ingester-zone-a-partition-1",
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			cfg := KafkaConfig{ConsumerGroup: testData.consumerGroup}
			assert.Equal(t, testData.expected, cfg.GetConsumerGroup(testData.instanceID, testData.partitionID))
		})
	}
}
func TestExhaustiveSASLMechanismOptions(t *testing.T) {
	for _, o := range saslMechanismOptions {
		require.NoError(t, new(SASLMechanism).Set(string(o)))
		require.NotErrorIs(t, (&KafkaAuthConfig{Mechanism: o}).Validate(), ErrInvalidSASLMechanism)
	}
}

func TestKafkaConfig_WriteCompartmentConfig(t *testing.T) {
	base := KafkaConfig{}
	flagext.DefaultValues(&base)
	base.Address = flagext.StringSliceCSV{"kafka-wc-<write-compartment-id>:9092"}
	base.Topic = "ingest-rc-<read-compartment-id>"
	base.SASL.Username = "user-wc-<write-compartment-id>"
	base.SASL.Password = flagext.SecretWithValue("pass-wc-<write-compartment-id>")

	for writeCompartmentID := 0; writeCompartmentID < 3; writeCompartmentID++ {
		cfg := base.WriteCompartmentConfig(writeCompartmentID)

		// The connection coordinates are resolved for the write compartment.
		assert.Equal(t, flagext.StringSliceCSV{fmt.Sprintf("kafka-wc-%d:9092", writeCompartmentID)}, cfg.Address)
		assert.Equal(t, fmt.Sprintf("user-wc-%d", writeCompartmentID), cfg.SASL.Username)
		assert.Equal(t, fmt.Sprintf("pass-wc-%d", writeCompartmentID), cfg.SASL.Password.String())
		// The topic is templated by read compartment, not write compartment, so it's left unchanged.
		assert.Equal(t, "ingest-rc-<read-compartment-id>", cfg.Topic)
		// Inherited defaults are preserved.
		assert.Equal(t, base.SASL.Mechanism, cfg.SASL.Mechanism)
	}
}

func TestWriteCompartmentConfigs(t *testing.T) {
	newBase := func() KafkaConfig {
		base := KafkaConfig{}
		flagext.DefaultValues(&base)
		base.Address = flagext.StringSliceCSV{"kafka-wc-<write-compartment-id>:9092"}
		base.Topic = "ingest-rc-<read-compartment-id>"
		base.SASL.Username = "user-wc-<write-compartment-id>"
		base.SASL.Password = flagext.SecretWithValue("pass-wc-<write-compartment-id>")
		base.FetchConcurrencyMax = 12
		base.MaxBufferedBytes = 1_000_000_000
		base.IngestionConcurrencyMax = 8
		return base
	}

	t.Run("resolves each cluster, applies the topic, and divides per-client budgets", func(t *testing.T) {
		const numCompartments = 4
		cfgs := WriteCompartmentConfigs(newBase(), numCompartments, "ingest-rc-2")
		require.Len(t, cfgs, numCompartments)

		for wc, cfg := range cfgs {
			assert.Equal(t, flagext.StringSliceCSV{fmt.Sprintf("kafka-wc-%d:9092", wc)}, cfg.Address)
			assert.Equal(t, fmt.Sprintf("user-wc-%d", wc), cfg.SASL.Username)
			assert.Equal(t, fmt.Sprintf("pass-wc-%d", wc), cfg.SASL.Password.String())
			// The topic is resolved for the given read compartment.
			assert.Equal(t, "ingest-rc-2", cfg.Topic)
			// The per-client budgets are the global value split across compartments, so peak
			// resource usage stays independent of the compartment count.
			assert.Equal(t, 12/numCompartments, cfg.FetchConcurrencyMax)
			assert.Equal(t, 1_000_000_000/numCompartments, cfg.MaxBufferedBytes)
			assert.Equal(t, 8/numCompartments, cfg.IngestionConcurrencyMax)
		}
	})

	t.Run("a single compartment keeps the full budget", func(t *testing.T) {
		cfgs := WriteCompartmentConfigs(newBase(), 1, "ingest-rc-0")
		require.Len(t, cfgs, 1)
		assert.Equal(t, 12, cfgs[0].FetchConcurrencyMax)
		assert.Equal(t, 1_000_000_000, cfgs[0].MaxBufferedBytes)
		assert.Equal(t, 8, cfgs[0].IngestionConcurrencyMax)
	})

	t.Run("a positive budget never collapses to zero", func(t *testing.T) {
		base := newBase()
		base.FetchConcurrencyMax = 3
		cfgs := WriteCompartmentConfigs(base, 8, "ingest-rc-0")
		require.Len(t, cfgs, 8)
		for _, cfg := range cfgs {
			assert.Equal(t, 1, cfg.FetchConcurrencyMax)
		}
	})

	t.Run("a disabled budget stays disabled", func(t *testing.T) {
		base := newBase()
		base.FetchConcurrencyMax = 0
		base.MaxBufferedBytes = 0
		base.IngestionConcurrencyMax = 0
		cfgs := WriteCompartmentConfigs(base, 4, "ingest-rc-0")
		require.Len(t, cfgs, 4)
		for _, cfg := range cfgs {
			assert.Zero(t, cfg.FetchConcurrencyMax)
			assert.Zero(t, cfg.MaxBufferedBytes)
			assert.Zero(t, cfg.IngestionConcurrencyMax)
		}
	})
}

func TestDivideBudget(t *testing.T) {
	tests := map[string]struct {
		budget          int
		numCompartments int
		expected        int
	}{
		"divides evenly":                          {budget: 12, numCompartments: 4, expected: 3},
		"floors to integer division":              {budget: 10, numCompartments: 3, expected: 3},
		"single compartment keeps full budget":    {budget: 12, numCompartments: 1, expected: 12},
		"zero compartments keep full budget":      {budget: 12, numCompartments: 0, expected: 12},
		"positive budget never collapses to zero": {budget: 3, numCompartments: 8, expected: 1},
		"disabled budget stays disabled":          {budget: 0, numCompartments: 4, expected: 0},
		"negative budget left untouched":          {budget: -1, numCompartments: 4, expected: -1},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, divideBudget(tc.budget, tc.numCompartments))
		})
	}
}

func TestKafkaConfig_ToWarpstreamClientOptions(t *testing.T) {
	baseConfig := func() KafkaConfig {
		return KafkaConfig{
			Backend:                                KafkaBackendWarpstream,
			Address:                                []string{"a:9092", "b:9092"},
			Topic:                                  "ingest",
			ClientID:                               "client-1",
			DialTimeout:                            3 * time.Second,
			WriteTimeout:                           7 * time.Second,
			WriteTimeoutOverhead:                   DefaultKafkaRequestTimeoutOverhead,
			WarpstreamHealthCheckSlowMultiplier:    1.5,
			WarpstreamHealthCheckMaxSlowFraction:   0.4,
			WarpstreamHealthCheckFaultyThreshold:   0.06,
			WarpstreamHealthCheckMaxFaultyFraction: 0.5,
			WarpstreamHedgeMinDelay:                15 * time.Millisecond,
			WarpstreamHedgeMaxAgents:               4,
			WarpstreamDemoterProbeInterval:         2 * time.Second,
		}
	}

	t.Run("maps warpstream-relevant fields onto the applied config", func(t *testing.T) {
		cfg := baseConfig()

		opts, err := cfg.ToWarpstreamClientOptions()
		require.NoError(t, err)

		// wgo.NewConfig applies the options on top of its defaults, so we can
		// assert the resulting config field by field.
		wsCfg := wgo.NewConfig(opts...)
		assert.Equal(t, []string{"a:9092", "b:9092"}, wsCfg.Address)
		assert.Equal(t, "ingest", wsCfg.Topic)
		assert.Equal(t, "client-1", wsCfg.ClientID)
		assert.Equal(t, 3*time.Second, wsCfg.DialTimeout)
		assert.Equal(t, 7*time.Second, wsCfg.WriteTimeout)
		// Linger, batch max bytes, and metadata refresh interval mirror the
		// kafka-backend defaults; ClusterStatsTTL is hardcoded to 1s.
		assert.Equal(t, defaultProducerLinger, wsCfg.Linger)
		assert.Equal(t, int32(producerBatchMaxBytes), wsCfg.BatchMaxBytes)
		assert.Equal(t, DefaultMetadataRefreshInterval, wsCfg.MetadataRefreshInterval)
		assert.Equal(t, time.Second, wsCfg.ClusterStatsTTL)
		assert.Equal(t, 1.5, wsCfg.HealthCheck.SlowMultiplier)
		assert.Equal(t, 0.4, wsCfg.HealthCheck.MaxSlowFraction)
		assert.Equal(t, 0.06, wsCfg.HealthCheck.FaultyThreshold)
		assert.Equal(t, 0.5, wsCfg.HealthCheck.MaxFaultyFraction)
		assert.Equal(t, 15*time.Millisecond, wsCfg.Hedger.MinHedgeDelay)
		assert.Equal(t, 4, wsCfg.Hedger.MaxHedgeAgents)
		assert.Equal(t, 2*time.Second, wsCfg.Demoter.ProbeInterval)
		// The per-attempt produce timeout plus its overhead must sum to
		// WriteTimeout so the whole hedge cascade fits within it.
		assert.Equal(t, 7*time.Second-DefaultKafkaRequestTimeoutOverhead, wsCfg.DirectProducer.ProduceRequestTimeout)
		assert.Equal(t, DefaultKafkaRequestTimeoutOverhead, wsCfg.DirectProducer.ProduceRequestTimeoutOverhead)
		assert.False(t, wsCfg.TLSEnabled)
		assert.Nil(t, wsCfg.TLSConfig)

		// The applied config must satisfy wgo's own validation.
		require.NoError(t, wsCfg.Validate())
	})

	t.Run("honors a custom write timeout overhead", func(t *testing.T) {
		cfg := baseConfig()
		cfg.WriteTimeoutOverhead = time.Second

		opts, err := cfg.ToWarpstreamClientOptions()
		require.NoError(t, err)

		wsCfg := wgo.NewConfig(opts...)
		assert.Equal(t, 7*time.Second-time.Second, wsCfg.DirectProducer.ProduceRequestTimeout)
		assert.Equal(t, time.Second, wsCfg.DirectProducer.ProduceRequestTimeoutOverhead)
		require.NoError(t, wsCfg.Validate())
	})

	t.Run("enables TLS on the applied config when configured", func(t *testing.T) {
		cfg := baseConfig()
		cfg.TLSEnabled = true
		cfg.TLS.InsecureSkipVerify = true

		opts, err := cfg.ToWarpstreamClientOptions()
		require.NoError(t, err)

		wsCfg := wgo.NewConfig(opts...)
		assert.True(t, wsCfg.TLSEnabled)
		assert.NotNil(t, wsCfg.TLSConfig)
	})

	t.Run("fails when TLS is enabled but misconfigured", func(t *testing.T) {
		cfg := baseConfig()
		cfg.TLSEnabled = true
		// A client cert without a key fails GetTLSConfig.
		cfg.TLS.CertPath = "/does/not/exist.crt"

		_, err := cfg.ToWarpstreamClientOptions()
		require.Error(t, err)
	})
}
