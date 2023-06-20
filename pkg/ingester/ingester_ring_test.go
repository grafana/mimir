// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/ring"
	"github.com/stretchr/testify/assert"
)

func TestRingConfig_DefaultConfigToLifecyclerConfig(t *testing.T) {
	cfg := RingConfig{}
	expected := ring.LifecyclerConfig{}
	flagext.DefaultValues(&cfg, &expected)

	// The default config of the compactor ring must be the exact same
	// of the default lifecycler config, except few options which are
	// intentionally overridden
	expected.ListenPort = cfg.ListenPort
	expected.RingConfig.ReplicationFactor = cfg.ReplicationFactor
	expected.RingConfig.SubringCacheDisabled = false
	expected.RingConfig.KVStore.Store = "memberlist"
	expected.NumTokens = cfg.NumTokens
	expected.MinReadyDuration = cfg.MinReadyDuration
	expected.FinalSleep = cfg.FinalSleep
	expected.ReadinessCheckRingHealth = false
	expected.HeartbeatPeriod = 15 * time.Second
	expected.RingTokenGenerator = ring.NewRandomTokenGenerator()

	assert.Equal(t, expected, cfg.ToLifecyclerConfig(log.NewNopLogger()))
}

func TestRingConfig_CustomConfigToLifecyclerConfig(t *testing.T) {
	cfg := RingConfig{}
	expected := ring.LifecyclerConfig{}
	flagext.DefaultValues(&cfg, &expected)

	cfg.HeartbeatPeriod = 1 * time.Second
	cfg.HeartbeatTimeout = 10 * time.Second
	cfg.ReplicationFactor = 10
	cfg.ZoneAwarenessEnabled = true
	cfg.ExcludedZones = []string{"zone-a", "zone-b"}
	cfg.TokensFilePath = "/tokens.file"
	cfg.NumTokens = 1234
	cfg.InstanceID = "instance-10"
	cfg.InstanceInterfaceNames = []string{"abc"}
	cfg.InstancePort = 1111
	cfg.InstanceAddr = "1.2.3.4"
	cfg.InstanceZone = "zone-c"
	cfg.UnregisterOnShutdown = true
	cfg.ObservePeriod = 10 * time.Minute
	cfg.MinReadyDuration = 3 * time.Minute
	cfg.FinalSleep = 2 * time.Minute
	cfg.ListenPort = 10
	cfg.TokenGeneratorStrategy = spreadMinimizingTokenGeneration
	cfg.SpreadMinimizingZones = []string{"zone-a", "zone-b", "zone-c"}

	// The lifecycler config should be generated based upon the ingester ring config
	expected.RingConfig.KVStore.Store = "memberlist"
	expected.RingConfig.HeartbeatTimeout = cfg.HeartbeatTimeout
	expected.RingConfig.ReplicationFactor = cfg.ReplicationFactor
	expected.RingConfig.ZoneAwarenessEnabled = cfg.ZoneAwarenessEnabled
	expected.RingConfig.ExcludedZones = cfg.ExcludedZones
	expected.RingConfig.SubringCacheDisabled = false // Hardcoded

	expected.NumTokens = cfg.NumTokens
	expected.HeartbeatPeriod = cfg.HeartbeatPeriod
	expected.HeartbeatTimeout = cfg.HeartbeatTimeout
	expected.ObservePeriod = cfg.ObservePeriod
	expected.JoinAfter = 0
	expected.MinReadyDuration = cfg.MinReadyDuration
	expected.InfNames = cfg.InstanceInterfaceNames
	expected.FinalSleep = cfg.FinalSleep
	expected.TokensFilePath = cfg.TokensFilePath
	expected.Zone = cfg.InstanceZone
	expected.UnregisterOnShutdown = cfg.UnregisterOnShutdown
	expected.ReadinessCheckRingHealth = false
	expected.Addr = cfg.InstanceAddr
	expected.Port = cfg.InstancePort
	expected.ID = cfg.InstanceID

	expected.ID = cfg.InstanceID
	expected.InfNames = cfg.InstanceInterfaceNames
	expected.Port = cfg.InstancePort
	expected.Addr = cfg.InstanceAddr
	expected.ListenPort = cfg.ListenPort

	logger := log.NewNopLogger()
	tokenGenerator, err := ring.NewSpreadMinimizingTokenGenerator(expected.ID, expected.Zone, cfg.SpreadMinimizingZones, logger)
	assert.NoError(t, err)
	expected.RingTokenGenerator = tokenGenerator

	assert.Equal(t, expected, cfg.ToLifecyclerConfig(logger))
}

func TestRingConfig_CustomTokenGenerator(t *testing.T) {
	instanceID := "instance-10"
	instanceZone := "zone-a"
	wrongZone := "zone-d"
	spreadMinimizingZones := []string{"zone-a", "zone-b", "zone-c"}

	tests := map[string]struct {
		zone                    string
		tokenGenerationStrategy string
		spreadMinimizingZones   []string
		expectedResultStrategy  string
	}{
		"spread-min-tokens and correct zones give a SpreadMinimizingTokenGenerator": {
			zone:                    instanceZone,
			tokenGenerationStrategy: spreadMinimizingTokenGeneration,
			spreadMinimizingZones:   spreadMinimizingZones,
			expectedResultStrategy:  spreadMinimizingTokenGeneration,
		},
		"spread-min-tokens and a wrong zone give a RandomTokenGenerator": {
			zone:                    wrongZone,
			tokenGenerationStrategy: spreadMinimizingTokenGeneration,
			spreadMinimizingZones:   spreadMinimizingZones,
			expectedResultStrategy:  randomTokenGeneration,
		},
		"random-tokens gives a RandomTokenGenerator": {
			zone:                    instanceZone,
			tokenGenerationStrategy: randomTokenGeneration,
			expectedResultStrategy:  randomTokenGeneration,
		},
		"unknown token generation strategy gives a RandomTokenGenerator": {
			zone:                    instanceZone,
			tokenGenerationStrategy: "bla-bla-tokens",
			expectedResultStrategy:  randomTokenGeneration,
		},
	}

	for _, testData := range tests {
		cfg := RingConfig{}
		cfg.InstanceID = instanceID
		cfg.InstanceZone = testData.zone
		cfg.TokenGeneratorStrategy = testData.tokenGenerationStrategy
		cfg.SpreadMinimizingZones = testData.spreadMinimizingZones
		lifecyclerConfig := cfg.ToLifecyclerConfig(log.NewNopLogger())
		if testData.expectedResultStrategy == randomTokenGeneration {
			tokenGenerator, ok := lifecyclerConfig.RingTokenGenerator.(*ring.RandomTokenGenerator)
			assert.True(t, ok)
			assert.NotNil(t, tokenGenerator)
		} else if testData.expectedResultStrategy == spreadMinimizingTokenGeneration {
			tokenGenerator, ok := lifecyclerConfig.RingTokenGenerator.(*ring.SpreadMinimizingTokenGenerator)
			assert.True(t, ok)
			assert.NotNil(t, tokenGenerator)
		} else {
			assert.Fail(t, "This case is not supported")
		}
	}
}
