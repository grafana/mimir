// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/ring"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	instanceID   = "instance-10"
	instanceZone = "zone-a"
	wrongZone    = "zone-d"
)

var (
	spreadMinimizingZones = []string{"zone-a", "zone-b", "zone-c"}
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
	cfg.TokenGenerationStrategy = spreadMinimizingTokenGeneration
	cfg.SpreadMinimizingZones = []string{"zone-a", "zone-b", "zone-c"}
	cfg.SpreadMinimizingJoinRingInOrder = true

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
	tokenGenerator, err := ring.NewSpreadMinimizingTokenGenerator(expected.ID, expected.Zone, cfg.SpreadMinimizingZones, cfg.SpreadMinimizingJoinRingInOrder, logger)
	require.NoError(t, err)
	expected.RingTokenGenerator = tokenGenerator

	assert.Equal(t, expected, cfg.ToLifecyclerConfig(logger))
}

func TestRingConfig_Validate(t *testing.T) {
	tests := map[string]struct {
		zone                            string
		tokenGenerationStrategy         string
		spreadMinimizingJoinRingInOrder bool
		spreadMinimizingZones           []string
		expectedError                   error
		tokensFilePath                  string
	}{
		"spread-minimizing and correct zones pass validation": {
			zone:                    instanceZone,
			tokenGenerationStrategy: spreadMinimizingTokenGeneration,
			spreadMinimizingZones:   spreadMinimizingZones,
		},
		"spread-minimizing and no spread-minimizing-zones zone don't pass validation": {
			zone:                    instanceZone,
			tokenGenerationStrategy: spreadMinimizingTokenGeneration,
			expectedError:           fmt.Errorf("number of zones 0 is not correct: it must be greater than 0 and less or equal than 8"),
		},
		"spread-minimizing and a wrong zone don't pass validation": {
			zone:                    wrongZone,
			tokenGenerationStrategy: spreadMinimizingTokenGeneration,
			spreadMinimizingZones:   spreadMinimizingZones,
			expectedError:           fmt.Errorf("zone %s is not valid", wrongZone),
		},
		"spread-minimizing and tokens-file-path set don't pass validation": {
			zone:                    instanceZone,
			tokenGenerationStrategy: spreadMinimizingTokenGeneration,
			spreadMinimizingZones:   spreadMinimizingZones,
			tokensFilePath:          "/path/tokens",
			expectedError:           fmt.Errorf("%q token generation strategy requires %q to be empty", spreadMinimizingTokenGeneration, tokensFilePathFlag),
		},
		"spread-minimizing and spread-minimizing-join-ring-in-order pass validation": {
			zone:                            instanceZone,
			tokenGenerationStrategy:         spreadMinimizingTokenGeneration,
			spreadMinimizingZones:           spreadMinimizingZones,
			spreadMinimizingJoinRingInOrder: true,
		},
		"random passes validation": {
			zone:                    instanceZone,
			tokenGenerationStrategy: randomTokenGeneration,
		},
		"random and tokens-file-path set pass validation": {
			zone:                    instanceZone,
			tokenGenerationStrategy: randomTokenGeneration,
			tokensFilePath:          "/path/tokens",
		},
		"random and spread-minimizing-join-ring-in-order don't pass validation": {
			zone:                            instanceZone,
			tokenGenerationStrategy:         randomTokenGeneration,
			spreadMinimizingJoinRingInOrder: true,
			expectedError:                   fmt.Errorf("%q must be false when using %q token generation strategy", spreadMinimizingJoinRingInOrderFlag, randomTokenGeneration),
		},
		"unknown token generation doesn't pass validation": {
			zone:                    instanceZone,
			tokenGenerationStrategy: "bla-bla-tokens",
			expectedError:           fmt.Errorf("unsupported token generation strategy (%q) has been chosen for %s", "bla-bla-tokens", tokenGenerationStrategyFlag),
		},
	}

	for _, testData := range tests {
		cfg := RingConfig{}
		cfg.InstanceID = instanceID
		cfg.InstanceZone = testData.zone
		cfg.TokenGenerationStrategy = testData.tokenGenerationStrategy
		cfg.SpreadMinimizingJoinRingInOrder = testData.spreadMinimizingJoinRingInOrder
		cfg.SpreadMinimizingZones = testData.spreadMinimizingZones
		cfg.TokensFilePath = testData.tokensFilePath
		err := cfg.Validate()
		if testData.expectedError == nil {
			require.NoError(t, err)
		} else {
			require.Error(t, err)
			require.Equal(t, testData.expectedError, err)
		}
	}
}

func TestRingConfig_CustomTokenGenerator(t *testing.T) {
	tests := map[string]struct {
		zone                    string
		tokenGenerationStrategy string
		spreadMinimizingZones   []string
		expectedResultStrategy  string
	}{
		"spread-minimizing and correct zones give a SpreadMinimizingTokenGenerator": {
			zone:                    instanceZone,
			tokenGenerationStrategy: spreadMinimizingTokenGeneration,
			spreadMinimizingZones:   spreadMinimizingZones,
			expectedResultStrategy:  spreadMinimizingTokenGeneration,
		},
		"random gives a RandomTokenGenerator": {
			zone:                    instanceZone,
			tokenGenerationStrategy: randomTokenGeneration,
			expectedResultStrategy:  randomTokenGeneration,
		},
	}

	for _, testData := range tests {
		cfg := RingConfig{}
		cfg.InstanceID = instanceID
		cfg.InstanceZone = testData.zone
		cfg.TokenGenerationStrategy = testData.tokenGenerationStrategy
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
			assert.Nil(t, lifecyclerConfig.RingTokenGenerator)
		}
	}
}

func TestRingConfig_SpreadMinimizingJoinRingInOrder(t *testing.T) {
	tests := map[string]struct {
		zone                            string
		tokenGenerationStrategy         string
		spreadMinimizingJoinRingInOrder bool
		expectedCanJoinEnabled          bool
	}{
		"spread-minimizing and spread-minimizing-join-ring-in-order allow can join": {
			zone:                            instanceZone,
			tokenGenerationStrategy:         spreadMinimizingTokenGeneration,
			spreadMinimizingJoinRingInOrder: true,
			expectedCanJoinEnabled:          true,
		},
		"spread-minimizing without spread-minimizing-join-ring-in-order doesn't allow can join": {
			zone:                            instanceZone,
			tokenGenerationStrategy:         spreadMinimizingTokenGeneration,
			spreadMinimizingJoinRingInOrder: false,
			expectedCanJoinEnabled:          false,
		},
		"random doesn't allow can join": {
			zone:                    instanceZone,
			tokenGenerationStrategy: randomTokenGeneration,
			expectedCanJoinEnabled:  false,
		},
	}

	for _, testData := range tests {
		cfg := RingConfig{}
		cfg.InstanceID = instanceID
		cfg.InstanceZone = instanceZone
		cfg.TokenGenerationStrategy = testData.tokenGenerationStrategy
		cfg.SpreadMinimizingJoinRingInOrder = testData.spreadMinimizingJoinRingInOrder
		cfg.SpreadMinimizingZones = spreadMinimizingZones
		lifecyclerConfig := cfg.ToLifecyclerConfig(log.NewNopLogger())
		tokenGenerator := lifecyclerConfig.RingTokenGenerator
		assert.NotNil(t, tokenGenerator)
		assert.Equal(t, testData.spreadMinimizingJoinRingInOrder, tokenGenerator.CanJoinEnabled())
	}
}
