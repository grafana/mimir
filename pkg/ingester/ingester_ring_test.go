// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"fmt"
	"testing"
	"time"

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

	// The default config of the ingester ring must be the exact same
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
	expected.RingTokenGenerator = nil

	result := cfg.ToLifecyclerConfig()
	assert.IsType(t, &ring.RandomTokenGenerator{}, result.RingTokenGenerator)
	result.RingTokenGenerator = nil

	assert.Equal(t, expected, result)
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
	cfg.TokenGenerationStrategy = tokenGenerationSpreadMinimizing
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

	tokenGenerator, err := ring.NewSpreadMinimizingTokenGenerator(expected.ID, expected.Zone, cfg.SpreadMinimizingZones, cfg.SpreadMinimizingJoinRingInOrder)
	require.NoError(t, err)
	expected.RingTokenGenerator = tokenGenerator

	assert.Equal(t, expected, cfg.ToLifecyclerConfig())
}

func TestRingConfig_Validate(t *testing.T) {
	tests := map[string]struct {
		instanceID                      string
		zone                            string
		tokenGenerationStrategy         string
		spreadMinimizingJoinRingInOrder bool
		spreadMinimizingZones           []string
		expectedError                   error
		tokensFilePath                  string
	}{
		"spread-minimizing and correct zones pass validation": {
			instanceID:              instanceID,
			zone:                    instanceZone,
			tokenGenerationStrategy: tokenGenerationSpreadMinimizing,
			spreadMinimizingZones:   spreadMinimizingZones,
		},
		"spread-minimizing and a wrong instance-id don't pass validation": {
			instanceID:              "broken-instance-48pgb", // InstanceID is expected to end up with a number.
			zone:                    instanceZone,
			tokenGenerationStrategy: tokenGenerationSpreadMinimizing,
			spreadMinimizingZones:   spreadMinimizingZones,
			expectedError:           fmt.Errorf("%s: unable to extract instance id", ErrSpreadMinimizingValidation),
		},
		"spread-minimizing and no spread-minimizing-zones zone don't pass validation": {
			instanceID:              instanceID,
			zone:                    instanceZone,
			tokenGenerationStrategy: tokenGenerationSpreadMinimizing,
			expectedError:           fmt.Errorf("number of zones 0 is not correct: it must be greater than 0 and less or equal than 8"),
		},
		"spread-minimizing and a wrong zone don't pass validation": {
			instanceID:              instanceID,
			zone:                    wrongZone,
			tokenGenerationStrategy: tokenGenerationSpreadMinimizing,
			spreadMinimizingZones:   spreadMinimizingZones,
			expectedError:           fmt.Errorf("zone %s is not valid", wrongZone),
		},
		"spread-minimizing and tokens-file-path set don't pass validation": {
			instanceID:              instanceID,
			zone:                    instanceZone,
			tokenGenerationStrategy: tokenGenerationSpreadMinimizing,
			spreadMinimizingZones:   spreadMinimizingZones,
			tokensFilePath:          "/path/tokens",
			expectedError:           fmt.Errorf("strategy requires %q to be empty", flagTokensFilePath),
		},
		"spread-minimizing and spread-minimizing-join-ring-in-order pass validation": {
			instanceID:                      instanceID,
			zone:                            instanceZone,
			tokenGenerationStrategy:         tokenGenerationSpreadMinimizing,
			spreadMinimizingZones:           spreadMinimizingZones,
			spreadMinimizingJoinRingInOrder: true,
		},
		"random passes validation": {
			instanceID:              instanceID,
			zone:                    instanceZone,
			tokenGenerationStrategy: tokenGenerationRandom,
		},
		"random and tokens-file-path set pass validation": {
			instanceID:              instanceID,
			zone:                    instanceZone,
			tokenGenerationStrategy: tokenGenerationRandom,
			tokensFilePath:          "/path/tokens",
		},
		"random and spread-minimizing-join-ring-in-order don't pass validation": {
			instanceID:                      instanceID,
			zone:                            instanceZone,
			tokenGenerationStrategy:         tokenGenerationRandom,
			spreadMinimizingJoinRingInOrder: true,
			expectedError:                   fmt.Errorf("%q must be false when using %q token generation strategy", flagSpreadMinimizingJoinRingInOrder, tokenGenerationRandom),
		},
		"unknown token generation doesn't pass validation": {
			instanceID:              instanceID,
			zone:                    instanceZone,
			tokenGenerationStrategy: "bla-bla-tokens",
			expectedError:           fmt.Errorf("unsupported token generation strategy (%q) has been chosen for %s", "bla-bla-tokens", flagTokenGenerationStrategy),
		},
	}

	for _, testData := range tests {
		cfg := RingConfig{}
		cfg.InstanceID = testData.instanceID
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
			require.ErrorContains(t, err, testData.expectedError.Error())
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
			tokenGenerationStrategy: tokenGenerationSpreadMinimizing,
			spreadMinimizingZones:   spreadMinimizingZones,
			expectedResultStrategy:  tokenGenerationSpreadMinimizing,
		},
		"random gives a RandomTokenGenerator": {
			zone:                    instanceZone,
			tokenGenerationStrategy: tokenGenerationRandom,
			expectedResultStrategy:  tokenGenerationRandom,
		},
	}

	for _, testData := range tests {
		cfg := RingConfig{}
		cfg.InstanceID = instanceID
		cfg.InstanceZone = testData.zone
		cfg.TokenGenerationStrategy = testData.tokenGenerationStrategy
		cfg.SpreadMinimizingZones = testData.spreadMinimizingZones
		lifecyclerConfig := cfg.ToLifecyclerConfig()
		if testData.expectedResultStrategy == tokenGenerationRandom {
			tokenGenerator, ok := lifecyclerConfig.RingTokenGenerator.(*ring.RandomTokenGenerator)
			assert.True(t, ok)
			assert.NotNil(t, tokenGenerator)
		} else if testData.expectedResultStrategy == tokenGenerationSpreadMinimizing {
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
			tokenGenerationStrategy:         tokenGenerationSpreadMinimizing,
			spreadMinimizingJoinRingInOrder: true,
			expectedCanJoinEnabled:          true,
		},
		"spread-minimizing without spread-minimizing-join-ring-in-order doesn't allow can join": {
			zone:                            instanceZone,
			tokenGenerationStrategy:         tokenGenerationSpreadMinimizing,
			spreadMinimizingJoinRingInOrder: false,
			expectedCanJoinEnabled:          false,
		},
		"random doesn't allow can join": {
			zone:                    instanceZone,
			tokenGenerationStrategy: tokenGenerationRandom,
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
		lifecyclerConfig := cfg.ToLifecyclerConfig()
		tokenGenerator := lifecyclerConfig.RingTokenGenerator
		assert.NotNil(t, tokenGenerator)
		assert.Equal(t, testData.spreadMinimizingJoinRingInOrder, tokenGenerator.CanJoinEnabled())
	}
}
