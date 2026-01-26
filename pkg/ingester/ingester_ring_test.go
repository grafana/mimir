// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv/consul"
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

func TestRingConfig_DefaultConfig(t *testing.T) {
	cfg := RingConfig{}
	expected := ring.LifecyclerConfig{}
	flagext.DefaultValues(&cfg, &expected)

	t.Run("ToLifecyclerConfig()", func(t *testing.T) {
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
	})

	t.Run("ToTokenlessBasicLifecyclerConfig() should match ToLifecyclerConfig()", func(t *testing.T) {
		// Test that ToTokenlessBasicLifecyclerConfig() produces a config consistent with ToLifecyclerConfig().
		expectedCfg := cfg.ToLifecyclerConfig()

		// Create a classic Lifecycler from ToLifecyclerConfig to get the computed address.
		kvClient, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
		t.Cleanup(func() { assert.NoError(t, closer.Close()) })
		expectedCfg.RingConfig.KVStore.Mock = kvClient
		expectedLifecycler, err := ring.NewLifecycler(expectedCfg, nil, IngesterRingName, IngesterRingKey, false, log.NewNopLogger(), nil)
		require.NoError(t, err)

		// Assert that common fields are consistent between the two configs.
		basicLifecyclerCfg, err := cfg.ToTokenlessBasicLifecyclerConfig(log.NewNopLogger())
		require.NoError(t, err)

		assert.Equal(t, expectedLifecycler.ID, basicLifecyclerCfg.ID)
		assert.Equal(t, expectedLifecycler.Addr, basicLifecyclerCfg.Addr)
		assert.Equal(t, expectedLifecycler.Zone, basicLifecyclerCfg.Zone)
		assert.Equal(t, expectedCfg.HeartbeatPeriod, basicLifecyclerCfg.HeartbeatPeriod)
		assert.Equal(t, expectedCfg.HeartbeatTimeout, basicLifecyclerCfg.HeartbeatTimeout)
		assert.Equal(t, !expectedCfg.UnregisterOnShutdown, basicLifecyclerCfg.KeepInstanceInTheRingOnShutdown)
	})

	t.Run("ToTokenlessBasicLifecyclerConfig()", func(t *testing.T) {
		basicLifecyclerCfg, err := cfg.ToTokenlessBasicLifecyclerConfig(log.NewNopLogger())
		require.NoError(t, err)

		assert.Equal(t, 0, basicLifecyclerCfg.NumTokens)
		assert.Equal(t, time.Duration(0), basicLifecyclerCfg.TokensObservePeriod)
		assert.Equal(t, !cfg.UnregisterOnShutdown, basicLifecyclerCfg.KeepInstanceInTheRingOnShutdown)
	})
}

func TestRingConfig_CustomConfig(t *testing.T) {
	cfg := RingConfig{}
	expected := ring.LifecyclerConfig{}
	flagext.DefaultValues(&cfg, &expected)

	cfg.HeartbeatPeriod = 1 * time.Second
	cfg.HeartbeatTimeout = 10 * time.Second
	cfg.ReplicationFactor = 10
	cfg.ZoneAwarenessEnabled = true
	cfg.ExcludedZones = []string{"zone-a", "zone-b"}
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
	require.NoError(t, cfg.Validate())

	t.Run("ToLifecyclerConfig()", func(t *testing.T) {
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
	})

	t.Run("ToTokenlessBasicLifecyclerConfig() should match ToLifecyclerConfig()", func(t *testing.T) {
		// Test that ToTokenlessBasicLifecyclerConfig() produces a config consistent with ToLifecyclerConfig().
		expectedCfg := cfg.ToLifecyclerConfig()

		// Create a classic Lifecycler from ToLifecyclerConfig to get the computed address.
		kvClient, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
		t.Cleanup(func() { assert.NoError(t, closer.Close()) })
		expectedCfg.RingConfig.KVStore.Mock = kvClient
		expectedLifecycler, err := ring.NewLifecycler(expectedCfg, nil, IngesterRingName, IngesterRingKey, false, log.NewNopLogger(), nil)
		require.NoError(t, err)

		// Assert that common fields are consistent between the two configs.
		basicLifecyclerCfg, err := cfg.ToTokenlessBasicLifecyclerConfig(log.NewNopLogger())
		require.NoError(t, err)

		assert.Equal(t, expectedLifecycler.ID, basicLifecyclerCfg.ID)
		assert.Equal(t, expectedLifecycler.Addr, basicLifecyclerCfg.Addr)
		assert.Equal(t, expectedLifecycler.Zone, basicLifecyclerCfg.Zone)
		assert.Equal(t, expectedCfg.HeartbeatPeriod, basicLifecyclerCfg.HeartbeatPeriod)
		assert.Equal(t, expectedCfg.HeartbeatTimeout, basicLifecyclerCfg.HeartbeatTimeout)
		assert.Equal(t, !expectedCfg.UnregisterOnShutdown, basicLifecyclerCfg.KeepInstanceInTheRingOnShutdown)
	})

	t.Run("ToTokenlessBasicLifecyclerConfig()", func(t *testing.T) {
		basicLifecyclerCfg, err := cfg.ToTokenlessBasicLifecyclerConfig(log.NewNopLogger())
		require.NoError(t, err)

		assert.Equal(t, 0, basicLifecyclerCfg.NumTokens)
		assert.Equal(t, time.Duration(0), basicLifecyclerCfg.TokensObservePeriod)
		assert.Equal(t, !cfg.UnregisterOnShutdown, basicLifecyclerCfg.KeepInstanceInTheRingOnShutdown)
	})
}

func TestRingConfig_Validate(t *testing.T) {
	tests := map[string]struct {
		numTokens                       int
		instanceID                      string
		zone                            string
		tokenGenerationStrategy         string
		spreadMinimizingJoinRingInOrder bool
		spreadMinimizingZones           []string
		expectedError                   error
		tokensFilePath                  string
	}{
		"spread-minimizing and correct zones pass validation": {
			numTokens:               128,
			instanceID:              instanceID,
			zone:                    instanceZone,
			tokenGenerationStrategy: tokenGenerationSpreadMinimizing,
			spreadMinimizingZones:   spreadMinimizingZones,
		},
		"spread-minimizing and a wrong instance-id don't pass validation": {
			numTokens:               128,
			instanceID:              "broken-instance-48pgb", // InstanceID is expected to end up with a number.
			zone:                    instanceZone,
			tokenGenerationStrategy: tokenGenerationSpreadMinimizing,
			spreadMinimizingZones:   spreadMinimizingZones,
			expectedError:           fmt.Errorf("%s: unable to extract instance id", ErrSpreadMinimizingValidation),
		},
		"spread-minimizing and no spread-minimizing-zones zone don't pass validation": {
			numTokens:               128,
			instanceID:              instanceID,
			zone:                    instanceZone,
			tokenGenerationStrategy: tokenGenerationSpreadMinimizing,
			expectedError:           fmt.Errorf("number of zones 0 is not correct: it must be greater than 0 and less or equal than 8"),
		},
		"spread-minimizing and a wrong zone don't pass validation": {
			numTokens:               128,
			instanceID:              instanceID,
			zone:                    wrongZone,
			tokenGenerationStrategy: tokenGenerationSpreadMinimizing,
			spreadMinimizingZones:   spreadMinimizingZones,
			expectedError:           fmt.Errorf("zone %s is not valid", wrongZone),
		},
		"spread-minimizing and tokens-file-path set don't pass validation": {
			numTokens:               128,
			instanceID:              instanceID,
			zone:                    instanceZone,
			tokenGenerationStrategy: tokenGenerationSpreadMinimizing,
			spreadMinimizingZones:   spreadMinimizingZones,
			tokensFilePath:          "/path/tokens",
			expectedError:           fmt.Errorf("strategy requires %q to be empty", flagTokensFilePath),
		},
		"spread-minimizing and spread-minimizing-join-ring-in-order pass validation": {
			numTokens:                       128,
			instanceID:                      instanceID,
			zone:                            instanceZone,
			tokenGenerationStrategy:         tokenGenerationSpreadMinimizing,
			spreadMinimizingZones:           spreadMinimizingZones,
			spreadMinimizingJoinRingInOrder: true,
		},
		"random passes validation": {
			numTokens:               128,
			instanceID:              instanceID,
			zone:                    instanceZone,
			tokenGenerationStrategy: tokenGenerationRandom,
		},
		"random and tokens-file-path set pass validation": {
			numTokens:               128,
			instanceID:              instanceID,
			zone:                    instanceZone,
			tokenGenerationStrategy: tokenGenerationRandom,
			tokensFilePath:          "/path/tokens",
		},
		"random and spread-minimizing-join-ring-in-order don't pass validation": {
			numTokens:                       128,
			instanceID:                      instanceID,
			zone:                            instanceZone,
			tokenGenerationStrategy:         tokenGenerationRandom,
			spreadMinimizingJoinRingInOrder: true,
			expectedError:                   fmt.Errorf("%q must be false when using %q token generation strategy", flagSpreadMinimizingJoinRingInOrder, tokenGenerationRandom),
		},
		"unknown token generation doesn't pass validation": {
			numTokens:               128,
			instanceID:              instanceID,
			zone:                    instanceZone,
			tokenGenerationStrategy: "bla-bla-tokens",
			expectedError:           fmt.Errorf("unsupported token generation strategy (%q) has been chosen for %s", "bla-bla-tokens", flagTokenGenerationStrategy),
		},
		"tokenless mode passes validation": {
			numTokens:               0,
			instanceID:              instanceID,
			zone:                    instanceZone,
			tokenGenerationStrategy: tokenGenerationRandom,
		},
		"tokenless mode with tokens-file-path set doesn't pass validation": {
			numTokens:               0,
			instanceID:              instanceID,
			zone:                    instanceZone,
			tokenGenerationStrategy: tokenGenerationRandom,
			tokensFilePath:          "/path/tokens",
			expectedError:           fmt.Errorf("tokens file path must be empty when ring tokens are disabled"),
		},
		"tokenless mode with spread-minimizing strategy doesn't pass validation": {
			numTokens:               0,
			instanceID:              instanceID,
			zone:                    instanceZone,
			tokenGenerationStrategy: tokenGenerationSpreadMinimizing,
			spreadMinimizingZones:   spreadMinimizingZones,
			expectedError:           fmt.Errorf("spread minimizing token generation strategy is not supported when ring tokens are disabled"),
		},
	}

	for _, testData := range tests {
		cfg := RingConfig{}
		cfg.NumTokens = testData.numTokens
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
		switch testData.expectedResultStrategy {
		case tokenGenerationRandom:
			tokenGenerator, ok := lifecyclerConfig.RingTokenGenerator.(*ring.RandomTokenGenerator)
			assert.True(t, ok)
			assert.NotNil(t, tokenGenerator)
		case tokenGenerationSpreadMinimizing:
			tokenGenerator, ok := lifecyclerConfig.RingTokenGenerator.(*ring.SpreadMinimizingTokenGenerator)
			assert.True(t, ok)
			assert.NotNil(t, tokenGenerator)
		default:
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
