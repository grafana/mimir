// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storegateway/gateway_ring_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package storegateway

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-kit/log"
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

func TestIsHealthyForStoreGatewayOperations(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		instance          *ring.InstanceDesc
		timeout           time.Duration
		ownerSyncExpected bool
		ownerReadExpected bool
		readExpected      bool
	}{
		"ACTIVE instance with last keepalive newer than timeout": {
			instance:          &ring.InstanceDesc{State: ring.ACTIVE, Timestamp: time.Now().Add(-30 * time.Second).Unix()},
			timeout:           time.Minute,
			ownerSyncExpected: true,
			ownerReadExpected: true,
			readExpected:      true,
		},
		"ACTIVE instance with last keepalive older than timeout": {
			instance:          &ring.InstanceDesc{State: ring.ACTIVE, Timestamp: time.Now().Add(-90 * time.Second).Unix()},
			timeout:           time.Minute,
			ownerSyncExpected: false,
			ownerReadExpected: false,
			readExpected:      false,
		},
		"JOINING instance with last keepalive newer than timeout": {
			instance:          &ring.InstanceDesc{State: ring.JOINING, Timestamp: time.Now().Add(-30 * time.Second).Unix()},
			timeout:           time.Minute,
			ownerSyncExpected: true,
			ownerReadExpected: false,
			readExpected:      false,
		},
		"LEAVING instance with last keepalive newer than timeout": {
			instance:          &ring.InstanceDesc{State: ring.LEAVING, Timestamp: time.Now().Add(-30 * time.Second).Unix()},
			timeout:           time.Minute,
			ownerSyncExpected: true,
			ownerReadExpected: false,
			readExpected:      false,
		},
		"PENDING instance with last keepalive newer than timeout": {
			instance:          &ring.InstanceDesc{State: ring.PENDING, Timestamp: time.Now().Add(-30 * time.Second).Unix()},
			timeout:           time.Minute,
			ownerSyncExpected: false,
			ownerReadExpected: false,
			readExpected:      false,
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			actual := testData.instance.IsHealthy(BlocksOwnerSync, testData.timeout, time.Now())
			assert.Equal(t, testData.ownerSyncExpected, actual)

			actual = testData.instance.IsHealthy(BlocksOwnerRead, testData.timeout, time.Now())
			assert.Equal(t, testData.ownerReadExpected, actual)

			actual = testData.instance.IsHealthy(BlocksRead, testData.timeout, time.Now())
			assert.Equal(t, testData.readExpected, actual)
		})
	}
}

func TestUnregisterOnShutdownFlag(t *testing.T) {
	logger := log.NewNopLogger()
	{
		cfg := RingConfig{UnregisterOnShutdown: true, InstanceAddr: "test"}
		lcCfg, err := cfg.ToLifecyclerConfig(logger)
		require.NoError(t, err)
		assert.False(t, lcCfg.KeepInstanceInTheRingOnShutdown)
	}

	{
		cfg := RingConfig{UnregisterOnShutdown: false, InstanceAddr: "test"}
		lcCfg, err := cfg.ToLifecyclerConfig(logger)
		require.NoError(t, err)
		assert.True(t, lcCfg.KeepInstanceInTheRingOnShutdown)
	}
}

func TestRingConfig_Validate(t *testing.T) {
	tests := map[string]struct {
		zone                            string
		tokenGenerationStrategy         string
		spreadMinimizingJoinRingInOrder bool
		spreadMinimizingZones           []string
		expectedError                   error
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
		lifecyclerConfig, err := cfg.ToLifecyclerConfig(log.NewNopLogger())
		require.NoError(t, err)
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
		lifecyclerConfig, err := cfg.ToLifecyclerConfig(log.NewNopLogger())
		require.NoError(t, err)
		tokenGenerator := lifecyclerConfig.RingTokenGenerator
		assert.NotNil(t, tokenGenerator)
		assert.Equal(t, testData.spreadMinimizingJoinRingInOrder, tokenGenerator.CanJoinEnabled())
	}
}
