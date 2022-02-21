// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"testing"
	"time"

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

	assert.Equal(t, expected, cfg.ToLifecyclerConfig())
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
	cfg.InstanceID = "instance"
	cfg.InstanceInterfaceNames = []string{"abc"}
	cfg.InstancePort = 1111
	cfg.InstanceAddr = "1.2.3.4"
	cfg.InstanceZone = "zone-X"
	cfg.UnregisterOnShutdown = true
	cfg.ObservePeriod = 10 * time.Minute
	cfg.JoinAfter = 5 * time.Minute
	cfg.MinReadyDuration = 3 * time.Minute
	cfg.FinalSleep = 2 * time.Minute
	cfg.ReadinessCheckRingHealth = false
	cfg.ListenPort = 10

	// The lifecycler config should be generated based upon the ingester ring config
	expected.RingConfig.KVStore.Store = "memberlist"
	expected.RingConfig.HeartbeatTimeout = cfg.HeartbeatTimeout
	expected.RingConfig.ReplicationFactor = cfg.ReplicationFactor
	expected.RingConfig.ZoneAwarenessEnabled = cfg.ZoneAwarenessEnabled
	expected.RingConfig.ExcludedZones = cfg.ExcludedZones
	expected.RingConfig.SubringCacheDisabled = false // Hardcoded

	expected.NumTokens = cfg.NumTokens
	expected.HeartbeatPeriod = cfg.HeartbeatPeriod
	expected.ObservePeriod = cfg.ObservePeriod
	expected.JoinAfter = cfg.JoinAfter
	expected.MinReadyDuration = cfg.MinReadyDuration
	expected.InfNames = cfg.InstanceInterfaceNames
	expected.FinalSleep = cfg.FinalSleep
	expected.TokensFilePath = cfg.TokensFilePath
	expected.Zone = cfg.InstanceZone
	expected.UnregisterOnShutdown = cfg.UnregisterOnShutdown
	expected.ReadinessCheckRingHealth = cfg.ReadinessCheckRingHealth
	expected.Addr = cfg.InstanceAddr
	expected.Port = cfg.InstancePort
	expected.ID = cfg.InstanceID

	expected.ID = cfg.InstanceID
	expected.InfNames = cfg.InstanceInterfaceNames
	expected.Port = cfg.InstancePort
	expected.Addr = cfg.InstanceAddr
	expected.ListenPort = cfg.ListenPort

	assert.Equal(t, expected, cfg.ToLifecyclerConfig())
}
