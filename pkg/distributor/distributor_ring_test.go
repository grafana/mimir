// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/distributor/distributor_ring_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package distributor

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

	flagext.DefaultValues(&cfg)
	flagext.DefaultValues(&expected)

	// The default config of the distributor ring must be the exact same
	// of the default lifecycler config, except few options which are
	// intentionally overridden
	expected.ListenPort = cfg.ListenPort
	expected.RingConfig.ReplicationFactor = 1
	expected.NumTokens = 1
	expected.MinReadyDuration = 0
	expected.FinalSleep = 0

	assert.Equal(t, expected, cfg.ToLifecyclerConfig())
}

func TestRingConfig_CustomConfigToLifecyclerConfig(t *testing.T) {
	cfg := RingConfig{}
	expected := ring.LifecyclerConfig{}

	flagext.DefaultValues(&cfg)
	flagext.DefaultValues(&expected)

	// Customize the distributor ring config
	cfg.HeartbeatPeriod = 1 * time.Second
	cfg.HeartbeatTimeout = 10 * time.Second
	cfg.InstanceID = "test"
	cfg.InstanceInterfaceNames = []string{"abc1"}
	cfg.InstancePort = 10
	cfg.InstanceAddr = "1.2.3.4"
	cfg.ListenPort = 10

	// The lifecycler config should be generated based upon the distributor
	// ring config
	expected.HeartbeatPeriod = cfg.HeartbeatPeriod
	expected.RingConfig.HeartbeatTimeout = cfg.HeartbeatTimeout
	expected.ID = cfg.InstanceID
	expected.InfNames = cfg.InstanceInterfaceNames
	expected.Port = cfg.InstancePort
	expected.Addr = cfg.InstanceAddr
	expected.ListenPort = cfg.ListenPort

	// Hardcoded config
	expected.RingConfig.ReplicationFactor = 1
	expected.NumTokens = 1
	expected.MinReadyDuration = 0
	expected.FinalSleep = 0

	assert.Equal(t, expected, cfg.ToLifecyclerConfig())
}
