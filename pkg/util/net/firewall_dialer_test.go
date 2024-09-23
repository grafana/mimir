// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/net/firewall_dialer_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package net

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFirewallDialer(t *testing.T) {
	blockedCIDR := flagext.CIDR{}
	require.NoError(t, blockedCIDR.Set("172.217.168.64/28"))

	type testCase struct {
		address       string
		expectBlocked bool
	}

	tests := map[string]struct {
		cfg   FirewallDialerConfigProvider
		cases []testCase
	}{
		"should not block traffic with no block config": {
			cfg: firewallCfgProvider{},
			cases: []testCase{
				{"localhost", false},
				{"127.0.0.1", false},
				{"0.0.0.0", false},
				{"google.com", false},
				{"172.217.168.78", false},
			},
		},
		"should support blocking private addresses": {
			cfg: firewallCfgProvider{
				blockPrivateAddresses: true,
			},
			cases: []testCase{
				{"localhost", true},
				{"127.0.0.1", true},
				{"0.0.0.0", true},
				{"192.168.0.1", true},
				{"10.0.0.1", true},
				{"google.com", false},
				{"172.217.168.78", false},
				{"fdf8:f53b:82e4::53", true},        // Local
				{"fe80::200:5aee:feaa:20a2", true},  // Link-local
				{"ff01::2f3b:56a1:88e4:7c9d", true}, // Interface-local multicast address
				{"2001:4860:4860::8844", false},     // Google DNS
				{"::ffff:172.217.168.78", false},    // IPv6 mapped v4 non-private
				{"::ffff:192.168.0.1", true},        // IPv6 mapped v4 private
			},
		},
		"should support blocking custom CIDRs": {
			cfg: firewallCfgProvider{
				blockCIDRNetworks: []flagext.CIDR{blockedCIDR},
			},
			cases: []testCase{
				{"localhost", false},
				{"127.0.0.1", false},
				{"192.168.0.1", false},
				{"10.0.0.1", false},
				{"172.217.168.78", true},
				{"fdf8:f53b:82e4::53", false},       // Local
				{"fe80::200:5aee:feaa:20a2", false}, // Link-local
				{"2001:4860:4860::8844", false},     // Google DNS
				{"::ffff:10.0.0.1", false},          // IPv6 mapped v4 non-blocked
				{"::ffff:172.217.168.78", true},     // IPv6 mapped v4 blocked
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			d := NewFirewallDialer(testData.cfg)

			for _, tc := range testData.cases {
				t.Run(fmt.Sprintf("address: %s", tc.address), func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
					defer cancel()

					conn, err := d.DialContext(ctx, "tcp", fmt.Sprintf("[%s]:80", tc.address))
					if conn != nil {
						require.NoError(t, conn.Close())
					}

					if tc.expectBlocked {
						assert.Error(t, err, errBlockedAddress.Error())
						assert.Contains(t, err.Error(), errBlockedAddress.Error())
					} else {
						// We're fine either if succeeded or triggered a different error (eg. connection refused).
						assert.True(t, err == nil || !strings.Contains(err.Error(), errBlockedAddress.Error()))
					}
				})
			}
		})
	}
}

type firewallCfgProvider struct {
	blockCIDRNetworks     []flagext.CIDR
	blockPrivateAddresses bool
}

func (p firewallCfgProvider) BlockCIDRNetworks() []flagext.CIDR {
	return p.blockCIDRNetworks
}

func (p firewallCfgProvider) BlockPrivateAddresses() bool {
	return p.blockPrivateAddresses
}
