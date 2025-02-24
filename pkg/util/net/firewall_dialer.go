// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/net/firewall_dialer.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package net

import (
	"context"
	"net"
	"syscall"

	"github.com/grafana/dskit/flagext"
	"github.com/pkg/errors"
)

var errBlockedAddress = errors.New("blocked address")
var errInvalidAddress = errors.New("invalid address")

type FirewallDialerConfigProvider interface {
	BlockCIDRNetworks() []flagext.CIDR
	BlockPrivateAddresses() bool
}

// FirewallDialer is a net dialer which integrates a firewall to block specific addresses.
type FirewallDialer struct {
	parent      *net.Dialer
	cfgProvider FirewallDialerConfigProvider
}

func NewFirewallDialer(cfgProvider FirewallDialerConfigProvider) *FirewallDialer {
	d := &FirewallDialer{cfgProvider: cfgProvider}
	d.parent = &net.Dialer{Control: d.control}
	return d
}

func (d *FirewallDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return d.parent.DialContext(ctx, network, address)
}

func (d *FirewallDialer) control(_, address string, _ syscall.RawConn) error {
	blockPrivateAddresses := d.cfgProvider.BlockPrivateAddresses()
	blockCIDRNetworks := d.cfgProvider.BlockCIDRNetworks()

	// Skip any control if no firewall has been configured.
	if !blockPrivateAddresses && len(blockCIDRNetworks) == 0 {
		return nil
	}

	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return errInvalidAddress
	}

	// We expect an IP as address because the DNS resolution already occurred.
	ip := net.ParseIP(host)
	if ip == nil {
		return errBlockedAddress
	}

	if blockPrivateAddresses && (ip.IsPrivate() || isLocal(ip)) {
		return errBlockedAddress
	}

	for _, cidr := range blockCIDRNetworks {
		if cidr.Value.Contains(ip) {
			return errBlockedAddress
		}
	}

	return nil
}

func isLocal(ip net.IP) bool {
	return ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsInterfaceLocalMulticast() || ip.IsUnspecified()
}
