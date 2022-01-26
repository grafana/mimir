// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/net.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package util

import (
	"fmt"
	"net"
	"strings"

	"github.com/go-kit/log/level"

	util_log "github.com/grafana/mimir/pkg/util/log"
)

// GetFirstAddressOf returns the first IPv4 address of the supplied interface names, omitting any 169.254.x.x automatic private IPs if possible.
func GetFirstAddressOf(names []string) (string, error) {
	var ipAddr string
	for _, name := range names {
		inf, err := net.InterfaceByName(name)
		if err != nil {
			level.Warn(util_log.Logger).Log("msg", "error getting interface", "inf", name, "err", err)
			continue
		}
		addrs, err := inf.Addrs()
		if err != nil {
			level.Warn(util_log.Logger).Log("msg", "error getting addresses for interface", "inf", name, "err", err)
			continue
		}
		if len(addrs) <= 0 {
			level.Warn(util_log.Logger).Log("msg", "no addresses found for interface", "inf", name, "err", err)
			continue
		}
		if ip := filterIPs(addrs); ip != "" {
			ipAddr = ip
		}
		if strings.HasPrefix(ipAddr, `169.254.`) || ipAddr == "" {
			continue
		}
		return ipAddr, nil
	}
	if ipAddr == "" {
		return "", fmt.Errorf("No address found for %s", names)
	}
	if strings.HasPrefix(ipAddr, `169.254.`) {
		level.Warn(util_log.Logger).Log("msg", "using automatic private ip", "address", ipAddr)
	}
	return ipAddr, nil
}

// filterIPs attempts to return the first non automatic private IP (APIPA / 169.254.x.x) if possible, only returning APIPA if available and no other valid IP is found.
func filterIPs(addrs []net.Addr) string {
	var ipAddr string
	for _, addr := range addrs {
		if v, ok := addr.(*net.IPNet); ok {
			if ip := v.IP.To4(); ip != nil {
				ipAddr = v.IP.String()
				if !strings.HasPrefix(ipAddr, `169.254.`) {
					return ipAddr
				}
			}
		}
	}
	return ipAddr
}

// Parses network interfaces and returns those having an address conformant to RFC1918
func PrivateNetworkInterfaces() []string {
	var privInts []string
	ints, err := net.Interfaces()
	if err != nil {
		level.Warn(util_log.Logger).Log("msg", "error getting interfaces", "err", err)
	}
	for _, i := range ints {
		fmt.Println(i.Index, i.Name)
		addrs, err := i.Addrs()
		if err != nil {
			level.Warn(util_log.Logger).Log("msg", "error getting addresses", "inf", i.Name, "err", err)
		}
		for _, a := range addrs {
			s := a.String()
			ip, _, err := net.ParseCIDR(s)
			if err != nil {
				level.Warn(util_log.Logger).Log("msg", "error getting ip address", "inf", i.Name, "addr", s, "err", err)
			}
			if ip.IsPrivate() {
				privInts = append(privInts, i.Name)
				break
			}
		}
	}
	if len(privInts) == 0 {
		level.Warn(util_log.Logger).Log("msg", "couldn't find any interfaces on private networks, defaulting to [eth0 en0]")
		return []string{"eth0", "en0"}
	}
	level.Info(util_log.Logger).Log("msg", "found interfaces on private networks:", "["+strings.Join(privInts, ", ")+"]")
	return privInts
}
