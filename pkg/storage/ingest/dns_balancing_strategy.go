// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"net"
	"strconv"
	"sync"

	"github.com/cespare/xxhash"
	"github.com/facette/natsort"
	"github.com/grafana/gomemcache/memcache"
)

var (
	addrsPool = sync.Pool{
		New: func() interface{} {
			addrs := make([]net.Addr, 0, 64)
			return &addrs
		},
	}
)

type dnsBalancingStrategyMod struct {
	servers memcache.ServerList
}

func (s *dnsBalancingStrategyMod) SetServers(servers ...string) error {
	sortedServers := make([]string, len(servers))
	copy(sortedServers, servers)
	natsort.Sort(sortedServers)

	return s.servers.SetServers(sortedServers...)
}

func (s *dnsBalancingStrategyMod) PickServer(key string) (net.Addr, error) {
	// Unfortunately we can't read the list of server addresses from
	// the original implementation, so we use Each() to fetch all of them.
	addrs := *(addrsPool.Get().(*[]net.Addr))
	err := s.servers.Each(func(addr net.Addr) error {
		addrs = append(addrs, addr)
		return nil
	})
	if err != nil {
		return nil, err
	}

	// No need of a mod in case of 0 or 1 servers.
	if len(addrs) == 0 {
		addrs = (addrs)[:0]
		addrsPool.Put(&addrs)
		return nil, memcache.ErrNoServers
	}
	if len(addrs) == 1 {
		picked := addrs[0]

		addrs = (addrs)[:0]
		addrsPool.Put(&addrs)

		return picked, nil
	}

	// If the key can be parsed as a number (e.g. partition ID) than we can just use it.
	var hash uint64
	if parsed, err := strconv.Atoi(key); err == nil {
		hash = uint64(parsed)
	} else {
		hash = xxhash.Sum64String(key)
	}

	// Pick a server using the mod.
	idx := hash % uint64(len(addrs))
	picked := (addrs)[idx]

	addrs = (addrs)[:0]
	addrsPool.Put(&addrs)

	return picked, nil
}
