// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package cache

import (
	"net"
	"sync"

	"github.com/cespare/xxhash/v2"
	"github.com/facette/natsort"
	"github.com/grafana/gomemcache/memcache"
)

// MemcachedJumpHashSelector implements the memcache.ServerSelector
// interface, utilizing a jump hash to distribute keys to servers.
//
// While adding or removing servers only requires 1/N keys to move,
// servers are treated as a stack and can only be pushed/popped.
// Therefore, MemcachedJumpHashSelector works best for servers
// with consistent DNS names where the naturally sorted order
// is predictable (ie. Kubernetes statefulsets).
type MemcachedJumpHashSelector struct {
	mu    sync.RWMutex
	addrs []net.Addr
}

// SetServers changes a MemcachedJumpHashSelector's set of servers at
// runtime and is safe for concurrent use by multiple goroutines.
//
// Each server is given equal weight. A server is given more weight
// if it's listed multiple times.
//
// SetServers returns an error if any of the server names fail to
// resolve. No attempt is made to connect to the server. If any
// error occurs, no changes are made to the internal server list.
//
// To minimize the number of rehashes for keys when scaling the
// number of servers in subsequent calls to SetServers, servers
// are stored in natural sort order.
func (s *MemcachedJumpHashSelector) SetServers(servers ...string) error {
	sortedServers := make([]string, len(servers))
	copy(sortedServers, servers)
	natsort.Sort(sortedServers)

	naddr, err := memcache.ResolveServers(sortedServers)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.addrs = naddr
	return nil
}

// PickServer returns the server address that a given item
// should be sharded onto.
func (s *MemcachedJumpHashSelector) PickServer(key string) (net.Addr, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// No need of a jump hash in case of 0 or 1 servers.
	if len(s.addrs) == 0 {
		return nil, memcache.ErrNoServers
	}
	if len(s.addrs) == 1 {
		return s.addrs[0], nil
	}

	// Pick a server using the jump hash.
	cs := xxhash.Sum64String(key)
	idx := jumpHash(cs, len(s.addrs))
	picked := s.addrs[idx]
	return picked, nil
}

// Each iterates over each server and calls the given function.
// If f returns a non-nil error, iteration will stop and that
// error will be returned.
func (s *MemcachedJumpHashSelector) Each(f func(net.Addr) error) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, a := range s.addrs {
		if err := f(a); nil != err {
			return err
		}
	}
	return nil
}
