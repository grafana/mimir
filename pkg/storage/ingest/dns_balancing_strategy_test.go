// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"fmt"
	"math"
	"strconv"
	"testing"

	"github.com/grafana/gomemcache/memcache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDnsBalancingStrategyMod_PickServer(t *testing.T) {
	t.Run("should return error if there's no server", func(t *testing.T) {
		s := &dnsBalancingStrategyMod{}

		addr, err := s.PickServer("0")
		require.Equal(t, memcache.ErrNoServers, err)
		assert.Nil(t, addr)
	})

	t.Run("should always return the same server is there's only 1", func(t *testing.T) {
		s := &dnsBalancingStrategyMod{}
		require.NoError(t, s.SetServers("1.1.1.1:12345"))

		for partitionID := 0; partitionID < 1000; partitionID++ {
			addr, err := s.PickServer(strconv.Itoa(partitionID))

			require.NoError(t, err)
			require.NotNil(t, addr)
			assert.Equal(t, "1.1.1.1:12345", addr.String())
		}
	})

	t.Run("should balance partitions to servers when key is partition ID", func(t *testing.T) {
		tolerance := 0.05

		for _, numServers := range []int{2, 5, 10, 100} {
			t.Run(fmt.Sprintf("num servers: %d", numServers), func(t *testing.T) {
				// Generate the servers list.
				addrs := make([]string, 0, numServers)
				for i := 0; i < numServers; i++ {
					addrs = append(addrs, fmt.Sprintf("1.1.1.%d:12345", i))
				}

				s := &dnsBalancingStrategyMod{}
				require.NoError(t, s.SetServers(addrs...))

				// Keep track of partitions by server.
				partitionsByServer := map[string]int{}

				for partitionID := 0; partitionID < 1000; partitionID++ {
					addr, err := s.PickServer(strconv.Itoa(partitionID))

					require.NoError(t, err)
					require.NotNil(t, addr)
					partitionsByServer[addr.String()]++
				}

				// Expect an almost perfect balance.
				perfectBalance := 1000. / float64(numServers)
				allowedDelta := perfectBalance * tolerance
				for server, count := range partitionsByServer {
					assert.InDeltaf(t, perfectBalance, float64(count), allowedDelta, "server: %s", server)
				}
			})
		}
	})

	t.Run("should approximately balance partitions to servers when key is a generic string", func(t *testing.T) {
		tolerance := 0.25

		for _, numServers := range []int{2, 5, 10} {
			t.Run(fmt.Sprintf("num servers: %d", numServers), func(t *testing.T) {
				// Generate the servers list.
				addrs := make([]string, 0, numServers)
				for i := 0; i < numServers; i++ {
					addrs = append(addrs, fmt.Sprintf("1.1.1.%d:12345", i))
				}

				s := &dnsBalancingStrategyMod{}
				require.NoError(t, s.SetServers(addrs...))

				// Keep track of partitions by server.
				partitionsByServer := map[string]int{}

				for i := 0; i < 1000; i++ {
					addr, err := s.PickServer(fmt.Sprintf("string-%d", i))

					require.NoError(t, err)
					require.NotNil(t, addr)
					partitionsByServer[addr.String()]++
				}

				// Expect an almost perfect balance.
				perfectBalance := 1000. / float64(numServers)
				allowedDelta := math.Ceil(perfectBalance * tolerance)
				for server, count := range partitionsByServer {
					assert.InDeltaf(t, perfectBalance, float64(count), allowedDelta, "server: %s", server)
				}
			})
		}
	})
}
