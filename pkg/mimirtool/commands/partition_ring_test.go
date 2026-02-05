// SPDX-License-Identifier: AGPL-3.0-only

package commands

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/dns"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv/codec"
	"github.com/grafana/dskit/kv/memberlist"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester"
)

func TestAddPartitionCommand(t *testing.T) {
	t.Run("successfully adds a new partition", func(t *testing.T) {
		ctx := context.Background()

		// Start a seed memberlist node.
		seedKV, seedClient := startMemberlistKV(t)

		// Get the seed node's listening port.
		seedAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(seedKV.GetListeningPort()))

		// Create the AddPartitionCommand.
		cmd := &AddPartitionCommand{
			memberlistJoin:     []string{seedAddr},
			memberlistBindPort: 0, // random port
			partitionIDs:       "0",
			partitionState:     "active",
			skipConfirmation:   true,
			logger:             log.NewNopLogger(),
		}

		// Run the add partition command.
		err := cmd.run()
		require.NoError(t, err)

		// Verify the partition was added by reading from the seed KV.
		// Use polling because memberlist is eventually consistent.
		require.Eventually(t, func() bool {
			val, err := seedClient.Get(ctx, ingester.PartitionRingKey)
			if err != nil || val == nil {
				return false
			}
			ringDesc, ok := val.(*ring.PartitionRingDesc)
			if !ok {
				return false
			}
			if !ringDesc.HasPartition(0) {
				return false
			}
			partition := ringDesc.Partitions[0]
			return partition.State == ring.PartitionActive && len(partition.Tokens) > 0
		}, 5*time.Second, 100*time.Millisecond, "partition 0 should be added with active state and tokens")
	})

	t.Run("fails if partition already exists", func(t *testing.T) {
		ctx := context.Background()

		// Start a seed memberlist node.
		seedKV, seedClient := startMemberlistKV(t)

		// Pre-create partition 0.
		err := seedClient.CAS(ctx, ingester.PartitionRingKey, func(in interface{}) (out interface{}, retry bool, err error) {
			ringDesc := ring.GetOrCreatePartitionRingDesc(in)
			ringDesc.AddPartition(0, ring.PartitionActive, time.Now())
			return ringDesc, true, nil
		})
		require.NoError(t, err)

		// Get the seed node's listening port.
		seedAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(seedKV.GetListeningPort()))

		// Try to add partition 0 again - should fail.
		cmd := &AddPartitionCommand{
			memberlistJoin:     []string{seedAddr},
			memberlistBindPort: 0,
			partitionIDs:       "0",
			partitionState:     "active",
			skipConfirmation:   true,
			logger:             log.NewNopLogger(),
		}

		err = cmd.run()
		require.Error(t, err)
		require.Contains(t, err.Error(), "partition 0 already exists")
	})

	t.Run("successfully adds a non-sequential partition", func(t *testing.T) {
		ctx := context.Background()

		// Start a seed memberlist node.
		seedKV, seedClient := startMemberlistKV(t)

		// Pre-create partition 0 to simulate an existing ring.
		err := seedClient.CAS(ctx, ingester.PartitionRingKey, func(in interface{}) (out interface{}, retry bool, err error) {
			ringDesc := ring.GetOrCreatePartitionRingDesc(in)
			ringDesc.AddPartition(0, ring.PartitionActive, time.Now())
			return ringDesc, true, nil
		})
		require.NoError(t, err)

		// Get the seed node's listening port.
		seedAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(seedKV.GetListeningPort()))

		// Add partition 2 (skipping partition 1) - should succeed.
		cmd := &AddPartitionCommand{
			memberlistJoin:     []string{seedAddr},
			memberlistBindPort: 0,
			partitionIDs:       "2",
			partitionState:     "active",
			skipConfirmation:   true,
			logger:             log.NewNopLogger(),
		}

		err = cmd.run()
		require.NoError(t, err)

		// Verify partition 2 was added.
		require.Eventually(t, func() bool {
			val, err := seedClient.Get(ctx, ingester.PartitionRingKey)
			if err != nil || val == nil {
				return false
			}
			ringDesc, ok := val.(*ring.PartitionRingDesc)
			if !ok {
				return false
			}
			return ringDesc.HasPartition(0) && ringDesc.HasPartition(2) && !ringDesc.HasPartition(1)
		}, 5*time.Second, 100*time.Millisecond, "partition 2 should be added while partition 1 does not exist")
	})

	t.Run("successfully adds multiple partitions", func(t *testing.T) {
		ctx := context.Background()

		// Start a seed memberlist node.
		seedKV, seedClient := startMemberlistKV(t)

		// Get the seed node's listening port.
		seedAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(seedKV.GetListeningPort()))

		// Create the AddPartitionCommand with multiple partition IDs.
		cmd := &AddPartitionCommand{
			memberlistJoin:     []string{seedAddr},
			memberlistBindPort: 0,
			partitionIDs:       "0, 1, 2",
			partitionState:     "active",
			skipConfirmation:   true,
			logger:             log.NewNopLogger(),
		}

		// Run the add partition command.
		err := cmd.run()
		require.NoError(t, err)

		// Verify all partitions were added.
		require.Eventually(t, func() bool {
			val, err := seedClient.Get(ctx, ingester.PartitionRingKey)
			if err != nil || val == nil {
				return false
			}
			ringDesc, ok := val.(*ring.PartitionRingDesc)
			if !ok {
				return false
			}
			return ringDesc.HasPartition(0) && ringDesc.HasPartition(1) && ringDesc.HasPartition(2)
		}, 5*time.Second, 100*time.Millisecond, "partitions 0, 1, 2 should all be added")
	})

	t.Run("fails if any partition already exists when adding multiple", func(t *testing.T) {
		ctx := context.Background()

		// Start a seed memberlist node.
		seedKV, seedClient := startMemberlistKV(t)

		// Pre-create partition 1.
		err := seedClient.CAS(ctx, ingester.PartitionRingKey, func(in interface{}) (out interface{}, retry bool, err error) {
			ringDesc := ring.GetOrCreatePartitionRingDesc(in)
			ringDesc.AddPartition(1, ring.PartitionActive, time.Now())
			return ringDesc, true, nil
		})
		require.NoError(t, err)

		// Get the seed node's listening port.
		seedAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(seedKV.GetListeningPort()))

		// Try to add partitions 0, 1, 2 - should fail because 1 already exists.
		cmd := &AddPartitionCommand{
			memberlistJoin:     []string{seedAddr},
			memberlistBindPort: 0,
			partitionIDs:       "0,1,2",
			partitionState:     "active",
			skipConfirmation:   true,
			logger:             log.NewNopLogger(),
		}

		err = cmd.run()
		require.Error(t, err)
		require.Contains(t, err.Error(), "partition 1 already exists")

		// Verify that partition 0 was NOT added (atomic failure).
		val, err := seedClient.Get(ctx, ingester.PartitionRingKey)
		require.NoError(t, err)
		ringDesc := val.(*ring.PartitionRingDesc)
		require.False(t, ringDesc.HasPartition(0), "partition 0 should not be added when operation fails")
		require.True(t, ringDesc.HasPartition(1), "partition 1 should still exist")
		require.False(t, ringDesc.HasPartition(2), "partition 2 should not be added when operation fails")
	})
}

func TestRemovePartitionCommand(t *testing.T) {
	t.Run("successfully removes a partition with no owners", func(t *testing.T) {
		ctx := context.Background()

		// Start a seed memberlist node.
		seedKV, seedClient := startMemberlistKV(t)

		// Pre-create partition 0 with no owners.
		err := seedClient.CAS(ctx, ingester.PartitionRingKey, func(in interface{}) (out interface{}, retry bool, err error) {
			ringDesc := ring.GetOrCreatePartitionRingDesc(in)
			ringDesc.AddPartition(0, ring.PartitionInactive, time.Now())
			return ringDesc, true, nil
		})
		require.NoError(t, err)

		// Get the seed node's listening port.
		seedAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(seedKV.GetListeningPort()))

		// Create the RemovePartitionCommand.
		cmd := &RemovePartitionCommand{
			memberlistJoin:     []string{seedAddr},
			memberlistBindPort: 0, // random port
			partitionIDs:       "0",
			skipConfirmation:   true,
			logger:             log.NewNopLogger(),
		}

		// Run the remove partition command.
		err = cmd.run()
		require.NoError(t, err)

		// Verify the partition was removed by reading from the seed KV.
		// Use polling because memberlist is eventually consistent.
		require.Eventually(t, func() bool {
			val, err := seedClient.Get(ctx, ingester.PartitionRingKey)
			if err != nil || val == nil {
				return false
			}
			ringDesc, ok := val.(*ring.PartitionRingDesc)
			if !ok {
				return false
			}
			return !ringDesc.HasPartition(0)
		}, 5*time.Second, 100*time.Millisecond, "partition 0 should be removed")
	})

	t.Run("fails if partition does not exist", func(t *testing.T) {
		// Start a seed memberlist node.
		seedKV, _ := startMemberlistKV(t)

		// Get the seed node's listening port.
		seedAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(seedKV.GetListeningPort()))

		// Try to remove partition 0 which doesn't exist - should fail.
		cmd := &RemovePartitionCommand{
			memberlistJoin:     []string{seedAddr},
			memberlistBindPort: 0,
			partitionIDs:       "0",
			skipConfirmation:   true,
			logger:             log.NewNopLogger(),
		}

		err := cmd.run()
		require.Error(t, err)
		require.Contains(t, err.Error(), "partition 0 does not exist")
	})

	t.Run("fails if partition has owners", func(t *testing.T) {
		ctx := context.Background()

		// Start a seed memberlist node.
		seedKV, seedClient := startMemberlistKV(t)

		// Pre-create partition 0 with an owner.
		err := seedClient.CAS(ctx, ingester.PartitionRingKey, func(in interface{}) (out interface{}, retry bool, err error) {
			ringDesc := ring.GetOrCreatePartitionRingDesc(in)
			now := time.Now()
			ringDesc.AddPartition(0, ring.PartitionActive, now)
			ringDesc.AddOrUpdateOwner("ingester-0", ring.OwnerActive, 0, now)
			return ringDesc, true, nil
		})
		require.NoError(t, err)

		// Get the seed node's listening port.
		seedAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(seedKV.GetListeningPort()))

		// Try to remove partition 0 which has an owner - should fail.
		cmd := &RemovePartitionCommand{
			memberlistJoin:     []string{seedAddr},
			memberlistBindPort: 0,
			partitionIDs:       "0",
			skipConfirmation:   true,
			logger:             log.NewNopLogger(),
		}

		err = cmd.run()
		require.Error(t, err)
		require.Contains(t, err.Error(), "partition 0 has 1 owner(s), cannot remove")
	})

	t.Run("successfully removes multiple partitions", func(t *testing.T) {
		ctx := context.Background()

		// Start a seed memberlist node.
		seedKV, seedClient := startMemberlistKV(t)

		// Pre-create partitions 0, 1, 2 with no owners.
		err := seedClient.CAS(ctx, ingester.PartitionRingKey, func(in interface{}) (out interface{}, retry bool, err error) {
			ringDesc := ring.GetOrCreatePartitionRingDesc(in)
			now := time.Now()
			ringDesc.AddPartition(0, ring.PartitionInactive, now)
			ringDesc.AddPartition(1, ring.PartitionInactive, now)
			ringDesc.AddPartition(2, ring.PartitionInactive, now)
			return ringDesc, true, nil
		})
		require.NoError(t, err)

		// Get the seed node's listening port.
		seedAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(seedKV.GetListeningPort()))

		// Remove partitions 0 and 2.
		cmd := &RemovePartitionCommand{
			memberlistJoin:     []string{seedAddr},
			memberlistBindPort: 0,
			partitionIDs:       "0,2",
			skipConfirmation:   true,
			logger:             log.NewNopLogger(),
		}

		err = cmd.run()
		require.NoError(t, err)

		// Verify partitions 0 and 2 were removed, but 1 remains.
		require.Eventually(t, func() bool {
			val, err := seedClient.Get(ctx, ingester.PartitionRingKey)
			if err != nil || val == nil {
				return false
			}
			ringDesc, ok := val.(*ring.PartitionRingDesc)
			if !ok {
				return false
			}
			return !ringDesc.HasPartition(0) && ringDesc.HasPartition(1) && !ringDesc.HasPartition(2)
		}, 5*time.Second, 100*time.Millisecond, "partitions 0 and 2 should be removed, partition 1 should remain")
	})

	t.Run("fails if any partition has owners when removing multiple", func(t *testing.T) {
		ctx := context.Background()

		// Start a seed memberlist node.
		seedKV, seedClient := startMemberlistKV(t)

		// Pre-create partitions 0, 1, 2 where partition 1 has an owner.
		err := seedClient.CAS(ctx, ingester.PartitionRingKey, func(in interface{}) (out interface{}, retry bool, err error) {
			ringDesc := ring.GetOrCreatePartitionRingDesc(in)
			now := time.Now()
			ringDesc.AddPartition(0, ring.PartitionInactive, now)
			ringDesc.AddPartition(1, ring.PartitionActive, now)
			ringDesc.AddOrUpdateOwner("ingester-0", ring.OwnerActive, 1, now)
			ringDesc.AddPartition(2, ring.PartitionInactive, now)
			return ringDesc, true, nil
		})
		require.NoError(t, err)

		// Get the seed node's listening port.
		seedAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(seedKV.GetListeningPort()))

		// Try to remove partitions 0, 1, 2 - should fail because 1 has an owner.
		cmd := &RemovePartitionCommand{
			memberlistJoin:     []string{seedAddr},
			memberlistBindPort: 0,
			partitionIDs:       "0,1,2",
			skipConfirmation:   true,
			logger:             log.NewNopLogger(),
		}

		err = cmd.run()
		require.Error(t, err)
		require.Contains(t, err.Error(), "partition 1 has 1 owner(s), cannot remove")

		// Verify that partition 0 was NOT removed (atomic failure).
		val, err := seedClient.Get(ctx, ingester.PartitionRingKey)
		require.NoError(t, err)
		ringDesc := val.(*ring.PartitionRingDesc)
		require.True(t, ringDesc.HasPartition(0), "partition 0 should still exist when operation fails")
		require.True(t, ringDesc.HasPartition(1), "partition 1 should still exist")
		require.True(t, ringDesc.HasPartition(2), "partition 2 should still exist when operation fails")
	})
}

// startMemberlistKV starts a memberlist KV node for testing and returns both the KV and a client.
// The KV is automatically stopped when the test completes.
func startMemberlistKV(t *testing.T) (*memberlist.KV, *memberlist.Client) {
	t.Helper()

	var cfg memberlist.KVConfig
	flagext.DefaultValues(&cfg)

	cfg.TCPTransport.BindAddrs = []string{"127.0.0.1"}
	cfg.TCPTransport.BindPort = 0 // random port
	cfg.GossipInterval = 100 * time.Millisecond
	cfg.GossipNodes = 3
	cfg.PushPullInterval = 5 * time.Second
	cfg.Codecs = []codec.Codec{ring.GetCodec(), ring.GetPartitionRingCodec()}

	logger := log.NewNopLogger()
	dnsProvider := dns.NewProvider(logger, nil, dns.GolangResolverType)

	mkv := memberlist.NewKV(cfg, logger, dnsProvider, nil)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), mkv))

	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), mkv))
	})

	client, err := memberlist.NewClient(mkv, ring.GetPartitionRingCodec())
	require.NoError(t, err)

	return mkv, client
}
