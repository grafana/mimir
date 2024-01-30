package ring

import (
	"fmt"
	"time"
)

type PartitionInstanceRing struct {
	// TODO doc: these are mutually exclusive. Access the partitions ring via .PartitionRing()
	partitionsRingWatcher *PartitionRingWatcher
	partitionsRing        *PartitionRing

	instancesRing    *Ring
	heartbeatTimeout time.Duration
}

func NewPartitionInstanceRing(partitionsRingWatcher *PartitionRingWatcher, instancesRing *Ring, heartbeatTimeout time.Duration) *PartitionInstanceRing {
	return &PartitionInstanceRing{
		partitionsRingWatcher: partitionsRingWatcher,
		instancesRing:         instancesRing,
		heartbeatTimeout:      heartbeatTimeout,
	}
}

// GetReplicationSetsForOperation returns one ReplicationSet for each partition and returns ReplicationSet for all partitions.
// If there are not enough owners for partitions, error is returned.
//
// For querying instances, basic idea is that we need to query *ALL* partitions in the ring (or subring).
// For each partition, each owner is a full replica, so it's enough to query single instance only.
// GetReplicationSetsForOperation returns all healthy owners for each partition according to op and the heartbeat timeout.
// GetReplicationSetsForOperation returns an error which Is(ErrTooManyUnhealthyInstances) if there are no healthy owners for some partition.
// GetReplicationSetsForOperation returns ErrEmptyRing if there are no partitions in the ring.
func (pr *PartitionInstanceRing) GetReplicationSetsForOperation(op Operation) ([]ReplicationSet, error) {
	// TODO cleanup this design to avoid having to access to .desc
	partitionsRing := pr.PartitionRing()
	partitionsRingDesc := partitionsRing.desc
	partitionsRingOwners := partitionsRing.PartitionOwners()

	if len(partitionsRingDesc.Partitions) == 0 {
		return nil, ErrEmptyRing
	}
	now := time.Now()

	result := make([]ReplicationSet, 0, len(partitionsRingDesc.Partitions))
	for pid := range partitionsRingDesc.Partitions {
		owners := partitionsRingOwners[pid]
		instances := make([]InstanceDesc, 0, len(owners))

		for _, instanceID := range owners {
			instance, err := pr.instancesRing.GetInstance(instanceID)
			if err != nil {
				return nil, err
			}

			if !instance.IsHealthy(op, pr.heartbeatTimeout, now) {
				continue
			}

			instances = append(instances, instance)
		}

		if len(instances) == 0 {
			return nil, fmt.Errorf("partition %d: %w", pid, ErrTooManyUnhealthyInstances)
		}

		result = append(result, ReplicationSet{
			Instances:            instances,
			MaxUnavailableZones:  len(instances) - 1, // We need response from at least 1 owner.
			ZoneAwarenessEnabled: true,
		})
	}
	return result, nil
}

func (pr *PartitionInstanceRing) InstancesCount() int {
	// Number of partitions.
	return len(pr.PartitionRing().PartitionOwners())
}

func (pr *PartitionInstanceRing) ReplicationFactor() int {
	// Each key is always stored into single partition only.
	return 1
}

func (pr *PartitionInstanceRing) Get(key uint32, _ Operation) (ReplicationSet, error) {
	partitionsRing := pr.PartitionRing()

	pid, _, err := partitionsRing.ActivePartitionForKey(key)
	if err != nil {
		return ReplicationSet{}, err
	}

	return ReplicationSet{
		Instances: []InstanceDesc{{
			Addr:      fmt.Sprintf("%d", pid),
			Timestamp: time.Now().Unix(),
			State:     ACTIVE,
			Id:        fmt.Sprintf("%d", pid),
		}},
		MaxErrors:            0,
		MaxUnavailableZones:  0,
		ZoneAwarenessEnabled: false,
	}, nil
}

func (pr *PartitionInstanceRing) ShuffleRingPartitions(identifier string, size int, lookbackPeriod time.Duration, now time.Time) (*PartitionInstanceRing, error) {
	subRing, err := pr.PartitionRing().ShuffleRingPartitions(identifier, size, lookbackPeriod, now)
	if err != nil {
		return nil, err
	}

	return &PartitionInstanceRing{
		partitionsRing:   subRing,
		instancesRing:    pr.instancesRing,
		heartbeatTimeout: pr.heartbeatTimeout,
	}, nil
}

func (pr *PartitionInstanceRing) PartitionRing() *PartitionRing {
	if pr.partitionsRingWatcher != nil {
		return pr.partitionsRingWatcher.GetRing()
	}
	return pr.partitionsRing
}
