package ring

import (
	"fmt"
	"time"
)

type PartitionRingReader interface {
	// PartitionRing returns a snapshot of the PartitionRing. This function must never return nil.
	// If the ring is empty or unknown, an empty PartitionRing can be returned.
	PartitionRing() *PartitionRing
}

// PartitionInstanceRing holds a partitions ring and a instances ring, and provide functions
// to look up the intersection of the two (e.g. healthy instances by partition).
type PartitionInstanceRing struct {
	partitionsRingReader PartitionRingReader
	instancesRing        *Ring
	heartbeatTimeout     time.Duration
}

func NewPartitionInstanceRing(partitionsRingWatcher PartitionRingReader, instancesRing *Ring, heartbeatTimeout time.Duration) *PartitionInstanceRing {
	return &PartitionInstanceRing{
		partitionsRingReader: partitionsRingWatcher,
		instancesRing:        instancesRing,
		heartbeatTimeout:     heartbeatTimeout,
	}
}

func (r *PartitionInstanceRing) PartitionRing() *PartitionRing {
	return r.partitionsRingReader.PartitionRing()
}

func (r *PartitionInstanceRing) InstanceRing() *Ring {
	return r.instancesRing
}

// GetReplicationSetsForOperation returns one ReplicationSet for each partition in the ring.
// A ReplicationSet is returned for every partition in ring. If there are no healthy owners
// for a partition, an error is returned.
func (r *PartitionInstanceRing) GetReplicationSetsForOperation(op Operation) ([]ReplicationSet, error) {
	partitionsRing := r.PartitionRing()
	partitionsRingDesc := partitionsRing.desc

	if len(partitionsRingDesc.Partitions) == 0 {
		return nil, ErrEmptyRing
	}

	now := time.Now()
	result := make([]ReplicationSet, 0, len(partitionsRingDesc.Partitions))

	for partitionID := range partitionsRingDesc.Partitions {
		ownerIDs := partitionsRing.PartitionOwnerIDs(partitionID)
		instances := make([]InstanceDesc, 0, len(ownerIDs))

		for _, instanceID := range ownerIDs {
			instance, err := r.instancesRing.GetInstance(instanceID)
			if err != nil {
				// If an instance doesn't exist in the instances ring we don't return an error
				// but lookup for other instances of the partition.
				continue
			}

			if !instance.IsHealthy(op, r.heartbeatTimeout, now) {
				continue
			}

			instances = append(instances, instance)
		}

		if len(instances) == 0 {
			return nil, fmt.Errorf("partition %d: %w", partitionID, ErrTooManyUnhealthyInstances)
		}

		result = append(result, ReplicationSet{
			Instances:            instances,
			MaxUnavailableZones:  len(instances) - 1, // We need response from at least 1 owner.
			ZoneAwarenessEnabled: true,               // Partitions has no concept of zone, but we enable it in order to support ring's requests minimization feature.
		})
	}
	return result, nil
}

// ShuffleShard wraps PartitionRing.ShuffleShard().
//
// The PartitionRing embedded in the returned PartitionInstanceRing is based on a snapshot of the partitions ring
// at the time this function gets called. This means that subsequent changes to the partitions ring will not
// be reflected in the returned PartitionInstanceRing.
func (r *PartitionInstanceRing) ShuffleShard(identifier string, size int) (*PartitionInstanceRing, error) {
	partitionsSubring, err := r.PartitionRing().ShuffleShard(identifier, size)
	if err != nil {
		return nil, err
	}

	return NewPartitionInstanceRing(newStaticPartitionRingReader(partitionsSubring), r.instancesRing, r.heartbeatTimeout), nil
}

// ShuffleShardWithLookback wraps PartitionRing.ShuffleShardWithLookback().
//
// The PartitionRing embedded in the returned PartitionInstanceRing is based on a snapshot of the partitions ring
// at the time this function gets called. This means that subsequent changes to the partitions ring will not
// be reflected in the returned PartitionInstanceRing.
func (r *PartitionInstanceRing) ShuffleShardWithLookback(identifier string, size int, lookbackPeriod time.Duration, now time.Time) (*PartitionInstanceRing, error) {
	partitionsSubring, err := r.PartitionRing().ShuffleShardWithLookback(identifier, size, lookbackPeriod, now)
	if err != nil {
		return nil, err
	}

	return NewPartitionInstanceRing(newStaticPartitionRingReader(partitionsSubring), r.instancesRing, r.heartbeatTimeout), nil
}

type staticPartitionRingReader struct {
	ring *PartitionRing
}

func newStaticPartitionRingReader(ring *PartitionRing) staticPartitionRingReader {
	return staticPartitionRingReader{
		ring: ring,
	}
}

func (m staticPartitionRingReader) PartitionRing() *PartitionRing {
	return m.ring
}
