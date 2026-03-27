// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"fmt"

	"github.com/grafana/dskit/ring"
)

// InstanceKeys holds the ring instance descriptor and the indexes of keys assigned to it.
type InstanceKeys struct {
	Instance ring.InstanceDesc
	Indexes  []int
}

// SplitKeysByInstance groups the input keys by the ring instance they belong to, returning
// the instance descriptor and the original indexes of keys assigned to each instance.
// This is a simplified alternative to ring.DoBatchWithOptions for cases where replication
// factor is 1 and no quorum tracking is needed (e.g. partition rings).
func SplitKeysByInstance(ctx context.Context, op ring.Operation, r ring.DoBatchRing, keys []uint32) ([]InstanceKeys, error) {
	if r.InstancesCount() <= 0 {
		return nil, fmt.Errorf("SplitKeysByInstance: InstancesCount <= 0")
	}

	if len(keys) == 0 {
		return nil, nil
	}

	expectedKeysPerInstance := len(keys)*(r.ReplicationFactor()+1)/r.InstancesCount() + 1

	// We use a map to group by instance, then convert to a slice at the end.
	type entry struct {
		desc    ring.InstanceDesc
		indexes []int
	}
	instances := make(map[string]*entry, r.InstancesCount())

	var (
		bufDescs [ring.GetBufferSize]ring.InstanceDesc
		bufHosts [ring.GetBufferSize]string
		bufZones [ring.GetBufferSize]string
	)
	for i, key := range keys {
		if i%10e3 == 0 {
			if err := context.Cause(ctx); err != nil {
				return nil, err
			}
		}

		replicationSet, err := r.Get(key, op, bufDescs[:0], bufHosts[:0], bufZones[:0])
		if err != nil {
			return nil, err
		}

		for _, desc := range replicationSet.Instances {
			curr, found := instances[desc.Addr]
			if !found {
				curr = &entry{
					desc:    desc,
					indexes: make([]int, 0, expectedKeysPerInstance),
				}
				instances[desc.Addr] = curr
			}
			curr.indexes = append(curr.indexes, i)
		}
	}

	result := make([]InstanceKeys, 0, len(instances))
	for _, instance := range instances {
		result = append(result, InstanceKeys{
			Instance: instance.desc,
			Indexes:  instance.indexes,
		})
	}

	return result, nil
}
