package ring

import (
	"fmt"
	"strconv"
	"time"

	"github.com/gogo/protobuf/proto"
	"golang.org/x/exp/slices"

	"github.com/grafana/dskit/kv/codec"
	"github.com/grafana/dskit/kv/memberlist"
)

func (m *PartitionDesc) IsActive() bool {
	return m.GetState() == PartitionActive
}

func (m *PartitionDesc) BecameActiveAfter(ts int64) bool {
	return m.IsActive() && m.GetStateTimestamp() >= ts
}

func (m *OwnerDesc) IsHealthy(op Operation, heartbeatTimeout time.Duration, now time.Time) bool {
	healthy := op.IsInstanceInStateHealthy(m.State)

	return healthy && IsHeartbeatHealthy(time.Unix(m.Heartbeat, 0), heartbeatTimeout, now)
}

func NewPartitionRingDesc() *PartitionRingDesc {
	return &PartitionRingDesc{
		Partitions: map[int32]PartitionDesc{},
		Owners:     map[string]OwnerDesc{},
	}
}

// PartitionRingDescFactory makes new Descs
func PartitionRingDescFactory() proto.Message {
	return NewPartitionRingDesc()
}

func GetPartitionRingCodec() codec.Codec {
	return codec.NewProtoCodec("partitionRingDesc", PartitionRingDescFactory)
}

// TokensAndTokenPartitions returns the list of tokens and a mapping of each token to its corresponding partition ID.
func (m *PartitionRingDesc) TokensAndTokenPartitions() (Tokens, map[Token]int32) {
	allTokens := make(Tokens, 0, len(m.Partitions)*optimalTokensPerInstance)

	out := make(map[Token]int32, len(m.Partitions)*optimalTokensPerInstance)
	for partitionID, partition := range m.Partitions {
		for _, token := range partition.Tokens {
			out[Token(token)] = partitionID

			allTokens = append(allTokens, token)
		}
	}

	slices.Sort(allTokens)
	return allTokens, out
}

func (m *PartitionRingDesc) NumberOfPartitionOwners(partitionID int32) int {
	owners := 0
	for _, o := range m.Owners {
		if o.OwnedPartition == partitionID {
			owners++
		}
	}
	return owners
}

func (m *PartitionRingDesc) PartitionOwners() map[int32][]string {
	out := make(map[int32][]string, len(m.Partitions))
	for id, o := range m.Owners {
		out[o.OwnedPartition] = append(out[o.OwnedPartition], id)
	}
	return out
}

func (m *PartitionRingDesc) AddPartition(id int32, state PartitionState, now time.Time) {
	// Spread-minimizing token generator is deterministic unique-token generator for given id and zone.
	// Partitions don't use zones.
	tg := NewSpreadMinimizingTokenGeneratorForInstanceAndZoneID("", int(id), 0, false)

	tokens := tg.GenerateTokens(optimalTokensPerInstance, nil)

	m.Partitions[id] = PartitionDesc{
		Tokens:         tokens,
		State:          state,
		StateTimestamp: now.Unix(),
	}
}

func (m *PartitionRingDesc) UpdatePartitionState(id int32, state PartitionState, now time.Time) bool {
	d, ok := m.Partition(id)
	if !ok {
		return false
	}

	if d.State == state {
		return false
	}

	d.State = state
	d.StateTimestamp = now.Unix()
	m.Partitions[id] = d
	return true
}

func (m *PartitionRingDesc) Partition(id int32) (PartitionDesc, bool) {
	p, ok := m.Partitions[id]
	return p, ok
}

func (m *PartitionRingDesc) RemovePartition(id int32) {
	delete(m.Partitions, id)
}

// WithPartitions returns a new PartitionRingDesc with only the specified partitions and their owners included.
func (m *PartitionRingDesc) WithPartitions(partitions map[int32]struct{}) PartitionRingDesc {
	newPartitions := make(map[int32]PartitionDesc, len(partitions))
	newOwners := make(map[string]OwnerDesc, len(partitions)*2) // assuming two owners per partition.

	for pid, p := range m.Partitions {
		if _, ok := partitions[pid]; ok {
			newPartitions[pid] = p
		}
	}

	for oid, o := range m.Owners {
		if _, ok := partitions[o.OwnedPartition]; ok {
			newOwners[oid] = o
		}
	}

	return PartitionRingDesc{
		Partitions: newPartitions,
		Owners:     newOwners,
	}
}

// AddOrUpdateOwner adds or updates owner entry in the ring. Returns true, if entry was added or updated, false if entry is unchanged.
func (m *PartitionRingDesc) AddOrUpdateOwner(id, address, zone string, ownedPartition int32, state InstanceState, heartbeat time.Time) bool {
	prev, ok := m.Owners[id]
	updated := OwnerDesc{
		Id:             id,
		Addr:           address,
		Zone:           zone,
		OwnedPartition: ownedPartition,
		Heartbeat:      heartbeat.Unix(),
		State:          state,
	}

	if !ok || !prev.Equal(updated) {
		m.Owners[id] = updated
		return true
	}
	return false
}

func (m *PartitionRingDesc) RemoveOwner(id string) {
	delete(m.Owners, id)
}

func (m *PartitionRingDesc) Merge(mergeable memberlist.Mergeable, localCAS bool) (memberlist.Mergeable, error) {
	if mergeable == nil {
		return nil, nil
	}

	other, ok := mergeable.(*PartitionRingDesc)
	if !ok {
		// This method only deals with non-nil rings.
		return nil, fmt.Errorf("expected *PartitionRingDesc, got %T", mergeable)
	}

	change := NewPartitionRingDesc()

	// Handle partitions.
	for pid, otherPart := range other.Partitions {
		changed := false

		thisPart, exists := m.Partitions[pid]
		if !exists {
			changed = true
			thisPart = otherPart
		} else {
			// We don't need to check for conflicts: all partitions have unique tokens already, since our token generator is deterministic.
			// The only problem could be that over time token generation algorithm changes and starts producing different tokens.
			// We can detect that by adding "token generator version" into the PartitionDesc, and then preserving tokens generated by latest version only.
			// We can solve this in the future, when needed.
			if len(thisPart.Tokens) == 0 && len(otherPart.Tokens) > 0 {
				thisPart.Tokens = otherPart.Tokens
				changed = true
			}

			if otherPart.StateTimestamp > thisPart.StateTimestamp {
				changed = true

				thisPart.State = otherPart.State
				thisPart.StateTimestamp = otherPart.StateTimestamp
			}
		}

		if changed {
			m.Partitions[pid] = thisPart
			change.Partitions[pid] = thisPart
		}
	}

	if localCAS {
		// Let's mark all missing partitions in incoming change as deleted.
		// This breaks commutativity! But we only do it locally, not when gossiping with others.
		for pid, thisPart := range m.Partitions {
			if _, exists := other.Partitions[pid]; !exists && thisPart.State != PartitionDeleted {
				// Partition was removed from the ring. We need to preserve it locally, but we set state to PartitionDeleted.
				thisPart.State = PartitionDeleted
				thisPart.StateTimestamp = time.Now().Unix()
				m.Partitions[pid] = thisPart
				change.Partitions[pid] = thisPart
			}
		}
	}

	// Now let's handle owners. Owners don't have tokens, which simplifies things compared to "normal" ring.
	for id, otherOwner := range other.Owners {
		thisOwner := m.Owners[id]

		// ting.Heartbeat will be 0, if there was no such ingester in our version
		if otherOwner.Heartbeat > thisOwner.Heartbeat || (otherOwner.Heartbeat == thisOwner.Heartbeat && otherOwner.State == LEFT && thisOwner.State != LEFT) {
			m.Owners[id] = otherOwner
			change.Owners[id] = otherOwner
		}
	}

	if localCAS {
		// Mark all missing owners as deleted.
		// This breaks commutativity! But we only do it locally, not when gossiping with others.
		for id, thisOwner := range m.Owners {
			if _, exists := other.Owners[id]; !exists && thisOwner.State != LEFT {
				// missing, let's mark our ingester as LEFT
				thisOwner.State = LEFT
				// We are deleting entry "now", and should not keep old timestamp, because there may already be pending
				// message in the gossip network with newer timestamp (but still older than "now").
				// Such message would "resurrect" this deleted entry.
				thisOwner.Heartbeat = time.Now().Unix()

				m.Owners[id] = thisOwner
				change.Owners[id] = thisOwner
			}
		}
	}

	// If nothing changed, report nothing.
	if len(change.Partitions) == 0 && len(change.Owners) == 0 {
		return nil, nil
	}

	return change, nil
}

func (m *PartitionRingDesc) MergeContent() []string {
	result := make([]string, len(m.Partitions)+len(m.Owners))

	// We're assuming that partition IDs and instance IDs are not colliding (ie. no instance is called "1").
	for pid := range m.Partitions {
		result = append(result, strconv.Itoa(int(pid)))
	}

	for id := range m.Owners {
		result = append(result, fmt.Sprintf(id))
	}
	return result
}

func (m *PartitionRingDesc) RemoveTombstones(limit time.Time) (total, removed int) {
	for pid, part := range m.Partitions {
		if part.State == PartitionDeleted {
			if limit.IsZero() || time.Unix(part.StateTimestamp, 0).Before(limit) {
				delete(m.Partitions, pid)
				removed++
			} else {
				total++
			}
		}
	}

	for n, ing := range m.Owners {
		if ing.State == LEFT {
			if limit.IsZero() || time.Unix(ing.Heartbeat, 0).Before(limit) {
				delete(m.Owners, n)
				removed++
			} else {
				total++
			}
		}
	}
	return
}

func (m *PartitionRingDesc) Clone() memberlist.Mergeable {
	return proto.Clone(m).(*PartitionRingDesc)
}
