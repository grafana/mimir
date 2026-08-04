// SPDX-License-Identifier: AGPL-3.0-only

package readcacheassignment

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// Replica is one concrete readcache pod that serves a logical slot.
type Replica struct {
	InstanceID string
	Zone       string
}

// ReplicaMap maps a logical slot ID (the instance_id stored in LogEntry)
// to its ordered concrete zone replicas. An empty / nil map means
// identity: the logical ID is also the concrete dial target (RF=1 /
// legacy single-pool deployments).
//
// Under RF=2 the log keeps one lease row per partition naming the
// logical slot (e.g. "readcache-5"); the map expands that to
// [{readcache-zone-a-5, zone-a}, {readcache-zone-b-5, zone-b}].
type ReplicaMap map[string][]Replica

// Expand returns the concrete replicas for logicalID. When the map is
// nil/empty or has no entry for logicalID, returns a single identity
// replica (InstanceID=logicalID, Zone="").
func (m ReplicaMap) Expand(logicalID string) []Replica {
	if logicalID == "" {
		return nil
	}
	if reps, ok := m[logicalID]; ok && len(reps) > 0 {
		out := make([]Replica, len(reps))
		copy(out, reps)
		return out
	}
	return []Replica{{InstanceID: logicalID}}
}

// ConcreteIDs returns just the instance IDs from Expand.
func (m ReplicaMap) ConcreteIDs(logicalID string) []string {
	reps := m.Expand(logicalID)
	out := make([]string, len(reps))
	for i, r := range reps {
		out[i] = r.InstanceID
	}
	return out
}

// OwnsLogical reports whether concreteID is a replica of logicalID.
func (m ReplicaMap) OwnsLogical(concreteID, logicalID string) bool {
	if concreteID == "" || logicalID == "" {
		return false
	}
	if concreteID == logicalID {
		return true
	}
	for _, r := range m.Expand(logicalID) {
		if r.InstanceID == concreteID {
			return true
		}
	}
	return false
}

// LogicalForConcrete returns the logical slot ID that concreteID
// belongs to, if any. Prefers an explicit map entry; otherwise falls
// back to ParseInstanceIdentity.
func (m ReplicaMap) LogicalForConcrete(concreteID string) (string, bool) {
	if concreteID == "" {
		return "", false
	}
	for logical, reps := range m {
		for _, r := range reps {
			if r.InstanceID == concreteID {
				return logical, true
			}
		}
	}
	id, ok := ParseInstanceIdentity(concreteID)
	if !ok {
		// Unparseable names are their own logical ID (legacy pods).
		return concreteID, true
	}
	return id.LogicalID, true
}

// Equal reports whether m and other describe the same mapping.
func (m ReplicaMap) Equal(other ReplicaMap) bool {
	if len(m) != len(other) {
		return false
	}
	for k, reps := range m {
		oreps, ok := other[k]
		if !ok || len(reps) != len(oreps) {
			return false
		}
		for i := range reps {
			if reps[i] != oreps[i] {
				return false
			}
		}
	}
	return true
}

// Clone returns a deep copy.
func (m ReplicaMap) Clone() ReplicaMap {
	if m == nil {
		return nil
	}
	out := make(ReplicaMap, len(m))
	for k, reps := range m {
		cp := make([]Replica, len(reps))
		copy(cp, reps)
		out[k] = cp
	}
	return out
}

// SortedLogicalIDs returns logical IDs in sorted order.
func (m ReplicaMap) SortedLogicalIDs() []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// InstanceIdentity is the parsed form of a readcache instance ID.
type InstanceIdentity struct {
	// LogicalID is the zone-stripped slot name, e.g. "readcache-5".
	LogicalID string
	// Zone is the availability zone label, e.g. "zone-a", or "" when
	// the name has no zone segment (legacy single-pool pods).
	Zone string
	// Ordinal is the StatefulSet ordinal.
	Ordinal int
}

// zonalInstanceRE matches names like "readcache-zone-a-5" or
// "readcache-zone-b-12".
var zonalInstanceRE = regexp.MustCompile(`^(.+)-zone-([a-z0-9]+)-(\d+)$`)

// nonZonalInstanceRE matches names like "readcache-5".
var nonZonalInstanceRE = regexp.MustCompile(`^(.+)-(\d+)$`)

// ParseInstanceIdentity extracts logical ID / zone / ordinal from a
// concrete instance ID. Returns ok=false when the name does not end
// in -<ordinal>.
func ParseInstanceIdentity(instanceID string) (InstanceIdentity, bool) {
	if m := zonalInstanceRE.FindStringSubmatch(instanceID); m != nil {
		ord, err := strconv.Atoi(m[3])
		if err != nil {
			return InstanceIdentity{}, false
		}
		return InstanceIdentity{
			LogicalID: fmt.Sprintf("%s-%s", m[1], m[3]),
			Zone:      "zone-" + m[2],
			Ordinal:   ord,
		}, true
	}
	if m := nonZonalInstanceRE.FindStringSubmatch(instanceID); m != nil {
		// Reject names that still look zonal but failed the stricter
		// pattern (shouldn't happen). Also reject bare prefixes.
		if strings.Contains(m[1], "-zone-") {
			return InstanceIdentity{}, false
		}
		ord, err := strconv.Atoi(m[2])
		if err != nil {
			return InstanceIdentity{}, false
		}
		return InstanceIdentity{
			LogicalID: instanceID,
			Zone:      "",
			Ordinal:   ord,
		}, true
	}
	return InstanceIdentity{}, false
}

// LogicalSlotID builds the logical slot name for prefix + ordinal,
// e.g. ("readcache", 5) → "readcache-5".
func LogicalSlotID(prefix string, ordinal int) string {
	if prefix == "" {
		prefix = "readcache"
	}
	return fmt.Sprintf("%s-%d", prefix, ordinal)
}

// BuildReplicaMap groups concrete ring members into a ReplicaMap keyed
// by logical slot ID. Members that share a logical ID become replicas
// of that slot (ordered by zone, then instance ID for stability).
// Members that cannot be parsed are recorded as identity replicas of
// themselves.
func BuildReplicaMap(instances []Replica) ReplicaMap {
	if len(instances) == 0 {
		return nil
	}
	grouped := make(map[string][]Replica, len(instances))
	for _, inst := range instances {
		if inst.InstanceID == "" {
			continue
		}
		logical := inst.InstanceID
		zone := inst.Zone
		if id, ok := ParseInstanceIdentity(inst.InstanceID); ok {
			logical = id.LogicalID
			if zone == "" {
				zone = id.Zone
			}
		}
		grouped[logical] = append(grouped[logical], Replica{
			InstanceID: inst.InstanceID,
			Zone:       zone,
		})
	}
	for logical, reps := range grouped {
		sort.Slice(reps, func(i, j int) bool {
			if reps[i].Zone != reps[j].Zone {
				return reps[i].Zone < reps[j].Zone
			}
			return reps[i].InstanceID < reps[j].InstanceID
		})
		grouped[logical] = reps
	}
	return ReplicaMap(grouped)
}

// DesiredLogicalSlots returns logical slot IDs for ordinals
// [0, desired). prefix defaults to "readcache".
func DesiredLogicalSlots(prefix string, desired int) []string {
	if desired <= 0 {
		return nil
	}
	out := make([]string, desired)
	for i := 0; i < desired; i++ {
		out[i] = LogicalSlotID(prefix, i)
	}
	return out
}
