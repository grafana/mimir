// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"github.com/cespare/xxhash/v2"
	"github.com/grafana/dskit/ring"
)

// homeZoneOwner returns the ruler that owns the rule group with the key.
// Rendezvous hashing over the ring zones gives each rule group a home zone.
// Each zone is the home zone of an equal share of the rule groups.
// A rollout replaces rulers but not zones, so a rule group stays in its home zone.
//
// The owner is the first healthy ruler of the home zone in token order from the key.
// If the home zone has no healthy ruler, the owner comes from the zone with the next rank.
func homeZoneOwner(r ring.ReadRing, key uint32, op ring.Operation) (ring.InstanceDesc, error) {
	var home string
	var homeScore uint64
	for i, zone := range r.Zones() {
		if s := zoneScore(key, zone); i == 0 || s > homeScore {
			home, homeScore = zone, s
		}
	}

	// A walk for k rulers starts with the rulers of a walk for fewer rulers.
	// The ring can change between reads, so the loop stops at the count read here.
	n := r.InstancesCount()
	for k := 1; ; k = min(2*k, n) {
		set, err := r.GetWithOptions(key, op, ring.WithReplicationFactor(k))
		if err == nil {
			// The owner is the first ruler of the zone with the highest rank in the set.
			owner, ownerScore := set.Instances[0], zoneScore(key, set.Instances[0].Zone)
			for _, instance := range set.Instances[1:] {
				if s := zoneScore(key, instance.Zone); s > ownerScore {
					owner, ownerScore = instance, s
				}
			}
			if owner.Zone == home || k >= n {
				return owner, nil
			}
		} else if k >= n {
			return ring.InstanceDesc{}, err
		}
	}
}

// zoneScore returns the rendezvous hashing score of the zone for the key.
// The murmur3 64-bit finalizer mixes the key into all bits of the zone hash.
func zoneScore(key uint32, zone string) uint64 {
	h := xxhash.Sum64String(zone) ^ uint64(key)
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33
	return h
}
