package eventokendistributor

import (
	"fmt"
	"sort"
	"time"

	"github.com/grafana/dskit/ring"
	"golang.org/x/exp/slices"
)

type Ring struct {
	ringDesc         *ring.Desc
	zones            []string
	maxTokenValue    uint32
	tokenDistributor TokenDistributorInterface
}

type InstanceInfo struct {
	InstanceID string
	Zone       string
}

func NewRing(ringDesc *ring.Desc, zones []string, maxTokenValue uint32, tokenDistributor TokenDistributorInterface) *Ring {
	sort.Strings(zones)
	return &Ring{
		ringDesc:         ringDesc,
		zones:            zones,
		maxTokenValue:    maxTokenValue,
		tokenDistributor: tokenDistributor,
	}
}

// AddInstance adds the instance with the given id from the given zone to the ring. It also assigns the set of unique,
// sorted and equidistant tokens to the new instance.
func (r *Ring) AddInstance(instanceID int, zone string) error {
	instancesByZone := r.getInstancesByZone()
	instances, ok := instancesByZone[zone]
	if !ok {
		instances = map[string]ring.InstanceDesc{}
	}
	instance := fmt.Sprintf("instance-%s-%d", zone, instanceID)
	_, ok = instances[instance]
	if ok {
		return fmt.Errorf("instance %s with id %d already existis in zone %s", instance, instanceID, zone)
	}
	zoneID, err := r.getZoneID(zone)
	if err != nil {
		return err
	}
	tokens, err := r.tokenDistributor.CalculateTokens(instanceID, zoneID, nil)
	if err != nil {
		return err
	}

	r.ringDesc.AddIngester(instance, instance, zone, tokens, ring.ACTIVE, time.Now())
	return nil
}

func (r *Ring) getZoneID(zone string) (int, error) {
	index := sort.SearchStrings(r.zones, zone)
	if index > len(r.zones) {
		return -1, fmt.Errorf("zone %s is not valid", zone)
	}
	return index, nil
}

// getInstancesByZone returns instances grouped by zone.
func (r *Ring) getInstancesByZone() map[string]map[string]ring.InstanceDesc {
	instances := r.ringDesc.GetIngesters()
	instancesByZone := map[string]map[string]ring.InstanceDesc{}
	for instance, instanceDesc := range instances {
		instancesInZone, ok := instancesByZone[instanceDesc.GetZone()]
		if !ok {
			instancesInZone = map[string]ring.InstanceDesc{}
		}
		instancesInZone[instance] = instanceDesc
		instancesByZone[instanceDesc.GetZone()] = instancesInZone
	}
	return instancesByZone
}

// getTokensByZone returns instances tokens grouped by zone. Tokens within each zone
// are guaranteed to be sorted.
func (r *Ring) getTokensByZone() map[string][]uint32 {
	tokensByZone := map[string][][]uint32{}
	for _, instance := range r.ringDesc.GetIngesters() {
		// Tokens may not be sorted for an older version, so we enforce sorting here.
		tokens := instance.Tokens
		if !sort.IsSorted(ring.Tokens(tokens)) {
			sort.Sort(ring.Tokens(tokens))
		}

		tokensByZone[instance.Zone] = append(tokensByZone[instance.Zone], tokens)
	}

	// Merge tokens per zone.
	return ring.MergeTokensByZone(tokensByZone)
}

// GetInstanceByToken returns a map of instances by token.
func (r *Ring) GetInstanceByToken() map[uint32]InstanceInfo {
	instanceByToken := map[uint32]InstanceInfo{}
	instances := r.ringDesc.GetIngesters()

	for instanceID, instance := range instances {
		info := InstanceInfo{
			InstanceID: instanceID,
			Zone:       instance.Zone,
		}

		for _, token := range instance.Tokens {
			instanceByToken[token] = info
		}
	}

	return instanceByToken
}

// GetTokens returns all current tokens
func (r *Ring) GetTokens() ring.Tokens {
	return r.ringDesc.GetTokens()
}

// GetInstances returns all current instances.
func (r *Ring) GetInstances() map[string]ring.InstanceDesc {
	return r.ringDesc.Ingesters
}

// getRegisteredOwnershipByInstanceByZone calculates the ownership map grouped by instance id and by zone
func (r *Ring) getRegisteredOwnershipByInstanceByZone() map[string]map[string]float64 {
	ownershipByInstanceByZone := make(map[string]map[string]float64, len(r.zones))
	tokensByZone := r.getTokensByZone()
	instanceByToken := r.GetInstanceByToken()
	for zone, tokens := range tokensByZone {
		ownershipByInstanceByZone[zone] = make(map[string]float64, len(r.ringDesc.GetIngesters()))
		if tokens == nil || len(tokens) == 0 {
			continue
		}
		prev := len(tokens) - 1
		for tk, token := range tokens {
			ownership := float64(distance(tokens[prev], token, r.maxTokenValue))
			ownershipByInstanceByZone[zone][instanceByToken[token].InstanceID] += ownership
			prev = tk
		}
	}
	return ownershipByInstanceByZone
}

func (r *Ring) getStatistics() RingStatistics {
	return NewRingStatistics(r.getRegisteredOwnershipByInstanceByZone(), r.maxTokenValue)
}

func (r *Ring) GetReplicationSet(token uint32, ringTokens ring.Tokens, instanceByToken map[uint32]InstanceInfo, op ring.Operation, bufInstanceIDs []InstanceInfo, bufHosts, bufZones []string) ([]InstanceInfo, error) {
	var (
		n          = len(r.zones)
		instances  = bufInstanceIDs[:0]
		start      = searchToken(ringTokens, token)
		iterations = 0

		// We use a slice instead of a map because it's faster to search within a
		// slice than lookup a map for a very low number of items.
		distinctHosts = bufHosts[:0]
		distinctZones = bufZones[:0]
	)

	for i := start; len(distinctHosts) < n && iterations < len(ringTokens); i++ {
		iterations++
		// Wrap i around in the ring.
		i %= len(ringTokens)
		token := ringTokens[i]

		info, ok := instanceByToken[token]
		if !ok {
			// This should never happen unless a bug in the ring code.
			return nil, ring.ErrInconsistentTokensInfo
		}

		// We want n *distinct* instances && distinct zones.
		if slices.Contains(distinctHosts, info.InstanceID) {
			continue
		}

		// Ignore if the instances don't have a zone set.
		if info.Zone != "" {
			if slices.Contains(distinctZones, info.Zone) {
				continue
			}
		}

		distinctHosts = append(distinctHosts, info.InstanceID)
		instance := r.ringDesc.Ingesters[info.InstanceID]

		// Check whether the replica set should be extended given we're including
		// this instance.
		if op.ShouldExtendReplicaSetOnState(instance.State) {
			n++
		} else if info.Zone != "" {
			// We should only add the zone if we are not going to extend,
			// as we want to extend the instance in the same AZ.
			distinctZones = append(distinctZones, info.Zone)
		}

		instances = append(instances, info)
	}

	return instances, nil
}

// searchToken returns the offset of the tokens entry holding the range for the provided key.
func searchToken(tokens []uint32, key uint32) int {
	i := sort.Search(len(tokens), func(x int) bool {
		return tokens[x] > key
	})
	if i >= len(tokens) {
		i = 0
	}
	return i
}
