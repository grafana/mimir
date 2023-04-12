package main

import (
	"fmt"
	"math/rand"
	"sort"

	"github.com/grafana/dskit/ring"
	"golang.org/x/exp/slices"
)

func analyseRing(ringStatus ringStatusDesc) error {
	const (
		replicationFactor    = 3
		zoneAwarenessEnabled = true
		numIterations        = 1000000
	)

	var (
		ringDesc            = ringStatus.toRingModel()
		ringTokens          = ringDesc.GetTokens()
		ringInstanceByToken = getRingInstanceByToken(ringDesc)
		bufDescs            [ring.GetBufferSize]string
		bufHosts            [ring.GetBufferSize]string
		bufZones            [ring.GetBufferSize]string
	)

	ownedTokens := map[string]int{}

	for i := 0; i < numIterations; i++ {
		key := rand.Uint32()

		ingesterIDs, err := ringGet(key, ringDesc, ringTokens, ringInstanceByToken, ring.WriteNoExtend, replicationFactor, zoneAwarenessEnabled, bufDescs[:0], bufHosts[:0], bufZones[:0])
		if err != nil {
			return err
		}

		for _, ingesterID := range ingesterIDs {
			ownedTokens[ingesterID]++
		}
	}

	// Compute the per-ingester % of owned tokens.
	type ingesterOwnership struct {
		id         string
		percentage float64
	}

	ownership := []ingesterOwnership{}
	for id, numTokens := range ownedTokens {
		ownership = append(ownership, ingesterOwnership{
			id:         id,
			percentage: (float64(numTokens) / numIterations) * 100,
		})
	}

	slices.SortFunc(ownership, func(a, b ingesterOwnership) bool {
		return a.percentage < b.percentage
	})

	for _, ingester := range ownership {
		fmt.Println(fmt.Sprintf("%s,%.3f", ingester.id, ingester.percentage))
	}

	return nil
}

func ringGet(key uint32, ringDesc *ring.Desc, ringTokens []uint32, ringInstanceByToken map[uint32]instanceInfo, op ring.Operation, replicationFactor int, zoneAwarenessEnabled bool, bufInstanceIDs, bufHosts, bufZones []string) ([]string, error) {
	var (
		n          = replicationFactor
		instances  = bufInstanceIDs[:0]
		start      = searchToken(ringTokens, key)
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

		info, ok := ringInstanceByToken[token]
		if !ok {
			// This should never happen unless a bug in the ring code.
			return nil, ring.ErrInconsistentTokensInfo
		}

		// We want n *distinct* instances && distinct zones.
		if slices.Contains(distinctHosts, info.InstanceID) {
			continue
		}

		// Ignore if the instances don't have a zone set.
		if zoneAwarenessEnabled && info.Zone != "" {
			if slices.Contains(distinctZones, info.Zone) {
				continue
			}
		}

		distinctHosts = append(distinctHosts, info.InstanceID)
		instance := ringDesc.Ingesters[info.InstanceID]

		// Check whether the replica set should be extended given we're including
		// this instance.
		if op.ShouldExtendReplicaSetOnState(instance.State) {
			n++
		} else if zoneAwarenessEnabled && info.Zone != "" {
			// We should only add the zone if we are not going to extend,
			// as we want to extend the instance in the same AZ.
			distinctZones = append(distinctZones, info.Zone)
		}

		instances = append(instances, info.InstanceID)
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

type instanceInfo struct {
	InstanceID string
	Zone       string
}

func getRingInstanceByToken(desc *ring.Desc) map[uint32]instanceInfo {
	out := map[uint32]instanceInfo{}

	for instanceID, instance := range desc.Ingesters {
		info := instanceInfo{
			InstanceID: instanceID,
			Zone:       instance.Zone,
		}

		for _, token := range instance.Tokens {
			out[token] = info
		}
	}

	return out
}
