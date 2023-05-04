package tokendistributor

import (
	"fmt"
	"os"
	"sort"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/ring"
	"golang.org/x/exp/slices"
)

const GetBufferSize = 5

type ReplicationStrategy interface {
	// getReplicationFactor returns the replication factor for this strategy
	getReplicationFactor() int

	// getReplicationSet returns getReplicationFactor() of Instances representing the full set of replicas of the given key.
	getReplicaSet(key Token, sortedRingTokens []Token, ringInstanceByToken map[Token]Instance) ([]Instance, error)

	// getLastReplicationToken returns the token that holds the last replica for the given token
	getLastReplicaToken(key Token, sortedRingTokens []Token, ringInstanceByToken map[Token]Instance) (Token, error)

	// getReplicaStart returns the most far away token in the ring whose replication ends in the given instance with the given key
	getReplicaStart(key Token, sortedRingTokens []Token, ringInstanceByToken map[Token]Instance) (Token, error)

	// addInstance add the given instance with the given zone to the replication strategy
	addInstance(instance Instance, zone Zone)
}

// SimpleReplicationStrategy is an implementation of replicationStrategy which is not zone-aware

type SimpleReplicationStrategy struct {
	replicationFactor int
	bufInstances      []Instance
	logger            log.Logger
}

// newSimpleReplicationStrategy creates a new SimpleReplicationStrategy.
// replicationFactor is the required replication factor.
// bufInstances is a slices to be overwritten for the return value to avoid memory allocation;
func newSimpleReplicationStrategy(replicationFactor int, bufInstances []Instance) SimpleReplicationStrategy {
	if bufInstances == nil {
		bufInstances = make([]Instance, 0, GetBufferSize)
	}
	logger := log.NewLogfmtLogger(os.Stdout)
	return SimpleReplicationStrategy{
		replicationFactor: replicationFactor,
		bufInstances:      bufInstances,
		logger:            logger,
	}
}

func (s SimpleReplicationStrategy) getReplicationFactor() int {
	return s.replicationFactor
}

func (s SimpleReplicationStrategy) addInstance(instance Instance, zone Zone) {

}

func (s SimpleReplicationStrategy) getReplicaSetWithLastToken(key Token, sortedRingTokens []Token, ringInstanceByToken map[Token]Instance) ([]Instance, Token, error) {
	//level.Info(s.logger).Log("msg", "Getting replica set", "token", key)
	var (
		ringTokensCount = len(sortedRingTokens)
		start           = searchToken(sortedRingTokens, key)
		iterations      = 0

		// We use a slice instead of a map because it's faster to search within a
		// slice than lookup a map for a very low number of items.
		distinctInstances = s.bufInstances[:0]
	)

	currentToken := key
	for currentTokenIndex := start; len(distinctInstances) < s.replicationFactor && iterations < ringTokensCount; currentTokenIndex++ {
		iterations++
		// Wrap i around in the ring.
		currentTokenIndex %= ringTokensCount
		currentToken = sortedRingTokens[currentTokenIndex]
		currentInstance, ok := ringInstanceByToken[currentToken]
		if !ok {
			// This should never happen unless a bug in the ring code.
			return nil, 0, ring.ErrInconsistentTokensInfo
		}

		// We want s.replicationFactor *distinct* instances.
		if slices.Contains(distinctInstances, currentInstance) {
			//level.Debug(s.logger).Log("msg", fmt.Sprintf("Instance %s with currentToken %d has been ignored because it is already present in the replication set", currentInstance, currentToken))
			continue
		}

		distinctInstances = append(distinctInstances, currentInstance)
		//level.Info(s.logger).Log("msg", fmt.Sprintf("Instance %s with currentToken %d has been added to the replication set", currentInstance, currentToken))
	}
	//level.Info(s.logger).Log("msg", fmt.Sprintf("Returning replica set %v and last token %d", distinctInstances, currentToken))
	return distinctInstances, currentToken, nil
}

func (s SimpleReplicationStrategy) getReplicaSet(key Token, sortedRingTokens []Token, ringInstanceByToken map[Token]Instance) ([]Instance, error) {
	replicaSet, _, err := s.getReplicaSetWithLastToken(key, sortedRingTokens, ringInstanceByToken)
	return replicaSet, err
}

func (s SimpleReplicationStrategy) getLastReplicaToken(key Token, sortedRingTokens []Token, ringInstanceByToken map[Token]Instance) (Token, error) {
	_, lastReplicaToken, err := s.getReplicaSetWithLastToken(key, sortedRingTokens, ringInstanceByToken)
	return lastReplicaToken, err
}

func (s SimpleReplicationStrategy) getReplicaStart(terminalToken Token, sortedRingTokens []Token, ringInstanceByToken map[Token]Instance) (Token, error) {
	//level.Info(s.logger).Log("msg", "Getting replica start", "token", terminalToken)
	var (
		ringTokensCount = len(sortedRingTokens)
		// returns the smallest index i such that sortedRingTokens[i] > terminalToken, or 0 if no such index exists
		start      = searchTokenFloor(sortedRingTokens, terminalToken)
		iterations = 0

		// We use a slice instead of a map because it's faster to search within a
		// slice than lookup a map for a very low number of items.
		distinctInstances = s.bufInstances[:0]

		terminalInstance        = ringInstanceByToken[terminalToken]
		currentReplicationStart = terminalToken
		currentToken            = terminalToken
	)

	// we go backwards and look for all possible ring tokens that could belong to the replication set ending in given terminalToken and terminalInstance
	for i := start - 1; iterations < ringTokensCount; i-- {
		if i < 0 {
			i += ringTokensCount
		}
		iterations++
		currentToken = sortedRingTokens[i]
		currentInstance, ok := ringInstanceByToken[currentToken]
		if !ok {
			// This should never happen unless a bug in the ring code.
			return terminalToken, ring.ErrInconsistentTokensInfo
		}
		// as soon as we find an instance corresponding to the terminal instance, we return currentReplicationStart
		if terminalInstance == currentInstance {
			break
		}

		if !slices.Contains(distinctInstances, currentInstance) {
			distinctInstances = append(distinctInstances, currentInstance)
			if len(distinctInstances) == s.replicationFactor {
				break
			}

		}
		currentReplicationStart = currentToken
	}

	return currentReplicationStart, nil
}

type ZoneAwareReplicationStrategy struct {
	replicationFactor int
	zonesByInstance   map[Instance]Zone
	bufInstances      []Instance
	bufZones          []Zone
	logger            log.Logger
}

// NewZoneAwareReplicationStrategy creates a new ZoneAwareReplicationStrategy.
// replicationFactor is the required replication factor.
// zoneByInstance maps instances to zones.
// bufInstances and bufZones are slices to be overwritten for the return value to avoid memory allocation
func NewZoneAwareReplicationStrategy(replicationFactor int, zoneByInstance map[Instance]Zone, bufInstances []Instance, bufZones []Zone) ZoneAwareReplicationStrategy {
	if bufInstances == nil {
		bufInstances = make([]Instance, 0, GetBufferSize)
	}
	if bufZones == nil {
		bufZones = make([]Zone, 0, GetBufferSize)
	}
	logger := log.NewLogfmtLogger(os.Stdout)
	return ZoneAwareReplicationStrategy{
		replicationFactor: replicationFactor,
		zonesByInstance:   zoneByInstance,
		bufInstances:      bufInstances,
		bufZones:          bufZones,
		logger:            logger,
	}
}

func (z ZoneAwareReplicationStrategy) addInstance(instance Instance, zone Zone) {
	z.zonesByInstance[instance] = zone
}

func (z ZoneAwareReplicationStrategy) getReplicationFactor() int {
	return z.replicationFactor
}

func (z ZoneAwareReplicationStrategy) getReplicaSetWithLastToken(key Token, sortedRingTokens []Token, ringInstanceByToken map[Token]Instance) ([]Instance, Token, error) {
	//level.Info(z.logger).Log("msg", "Getting replica set", "token", key)
	var (
		ringTokensCount = len(sortedRingTokens)
		start           = searchToken(sortedRingTokens, key)
		iterations      = 0

		// We use a slice instead of a map because it's faster to search within a
		// slice than lookup a map for a very low number of items.
		distinctInstances = z.bufInstances[:0]
		distinctZones     = z.bufZones[:0]
	)

	currentToken := key
	for currentTokenIndex := start; len(distinctInstances) < z.replicationFactor && iterations < ringTokensCount; currentTokenIndex++ {
		iterations++
		// Wrap i around in the ring.
		currentTokenIndex %= ringTokensCount
		currentToken = sortedRingTokens[currentTokenIndex]
		currentInstance, ok := ringInstanceByToken[currentToken]
		if !ok {
			// This should never happen unless a bug in the ring code.
			return nil, 0, ring.ErrInconsistentTokensInfo
		}

		currentZone, ok := z.zonesByInstance[currentInstance]
		if !ok {
			// This should never happen unless a bug in the ring code.
			return nil, 0, fmt.Errorf("instance %s has no zone assigned when zone-awareness is enabled", currentInstance)
		}

		// We want z.replicationFactor *distinct* instances belonging to z.replicationFactor *distinct* zones.
		if slices.Contains(distinctInstances, currentInstance) || slices.Contains(distinctZones, currentZone) {
			//level.Debug(z.logger).Log("msg", fmt.Sprintf("Instance %s from zone %s with currentToken %d has been ignored because it is already present in the replication set", currentInstance, currentZone, currentToken))
			continue
		}

		distinctInstances = append(distinctInstances, currentInstance)
		distinctZones = append(distinctZones, currentZone)
		//level.Info(z.logger).Log("msg", fmt.Sprintf("Instance %s from zone %s with currentToken %d has been added to the replication set", currentInstance, currentZone, currentToken))
	}
	//level.Info(z.logger).Log("msg", fmt.Sprintf("Returning replica set %v and last token %d", distinctInstances, currentToken))
	return distinctInstances, currentToken, nil
}

func (z ZoneAwareReplicationStrategy) getReplicaSet(key Token, sortedRingTokens []Token, ringInstanceByToken map[Token]Instance) ([]Instance, error) {
	replicaSet, _, err := z.getReplicaSetWithLastToken(key, sortedRingTokens, ringInstanceByToken)
	return replicaSet, err
}

func (z ZoneAwareReplicationStrategy) getLastReplicaToken(key Token, sortedRingTokens []Token, ringInstanceByToken map[Token]Instance) (Token, error) {
	_, lastReplicaToken, err := z.getReplicaSetWithLastToken(key, sortedRingTokens, ringInstanceByToken)
	return lastReplicaToken, err
}

func (z ZoneAwareReplicationStrategy) getReplicaStart(terminalToken Token, sortedRingTokens []Token, ringInstanceByToken map[Token]Instance) (Token, error) {
	//level.Info(z.logger).Log("msg", "Getting replica start", "token", terminalToken, "instance", ringInstanceByToken[terminalToken], "zone", z.zonesByInstance[ringInstanceByToken[terminalToken]])
	var (
		terminalInstance = ringInstanceByToken[terminalToken]
		terminalZone     = z.zonesByInstance[terminalInstance]
		ringTokensCount  = len(sortedRingTokens)
		// returns the smallest index i such that sortedRingTokens[i] > terminalToken, or 0 if no such index exists
		start      = searchTokenFloor(sortedRingTokens, terminalToken)
		iterations = 0

		// We use a slice instead of a map because it's faster to search within a
		// slice than lookup a map for a very low number of items.
		distinctInstances       = z.bufInstances[:0]
		distinctZones           = z.bufZones[:0]
		currentReplicationStart = terminalToken
		currentToken            = terminalToken
	)

	//level.Debug(z.logger).Log("msg", fmt.Sprintf("Index %d  has been found for token %d", start, terminalToken))
	// we go backwards and look for all possible ring tokens that could belong to the replication set ending in given terminalToken and terminalInstance
	for i := start - 1; iterations < ringTokensCount; i-- {
		if i < 0 {
			i += ringTokensCount
		}
		iterations++
		currentToken = sortedRingTokens[i]
		currentInstance, ok := ringInstanceByToken[currentToken]
		if !ok {
			// This should never happen unless a bug in the ring code.
			return terminalToken, ring.ErrInconsistentTokensInfo
		}

		currentZone, ok := z.zonesByInstance[currentInstance]
		if !ok {
			// This should never happen unless a bug in the ring code.
			return 0, fmt.Errorf("instance %s has no zone assigned when zone-awareness is enabled", currentInstance)
		}

		// as soon as we find an instance with the zone corresponding to the zone of the terminal instance, we return currentReplicationStart
		if terminalZone == currentZone {
			//level.Info(z.logger).Log("msg", fmt.Sprintf("Instance %s from zone %s corresponding to the zone of the final instance has been found. Returning token %d", currentInstance, currentZone, currentReplicationStart))
			return currentReplicationStart, nil
		}

		currentReplicationStart = currentToken
		if slices.Contains(distinctInstances, currentInstance) || slices.Contains(distinctZones, currentZone) {
			//level.Debug(z.logger).Log("msg", fmt.Sprintf("Instance %s from zone %s with currentToken %d has been ignored because it is already present in the replication set", currentInstance, currentZone, currentToken))
			continue
		}
		distinctInstances = append(distinctInstances, currentInstance)
		distinctZones = append(distinctZones, currentZone)
		//level.Info(z.logger).Log("msg", fmt.Sprintf("Instance %s from zone %s with token %d has been added to the replica set", currentInstance, currentZone, currentToken))
	}

	return currentReplicationStart, nil
}

// searchToken returns the offset of the tokens entry holding the range for the provided key.
func searchToken(tokens []Token, key Token) int {
	i := sort.Search(len(tokens), func(x int) bool {
		return tokens[x] > key
	})
	if i >= len(tokens) {
		i = 0
	}
	return i
}

func searchTokenFloor(tokens []Token, key Token) int {
	i := sort.Search(len(tokens), func(x int) bool {
		return tokens[x] >= key
	})
	if i >= len(tokens) {
		i = len(tokens) - 1
	}
	return i
}
