package ring

import (
	"bytes"
	"fmt"
	"math/rand"
	"time"

	shardUtil "github.com/grafana/dskit/ring/shard"
)

var ErrNoActivePartitionFound = fmt.Errorf("no active partition found")

type PartitionRing struct {
	desc PartitionRingDesc

	ringTokens      Tokens
	tokenPartitions map[Token]int32
	partitionOwners map[int32][]string

	heartbeatTimeout time.Duration
}

func NewPartitionRing(desc PartitionRingDesc, heartbeatTimeout time.Duration) *PartitionRing {
	tokens, tokenPartitions := desc.TokensAndTokenPartitions()

	pr := PartitionRing{
		desc:             desc,
		ringTokens:       tokens,
		tokenPartitions:  tokenPartitions,
		partitionOwners:  desc.PartitionOwners(),
		heartbeatTimeout: heartbeatTimeout,
	}
	return &pr
}

// ActivePartitionForKey returns partition that should receive given key. Only active partitions are considered,
// and only one partition is returned.
func (pr *PartitionRing) ActivePartitionForKey(key uint32) (int32, PartitionDesc, error) {
	start := searchToken(pr.ringTokens, key)
	iterations := 0

	tokensCount := len(pr.ringTokens)
	for i := start; iterations < len(pr.ringTokens); i++ {
		iterations++

		if i >= tokensCount {
			i %= len(pr.ringTokens)
		}

		token := pr.ringTokens[i]

		pid, ok := pr.tokenPartitions[Token(token)]
		if !ok {
			return 0, PartitionDesc{}, ErrInconsistentTokensInfo
		}

		p, ok := pr.desc.Partition(pid)
		if !ok {
			return 0, PartitionDesc{}, ErrInconsistentTokensInfo
		}

		if p.IsActive() {
			return pid, p, nil
		}
	}
	return 0, PartitionDesc{}, ErrNoActivePartitionFound
}

func (pr *PartitionRing) ShuffleRingPartitions(identifier string, size int, lookbackPeriod time.Duration, now time.Time) (*PartitionRing, error) {
	partitions, err := pr.shuffleRingPartitions(identifier, size, lookbackPeriod, now)
	if err != nil {
		return nil, err
	}

	// nil is a special value indicating all partitions
	if partitions == nil {
		return pr, nil
	}

	return NewPartitionRing(pr.desc.WithPartitions(partitions), pr.heartbeatTimeout), nil
}

func (pr *PartitionRing) shuffleRingPartitions(identifier string, size int, lookbackPeriod time.Duration, now time.Time) (map[int32]struct{}, error) {
	if size <= 0 || size >= len(pr.desc.Partitions) {
		return nil, nil
	}

	lookbackUntil := now.Add(-lookbackPeriod).Unix()

	// Initialise the random generator used to select instances in the ring.
	// There are no zones
	random := rand.New(rand.NewSource(shardUtil.ShuffleShardSeed(identifier, "")))

	// To select one more instance while guaranteeing the "consistency" property,
	// we do pick a random value from the generator and resolve uniqueness collisions
	// (if any) continuing walking the ring.
	tokensCount := len(pr.ringTokens)

	result := make(map[int32]struct{}, size)
	for len(result) < size {
		start := searchToken(pr.ringTokens, random.Uint32())
		iterations := 0

		found := false
		for p := start; !found && iterations < tokensCount; p++ {
			iterations++

			if p >= tokensCount {
				p %= tokensCount
			}

			pid, ok := pr.tokenPartitions[Token(pr.ringTokens[p])]
			if !ok {
				return nil, ErrInconsistentTokensInfo
			}

			// Ensure we select new partition.
			if _, ok := result[pid]; ok {
				continue
			}

			// Include found partition in the result.
			result[pid] = struct{}{}

			p, ok := pr.desc.Partition(pid)
			if !ok {
				return nil, ErrInconsistentTokensInfo
			}

			// If this partition is inactive (read-only), or became active recently (based on lookback), we need to include more partitions.
			if !p.IsActive() || (lookbackPeriod > 0 && p.BecameActiveAfter(lookbackUntil)) {
				size++

				// If we now need to find all partitions, just return nil to indicate that.
				if size >= len(pr.desc.Partitions) {
					return nil, nil
				}
			}

			found = true
		}

		// If we iterated over all tokens, and no new partition has been found, we can stop looking for more partitions.
		if !found {
			break
		}
	}
	return result, nil
}

func (pr *PartitionRing) PartitionOwners() map[int32][]string {
	return pr.partitionOwners
}

func (pr *PartitionRing) String() string {
	buf := bytes.Buffer{}
	for pid, pd := range pr.desc.Partitions {
		buf.WriteString(fmt.Sprintf(" %d:%v", pid, pd.State.String()))
	}

	return fmt.Sprintf("PartitionRing{ownersCount: %d, partitionsCount: %d, partitions: {%s}}", len(pr.desc.Owners), len(pr.desc.Owners), buf.String())
}
