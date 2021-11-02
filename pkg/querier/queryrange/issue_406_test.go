package queryrange

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIssue406(t *testing.T) {
	const numSeries = 10000
	const maxShards = 512

	// Generate some random series hashes.
	var seriesHash []int64
	for i := 1; i <= numSeries; i++ {
		seriesHash = append(seriesHash, rand.Int63())
	}

	// When querying split-compacted block with shard X_of_Y we know that it will contain series that meet lset.Hash() % Y == X-1
	// When performing a querysharded query for shard N_of_M, we will be only interested in series that meet lset.Hash() % M == N-1
	// When Y=M*k or Y*k=M we can deduce which blocks can contain each set of query shards.

	unoptimizedQueriedBlocks := 0
	optimizedQueriedBlocks := 0

	// Using only powers of 2 for number of query/compactor shards is cheating... as one or the other optimization condition will always be true.
	for numQueryShards := int64(2); numQueryShards <= maxShards; numQueryShards *= 2 {
		for numCompactorShards := int64(2); numCompactorShards <= maxShards; numCompactorShards *= 2 {

			testUnoptimizedQueriedBlocks := 0
			testOptimizedQueriedBlocks := 0

			for _, hash := range seriesHash {
				// Compute the query shard ID for the given series.
				queryShardID := hash % numQueryShards

				// Compute the compactor shard ID where the series really is.
				realCompactorShardID := hash % numCompactorShards

				testUnoptimizedQueriedBlocks += int(numCompactorShards) // We would query ALL sharded blocks by default.

				// If M >= Y; M = Y*k: then series for N_of_M can be only in the block where N % Y = X
				// (and that block will contain series from k query shards)
				if numQueryShards >= numCompactorShards && numQueryShards%numCompactorShards == 0 {
					computedCompactorShardID := queryShardID % numCompactorShards

					require.Equalf(t, realCompactorShardID, computedCompactorShardID, "series hash: %d num compactor shards: %d num query shards: %d query shard ID: %d", hash, numCompactorShards, numQueryShards, queryShardID)

					testOptimizedQueriedBlocks += 1
					continue
				}

				// If Y >= M; Y = M*k: then series for N_of_M can be in any of (k) blocks where X % M == N
				if numCompactorShards >= numQueryShards && numCompactorShards%numQueryShards == 0 {
					computedCompactorShardIDs := make([]int64, 0, numCompactorShards/numQueryShards)

					for i := queryShardID; i < numCompactorShards; i += numQueryShards {
						computedCompactorShardIDs = append(computedCompactorShardIDs, i)
					}

					testOptimizedQueriedBlocks += len(computedCompactorShardIDs)

					require.Contains(t, computedCompactorShardIDs, realCompactorShardID, "series hash: %d num compactor shards: %d num query shards: %d query shard ID: %d", hash, numCompactorShards, numQueryShards, queryShardID)
					continue
				}

				// Can't optimize, simulate querying all sharded blocks.
				testOptimizedQueriedBlocks += int(numCompactorShards)
			}

			t.Logf("query shards: %d, compactor shards: %d, unoptimized queried blocks: %d, optimized queried blocks: %d (%g%%)\n", numQueryShards, numCompactorShards, testUnoptimizedQueriedBlocks, testOptimizedQueriedBlocks, 100*float64(testOptimizedQueriedBlocks)/float64(testUnoptimizedQueriedBlocks))

			unoptimizedQueriedBlocks += testUnoptimizedQueriedBlocks
			optimizedQueriedBlocks += testOptimizedQueriedBlocks
		}
	}

	t.Logf("Unoptimized queried blocks: %d\n", unoptimizedQueriedBlocks)
	t.Logf("Optimized queried blocks: %d (%g%%)\n", optimizedQueriedBlocks, 100*float64(optimizedQueriedBlocks)/float64(unoptimizedQueriedBlocks))
}
