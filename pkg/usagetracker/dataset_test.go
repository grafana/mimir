package usagetracker

import (
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/DmitriyVTitov/size"
	"github.com/bits-and-blooms/bloom/v3"
	"github.com/stretchr/testify/require"
)

func TestHLLAndBloom(t *testing.T) {
	for _, count := range []uint64{1e4, 1e5, 1e6, 1e7, 1e8} {
		t.Run(fmt.Sprintf("count=%d", count), func(t *testing.T) {
			filter := bloom.NewWithEstimates(1_000_000_000, 0.01)
			hll, err := NewHHL64(16)
			require.NoError(t, err)

			// Keep track of the hashes that we've seen for real.
			seen := make(map[uint64]struct{}, count)

			falsePositives := 0
			falseNegatives := 0

			buffer := make([]byte, 8)

			for i := uint64(0); i < count; i++ {
				x := rand.Uint64()
				for _, ok := seen[x]; ok; _, ok = seen[x] {
					x = rand.Uint64()
				}

				// Convert the random hash to []byte
				binary.BigEndian.PutUint64(buffer, x)

				// Check if it's a false positive.
				if filter.Test(buffer) {
					falsePositives++
				}

				// Add it.
				filter.Add(buffer)
				hll.AddUint64(x)

				// Check if it's a false positive (should never happen).
				if !filter.Test(buffer) {
					falseNegatives++
				}

				seen[x] = struct{}{}

				if i%128 == 0 {
					require.InEpsilonf(t, i+1, hll.Count(), 0.05, "expected %d, got %d", i, hll.Count())
				}
			}

			gotCount := hll.Count()
			t.Logf("hll size: %d", size.Of(hll))
			t.Logf("bloon size: %d", size.Of(filter))
			t.Logf("false negatives: %d", falseNegatives)
			t.Logf("false positives: %d", falsePositives)
			t.Logf("false positives error rate: %0.3f%%", 100*float64(falsePositives)/float64(count))
			t.Logf("count error rate: %0.3f%%", 100*(float64(gotCount)-float64(count))/float64(count))
			require.InEpsilonf(t, count, gotCount, 0.02, "expected %d, got %d", count, gotCount)
		})
	}
}
