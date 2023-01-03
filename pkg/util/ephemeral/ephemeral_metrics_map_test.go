package ephemeral

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/grafana/dskit/kv/memberlist"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Verify that *Metrics is mergeable.
var _ memberlist.Mergeable = &Metrics{}

func TestBasicFunctions(t *testing.T) {
	m := NewMetrics()

	const (
		name    = "test"
		another = "another"
	)

	assert.False(t, m.IsEphemeral(name))
	assert.False(t, m.IsEphemeral(another))

	m.AddEphemeral(name)
	assert.True(t, m.IsEphemeral(name))
	assert.False(t, m.IsEphemeral(another))

	m.AddEphemeral(name)
	assert.True(t, m.IsEphemeral(name))
	assert.False(t, m.IsEphemeral(another))

	m.RemoveEphemeral(name)
	assert.False(t, m.IsEphemeral(name))
	assert.False(t, m.IsEphemeral(another))

	m.RemoveEphemeral(name)
	assert.False(t, m.IsEphemeral(name))
	assert.False(t, m.IsEphemeral(another))
}

func TestMergeChange(t *testing.T) {
	now := time.Now()

	const (
		unchanged                                = "unchanged"
		newInM2                                  = "new"
		removedNonExistantInM2                   = "removed_nonexistant"
		existingInBothRemovedWithLowTimestamp    = "removed_with_low_timestamp"
		existingInBothRemovedWithHigherTimestamp = "removed_with_higher_timestamp"
	)
	m1 := NewMetrics()
	m1.addEphemeral(unchanged, now)
	m1.addEphemeral(existingInBothRemovedWithLowTimestamp, now)
	m1.addEphemeral(existingInBothRemovedWithHigherTimestamp, now)

	assert.True(t, m1.IsEphemeral(unchanged))
	assert.False(t, m1.IsEphemeral(newInM2))
	assert.False(t, m1.IsEphemeral(removedNonExistantInM2))
	assert.True(t, m1.IsEphemeral(existingInBothRemovedWithLowTimestamp))
	assert.True(t, m1.IsEphemeral(existingInBothRemovedWithHigherTimestamp))

	m2 := NewMetrics()
	m2.addEphemeral(newInM2, now)
	m2.removeEphemeral(removedNonExistantInM2, now)
	// merging "fourth" will preserve it as ephemeral, because created/deleted timestamps are the same
	m2.addEphemeral(existingInBothRemovedWithLowTimestamp, now)
	m2.removeEphemeral(existingInBothRemovedWithLowTimestamp, now)
	// "fifth" will be removed from m1, because it has higher timestamp
	m2.addEphemeral(existingInBothRemovedWithHigherTimestamp, now)
	m2.removeEphemeral(existingInBothRemovedWithHigherTimestamp, now.Add(time.Second))

	c, err := m1.Merge(m2, false)
	require.NoError(t, err)
	assert.True(t, m1.IsEphemeral(unchanged))
	assert.True(t, m1.IsEphemeral(newInM2))
	assert.False(t, m1.IsEphemeral(removedNonExistantInM2))
	assert.True(t, m1.IsEphemeral(existingInBothRemovedWithLowTimestamp))
	assert.False(t, m1.IsEphemeral(existingInBothRemovedWithHigherTimestamp))

	cm, ok := c.(*Metrics)
	require.True(t, ok)

	requireHasExactKeys(t, cm, newInM2, existingInBothRemovedWithHigherTimestamp)
}

func TestMergeProperties(t *testing.T) {
	const metrics = 10
	const rounds = 100
	const changesPerRound = 20

	ephemeralMetrics := NewMetrics()    // Mergeable ephemeral metrics map
	controlMetrics := map[string]bool{} // Control map to double check that mergeable-version works correctly.

	now := time.Now()
	rnd := rand.New(rand.NewSource(now.UnixNano()))
	t.Log("seed:", now.UnixNano())

	for round := 0; round < rounds; round++ {
		now = now.Add(time.Second)

		// Make a copy of ephemeral metrics, and modify it.
		updatedMetrics := ephemeralMetrics.Clone().(*Metrics)
		metricsChangedInThisRound := map[string]bool{}

		for change := 0; change < changesPerRound; change++ {
			m := metricName(rnd.Intn(metrics))
			metricsChangedInThisRound[m] = true
			ephemeral := !controlMetrics[m]
			controlMetrics[m] = ephemeral

			if ephemeral {
				updatedMetrics.addEphemeral(m, now)
			} else {
				updatedMetrics.removeEphemeral(m, now)
			}
		}

		// Run these tests first, as they don't modify input parameters.
		testIdempotency(t, ephemeralMetrics, updatedMetrics)
		testCommutativity(t, ephemeralMetrics, updatedMetrics)

		{
			anotherUpdate := ephemeralMetrics.Clone().(*Metrics)
			for i := 0; i < 5; i++ {
				m := metricName(rnd.Intn(metrics))

				if anotherUpdate.IsEphemeral(m) {
					anotherUpdate.removeEphemeral(m, now)
				} else {
					anotherUpdate.addEphemeral(m, now)
				}
			}

			testAssociativity(t, ephemeralMetrics, updatedMetrics, anotherUpdate)
		}

		// Now run actual Merge and verify its output.
		c, err := ephemeralMetrics.Merge(updatedMetrics, false)
		require.NoError(t, err)
		require.ElementsMatch(t, getKeys(c.(*Metrics).Metrics), getKeys(metricsChangedInThisRound))
		require.ElementsMatch(t, c.MergeContent(), getKeys(metricsChangedInThisRound))

		{
			// Verify that ephemeral metrics map matches control map.
			require.ElementsMatch(t, getKeys(ephemeralMetrics.Metrics), getKeys(controlMetrics))
			for k, e := range controlMetrics {
				require.Equal(t, e, ephemeralMetrics.IsEphemeral(k), k)
			}
		}
	}
}

func testEqual(t *testing.T, a, b memberlist.Mergeable) {
	require.Equal(t, a.(*Metrics), b.(*Metrics))
}

// Merge b into a
func merge(t *testing.T, a, b memberlist.Mergeable) {
	_, err := a.Merge(b, false)
	require.NoError(t, err)
}

// Idempotency: A.Merge(B).Merge(B) == A.Merge(B) (ie. second Merge of updates will not cause any more updates)
func testIdempotency(t *testing.T, a, b memberlist.Mergeable) {
	a1 := a.Clone()
	merge(t, a1, b)
	merge(t, a1, b)

	a2 := a.Clone()
	merge(t, a2, b)

	testEqual(t, a1, a2)
}

// Commutativity: A.Merge(B) == B.Merge(A)
func testCommutativity(t *testing.T, a, b memberlist.Mergeable) {
	a1 := a.Clone()
	merge(t, a1, b)

	b1 := b.Clone()
	merge(t, b1, a)

	testEqual(t, a1, b1)
}

// Associativity: (A.Merge(B)).Merge(C) == A.Merge(B.Merge(C))
func testAssociativity(t *testing.T, a, b, c memberlist.Mergeable) {
	a1 := a.Clone()

	merge(t, a1, b)
	merge(t, a1, c)

	b2 := b.Clone()
	merge(t, b2, c)
	a2 := a.Clone()
	merge(t, a2, b2)

	testEqual(t, a1, a2)
}

func metricName(ix int) string {
	return fmt.Sprintf("metric_%d", ix)
}

func getKeys[T any](m map[string]T) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func requireHasExactKeys(t *testing.T, m *Metrics, keys ...string) {
	require.ElementsMatch(t, getKeys(m.Metrics), keys)
}
