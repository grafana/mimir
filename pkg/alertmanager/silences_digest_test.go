// SPDX-License-Identifier: AGPL-3.0-only

package alertmanager

import (
	"bytes"
	"fmt"
	"hash/fnv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/alertmanager/cluster"
	"github.com/prometheus/alertmanager/cluster/clusterpb"
	"github.com/prometheus/alertmanager/silence/silencepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protodelim"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestSilencesDigest(t *testing.T) {
	t.Run("changes when the set changes", func(t *testing.T) {
		am := newTestAlertmanager(t)
		before, err := am.silencesDigest(t.Context())
		require.NoError(t, err)

		addTestSilence(t, am, "one")
		after, err := am.silencesDigest(t.Context())
		require.NoError(t, err)

		require.NotEqual(t, before, after)
	})

	t.Run("differs when membership differs even if the count is the same", func(t *testing.T) {
		amA := newTestAlertmanager(t)
		amB := newTestAlertmanager(t)

		// Same count (2 silences each), but different underlying silences. A bare count can't
		// distinguish this, which is why this hashes IDs instead.
		addTestSilence(t, amA, "shared")
		addTestSilence(t, amA, "only on A")
		addTestSilence(t, amB, "shared")
		addTestSilence(t, amB, "only on B")

		digestA, err := amA.silencesDigest(t.Context())
		require.NoError(t, err)
		digestB, err := amB.silencesDigest(t.Context())
		require.NoError(t, err)

		require.NotEqual(t, digestA, digestB)
	})

	t.Run("includes pending silences, not just active ones", func(t *testing.T) {
		am := newTestAlertmanager(t)
		before, err := am.silencesDigest(t.Context())
		require.NoError(t, err)

		require.NoError(t, am.silences.Set(t.Context(), &silencepb.Silence{
			Matchers: []*silencepb.Matcher{{Name: "a", Pattern: "b"}},
			StartsAt: timestamppb.New(time.Now().Add(time.Hour)),
			EndsAt:   timestamppb.New(time.Now().Add(2 * time.Hour)),
		}))

		after, err := am.silencesDigest(t.Context())
		require.NoError(t, err)
		require.NotEqual(t, before, after, "a pending silence should be reflected in the digest, not just an active one")
	})

	t.Run("differs when a silence is edited, even though its ID is unchanged", func(t *testing.T) {
		am := newTestAlertmanager(t)
		id := addTestSilence(t, am, "original")
		before, err := am.silencesDigest(t.Context())
		require.NoError(t, err)

		sil, err := am.silences.QueryOne(t.Context())
		require.NoError(t, err)
		require.Equal(t, id, sil.Id)
		sil.Comment = "edited"
		require.NoError(t, am.silences.Set(t.Context(), sil))

		after, err := am.silencesDigest(t.Context())
		require.NoError(t, err)
		require.NotEqual(t, before, after, "editing a silence bumps UpdatedAt, which the digest must pick up even though the ID stayed the same")
	})

	t.Run("matches once two replicas converge on the same silences", func(t *testing.T) {
		amA := newTestAlertmanager(t)
		amB := newTestAlertmanager(t)

		// Many silences so map iteration order differs between replicas, replicated to B via Merge.
		for i := 0; i < 32; i++ {
			addTestSilence(t, amA, fmt.Sprintf("replicated %d", i))
		}

		sils, err := amA.activeAndPendingSilences(t.Context())
		require.NoError(t, err)
		require.Len(t, sils, 32)

		for _, sil := range sils {
			var buf bytes.Buffer
			_, err = protodelim.MarshalTo(&buf, &silencepb.MeshSilence{Silence: sil, ExpiresAt: timestamppb.New(sil.GetEndsAt().AsTime().Add(time.Hour))})
			require.NoError(t, err)
			require.NoError(t, amB.silences.Merge(buf.Bytes()))
		}

		digestA, err := amA.silencesDigest(t.Context())
		require.NoError(t, err)
		digestB, err := amB.silencesDigest(t.Context())
		require.NoError(t, err)

		require.Equal(t, digestA, digestB, "identical silences must hash identically on both replicas, whatever order each one's store happens to return them in")
	})

	t.Run("changes when a silence is expired, which doesn't move silences.Version()", func(t *testing.T) {
		am := newTestAlertmanager(t)
		id := addTestSilence(t, am, "to expire")
		before, err := am.silencesDigest(t.Context())
		require.NoError(t, err)
		version := am.silences.Version()

		require.NoError(t, am.silences.Expire(t.Context(), id))
		require.Equal(t, version, am.silences.Version(), "if this changes, a digest cache keyed on Version() would become viable again")

		after, err := am.silencesDigest(t.Context())
		require.NoError(t, err)
		require.NotEqual(t, before, after)
		assert.Equal(t, fnv.New64a().Sum64(), after, "an expired silence must not still be hashed")
	})

	t.Run("an empty set hashes to the empty-input digest", func(t *testing.T) {
		am := newTestAlertmanager(t)

		digest, err := am.silencesDigest(t.Context())
		require.NoError(t, err)
		require.Equal(t, fnv.New64a().Sum64(), digest)
	})

	t.Run("drops a silence once it expires, even though nothing writes when EndsAt passes", func(t *testing.T) {
		am := newTestAlertmanager(t)
		require.NoError(t, am.silences.Set(t.Context(), &silencepb.Silence{
			Matchers: []*silencepb.Matcher{{Name: "a", Pattern: "b"}},
			StartsAt: timestamppb.Now(),
			EndsAt:   timestamppb.New(time.Now().Add(2 * time.Second)),
		}))

		before, err := am.silencesDigest(t.Context())
		require.NoError(t, err)
		require.NotEqual(t, fnv.New64a().Sum64(), before, "the silence expired before the first read, widen the window")

		time.Sleep(2100 * time.Millisecond)

		after, err := am.silencesDigest(t.Context())
		require.NoError(t, err)
		require.NotEqual(t, before, after, "the silence expired with no write in between, but the digest must still reflect it dropping out of the active/pending set")
		assert.Equal(t, fnv.New64a().Sum64(), after, "an expired silence must not still be hashed")
	})
	t.Run("an unchanged set is served from the cache, not recomputed on every read", func(t *testing.T) {
		am := newTestAlertmanager(t)
		addTestSilence(t, am, "one")

		_, err := am.silencesDigest(t.Context())
		require.NoError(t, err)

		// Poison the cache with an impossible value. A cache hit on the next read must return it
		// unchanged, while a recompute would overwrite it with the real digest.
		am.silencesDigestCache.mtx.Lock()
		am.silencesDigestCache.value = 0xDEADBEEF
		am.silencesDigestCache.mtx.Unlock()

		digest, err := am.silencesDigest(t.Context())
		require.NoError(t, err)
		assert.Equal(t, uint64(0xDEADBEEF), digest)
	})

	t.Run("changes when a peer's update to an existing silence is merged, which doesn't move silences.Version()", func(t *testing.T) {
		amA := newTestAlertmanager(t)
		amB := newTestAlertmanager(t)

		addTestSilence(t, amA, "original")
		silencesFrom := func(am *Alertmanager) *clusterpb.FullState {
			st, err := am.state.GetSilencesState()
			require.NoError(t, err)
			return st
		}
		require.NoError(t, amB.state.MergeFullStates([]*clusterpb.FullState{silencesFrom(amA)}))

		before, err := amB.silencesDigest(t.Context())
		require.NoError(t, err)
		version := amB.silences.Version()

		// Edit the silence in place on A, then merge A's silences state into B the way a resync does.
		// The comment makes the state bigger than half a gossip packet, so upstream's Merge treats it
		// as oversized and doesn't broadcast it, and only the merge itself can invalidate B's cache.
		sil, err := amA.silences.QueryOne(t.Context())
		require.NoError(t, err)
		sil.Comment = strings.Repeat("x", cluster.MaxGossipPacketSize)
		require.NoError(t, amA.silences.Set(t.Context(), sil))
		edited := silencesFrom(amA)
		require.True(t, cluster.OversizedMessage(edited.Parts[0].Data))
		require.NoError(t, amB.state.MergeFullStates([]*clusterpb.FullState{edited}))
		require.Equal(t, version, amB.silences.Version(), "if this changes, a digest cache keyed on Version() would become viable again")

		after, err := amB.silencesDigest(t.Context())
		require.NoError(t, err)
		require.NotEqual(t, before, after)

		want, err := amA.silencesDigest(t.Context())
		require.NoError(t, err)
		assert.Equal(t, want, after)
	})
}
