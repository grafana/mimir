// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"

	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
)

// Label lookups over the head let the pushes waiting on a shard through as they go, while series are added, leave for
// cold blocks and move to other arrays, and the postings are rebuilt: the values and names of the series there all
// along are always found.
func TestEngineLabelLookupsPauseWhileTheHeadChanges(t *testing.T) {
	ctx := context.Background()
	readPauses.Store(0)
	readRestarts.Store(0)
	engine, err := OpenEngine(t.TempDir(), "tenant", EngineOptions{Shards: 4, IdleEvictionMs: 60_000, SecondaryHashFunction: secondaryHash})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	// Enough series in each shard for a lookup to pause many times.
	const stable = 4000
	stablePods := make([]string, stable)
	for n := range stable {
		stablePods[n] = fmt.Sprintf("pod-stable-%d", n)
	}
	slices.Sort(stablePods)
	appendRound := func(ts int64, round int) {
		app := engine.Appender(ctx)
		for n := range stable {
			_, err := app.Append(0, promlabels.FromStrings("__name__", fmt.Sprintf("stable_%d", n%7), "job", "job-stable", "pod", fmt.Sprintf("pod-stable-%d", n)), 0, ts, 1, nil, nil, storage.AppendV2Options{})
			require.NoError(t, err)
		}
		// Series of a round, which stop and leave for cold blocks, and grow the groups past their room.
		for n := range 500 {
			_, err := app.Append(0, promlabels.FromStrings("__name__", fmt.Sprintf("stable_%d", n%7), "job", fmt.Sprintf("job-%d", round%5), "pod", fmt.Sprintf("pod-%d-%d", round, n), "round", fmt.Sprint(round)), 0, ts, 1, nil, nil, storage.AppendV2Options{})
			require.NoError(t, err)
		}
		require.NoError(t, app.Commit())
	}
	const minute = int64(60_000)
	appendRound(0, 0)
	done := make(chan struct{})
	var readers sync.WaitGroup
	errs := make(chan error, 16)
	contains := func(got []string, want ...string) error {
		for _, value := range want {
			if _, ok := slices.BinarySearch(got, value); !ok {
				return fmt.Errorf("%q missing among %d values", value, len(got))
			}
		}
		return nil
	}
	lookups := []func(q storage.Querier) error{
		// Without matchers: the label's postings.
		func(q storage.Querier) error {
			pods, _, err := q.LabelValues(ctx, "pod", nil)
			if err != nil {
				return err
			}
			return contains(pods, stablePods...)
		},
		// A regex over every value: the label's dictionary, then a walk of the series it selects.
		func(q storage.Querier) error {
			pods, _, err := q.LabelValues(ctx, "pod", nil, promlabels.MustNewMatcher(promlabels.MatchRegexp, "pod", ".*stable.*"))
			if err != nil {
				return err
			}
			return contains(pods, stablePods...)
		},
		// A walk of every series of a name group.
		func(q storage.Querier) error {
			jobs, _, err := q.LabelValues(ctx, "job", nil, promlabels.MustNewMatcher(promlabels.MatchEqual, "__name__", "stable_3"))
			if err != nil {
				return err
			}
			return contains(jobs, "job-stable")
		},
		func(q storage.Querier) error {
			names, _, err := q.LabelNames(ctx, nil)
			if err != nil {
				return err
			}
			return contains(names, "__name__", "job", "pod")
		},
		func(q storage.Querier) error {
			names, _, err := q.LabelNames(ctx, nil, promlabels.MustNewMatcher(promlabels.MatchRegexp, "pod", "pod-stable-1.*"))
			if err != nil {
				return err
			}
			return contains(names, "__name__", "job", "pod")
		},
	}
	for index := range 2 * len(lookups) {
		readers.Add(1)
		go func() {
			defer readers.Done()
			lookup := lookups[index%len(lookups)]
			for {
				select {
				case <-done:
					return
				default:
				}
				q, err := engine.Querier(0, 1<<40)
				if err != nil {
					errs <- err
					return
				}
				err = lookup(q)
				_ = q.Close()
				if err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	for round := 1; round <= 40; round++ {
		appendRound(int64(round)*5*minute, round)
		// The rounds before the last two are idle: they leave the head, for cold blocks, which the postings are then
		// rebuilt without.
		require.NoError(t, engine.Compact(ctx))
		if round%10 == 0 {
			engine.rebuildStalePostings()
		}
	}
	close(done)
	readers.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Positive(t, readPauses.Load(), "lookups paused")
	require.Positive(t, readRestarts.Load(), "lookups started again for the series removed or moved during a pause")
}
