// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
)

// Label lookups read the cold blocks with the shard unlocked while pushes, idle eviction into cold blocks, their merges
// and retention go on: what they find is always there and never another block's or a closed one's.
func TestEngineLabelLookupsReadColdBlocksWhileTheyChange(t *testing.T) {
	ctx := context.Background()
	engine, err := OpenEngine(t.TempDir(), "tenant", EngineOptions{Shards: 4, IdleEvictionMs: 60_000, RetentionMs: 0, SecondaryHashFunction: secondaryHash})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	appendBatch := func(ts int64, round int) {
		app := engine.Appender(ctx)
		for n := range 50 {
			// Series that never stop, and series of a round that stop and leave for cold blocks.
			_, err := app.Append(0, promlabels.FromStrings("__name__", "stable", "job", "job-stable", "pod", fmt.Sprintf("pod-stable-%d", n)), 0, ts, 1, nil, nil, storage.AppendV2Options{})
			require.NoError(t, err)
			_, err = app.Append(0, promlabels.FromStrings("__name__", fmt.Sprintf("churn_%d", n%5), "job", fmt.Sprintf("job-%d", round%7), "pod", fmt.Sprintf("pod-%d-%d", round, n)), 0, ts, 1, nil, nil, storage.AppendV2Options{})
			require.NoError(t, err)
		}
		require.NoError(t, app.Commit())
	}
	const minute = int64(60_000)
	appendBatch(0, 0)
	done := make(chan struct{})
	var readers sync.WaitGroup
	errs := make(chan error, 8)
	for range 4 {
		readers.Add(1)
		go func() {
			defer readers.Done()
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
				jobs, _, err := q.LabelValues(ctx, "job", nil)
				if err == nil {
					var pods []string
					pods, _, err = q.LabelValues(ctx, "pod", nil, promlabels.MustNewMatcher(promlabels.MatchRegexp, "job", "job-.*"))
					for _, pod := range pods {
						if !strings.HasPrefix(pod, "pod-") {
							err = fmt.Errorf("a pod value of another label or block: %q", pod)
						}
					}
				}
				if err == nil {
					var names []string
					names, _, err = q.LabelNames(ctx, nil)
					if err == nil && !slicesContainAll(names, "__name__", "job", "pod") {
						err = fmt.Errorf("label names %v", names)
					}
				}
				if err == nil && !slicesContainAll(jobs, "job-stable") {
					err = fmt.Errorf("the stable series' job is missing from %v", jobs)
				}
				for _, job := range jobs {
					if err == nil && !strings.HasPrefix(job, "job-") {
						err = fmt.Errorf("a job value of another label or block: %q", job)
					}
				}
				_ = q.Close()
				if err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	for round := 1; round < 60; round++ {
		appendBatch(int64(round)*5*minute, round)
		require.NoError(t, engine.Compact(ctx))
		if round%5 == 0 {
			// Retention drops the oldest blocks, which reads may still have.
			for _, shard := range engine.store.shards {
				shard.Lock()
				shard.cold.pruneBefore(int64(round-10) * 5 * minute)
				shard.Unlock()
			}
		}
	}
	close(done)
	readers.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}

func slicesContainAll(values []string, want ...string) bool {
	for _, w := range want {
		found := false
		for _, v := range values {
			found = found || v == w
		}
		if !found {
			return false
		}
	}
	return true
}
