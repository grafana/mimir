// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"fmt"
	"os"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
)

// selectorStats counts how often the same selector comes in, to find what a cache of what selectors match would save:
// the rulers and dashboards that send a selector again every minute are all but a cache hit. Temporary.
type selectorStats struct {
	mu      sync.Mutex
	started time.Time
	calls   map[string]*selectorCount
}

type selectorCount struct {
	n        int
	total    time.Duration
	first    time.Duration
	selected int
}

const selectorWindow = 10 * time.Minute

var selectors selectorStats

// selectorKey is the matchers without the query shard: shards of one query are the same selector.
func selectorKey(matchers []compiledMatcher) string {
	parts := make([]string, 0, len(matchers))
	for index := range matchers {
		m := &matchers[index]
		if m.kind == kindShard {
			continue
		}
		value := m.value
		if m.re != nil {
			value = m.re.pattern
		}
		parts = append(parts, fmt.Sprintf("%s:%d:%s", m.name, m.kind, value))
	}
	slices.Sort(parts)
	return strings.Join(parts, ",")
}

func (s *selectorStats) observe(tenant, key string, took time.Duration, selected int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	if s.calls == nil {
		s.calls, s.started = map[string]*selectorCount{}, now
	}
	count := s.calls[key]
	if count == nil {
		count = &selectorCount{first: took}
		s.calls[key] = count
	}
	count.n++
	count.total += took
	count.selected = selected
	if now.Sub(s.started) < selectorWindow {
		return
	}
	var calls, repeats int
	var total, repeated time.Duration
	type top struct {
		key   string
		count *selectorCount
	}
	var tops []top
	for key, count := range s.calls {
		calls += count.n
		repeats += count.n - 1
		total += count.total
		repeated += count.total - count.first
		tops = append(tops, top{key, count})
	}
	sort.Slice(tops, func(a, b int) bool { return tops[a].count.total > tops[b].count.total })
	fmt.Fprintf(os.Stderr, "selectorlog tenant=%s window=%s calls=%d unique=%d repeatPct=%.0f totalMs=%d repeatMs=%d\n", tenant, now.Sub(s.started).Round(time.Second), calls, len(s.calls), 100*float64(repeats)/float64(max(calls, 1)), total.Milliseconds(), repeated.Milliseconds())
	for index := 0; index < min(5, len(tops)); index++ {
		key := tops[index].key
		if len(key) > 160 {
			key = key[:160]
		}
		fmt.Fprintf(os.Stderr, "selectorlog   n=%d ms=%d selected=%d %s\n", tops[index].count.n, tops[index].count.total.Milliseconds(), tops[index].count.selected, key)
	}
	s.calls, s.started = nil, now
}
