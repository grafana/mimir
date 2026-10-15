// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"strings"
	"sync"
	"time"

	"go.uber.org/atomic"
)

// driver sends the bench's reads to both ingesters and compares their answers.
type driver struct {
	gen        *queryGen
	feed       *feed
	ingesters  [2]*ingesterProcess
	slots      chan struct{}
	timeout    time.Duration
	retryDelay time.Duration
	retries    int
	// Offsets of reads are this old, so that both ingesters already consumed them and reads don't wait.
	offsetAge time.Duration

	measuring atomic.Bool
	mu        sync.Mutex
	stats     map[string]*apiStats
	examples  []string
	pending   sync.WaitGroup
}

type apiStats struct {
	sent, dropped, compared, match, matchAfterRetry, mismatch, tooLarge int
	errors                                                              [2]int
	firstErrors                                                         [2]string
	latencies                                                           [2][]time.Duration
	items                                                               int64
}

func newDriver(gen *queryGen, f *feed, ingesters [2]*ingesterProcess, concurrency int, retryDelay time.Duration) *driver {
	return &driver{
		gen: gen, feed: f, ingesters: ingesters, slots: make(chan struct{}, concurrency), timeout: time.Minute,
		retryDelay: retryDelay, retries: 2, offsetAge: 2 * time.Second, stats: map[string]*apiStats{},
	}
}

func (d *driver) statsOf(api string) *apiStats {
	s, ok := d.stats[api]
	if !ok {
		s = &apiStats{}
		d.stats[api] = s
	}
	return s
}

// run sends each RPC's reads at its rate, as Poisson arrivals, until ctx is done.
func (d *driver) run(ctx context.Context, rates map[string]float64, seed uint64) {
	var wg sync.WaitGroup
	rpcs := make([]string, 0, len(rates))
	for rpc := range rates {
		rpcs = append(rpcs, rpc)
	}
	sort.Strings(rpcs)
	for i, rpc := range rpcs {
		rate := rates[rpc]
		if rate <= 0 {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewPCG(seed, uint64(i)))
			next := time.Now()
			for {
				next = next.Add(time.Duration(r.ExpFloat64() / rate * float64(time.Second)))
				if wait := time.Until(next); wait > 0 {
					select {
					case <-ctx.Done():
						return
					case <-time.After(wait):
					}
				} else if ctx.Err() != nil {
					return
				}
				// Shapes that read only what's older than the ingesters' data go elsewhere: draw another.
				var req *request
				for range 20 {
					if req = d.gen.next(r, rpc); req != nil {
						break
					}
				}
				if req == nil {
					continue
				}
				select {
				case d.slots <- struct{}{}:
				default:
					// The bench can't send this many reads at once: they would queue and measure the queue.
					d.mu.Lock()
					d.statsOf(rpc).dropped++
					d.mu.Unlock()
					continue
				}
				d.pending.Add(1)
				go func() {
					defer d.pending.Done()
					defer func() { <-d.slots }()
					d.compare(ctx, req, nil, 0)
				}()
			}
		}()
	}
	wg.Wait()
}

// compare reads the request from both ingesters. A retry only keeps the differences that were there before, and
// differ the same way.
func (d *driver) compare(ctx context.Context, req *request, before map[uint64]int, attempt int) {
	offset := d.feed.offsetProducedBefore(d.offsetAge)
	var results [2][]uint64
	var errs [2]error
	var latencies [2]time.Duration
	var wg sync.WaitGroup
	for i, ing := range d.ingesters {
		wg.Add(1)
		go func() {
			defer wg.Done()
			readCtx, cancel := context.WithTimeout(readContext(context.Background(), req.tenant, offset), d.timeout)
			defer cancel()
			start := time.Now()
			results[i], errs[i] = read(readCtx, ing.client, req, nil)
			latencies[i] = time.Since(start)
		}()
	}
	wg.Wait()
	measuring := d.measuring.Load()
	d.mu.Lock()
	s := d.statsOf(req.api)
	if attempt == 0 {
		s.sent++
		for i := range errs {
			if errs[i] != nil && !errors.Is(errs[i], errTooLarge) {
				s.errors[i]++
				if s.firstErrors[i] == "" {
					s.firstErrors[i] = errs[i].Error()
				}
			}
			if measuring && errs[i] == nil {
				s.latencies[i] = append(s.latencies[i], latencies[i])
			}
		}
	}
	d.mu.Unlock()
	if errs[0] != nil || errs[1] != nil {
		if errors.Is(errs[0], errTooLarge) || errors.Is(errs[1], errTooLarge) {
			d.mu.Lock()
			d.statsOf(req.api).tooLarge++
			d.mu.Unlock()
		}
		return
	}
	diff := difference(results[0], results[1])
	if before != nil {
		for item, sign := range diff {
			if before[item] != sign {
				delete(diff, item)
			}
		}
	}
	d.mu.Lock()
	if attempt == 0 {
		s.compared++
		s.items += int64(len(results[0]))
	}
	switch {
	case len(diff) == 0 && attempt == 0:
		s.match++
	case len(diff) == 0:
		s.matchAfterRetry++
	case attempt < d.retries:
		d.mu.Unlock()
		// Late samples and the active series' updates settle within the delay; what still differs after is a bug.
		d.pending.Add(1)
		go func() {
			defer d.pending.Done()
			select {
			case <-time.After(d.retryDelay):
			case <-ctx.Done():
				return
			}
			d.compare(ctx, req, diff, attempt+1)
		}()
		return
	default:
		s.mismatch++
		if len(d.examples) < 50 {
			d.mu.Unlock()
			example := d.describeMismatch(req, diff)
			d.mu.Lock()
			d.examples = append(d.examples, example)
		}
	}
	d.mu.Unlock()
}

// describeMismatch reads the request once more for what the differing hashes are.
func (d *driver) describeMismatch(req *request, diff map[uint64]int) string {
	var explain [2]map[uint64]string
	for i, ing := range d.ingesters {
		explain[i] = map[uint64]string{}
		for h := range diff {
			explain[i][h] = ""
		}
		ctx, cancel := context.WithTimeout(readContext(context.Background(), req.tenant, -1), d.timeout)
		_, _ = read(ctx, ing.client, req, explain[i])
		cancel()
	}
	var a, b []string
	for h, sign := range diff {
		item := explain[0][h] + explain[1][h]
		if item == "" {
			item = fmt.Sprintf("(gone since: %x)", h)
		}
		if sign > 0 {
			a = append(a, item)
		} else {
			b = append(b, item)
		}
	}
	sort.Strings(a)
	sort.Strings(b)
	return fmt.Sprintf("%s\n  only in A (%d): %s\n  only in B (%d): %s", req, len(a), strings.Join(a[:min(5, len(a))], " | "), len(b), strings.Join(b[:min(5, len(b))], " | "))
}

func (d *driver) resetLatencies() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, s := range d.stats {
		s.latencies = [2][]time.Duration{}
	}
}
