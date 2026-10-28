// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand/v2" //#nosec G404 -- Synthetic data and reads, reproducible from a seed: no need for a CSPRNG -- nosemgrep: math-random-used
	"slices"
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
	// The share of reads whose responses are compared.
	compareFraction float64
	// Offsets of reads are this old, so that both ingesters already consumed them and reads don't wait.
	offsetAge time.Duration

	measuring atomic.Bool
	// Reads slower than slowRead, written as they end.
	slowLog  io.Writer
	slowMu   sync.Mutex
	slowest  []SlowRead
	recorder *recorder
	mu       sync.Mutex
	stats    map[string]*apiStats
	examples []string
	pending  sync.WaitGroup

	// The window's reads B answered slowest compared to A, the most slower first.
	slowerInB []timedRead
}

type apiStats struct {
	sent, dropped, compared, match, matchAfterRetry, mismatch, tooLarge int
	errors                                                              [2]int
	firstErrors                                                         [2]string
	latencies                                                           [2][]time.Duration
	items                                                               int64
	// The slowest reads of the window on each ingester, slowest first.
	slowest [2][]timedRead
}

type timedRead struct {
	took    [2]time.Duration
	request string
}

// slowestKept is how many of each API's slowest reads on each ingester the report has.
const slowestKept = 10

// slowerInBKept is how many of the reads B answered slowest compared to A the report has.
const slowerInBKept = 100

// keepSlowerInB keeps the read among those B answered slowest compared to A. Callers hold d.mu.
func (d *driver) keepSlowerInB(took [2]time.Duration, req *request) {
	slower := took[1] - took[0]
	kept := d.slowerInB
	if slower <= 0 || (len(kept) == slowerInBKept && kept[len(kept)-1].took[1]-kept[len(kept)-1].took[0] >= slower) {
		return
	}
	position := sort.Search(len(kept), func(j int) bool { return kept[j].took[1]-kept[j].took[0] < slower })
	kept = slices.Insert(kept, position, timedRead{took: took, request: req.String()})
	d.slowerInB = kept[:min(len(kept), slowerInBKept)]
}

// keepSlowest keeps the read among the slowest on ingester i.
func (s *apiStats) keepSlowest(i int, took [2]time.Duration, req *request) {
	kept := s.slowest[i]
	if len(kept) == slowestKept && kept[len(kept)-1].took[i] >= took[i] {
		return
	}
	position := sort.Search(len(kept), func(j int) bool { return kept[j].took[i] < took[i] })
	kept = slices.Insert(kept, position, timedRead{took: took, request: req.String()})
	s.slowest[i] = kept[:min(len(kept), slowestKept)]
}

func newDriver(gen *queryGen, f *feed, ingesters [2]*ingesterProcess, concurrency int, retryDelay time.Duration) *driver {
	return &driver{
		gen: gen, feed: f, ingesters: ingesters, slots: make(chan struct{}, concurrency), timeout: time.Minute,
		retryDelay: retryDelay, retries: 2, offsetAge: 2 * time.Second, stats: map[string]*apiStats{}, compareFraction: 1,
	}
}

// slowRead is how long a read takes before the bench logs it, with its request, to find what an ingester struggles with.
const slowRead = 5 * time.Second

func (d *driver) logSlow(ingester string, took time.Duration, err error, req *request) {
	d.slowMu.Lock()
	defer d.slowMu.Unlock()
	line := fmt.Sprintf("%s slow read on %s: %s err=%v %s\n", time.Now().UTC().Format(time.TimeOnly), ingester, took.Round(time.Millisecond), err, req)
	if d.slowLog != nil {
		_, _ = io.WriteString(d.slowLog, line)
	}
	// The slowest reads, for the report.
	errText := ""
	if err != nil {
		errText = err.Error()
	}
	d.slowest = append(d.slowest, SlowRead{At: time.Now().Unix(), Ingester: ingester, Ms: float64(took.Microseconds()) / 1000, Error: errText, Request: req.String()})
	sort.Slice(d.slowest, func(a, b int) bool { return d.slowest[a].Ms > d.slowest[b].Ms })
	d.slowest = d.slowest[:min(len(d.slowest), 200)]
	log.Print(strings.TrimSpace(line[:min(len(line), 2000)]))
}

func (d *driver) statsOf(api string) *apiStats {
	s, ok := d.stats[api]
	if !ok {
		s = &apiStats{}
		d.stats[api] = s
	}
	return s
}

// run sends each RPC's reads at its rate, as Poisson arrivals, until ctx is done. Their retries wait on retries
// instead, so that the differences found last still get theirs once reads stop.
func (d *driver) run(ctx, retries context.Context, rates map[string]float64, seed uint64) {
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
					d.compare(retries, req, nil, 0)
				}()
			}
		}()
	}
	wg.Wait()
}

// compare reads the request from both ingesters. A retry only keeps the differences that were there before, and
// differ the same way.
func (d *driver) compare(ctx context.Context, req *request, before map[uint64]int, attempt int) {
	// Every read is sent and timed; only some are compared, whose normalizing takes the bench more CPU than the reads
	// take the ingesters.
	comparing := attempt > 0 || rand.Float64() < d.compareFraction
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
			var responses []any
			responses, errs[i] = fetch(readCtx, ing.client, req)
			latencies[i] = time.Since(start)
			if latencies[i] > slowRead {
				d.logSlow(ing.name, latencies[i], errs[i], req)
			}
			if errs[i] == nil && comparing {
				results[i], errs[i] = normalize(req, responses, nil)
			}
		}()
	}
	wg.Wait()
	measuring := d.measuring.Load()
	if attempt == 0 && d.recorder != nil {
		// A response too large to compare was still read.
		readErrs := errs
		for i := range readErrs {
			if errors.Is(readErrs[i], errTooLarge) {
				readErrs[i] = nil
			}
		}
		d.recorder.read(req.api, latencies, readErrs)
	}
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
				s.keepSlowest(i, latencies, req)
			}
		}
		if measuring && errs[0] == nil && errs[1] == nil {
			d.keepSlowerInB(latencies, req)
		}
	}
	d.mu.Unlock()
	if !comparing && errs[0] == nil && errs[1] == nil {
		return
	}
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
		if d.recorder != nil {
			d.recorder.outcome(req.api, false)
		}
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
		if d.recorder != nil {
			d.recorder.outcome(req.api, true)
		}
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
	d.slowerInB = nil
	for _, s := range d.stats {
		s.latencies = [2][]time.Duration{}
		s.slowest = [2][]timedRead{}
	}
}

// SlowRead is a read that took over slowRead on an ingester.
type SlowRead struct {
	At       int64   `json:"t"`
	Ingester string  `json:"ingester"`
	Ms       float64 `json:"ms"`
	Error    string  `json:"error,omitempty"`
	Request  string  `json:"request"`
}

func (d *driver) slowReads() []SlowRead {
	d.slowMu.Lock()
	defer d.slowMu.Unlock()
	return append([]SlowRead(nil), d.slowest...)
}
