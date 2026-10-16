// SPDX-License-Identifier: AGPL-3.0-only

// Command replay pushes a synthetic series population into a running Mimir
// through Prometheus remote write, and prints the population's ground-truth
// series counts for the same time grid, so that a count served by Mimir can
// be checked against what was written.
//
// Time model. A profile's population is defined over a fixed window
// [Start, End). The writer moves that window so that Start lands on the
// wall-clock time given by -start, which defaults to now minus -backfill
// rounded down to the scrape interval. Samples are written on the grid
// s_k = start + k*scrape-interval, one per series live at s_k, with every
// series of one grid point pushed before the next grid point begins. Grid
// points before now are the backfill and are written as fast as the
// -rate limit allows; with -live the writer then waits for each later
// grid point and writes it when the wall clock reaches it, so the head
// grows in real time.
//
// Out-of-order and age limits. Mimir's default out-of-order window is 0,
// so a sample is accepted only if it is newer than the series' previous
// sample and not older than about one hour before the newest sample the
// ingester has seen (half the 2h head chunk range). Writing in grid order
// satisfies both for any backfill length, as long as the target tenant
// holds no data newer than the backfill start: pushing a backfill into a
// tenant that already has recent samples needs
// -ingester.out-of-order-time-window set to at least the backfill length.
// The default -validation.past-grace-period of 0 means the distributor
// does not reject old samples by wall clock.
//
// Usage:
//
//	replay write -url http://localhost:8080 -tenant demo -profile live -backfill 1h -live
//	replay truth -profile live -start <printed by write> -selector '{__name__=~".+"}'
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/prometheus/promql/parser"
	"golang.org/x/time/rate"

	"github.com/grafana/mimir/tools/headcount/cardgen"
	"github.com/grafana/mimir/tools/headcount/model"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	var err error
	switch os.Args[1] {
	case "write":
		err = runWrite(os.Args[2:])
	case "truth":
		err = runTruth(os.Args[2:])
	default:
		usage()
	}
	if err != nil {
		log.Fatal(err)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: replay write [flags] | replay truth [flags]")
	os.Exit(2)
}

// common holds the flags that write and truth must agree on for a truth
// query to describe what the writer pushed.
type common struct {
	profile  string
	seed     int64
	start    string
	interval time.Duration
}

func (c *common) register(fs *flag.FlagSet) {
	fs.StringVar(&c.profile, "profile", "live", fmt.Sprintf("population profile, one of %v", cardgen.ProfileNames()))
	fs.Int64Var(&c.seed, "seed", 1, "population seed")
	fs.StringVar(&c.start, "start", "", "wall-clock time (RFC 3339) the population's window starts at; write defaults it to now minus -backfill, truth requires it")
	fs.DurationVar(&c.interval, "scrape-interval", 15*time.Second, "spacing of sample timestamps")
}

// load generates the population shifted so that its window starts at
// start, and returns it with the shifted config.
func (c *common) load(start time.Time) (*model.Model, model.Config, error) {
	p, err := cardgen.LoadProfile(c.profile, c.seed)
	if err != nil {
		return nil, model.Config{}, err
	}
	cfg := shiftConfig(p.Population, start)
	m, err := model.New(cfg)
	if err != nil {
		return nil, model.Config{}, err
	}
	return m, cfg, nil
}

func parseTime(s string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("parsing time %q: %w", s, err)
	}
	return t.UTC(), nil
}

func runWrite(args []string) error {
	fs := flag.NewFlagSet("write", flag.ExitOnError)
	var c common
	c.register(fs)
	url := fs.String("url", "http://localhost:8080", "Mimir base URL; /api/v1/push is appended")
	tenant := fs.String("tenant", "demo", "tenant ID sent as X-Scope-OrgID")
	backfill := fs.Duration("backfill", 0, "how far before now the population window starts when -start is unset; this span is written as fast as -rate allows")
	live := fs.Bool("live", false, "after the backfill, keep writing each grid point when the wall clock reaches it")
	end := fs.String("end", "", "wall-clock time (RFC 3339) to stop at; default is the end of the population window")
	batch := fs.Int("batch", 1000, "series per push request")
	concurrency := fs.Int("concurrency", 4, "push requests in flight at once")
	limit := fs.Float64("rate", 80_000, "maximum samples per second during the backfill; 0 disables the limit")
	retries := fs.Int("retries", 10, "retries for 429 and 5xx responses")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *batch <= 0 || *concurrency <= 0 {
		return errors.New("-batch and -concurrency must be positive")
	}

	now := time.Now().UTC()
	var start time.Time
	if c.start != "" {
		var err error
		if start, err = parseTime(c.start); err != nil {
			return err
		}
	} else {
		start = alignDown(now.Add(-*backfill), c.interval)
	}
	if !start.Equal(alignDown(start, c.interval)) {
		return fmt.Errorf("-start %s is not a multiple of -scrape-interval %s", start.Format(time.RFC3339Nano), c.interval)
	}

	m, cfg, err := c.load(start)
	if err != nil {
		return err
	}
	stop := cfg.End
	if *end != "" {
		e, err := parseTime(*end)
		if err != nil {
			return err
		}
		if e.Before(stop) {
			stop = e
		}
	}
	if !*live && now.Before(stop) {
		stop = now
	}
	log.Printf("profile %s seed %d: %d series, window starts %s, writing grid points in [%s, %s) every %s (pass -start %s to truth)",
		c.profile, c.seed, len(m.Series), start.Format(time.RFC3339), start.Format(time.RFC3339), stop.Format(time.RFC3339), c.interval, start.Format(time.RFC3339))

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	w := &writer{
		pusher: &pusher{
			url:     strings.TrimRight(*url, "/") + "/api/v1/push",
			tenant:  *tenant,
			client:  &http.Client{Timeout: 60 * time.Second},
			retries: *retries,
		},
		series:      m.Series,
		batch:       *batch,
		concurrency: *concurrency,
		limiter:     rate.NewLimiter(rate.Inf, 0),
	}
	if *limit > 0 {
		w.limiter = rate.NewLimiter(rate.Limit(*limit), int(*limit))
	}

	steps := stepTimes(start, stop, c.interval)
	var written, samples int
	t0 := time.Now()
	for _, t := range steps {
		wall := time.UnixMilli(t)
		if wait := time.Until(wall); wait > 0 {
			if written > 0 && w.limiter.Limit() != rate.Inf {
				log.Printf("backfill done: %d grid points, %d samples in %s; now live", written, samples, time.Since(t0).Round(time.Second))
				w.limiter = rate.NewLimiter(rate.Inf, 0) // Live writes are paced by the grid, not the limiter.
			}
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(wait):
			}
		}
		n, err := w.writeStep(ctx, t, float64(written))
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("grid point %s: %w", wall.UTC().Format(time.RFC3339), err)
		}
		written++
		samples += n
		if written%20 == 0 || time.Until(wall) > -c.interval {
			log.Printf("wrote t=%s: %d live series (%d grid points, %d samples total)", wall.UTC().Format(time.RFC3339), n, written, samples)
		}
	}
	log.Printf("done: %d grid points, %d samples in %s", written, samples, time.Since(t0).Round(time.Second))
	return nil
}

// writer pushes the live series of one grid point at a time.
type writer struct {
	pusher      *pusher
	series      []model.Series
	batch       int
	concurrency int
	limiter     *rate.Limiter
}

// writeStep pushes a sample at t for every series live at t and returns
// how many it pushed. It returns after every push has been acknowledged,
// so samples reach Mimir in grid order per series.
func (w *writer) writeStep(ctx context.Context, t int64, value float64) (int, error) {
	live := liveAt(w.series, t)
	batches := make(chan []int)
	errs := make(chan error, w.concurrency)
	var wg sync.WaitGroup
	for range w.concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for b := range batches {
				if err := w.limiter.WaitN(ctx, len(b)); err != nil {
					errs <- err
					return
				}
				_, body, err := buildRequest(w.series, b, t, value)
				if err == nil {
					err = w.pusher.push(ctx, body)
				}
				if err != nil {
					errs <- err
					return
				}
			}
		}()
	}
send:
	for i := 0; i < len(live); i += w.batch {
		select {
		case batches <- live[i:min(i+w.batch, len(live))]:
		case <-ctx.Done():
			break send
		}
	}
	close(batches)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			return 0, err
		}
	}
	return len(live), nil
}

func runTruth(args []string) error {
	fs := flag.NewFlagSet("truth", flag.ExitOnError)
	var c common
	c.register(fs)
	selector := fs.String("selector", `{__name__=~".+"}`, "PromQL selector to count")
	at := fs.String("time", "", "evaluation time (RFC 3339); default now, rounded down to the scrape interval")
	lookback := fs.Duration("lookback", 5*time.Minute, "lookback of the PromQL instant query being checked")
	by := fs.String("by", "", "also print the count broken down by this label, as count by (label) would")
	top := fs.Int("top", 0, "with -by, print only the n largest values; 0 prints all")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if c.start == "" {
		return errors.New("-start is required and must be the value the writer printed")
	}
	start, err := parseTime(c.start)
	if err != nil {
		return err
	}
	evalTime := alignDown(time.Now(), c.interval)
	if *at != "" {
		if evalTime, err = parseTime(*at); err != nil {
			return err
		}
	}
	matchers, err := parser.NewParser(parser.Options{}).ParseMetricSelector(*selector)
	if err != nil {
		return fmt.Errorf("parsing selector: %w", err)
	}

	m, _, err := c.load(start)
	if err != nil {
		return err
	}

	// A PromQL instant query at t with lookback L selects the newest sample
	// of each series in the left-open window (t-L, t], so the model window
	// is [t-L+1ms, t+1ms).
	hi := evalTime.UnixMilli()
	lo := hi - lookback.Milliseconds() + 1
	fmt.Printf("eval_time=%s lookback=%s selector=%s\n", evalTime.Format(time.RFC3339), *lookback, *selector)
	fmt.Printf("truth=%d\n", m.Truth(matchers, lo, hi+1))
	keep := func(s model.Series) bool {
		for _, mt := range matchers {
			if !mt.Matches(s.Labels.Get(mt.Name)) {
				return false
			}
		}
		return true
	}
	fmt.Printf("sampled_truth=%d\n", sampledTruth(m.Series, keep, c.interval, lo, hi))

	if *by != "" {
		counts := m.TruthBy(matchers, *by, lo, hi+1)
		type kv struct {
			k string
			v int
		}
		var rows []kv
		for k, v := range counts {
			rows = append(rows, kv{k, v})
		}
		sort.Slice(rows, func(i, j int) bool {
			if rows[i].v != rows[j].v {
				return rows[i].v > rows[j].v
			}
			return rows[i].k < rows[j].k
		})
		if *top > 0 && len(rows) > *top {
			rows = rows[:*top]
		}
		fmt.Printf("by %s: %d values\n", *by, len(counts))
		for _, r := range rows {
			fmt.Printf("%s\t%d\n", r.k, r.v)
		}
	}
	return nil
}
