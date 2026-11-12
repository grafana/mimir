// SPDX-License-Identifier: AGPL-3.0-only

// Command compare checks the cardinality estimate route against PromQL on the same window, through one query-frontend.
//
// Each round it picks the tenant's largest metric names and highest-cardinality labels, fills each query shape with them on block, ingester and boundary windows, and runs a few pairs: the estimate, and the truth count by (<label>) (last_over_time(<selector>[<window>])). It diffs the two tables group by group, reruns a mismatching pair once, and puts every difference in a class or reports it as a mismatch to file a Bug issue for. Results are Prometheus metrics on -listen and one log line per pair.
//
// Both requests load the cell's read path: keep -pairs-per-round and -request-gap at the rate agreed with the cell's owners.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/flagext"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/model"
)

type config struct {
	address, tenant, listen string
	interval, gap, timeout  time.Duration
	pairsPerRound           int
	queries                 flagext.StringSliceCSV
	windowLengths           flagext.StringSliceCSV
	windows                 windowConfig
	targets                 targetConfig
	pairs                   pairConfig
	classify                classifyConfig
	edgeSlack               time.Duration
	fallbackNames           int
	seed                    uint64
}

func (c *config) register(f *flag.FlagSet) {
	f.StringVar(&c.address, "address", "", "base URL of the query-frontend, such as http://localhost:8080 through a port-forward (required)")
	f.StringVar(&c.tenant, "tenant", "", "tenant ID sent as X-Scope-OrgID (required)")
	f.StringVar(&c.listen, "listen", "", "address to serve /metrics on; empty serves nothing")
	f.DurationVar(&c.interval, "interval", 0, "time between rounds; 0 runs one round and exits, non-zero if a pair mismatched")
	f.DurationVar(&c.gap, "request-gap", 2*time.Second, "pause between consecutive requests")
	f.DurationVar(&c.timeout, "timeout", 2*time.Minute, "timeout of each request")
	f.IntVar(&c.pairsPerRound, "pairs-per-round", 4, "pairs compared each round, drawn at random from every query on every window")
	c.queries = allQueries
	f.Var(&c.queries, "queries", "comma-separated queries to compare, from "+strings.Join(allQueries, ", "))
	c.windowLengths = []string{"1h", "6h"}
	f.Var(&c.windowLengths, "windows", "comma-separated window lengths")
	f.DurationVar(&c.windows.queryIngestersWithin, "query-ingesters-within", 13*time.Hour, "the cell's -querier.query-ingesters-within, which places the block, ingester and boundary windows")
	f.DurationVar(&c.windows.lag, "lag", 5*time.Minute, "how far behind now ingester and boundary windows end")
	f.DurationVar(&c.windows.blocksMargin, "blocks-margin", time.Hour, "how far before now minus -query-ingesters-within block windows end")
	f.IntVar(&c.targets.names, "top-names", 5, "largest metric names to draw from each round")
	f.IntVar(&c.targets.labels, "top-labels", 5, "highest-cardinality labels to draw from for each name")
	f.StringVar(&c.targets.matcherLabel, "matcher-label", "job", "label whose largest values serve as the arbitrary matcher, and which the tenant-wide label table groups by")
	f.IntVar(&c.pairs.steps, "steps", 4, "buckets of the per-step query")
	f.IntVar(&c.pairs.tableLimit, "table-limit", 0, "limit on the tenant-wide tables; 0 asks for the whole table. Groups left out are checked through group_count and total")
	f.Float64Var(&c.classify.sketchSigmas, "sketch-sigmas", 3, "standard errors of the HLL sketch a sketched group may be off by")
	f.DurationVar(&c.edgeSlack, "edge-slack", time.Minute, "how far the edge probe widens the window on each side; 0 disables the probe")
	f.IntVar(&c.fallbackNames, "fallback-names", 5, "when PromQL refuses a table by metric name, how many of the largest names to count one at a time instead")
	f.Uint64Var(&c.seed, "seed", 0, "seed for drawing targets and pairs; 0 uses the time")
}

func (c *config) validate() error {
	if c.address == "" || c.tenant == "" {
		return errors.New("-address and -tenant are required")
	}
	for _, q := range c.queries {
		if !slices.Contains(allQueries, q) {
			return fmt.Errorf("unknown query %q, want one of %s", q, strings.Join(allQueries, ", "))
		}
	}
	c.windows.lengths = nil
	for _, s := range c.windowLengths {
		d, err := model.ParseDuration(s)
		if err != nil || d <= 0 {
			return fmt.Errorf("window %q is not a positive duration", s)
		}
		c.windows.lengths = append(c.windows.lengths, time.Duration(d))
	}
	if c.pairsPerRound <= 0 {
		return errors.New("-pairs-per-round must be positive")
	}
	return nil
}

func main() {
	var cfg config
	cfg.register(flag.CommandLine)
	if err := flagext.ParseFlagsWithoutArguments(flag.CommandLine); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if err := cfg.validate(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	logger := level.NewFilter(log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr)), level.AllowInfo())
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)

	reg := prometheus.NewRegistry()
	if cfg.listen != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
		srv := &http.Server{Addr: cfg.listen, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
		go func() {
			if err := srv.ListenAndServe(); err != nil {
				level.Error(logger).Log("msg", "serving metrics", "err", err)
				os.Exit(1)
			}
		}()
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	mismatched, err := run(ctx, cfg, newMetrics(reg), logger, time.Now)
	if err != nil {
		level.Error(logger).Log("msg", "compare failed", "err", err)
		os.Exit(1)
	}
	if mismatched {
		os.Exit(1)
	}
}

// run runs rounds until ctx ends, or one round when cfg.interval is 0. It reports whether any pair mismatched.
func run(ctx context.Context, cfg config, m *metrics, logger log.Logger, now func() time.Time) (bool, error) {
	seed := cfg.seed
	if seed == 0 {
		seed = uint64(now().UnixNano())
	}
	rng := rand.New(rand.NewPCG(seed, seed))
	r := &runner{
		client:        &client{baseURL: strings.TrimRight(cfg.address, "/"), tenant: cfg.tenant, http: &http.Client{Timeout: cfg.timeout}},
		cfg:           cfg.classify,
		m:             m,
		logger:        logger,
		gap:           cfg.gap,
		edgeSlack:     cfg.edgeSlack,
		fallbackNames: cfg.fallbackNames,
	}
	pcfg := cfg.pairs
	pcfg.matcherLabel = cfg.targets.matcherLabel
	mismatched := false
	for {
		t := now()
		// Targets come from the last hour the ingesters hold.
		recent := window{kind: windowIngesters, end: t.Add(-cfg.windows.lag).Truncate(time.Minute).UnixMilli()}
		recent.start = recent.end - time.Hour.Milliseconds()
		targets, err := pickTargets(ctx, r.client, cfg.targets, recent)
		if err != nil {
			m.rounds.WithLabelValues("failed").Inc()
			if cfg.interval == 0 {
				return mismatched, fmt.Errorf("picking targets: %w", err)
			}
			level.Warn(logger).Log("msg", "picking targets failed; skipping the round", "err", err)
		} else {
			cands := candidates(cfg.queries, targets, cfg.windows.windows(t), pcfg, rng)
			rng.Shuffle(len(cands), func(i, j int) { cands[i], cands[j] = cands[j], cands[i] })
			if len(cands) > cfg.pairsPerRound {
				cands = cands[:cfg.pairsPerRound]
			}
			for _, p := range cands {
				if ctx.Err() != nil {
					return mismatched, nil
				}
				r.sleep(ctx)
				if res := r.run(ctx, p); res.Outcome == outcomeMismatch {
					mismatched = true
				}
			}
			m.rounds.WithLabelValues("ok").Inc()
		}
		m.lastRound.Set(float64(now().Unix()))
		if cfg.interval == 0 {
			return mismatched, nil
		}
		select {
		case <-ctx.Done():
			return mismatched, nil
		case <-time.After(cfg.interval):
		}
	}
}
