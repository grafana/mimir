// SPDX-License-Identifier: AGPL-3.0-only

// Command compare checks the cardinality estimate route against PromQL on the same window, through one query-frontend.
//
// Each round it picks the tenant's largest metric names and highest-cardinality labels, fills each query shape with them on block, ingester, boundary and recent windows, and runs a few pairs: the estimate, and the truth count by (<label>) (last_over_time(<selector>[<window>])). It diffs the two tables group by group, reruns a mismatching pair once, and puts every difference in a class or reports it as a mismatch to file a Bug issue for. With -burst, it also sends estimates at once every -burst-interval to fill the count limits. With -estimate-only, it sends only the estimates, to measure their latency and cost without PromQL load. Results are Prometheus metrics on -listen and one log line per pair.
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
	burst                   int
	burstInterval           time.Duration
	scenarioPath            string
	estimateOnly            bool
	scenarios               []scenario
}

func (c *config) register(f *flag.FlagSet) {
	f.StringVar(&c.address, "address", "", "base URL of the query-frontend, such as http://localhost:8080 through a port-forward (required)")
	f.StringVar(&c.tenant, "tenant", "", "tenant ID sent as X-Scope-OrgID (required)")
	f.StringVar(&c.listen, "listen", "", "address to serve /metrics on; empty serves nothing")
	f.DurationVar(&c.interval, "interval", 0, "time between rounds; 0 runs one round and exits with 1 if a pair mismatched, or 3 if no pair passed, was tolerated or, with -estimate-only, answered")
	f.DurationVar(&c.gap, "request-gap", 2*time.Second, "pause between consecutive requests")
	f.DurationVar(&c.timeout, "timeout", 2*time.Minute, "timeout of each request")
	f.IntVar(&c.pairsPerRound, "pairs-per-round", 4, "pairs compared each round, drawn at random from every query on every window")
	c.queries = allQueries
	f.Var(&c.queries, "queries", "comma-separated queries to compare, from "+strings.Join(allQueries, ", "))
	c.windowLengths = []string{"1h", "6h", "24h"}
	f.Var(&c.windowLengths, "windows", "comma-separated window lengths")
	f.DurationVar(&c.windows.queryIngestersWithin, "query-ingesters-within", 13*time.Hour, "the cell's -querier.query-ingesters-within, which places the block, ingester, boundary and recent windows")
	f.DurationVar(&c.windows.lag, "lag", 5*time.Minute, "how far behind now ingester and boundary windows end")
	f.DurationVar(&c.windows.blocksMargin, "blocks-margin", time.Hour, "how far before now minus -query-ingesters-within block windows end")
	f.IntVar(&c.targets.names, "top-names", 5, "largest metric names to draw from each round")
	f.IntVar(&c.targets.labels, "top-labels", 5, "highest-cardinality labels to draw from for each name")
	f.StringVar(&c.targets.matcherLabel, "matcher-label", "job", "label whose largest values serve as the arbitrary matcher, and which the tenant-wide label table groups by")
	f.IntVar(&c.pairs.steps, "steps", 4, "buckets of the per-step query")
	f.IntVar(&c.pairs.tableLimit, "table-limit", 50, "limit on the tenant-wide tables; 0 asks for the whole table. Groups left out are checked through group_count and total")
	f.Int64Var(&c.pairs.maxReferenceSeries, "max-reference-series", 100_000, "when the tenant has more series than this over the last hour, the tenant-wide tables skip their grouped PromQL truth and count the -fallback-names largest groups one at a time; set it to the tenant's max fetched series per query. 0 never skips")
	f.Float64Var(&c.classify.sketchSigmas, "sketch-sigmas", 3, "standard errors of the HLL sketch a sketched group may be off by")
	f.DurationVar(&c.edgeSlack, "edge-slack", 5*time.Minute, "how far before the window the edge probe looks for series counted without a sample in it; set it to the tenant's largest scrape interval. 0 disables the probe")
	f.IntVar(&c.fallbackNames, "fallback-names", 5, "when PromQL refuses a table on a per-query limit, how many of its largest groups to count one at a time instead")
	f.Uint64Var(&c.seed, "seed", 0, "seed for drawing targets and pairs; 0 uses the time")
	f.IntVar(&c.burst, "burst", 0, "estimates sent at once, without their truth, to fill the count limits of the store-gateways and ingesters; 0 sends no burst")
	f.DurationVar(&c.burstInterval, "burst-interval", time.Hour, "time between bursts; the first round sends one")
	f.BoolVar(&c.estimateOnly, "estimate-only", false, "send only the estimate of each pair, without its PromQL truth, to measure the estimate's latency and cost apart from PromQL's; nothing is compared")
	f.StringVar(&c.scenarioPath, "scenario", "", "YAML file of hand-written pairs, each run every round next to the drawn ones; see the README")
}

// maxBurst caps -burst: 16 fills the default count limits of the store-gateways and ingesters, and more only adds load.
const maxBurst = 64

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
	for _, f := range []struct {
		name  string
		value int
	}{{"-pairs-per-round", c.pairsPerRound}, {"-top-names", c.targets.names}, {"-top-labels", c.targets.labels}, {"-fallback-names", c.fallbackNames}} {
		if f.value <= 0 {
			return fmt.Errorf("%s must be positive", f.name)
		}
	}
	if c.timeout <= 0 {
		return errors.New("-timeout must be positive")
	}
	if c.interval < 0 || c.gap < 0 || c.edgeSlack < 0 {
		return errors.New("-interval, -request-gap and -edge-slack must not be negative")
	}
	if c.pairs.tableLimit < 0 || c.pairs.steps < 0 || c.pairs.maxReferenceSeries < 0 {
		return errors.New("-table-limit, -steps and -max-reference-series must not be negative")
	}
	if c.burst < 0 || c.burst > maxBurst {
		return fmt.Errorf("-burst must be between 0 and %d", maxBurst)
	}
	c.scenarios = nil
	if c.scenarioPath != "" {
		var err error
		if c.scenarios, err = loadScenarios(c.scenarioPath); err != nil {
			return fmt.Errorf("-scenario: %w", err)
		}
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
	sum, err := run(ctx, cfg, newMetrics(reg, cfg.timeout), logger, time.Now)
	if err != nil {
		level.Error(logger).Log("msg", "compare failed", "err", err)
		os.Exit(1)
	}
	switch {
	case sum.mismatched:
		os.Exit(1)
	case cfg.interval == 0 && sum.compared == 0:
		level.Error(logger).Log("msg", "no pair passed, was tolerated or answered; see the refusals")
		os.Exit(exitNothingCompared)
	}
}

// exitNothingCompared is the one-shot exit code when no pair passed, was tolerated or answered, so a round that was all refusals doesn't pass.
const exitNothingCompared = 3

// summary is what the rounds found.
type summary struct {
	// mismatched is set when a pair mismatched.
	mismatched bool
	// compared counts the pairs that passed or were tolerated, and with -estimate-only the estimates that answered.
	compared int
}

// run runs rounds until ctx ends, or one round when cfg.interval is 0.
func run(ctx context.Context, cfg config, m *metrics, logger log.Logger, now func() time.Time) (summary, error) {
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
	var sum summary
	var lastBurst time.Time
	for {
		t := now()
		// Targets come from the last hour the ingesters hold.
		recent := window{kind: windowIngesters, end: t.Add(-cfg.windows.lag).Truncate(time.Minute).UnixMilli()}
		recent.start = recent.end - time.Hour.Milliseconds()
		tcfg := cfg.targets
		tcfg.facets = slices.ContainsFunc(cfg.queries, func(q string) bool { return slices.Contains(facetQueries, q) })
		targets, err := pickTargets(ctx, r.client, tcfg, recent, logger, func() { r.sleep(ctx) })
		if err != nil {
			m.rounds.WithLabelValues("failed").Inc()
			if cfg.interval == 0 {
				return sum, fmt.Errorf("picking targets: %w", err)
			}
			level.Warn(logger).Log("msg", "picking targets failed; skipping the round", "err", err)
		} else {
			for _, class := range targets.facetRefusals {
				m.refusals.WithLabelValues(queryFacetProbe, sideEstimate, class).Inc()
			}
			cands := candidates(cfg.queries, targets, cfg.windows.windows(t), pcfg, rng)
			rng.Shuffle(len(cands), func(i, j int) { cands[i], cands[j] = cands[j], cands[i] })
			if cfg.burst > 0 && (lastBurst.IsZero() || t.Sub(lastBurst) >= cfg.burstInterval) {
				r.sleep(ctx)
				r.burst(ctx, cands, cfg.burst)
				lastBurst = t
			}
			if len(cands) > cfg.pairsPerRound {
				cands = cands[:cfg.pairsPerRound]
			}
			scenarioCands, unplaced := scenarioPairs(cfg.scenarios, cfg.windows, t, rng)
			if len(unplaced) > 0 {
				level.Warn(logger).Log("msg", "no window fits these scenarios; skipping them this round", "scenarios", strings.Join(unplaced, ","))
			}
			cands = append(cands, scenarioCands...)
			for _, p := range cands {
				if ctx.Err() != nil {
					return sum, nil
				}
				r.sleep(ctx)
				var outcome string
				if cfg.estimateOnly {
					outcome = r.estimateOnly(ctx, p)
				} else {
					outcome = r.run(ctx, p).Outcome
				}
				switch outcome {
				case outcomeMismatch:
					sum.mismatched = true
				case outcomePass, outcomeTolerated, outcomeAnswered:
					sum.compared++
				}
			}
			m.rounds.WithLabelValues("ok").Inc()
		}
		m.lastRound.Set(float64(now().Unix()))
		if cfg.interval == 0 {
			return sum, nil
		}
		select {
		case <-ctx.Done():
			return sum, nil
		case <-time.After(cfg.interval):
		}
	}
}
