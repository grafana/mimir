// SPDX-License-Identifier: AGPL-3.0-only

// Command cardpoc runs the index-header series-count experiments against
// bucket snapshots built by tools/headcount/fixtures, and exits non-zero if
// any of them fails.
//
// With bucket directories as arguments it runs E1 on each. With -snapshots
// it rebuilds the population from -profile and -seed and runs the
// experiments named by -only against the l1, handover and compacted
// snapshots under that directory, without rebuilding them.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/grafana/mimir/tools/headcount/cardgen"
	"github.com/grafana/mimir/tools/headcount/cardpoc"
	"github.com/grafana/mimir/tools/headcount/model"
)

func main() {
	snapshots := flag.String("snapshots", "", "directory holding the l1, handover and compacted snapshots from tools/headcount/fixtures")
	profile := flag.String("profile", "small", fmt.Sprintf("with -snapshots: the profile the snapshots were built from, one of %v", cardgen.ProfileNames()))
	seed := flag.Int64("seed", 1, "with -snapshots: the seed the snapshots were built with")
	only := flag.String("only", "e6,e9", "with -snapshots: comma-separated experiments to run, from e6, e9, e10, names and all")
	budget := flag.Int("budget", 10_000, "with -only e10: series budget for the budgeted breakdown")
	out := flag.String("out", "", "with -only names: directory to write one <minT>-<maxT>.tsv of name and count per block range")
	flag.Parse()

	if *snapshots == "" {
		if flag.NArg() == 0 {
			log.Fatalf("usage: %s <bucket-dir>... | -snapshots <dir> [-profile p] [-seed n] [-only e6,e9]", os.Args[0])
		}
		if !runE1(flag.Args()) {
			os.Exit(1)
		}
		return
	}

	p, err := cardgen.LoadProfile(*profile, *seed)
	if err != nil {
		log.Fatal(err)
	}
	pop, err := model.New(p.Population)
	if err != nil {
		log.Fatalf("generating population: %v", err)
	}
	if !runSnapshots(*snapshots, pop, strings.Split(*only, ","), *out, *budget) {
		os.Exit(1)
	}
}

func runE1(dirs []string) bool {
	pass := true
	for _, dir := range dirs {
		results, err := cardpoc.RunE1(dir)
		if err != nil {
			log.Fatalf("%s: %v", dir, err)
		}

		var totalChecked, mismatches int
		for _, r := range results {
			totalChecked += r.ValuesChecked
			if !r.Exact() {
				mismatches++
				fmt.Printf("MISMATCH block=%s noMatcher=%q values=%v\n", r.Block, r.NoMatcherMismatch, r.Mismatches)
			}
		}
		fmt.Printf("E1 %s: %d blocks, %d values checked, %d mismatches\n", dir, len(results), totalChecked, mismatches)
		pass = pass && mismatches == 0
	}
	return pass
}

// Must match the arguments tools/headcount/fixtures/cmd passes to RunAll.
const (
	e6Threshold = 10_000
	e9TopN      = 5
)

func runSnapshots(dir string, pop *model.Model, experiments []string, out string, budget int) bool {
	l1, handover, compacted := filepath.Join(dir, "l1"), filepath.Join(dir, "handover"), filepath.Join(dir, "compacted")

	pass := true
	for _, exp := range experiments {
		switch strings.TrimSpace(exp) {
		case "all":
			r, err := cardpoc.RunAll(l1, handover, compacted, pop, e6Threshold, e9TopN)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Print(r.String(), r.Details())
			pass = pass && r.Pass()
		case "e6":
			r, err := cardpoc.RunE6(compacted, pop, e6Threshold)
			if err != nil {
				log.Fatalf("E6: %v", err)
			}
			p95Err, maxErr := r.HLLErrorStats()
			fmt.Printf("%s E6 %d names, HLL |error| p95=%.2f%% max=%.2f%%\n", status(r.Pass()), len(r.Groups), 100*p95Err, 100*maxErr)
			fmt.Print(r.Details())
			pass = pass && r.Pass()
		case "e9":
			for _, k := range []int{3, 12} {
				r, err := cardpoc.RunE9(compacted, pop, k, e9TopN)
				if err != nil {
					log.Fatalf("E9 (K=%d): %v", k, err)
				}
				fmt.Printf("%s E9 K=%d: %d above, %d naive misses\n", status(r.Pass()), k, len(r.Above), len(r.NaiveMisses))
				fmt.Print(r.Details())
				pass = pass && r.Pass()
			}
		case "e10":
			r, err := runE10(compacted, pop, budget)
			if err != nil {
				log.Fatalf("E10: %v", err)
			}
			fmt.Printf("%s E10 %s by %s over %s..%s, budget %d series\n", status(r.Pass()), shortName(r.Exact.Metric), r.Exact.Label,
				rfc3339(r.Exact.MinT), rfc3339(r.Exact.MaxT), r.Budget.MaxSeries)
			for _, b := range []cardpoc.Breakdown{r.Exact, r.Budgeted} {
				fmt.Printf("  values=%-7d total=%-7d series_touched=%-7d postings_bytes=%-8d lower_bound=%-5v %s\n",
					len(b.Counts), b.Total(), b.SeriesTouched, b.PostingsBytes, b.LowerBound, b.Elapsed.Round(time.Millisecond))
			}
			truthTotal := 0
			for _, n := range r.Truth {
				truthTotal += n
			}
			fmt.Printf("  truth values=%d total=%d\n", len(r.Truth), truthTotal)
			pass = pass && r.Pass()
		case "names":
			if err := runNames(compacted, pop, out); err != nil {
				log.Fatalf("names: %v", err)
			}
		default:
			log.Fatalf("unknown experiment %q, want e6, e9, names or all", exp)
		}
	}
	return pass
}

// runNames prints, for every block range in the compacted snapshot, the
// per-name counts read from index-headers only, their total against the
// model's truth for that range, and the cost; with out set it also writes
// each range's counts as a TSV.
func runNames(compacted string, pop *model.Model, out string) error {
	ranges, err := cardpoc.BlockRanges(compacted)
	if err != nil {
		return err
	}
	for _, r := range ranges {
		nc, err := cardpoc.NameCountsForRange(compacted, r)
		if err != nil {
			return err
		}
		truth := pop.Truth(nil, r.MinT, r.MaxT)
		fmt.Printf("names %s..%s: %d blocks, %d names, total=%d truth=%d, %s, index-header files %.1f MiB\n",
			rfc3339(r.MinT), rfc3339(r.MaxT),
			len(nc.Blocks), len(nc.Counts), nc.Total(), truth, nc.Elapsed.Round(time.Millisecond), float64(nc.IndexHeaderBytes)/(1<<20))
		if out == "" {
			continue
		}
		if err := writeNameCounts(filepath.Join(out, fmt.Sprintf("%d-%d.tsv", r.MinT, r.MaxT)), nc); err != nil {
			return err
		}
	}
	return nil
}

func writeNameCounts(path string, nc cardpoc.NameCounts) error {
	names := make([]string, 0, len(nc.Counts))
	for name := range nc.Counts {
		names = append(names, name)
	}
	sort.Strings(names)
	var b strings.Builder
	for _, name := range names {
		fmt.Fprintf(&b, "%s\t%d\n", name, nc.Counts[name])
	}
	return os.WriteFile(path, []byte(b.String()), 0o644)
}

// runE10 breaks the profile's spike metric down by pod over the block
// range holding the start of its spike day.
func runE10(compacted string, pop *model.Model, budget int) (cardpoc.E10Result, error) {
	cfg := pop.Config()
	if cfg.SpikeMetric < 0 {
		return cardpoc.E10Result{}, fmt.Errorf("the profile has no spike metric")
	}
	prefix := fmt.Sprintf("metric_%06d_", cfg.SpikeMetric)
	var metric string
	for _, s := range pop.Series {
		if name := s.Labels.Get("__name__"); strings.HasPrefix(name, prefix) {
			metric = name
			break
		}
	}
	spikeStart := cfg.Start.Add(time.Duration(cfg.SpikeDay) * 24 * time.Hour).UnixMilli()
	ranges, err := cardpoc.BlockRanges(compacted)
	if err != nil {
		return cardpoc.E10Result{}, err
	}
	for _, r := range ranges {
		if r.MinT <= spikeStart && spikeStart < r.MaxT {
			return cardpoc.RunE10(compacted, pop, r, metric, "pod", cardpoc.Budget{MaxSeries: budget})
		}
	}
	return cardpoc.E10Result{}, fmt.Errorf("no block range holds the spike day start")
}

func rfc3339(ms int64) string { return time.UnixMilli(ms).UTC().Format(time.RFC3339) }

// shortName trims a generated metric name to its "metric_NNNNNN" prefix.
func shortName(name string) string {
	if len(name) > len("metric_000000") {
		return name[:len("metric_000000")]
	}
	return name
}

func status(pass bool) string {
	if pass {
		return "PASS"
	}
	return "FAIL"
}
