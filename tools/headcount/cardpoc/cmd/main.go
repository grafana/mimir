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
	only := flag.String("only", "e6,e9", "with -snapshots: comma-separated experiments to run, from e6, e9, names and all")
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
	if !runSnapshots(*snapshots, pop, strings.Split(*only, ","), *out) {
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

func runSnapshots(dir string, pop *model.Model, experiments []string, out string) bool {
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
			time.UnixMilli(r.MinT).UTC().Format(time.RFC3339), time.UnixMilli(r.MaxT).UTC().Format(time.RFC3339),
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

func status(pass bool) string {
	if pass {
		return "PASS"
	}
	return "FAIL"
}
