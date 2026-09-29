// SPDX-License-Identifier: AGPL-3.0-only

// Command cardpoc runs the index-header series-count experiments against
// one or more bucket snapshots built by tools/headcount/fixtures, and exits
// non-zero if any of them fails.
package main

import (
	"fmt"
	"log"
	"os"

	"github.com/grafana/mimir/tools/headcount/cardpoc"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("usage: %s <bucket-dir>...", os.Args[0])
	}

	var anyFailed bool
	for _, dir := range os.Args[1:] {
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
		anyFailed = anyFailed || mismatches > 0
	}

	if anyFailed {
		os.Exit(1)
	}
}
