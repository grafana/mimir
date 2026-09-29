// SPDX-License-Identifier: AGPL-3.0-only

// Command fixtures builds the l1, handover and compacted bucket snapshots
// for one scale-tier profile by driving a real Mimir compactor process.
package main

import (
	"flag"
	"fmt"
	"log"
	"os/exec"
	"path/filepath"
	"runtime"
	"time"

	"github.com/grafana/mimir/tools/headcount/cardgen"
	"github.com/grafana/mimir/tools/headcount/fixtures"
	"github.com/grafana/mimir/tools/headcount/model"
)

func main() {
	dir := flag.String("dir", "", "directory to build the l1/handover/compacted snapshots in (required)")
	profile := flag.String("profile", "small", fmt.Sprintf("scale tier: one of %v", cardgen.ProfileNames()))
	seed := flag.Int64("seed", 1, "seed for the population and the generated block IDs")
	mimirBinary := flag.String("mimir-binary", "", "path to a built mimir binary; built into <dir>/mimir if empty")
	flag.Parse()

	if *dir == "" {
		log.Fatal("-dir is required")
	}

	p, err := cardgen.LoadProfile(*profile, *seed)
	if err != nil {
		log.Fatal(err)
	}

	bin := *mimirBinary
	if bin == "" {
		bin = filepath.Join(*dir, "mimir")
		log.Printf("building mimir into %s", bin)
		if err := buildMimir(bin); err != nil {
			log.Fatalf("building mimir: %v", err)
		}
	}

	t0 := time.Now()
	pop, err := model.New(p.Population)
	if err != nil {
		log.Fatalf("generating population: %v", err)
	}
	log.Printf("population: %d series across %d metric names in %s", len(pop.Series), p.Population.MetricNames, time.Since(t0))

	t0 = time.Now()
	result, err := fixtures.Build(pop, p.Partitions, *seed, fixtures.Config{Dir: *dir, MimirBinary: bin})
	if err != nil {
		log.Fatalf("building fixtures: %v", err)
	}
	log.Printf("built l1, handover and compacted snapshots in %s", time.Since(t0))
	log.Printf("l1:        %s", result.L1)
	log.Printf("handover:  %s", result.Handover)
	log.Printf("compacted: %s", result.Compacted)
}

// buildMimir compiles the mimir binary this worktree's cmd/mimir points at,
// so fixtures always exercises the compactor code the rest of this branch
// was written against rather than a stale binary on $PATH.
func buildMimir(out string) error {
	// Resolve the module root from this file's own location, so the tool
	// works regardless of the caller's working directory.
	_, thisFile, _, _ := runtime.Caller(0)
	moduleRoot := filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "..")

	cmd := exec.Command("go", "build", "-o", out, "./cmd/mimir")
	cmd.Dir = moduleRoot
	out2, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%w: %s", err, out2)
	}
	return nil
}
