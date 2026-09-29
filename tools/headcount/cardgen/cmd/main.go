// SPDX-License-Identifier: AGPL-3.0-only

// Command cardgen writes a synthetic tenant's level-1 blocks into a
// filesystem bucket, ready for a native compactor run.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/grafana/mimir/tools/headcount/cardgen"
	"github.com/grafana/mimir/tools/headcount/model"
)

func main() {
	bucket := flag.String("bucket", "", "filesystem bucket directory (required)")
	profile := flag.String("profile", "small", fmt.Sprintf("scale tier: one of %v", cardgen.ProfileNames()))
	seed := flag.Int64("seed", 1, "seed for both the population and the generated block IDs")
	flag.Parse()

	if *bucket == "" {
		log.Fatal("-bucket is required")
	}

	p, err := cardgen.LoadProfile(*profile, *seed)
	if err != nil {
		log.Fatal(err)
	}

	t0 := time.Now()
	pop, err := model.New(p.Population)
	if err != nil {
		log.Fatalf("generating population: %v", err)
	}
	log.Printf("population: %d series across %d metric names in %s", len(pop.Series), p.Population.MetricNames, time.Since(t0))

	userDir := filepath.Join(*bucket, "anonymous")
	if err := os.MkdirAll(userDir, 0o755); err != nil {
		log.Fatal(err)
	}

	t0 = time.Now()
	metas, err := cardgen.Generate(pop, userDir, cardgen.Config{Partitions: p.Partitions, Seed: *seed})
	if err != nil {
		log.Fatalf("generating blocks: %v", err)
	}
	log.Printf("wrote %d blocks in %s", len(metas), time.Since(t0))
}
