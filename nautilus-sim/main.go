package main

import (
	"fmt"
	"os"
	"time"
)

func main() {
	cfg := parseConfig()

	start := time.Now()
	fmt.Fprintf(os.Stderr, "Nautilus Sharding Simulator\n")
	fmt.Fprintf(os.Stderr, "  partitions=%d ingesters=%d series=%d metrics=%d steps=%d\n",
		cfg.NumPartitions, cfg.NumIngesters, cfg.NumSyntheticSeries, cfg.NumSyntheticMetrics, cfg.NumSteps)
	fmt.Fprintf(os.Stderr, "  ingestion rebalance interval=%d, query rebalance interval=%d\n",
		cfg.IngestionRebalanceInterval, cfg.QueryRebalanceInterval)
	fmt.Fprintf(os.Stderr, "  balance target=%.2f, seed=%d\n", cfg.BalanceTarget, cfg.Seed)

	sim, err := NewSimulation(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "Initialized in %v\n", time.Since(start))
	sim.Run()
	fmt.Fprintf(os.Stderr, "Done in %v\n", time.Since(start))
}
