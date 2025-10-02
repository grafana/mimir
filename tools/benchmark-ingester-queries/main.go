// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/grafana/mimir/pkg/ingester/lookupplan/benchmarks"
)

func main() {
	app := &app{}
	if err := app.parseArgs(os.Args); err != nil {
		slog.Error("failed to parse arguments", "err", err)
		os.Exit(1)
	}

	if err := app.run(); err != nil {
		slog.Error("unexpected error", "err", err)
		os.Exit(1)
	}
}

type app struct {
	blockDir        string
	count           uint
	ingesterAddress string
	cleanup         func()
}

func (a *app) parseArgs(args []string) error {
	fs := flag.NewFlagSet(args[0], flag.ExitOnError)

	fs.StringVar(&a.blockDir, "block-dir", ".", "Directory containing blocks to load")
	fs.UintVar(&a.count, "count", 1, "Number of benchmark iterations to run")

	if err := fs.Parse(args[1:]); err != nil {
		return fmt.Errorf("failed to parse flags: %w", err)
	}

	// Ensure block directory is absolute
	absBlockDir, err := filepath.Abs(a.blockDir)
	if err != nil {
		return fmt.Errorf("failed to resolve block directory path: %w", err)
	}
	a.blockDir = absBlockDir

	return nil
}

func (a *app) run() error {
	slog.Info("starting ingester with block directory", "block_dir", a.blockDir)

	// Start ingester and load blocks
	addr, cleanup, err := benchmarks.StartIngesterAndLoadBlocks(a.blockDir, nil)
	if err != nil {
		return fmt.Errorf("failed to start ingester: %w", err)
	}
	defer cleanup()

	a.ingesterAddress = addr
	a.cleanup = cleanup

	slog.Info("ingester started successfully", "address", addr)

	// Run basic benchmarks
	return a.runBasicBenchmarks()
}

func (a *app) runBasicBenchmarks() error {
	slog.Info("running basic benchmarks", "iterations", a.count)

	// Simple benchmark: measure ingester response time
	start := time.Now()

	for i := uint(0); i < a.count; i++ {
		// For now, just measure the time it takes to have the ingester running
		// In future iterations, we can add actual query execution here
		iterStart := time.Now()

		// Placeholder for actual query execution
		// TODO: Add query execution using createIngesterQueryable
		time.Sleep(1 * time.Millisecond) // Simulate minimal work

		iterDuration := time.Since(iterStart)
		slog.Info("benchmark iteration completed",
			"iteration", i+1,
			"duration", iterDuration,
			"address", a.ingesterAddress)
	}

	totalDuration := time.Since(start)
	avgDuration := totalDuration / time.Duration(a.count)

	slog.Info("benchmark completed",
		"total_duration", totalDuration,
		"avg_duration", avgDuration,
		"iterations", a.count)

	return nil
}
