// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/prometheus/model/labels"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/grafana/mimir/pkg/ingester/client"
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

		// Execute a simple query to benchmark ingester performance
		err := a.executeQuery("__name__", "a_100")
		if err != nil {
			slog.Warn("query execution failed", "error", err, "iteration", i+1)
		}

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

// executeQuery executes a simple query against the ingester using gRPC client
func (a *app) executeQuery(labelName, labelValue string) error {
	// Create gRPC connection to ingester
	conn, err := grpc.Dial(a.ingesterAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to ingester: %w", err)
	}
	defer conn.Close()

	// Create ingester client
	ingesterClient := client.NewIngesterClient(conn)

	// Create label matcher
	matcher, err := labels.NewMatcher(labels.MatchEqual, labelName, labelValue)
	if err != nil {
		return fmt.Errorf("failed to create label matcher: %w", err)
	}

	// Convert to client format
	labelMatchers, err := client.ToLabelMatchers([]*labels.Matcher{matcher})
	if err != nil {
		return fmt.Errorf("failed to convert label matchers: %w", err)
	}

	// Create query request
	now := time.Now()
	req := &client.QueryRequest{
		StartTimestampMs: now.Add(-time.Hour).UnixMilli(), // 1 hour ago
		EndTimestampMs:   now.UnixMilli(),                 // now
		Matchers:         labelMatchers,
	}

	// Execute query using QueryStream with user context
	ctx := user.InjectOrgID(context.Background(), "benchmark-tenant")
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	stream, err := ingesterClient.QueryStream(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}

	// Read response stream
	seriesCount := 0
	sampleCount := 0
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to receive query response: %w", err)
		}

		if len(resp.Chunkseries) > 0 {
			for _, series := range resp.Chunkseries {
				seriesCount++
				sampleCount += len(series.Chunks)
			}
		}
	}

	slog.Debug("query executed successfully",
		"label", fmt.Sprintf("%s=%s", labelName, labelValue),
		"series_count", seriesCount,
		"sample_count", sampleCount)

	return nil
}
