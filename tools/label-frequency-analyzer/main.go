// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"

	"github.com/grafana/dskit/flagext"
)

type config struct {
	PortForwardPort int
	TenantIDs       flagext.StringSliceCSV
	NumWorkers      int
	BatchSize       int
	TopN            int
}

type labelValuesResponse struct {
	Status string   `json:"status"`
	Data   []string `json:"data"`
}

type activeSeriesResponse struct {
	Data []map[string]string `json:"data"`
}

type stringCount struct {
	str   string
	count uint64
}

func main() {
	// Clean up all flags registered via init() methods of 3rd-party libraries.
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	cfg := config{}
	cfg.TenantIDs = flagext.StringSliceCSV{}
	flag.IntVar(&cfg.PortForwardPort, "port", 0, "Port number of the port-forward to cortex-gw-internal")
	flag.Var(&cfg.TenantIDs, "tenants", "Comma-separated list of tenant IDs to analyze")
	flag.IntVar(&cfg.NumWorkers, "workers", 10, "Number of worker goroutines to use for fetching active series")
	flag.IntVar(&cfg.BatchSize, "batch-size", 50, "Number of series names to process in each batch")
	flag.IntVar(&cfg.TopN, "top-n", 200, "Number of most common strings to display")

	// Parse CLI flags.
	if err := flagext.ParseFlagsWithoutArguments(flag.CommandLine); err != nil {
		log.Fatalln(err.Error())
	}

	if cfg.PortForwardPort == 0 {
		log.Fatalln("port-forward port must be specified")
	}

	if len(cfg.TenantIDs) == 0 {
		log.Fatalln("no tenant IDs specified")
	}

	if cfg.NumWorkers <= 0 {
		log.Fatalln("number of workers must be positive")
	}

	if cfg.BatchSize <= 0 {
		log.Fatalln("batch size must be positive")
	}

	if cfg.TopN <= 0 {
		log.Fatalln("top-n must be positive")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT)
	defer cancel()

	for _, tenantID := range cfg.TenantIDs {
		if err := runAnalysis(ctx, cfg, tenantID); err != nil {
			log.Printf("analysis failed for tenant %s: %v", tenantID, err)
			continue
		}
	}
}

func fetchSeriesNames(ctx context.Context, port int, tenantID string) ([]string, error) {
	client := &http.Client{}
	req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("http://localhost:%d/prometheus/api/v1/label/__name__/values", port), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("X-Scope-OrgID", tenantID)

	log.Printf("Fetching series names for tenant %s...", tenantID)
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch series names: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to fetch series names: HTTP %d\nResponse: %s", resp.StatusCode, string(body))
	}

	var result labelValuesResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode series names response: %w", err)
	}

	if result.Status != "success" {
		return nil, fmt.Errorf("unexpected response status: %s", result.Status)
	}

	return result.Data, nil
}

func fetchActiveSeries(ctx context.Context, port int, tenantID string, seriesNames []string) (*activeSeriesResponse, error) {
	client := &http.Client{}
	// Create regex selector for the batch of series names
	selector := fmt.Sprintf(`{__name__=~"%s"}`, strings.Join(seriesNames, "|"))
	url := fmt.Sprintf("http://localhost:%d/prometheus/api/v1/cardinality/active_series?selector=%s", port, url.QueryEscape(selector))
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("X-Scope-OrgID", tenantID)
	req.Header.Set("Sharding-Control", "32")

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch active series: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to fetch active series: HTTP %d\nResponse: %s", resp.StatusCode, string(body))
	}

	var result activeSeriesResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode active series response: %w", err)
	}

	return &result, nil
}

func runAnalysis(ctx context.Context, cfg config, tenantID string) error {
	log.Printf("Analyzing tenant %s...", tenantID)

	seriesNames, err := fetchSeriesNames(ctx, cfg.PortForwardPort, tenantID)
	if err != nil {
		return err
	}

	log.Printf("Found %d series names for tenant %s", len(seriesNames), tenantID)

	// Create a channel for batches of series names
	seriesChan := make(chan []string)
	var wg sync.WaitGroup

	// Channel to collect worker results
	resultChan := make(chan map[string]uint64)

	// Calculate total number of batches
	totalBatches := (len(seriesNames) + cfg.BatchSize - 1) / cfg.BatchSize   // Ceiling division
	batchesPerWorker := (totalBatches + cfg.NumWorkers - 1) / cfg.NumWorkers // Ceiling division

	// Start worker goroutines
	for i := 0; i < cfg.NumWorkers; i++ {
		wg.Add(1)
		workerID := i // Capture worker ID for logging
		go func() {
			defer wg.Done()
			// Each worker has its own map for final counts
			workerCounts := make(map[string]uint64)
			batchCount := 0

			for {
				select {
				case batch, ok := <-seriesChan:
					if !ok {
						// Channel closed, send results and exit
						resultChan <- workerCounts
						return
					}

					batchCount++
					result, err := fetchActiveSeries(ctx, cfg.PortForwardPort, tenantID, batch)
					if err != nil {
						log.Printf("Worker %d: Failed to fetch active series (batch %d/%d): %v", workerID, batchCount, batchesPerWorker, err)
						continue
					}

					// First count strings within this batch only
					batchCounts := make(map[string]uint64)
					for _, series := range result.Data {
						for name, value := range series {
							batchCounts[name]++
							// Skip counting values for __name__ label
							if name != "__name__" {
								batchCounts[value]++
							}
						}
					}

					// Only keep strings that appear 10 or more times in this batch
					keptStrings := 0
					for str, count := range batchCounts {
						if count >= 10 {
							workerCounts[str] += count
							keptStrings++
						}
					}

					log.Printf("Worker %d: Found %d active series (batch %d/%d), kept %d/%d strings",
						workerID, len(result.Data), batchCount, batchesPerWorker, keptStrings, len(batchCounts))

				case <-ctx.Done():
					// Context cancelled, send partial results and exit
					resultChan <- workerCounts
					return
				}
			}
		}()
	}

	// Send batches of series names to workers
	go func() {
		defer close(seriesChan)
		for i := 0; i < len(seriesNames); i += cfg.BatchSize {
			end := i + cfg.BatchSize
			if end > len(seriesNames) {
				end = len(seriesNames)
			}
			batch := seriesNames[i:end]
			select {
			case seriesChan <- batch:
			case <-ctx.Done():
				return
			}
		}
	}()

	// Wait for all workers to finish sending work
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Merge all worker results
	labelCounts := make(map[string]uint64)
	for workerCounts := range resultChan {
		for str, count := range workerCounts {
			labelCounts[str] += count
		}
	}

	// Check if context was cancelled
	if ctx.Err() != nil {
		return fmt.Errorf("analysis interrupted: %w", ctx.Err())
	}

	// Convert map to slice for sorting
	counts := make([]stringCount, 0, len(labelCounts))
	for str, count := range labelCounts {
		counts = append(counts, stringCount{str, count})
	}

	// Sort by count in descending order
	sort.Slice(counts, func(i, j int) bool {
		return counts[i].count > counts[j].count
	})

	// Print top N most common strings
	log.Printf("\nTop %d most common strings for tenant %s:", cfg.TopN, tenantID)
	for i, sc := range counts {
		if i >= cfg.TopN {
			break
		}
		log.Printf("%d. %s: %d", i+1, sc.str, sc.count)
	}

	return nil
}
