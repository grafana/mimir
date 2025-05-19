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
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"

	"github.com/grafana/dskit/flagext"
)

type config struct {
	PortForwardPort int
	TenantIDs       flagext.StringSliceCSV
	NumWorkers      int
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
	flag.IntVar(&cfg.NumWorkers, "workers", 2, "Number of worker goroutines to use for fetching active series")

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

func fetchActiveSeries(ctx context.Context, port int, tenantID, seriesName string) (*activeSeriesResponse, error) {
	client := &http.Client{}
	url := fmt.Sprintf("http://localhost:%d/prometheus/api/v1/cardinality/active_series?selector=%s", port, seriesName)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("X-Scope-OrgID", tenantID)
	req.Header.Set("Sharding-Control", "4")

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

	// Create a channel for series names
	seriesChan := make(chan string)
	var wg sync.WaitGroup

	// Channel to collect worker results
	resultChan := make(chan map[string]uint64)

	// Start worker goroutines
	for i := 0; i < cfg.NumWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Each worker has its own map
			workerCounts := make(map[string]uint64)

			for {
				select {
				case seriesName, ok := <-seriesChan:
					if !ok {
						// Channel closed, send results and exit
						resultChan <- workerCounts
						return
					}

					result, err := fetchActiveSeries(ctx, cfg.PortForwardPort, tenantID, seriesName)
					if err != nil {
						log.Printf("Failed to fetch active series for %s: %v", seriesName, err)
						continue
					}

					// Count every string in the response using worker's local map
					for _, series := range result.Data {
						for name, value := range series {
							workerCounts[name]++
							workerCounts[value]++
						}
					}

					log.Printf("Found %d active series for %s", len(result.Data), seriesName)

				case <-ctx.Done():
					// Context cancelled, send partial results and exit
					resultChan <- workerCounts
					return
				}
			}
		}()
	}

	// Send series names to workers
	go func() {
		defer close(seriesChan)
		for _, seriesName := range seriesNames {
			select {
			case seriesChan <- seriesName:
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

	// Print top 500
	log.Printf("\nTop 500 most common strings for tenant %s:", tenantID)
	for i, sc := range counts {
		if i >= 500 {
			break
		}
		log.Printf("%d: %s (count: %d)", i+1, sc.str, sc.count)
	}

	return nil
}
