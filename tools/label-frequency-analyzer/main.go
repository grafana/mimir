// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bufio"
	"context"
	"encoding/csv"
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
	"time"

	"github.com/grafana/dskit/flagext"
)

const (
	fetchRetryAttempts = 5
	fetchRetryDelay    = 15 * time.Second
	pauseEveryNBatches = 50
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
	str             string
	count           uint64
	nameCount       uint64
	valueCount      uint64
	metricNameCount uint64
}

type stringFrequency struct {
	nameCount       uint64
	valueCount      uint64
	metricNameCount uint64
}

func (f stringFrequency) total() uint64 {
	return f.nameCount + f.valueCount + f.metricNameCount
}

type batchResult struct {
	stringFrequencies map[string]stringFrequency
}

func main() {
	if len(os.Args) < 2 {
		log.Fatalln("usage: label-frequency-analyzer <command> [args]\n\nCommands:\n  download    Download and analyze label frequencies from ingesters")
	}

	switch os.Args[1] {
	case "download":
		runDownloadCommand(os.Args[2:])
	default:
		log.Fatalf("unknown command: %s\n\nCommands:\n  download    Download and analyze label frequencies from ingesters", os.Args[1])
	}
}

func runDownloadCommand(args []string) {
	fs := flag.NewFlagSet("download", flag.ExitOnError)

	cfg := config{}
	cfg.TenantIDs = flagext.StringSliceCSV{}
	fs.IntVar(&cfg.PortForwardPort, "port", 0, "Port number of the port-forward to cortex-gw-internal")
	fs.Var(&cfg.TenantIDs, "tenants", "Comma-separated list of tenant IDs to analyze")
	fs.IntVar(&cfg.NumWorkers, "workers", 10, "Number of worker goroutines to use for fetching active series")
	fs.IntVar(&cfg.BatchSize, "batch-size", 50, "Number of series names to process in each batch")
	fs.IntVar(&cfg.TopN, "top-n", 5000, "Number of most common strings to display")

	if err := fs.Parse(args); err != nil {
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

func fetchSeriesNamesWithRetry(ctx context.Context, client *http.Client, port int, tenantID string, maxAttempts int, retryDelay time.Duration) ([]string, error) {
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		result, err := fetchSeriesNames(ctx, client, port, tenantID)
		if err == nil {
			return result, nil
		}
		lastErr = err
		if attempt < maxAttempts {
			log.Printf("Attempt %d/%d failed for tenant %s: %v. Retrying in %v...", attempt, maxAttempts, tenantID, err, retryDelay)
			select {
			case <-time.After(retryDelay):
			case <-ctx.Done():
				return nil, fmt.Errorf("context cancelled while waiting to retry: %w", ctx.Err())
			}
		}
	}
	return nil, fmt.Errorf("all %d attempts failed: %w", maxAttempts, lastErr)
}

func newHTTPClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
			IdleConnTimeout:     90 * time.Second,
		},
	}
}

func fetchSeriesNames(ctx context.Context, client *http.Client, port int, tenantID string) ([]string, error) {
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

func fetchActiveSeries(ctx context.Context, client *http.Client, port int, tenantID string, seriesNames []string) (*activeSeriesResponse, error) {
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

func fetchActiveSeriesWithRetry(ctx context.Context, client *http.Client, port int, tenantID string, seriesNames []string, maxAttempts int, retryDelay time.Duration) (*activeSeriesResponse, error) {
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		result, err := fetchActiveSeries(ctx, client, port, tenantID, seriesNames)
		if err == nil {
			return result, nil
		}
		lastErr = err
		if attempt < maxAttempts {
			log.Printf("Attempt %d/%d failed to fetch active series for tenant %s: %v. Retrying in %v...", attempt, maxAttempts, tenantID, err, retryDelay)
			select {
			case <-time.After(retryDelay):
			case <-ctx.Done():
				return nil, fmt.Errorf("context cancelled while waiting to retry: %w", ctx.Err())
			}
		}
	}
	return nil, fmt.Errorf("all %d attempts failed: %w", maxAttempts, lastErr)
}

func runAnalysis(ctx context.Context, cfg config, tenantID string) error {
	log.Printf("Analyzing tenant %s...", tenantID)

	client := newHTTPClient()
	seriesNames, err := fetchSeriesNamesWithRetry(ctx, client, cfg.PortForwardPort, tenantID, fetchRetryAttempts, fetchRetryDelay)
	if err != nil {
		return err
	}

	log.Printf("Found %d series names for tenant %s", len(seriesNames), tenantID)

	// Create a channel for batches of series names
	seriesChan := make(chan []string)
	var wg sync.WaitGroup

	// Channel to collect worker results
	resultChan := make(chan batchResult)

	// Calculate total number of batches
	totalBatches := (len(seriesNames) + cfg.BatchSize - 1) / cfg.BatchSize   // Ceiling division
	batchesPerWorker := (totalBatches + cfg.NumWorkers - 1) / cfg.NumWorkers // Ceiling division

	// Start worker goroutines
	for i := 0; i < cfg.NumWorkers; i++ {
		wg.Add(1)
		workerID := i // Capture worker ID for logging
		go func() {
			defer wg.Done()
			// Each worker has its own HTTP client with separate connection pool
			workerClient := newHTTPClient()
			// Each worker has its own result for final counts
			batchResult := batchResult{
				stringFrequencies: make(map[string]stringFrequency),
			}
			batchCount := 0

			for {
				select {
				case batch, ok := <-seriesChan:
					if !ok {
						// Channel closed, send results and exit
						resultChan <- batchResult
						return
					}

					batchCount++
					result, err := fetchActiveSeriesWithRetry(ctx, workerClient, cfg.PortForwardPort, tenantID, batch, fetchRetryAttempts, fetchRetryDelay)
					if err != nil {
						log.Printf("Worker %d: Failed to fetch active series (batch %d/%d): %v", workerID, batchCount, batchesPerWorker, err)
						continue
					}

					// First count strings within this batch only
					batchCounts := make(map[string]stringFrequency)
					for _, series := range result.Data {
						for name, value := range series {
							freq := batchCounts[name]
							freq.nameCount++
							batchCounts[name] = freq
							if name == "image" {
								continue
							}
							freq = batchCounts[value]
							if name == "__name__" {
								freq.metricNameCount++
							} else {
								freq.valueCount++
							}
							batchCounts[value] = freq
						}
					}

					// Only keep strings that appear 10 or more times in this batch
					keptStrings := 0
					for str, freq := range batchCounts {
						if freq.total() >= 10 {
							existing := batchResult.stringFrequencies[str]
							existing.nameCount += freq.nameCount
							existing.valueCount += freq.valueCount
							existing.metricNameCount += freq.metricNameCount
							batchResult.stringFrequencies[str] = existing
							keptStrings++
						}
					}

					log.Printf("Worker %d: Found %d active series (batch %d/%d), kept %d/%d strings",
						workerID, len(result.Data), batchCount, batchesPerWorker, keptStrings, len(batchCounts))

				case <-ctx.Done():
					// Context cancelled, send partial results and exit
					resultChan <- batchResult
					return
				}
			}
		}()
	}

	// Send batches of series names to workers
	go func() {
		defer close(seriesChan)
		reader := bufio.NewReader(os.Stdin)
		batchesSent := 0
		for i := 0; i < len(seriesNames); i += cfg.BatchSize {
			end := i + cfg.BatchSize
			if end > len(seriesNames) {
				end = len(seriesNames)
			}
			batch := seriesNames[i:end]
			select {
			case seriesChan <- batch:
				batchesSent++
				if batchesSent%pauseEveryNBatches == 0 {
					log.Printf("\n*** PAUSED after %d batches. Please restart your port-forward session. ***", batchesSent)
					log.Printf("*** Press ENTER to continue... ***\n")
					_, _ = reader.ReadString('\n')
					log.Printf("Resuming...")
				}
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
	labelCounts := make(map[string]stringFrequency)
	for result := range resultChan {
		for str, freq := range result.stringFrequencies {
			existing := labelCounts[str]
			existing.nameCount += freq.nameCount
			existing.valueCount += freq.valueCount
			existing.metricNameCount += freq.metricNameCount
			labelCounts[str] = existing
		}
	}

	// Check if context was cancelled
	if ctx.Err() != nil {
		return fmt.Errorf("analysis interrupted: %w", ctx.Err())
	}

	// Convert map to slice for sorting
	counts := make([]stringCount, 0, len(labelCounts))
	for str, freq := range labelCounts {
		counts = append(counts, stringCount{
			str:             str,
			count:           freq.total(),
			nameCount:       freq.nameCount,
			valueCount:      freq.valueCount,
			metricNameCount: freq.metricNameCount,
		})
	}

	// Sort by count in descending order
	sort.Slice(counts, func(i, j int) bool {
		return counts[i].count > counts[j].count
	})

	// Print top N most common strings
	log.Printf("\nTop %d most common strings for tenant %s:", cfg.TopN, tenantID)

	// Create CSV writer that writes to stdout
	w := csv.NewWriter(os.Stdout)
	defer w.Flush()

	// Write CSV header
	if err := w.Write([]string{"rank", "string", "count", "bytes", "countAsLabelName", "countAsLabelValue", "countAsMetricName"}); err != nil {
		return fmt.Errorf("failed to write CSV header: %w", err)
	}

	// Write data rows
	for i, sc := range counts {
		if i >= cfg.TopN {
			break
		}
		bytesUsed := len(sc.str) * int(sc.count)
		if err := w.Write([]string{
			fmt.Sprintf("%d", i+1),
			sc.str,
			fmt.Sprintf("%d", sc.count),
			fmt.Sprintf("%d", bytesUsed),
			fmt.Sprintf("%d", sc.nameCount),
			fmt.Sprintf("%d", sc.valueCount),
			fmt.Sprintf("%d", sc.metricNameCount),
		}); err != nil {
			return fmt.Errorf("failed to write CSV row: %w", err)
		}
	}

	return nil
}
