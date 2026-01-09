// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/mimir/pkg/querier/api"
)

func main() {
	cfg := &Config{}
	cfg.RegisterFlags(flag.CommandLine)

	if err := flagext.ParseFlagsWithoutArguments(flag.CommandLine); err != nil {
		fmt.Fprintf(os.Stderr, "error parsing flags: %v\n", err)
		os.Exit(1)
	}

	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "error validating config: %v\n", err)
		os.Exit(1)
	}

	if err := run(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run(cfg *Config) error {
	ctx := context.Background()

	// Create file cache.
	cache, err := NewFileCache(cfg.CacheEnabled, cfg.CacheDir)
	if err != nil {
		return fmt.Errorf("failed to create cache: %w", err)
	}

	// Create Mimir client (with caching wrapper).
	mimirClient := NewCachedMimirClient(
		NewMimirClient(cfg.MimirAddress, cfg.MimirAuthType, cfg.TenantID, cfg.MimirUsername, cfg.MimirPassword),
		cache,
		cfg.Namespace,
		cfg.TenantID,
	)

	// Step 1: Get all label names.
	fmt.Println("Fetching label names from Mimir...")
	labelNames, err := mimirClient.GetLabelNames(ctx)
	if err != nil {
		return fmt.Errorf("failed to get label names: %w", err)
	}

	fmt.Printf("Found %d label names\n", len(labelNames))

	// Step 2: Get series count for each label by querying label_values.
	fmt.Println("Fetching series counts per label...")

	allLabelStats, totalSeriesCount, failedLabels := fetchLabelValuesStats(ctx, mimirClient, labelNames)

	// Report failures.
	if len(failedLabels) > 0 {
		fmt.Printf("\nWarning: failed to fetch cardinality for %d labels: %v\n", len(failedLabels), failedLabels)
	}

	fmt.Printf("Total series count: %d\n", totalSeriesCount)

	// Step 3: Query Loki for query stats logs and analyze queries.
	fmt.Printf("\n--- Querying Loki for query stats logs ---\n")
	lokiClient := NewCachedLokiClient(
		NewLokiClient(cfg.LokiAddress, cfg.LokiAuthType, cfg.TenantID, cfg.LokiUsername, cfg.LokiPassword),
		cache,
	)
	analyzer := NewAnalyzer()

	// Query user queries from query-frontend.
	userQueriesStart := time.Time(cfg.UserQueriesStart)
	userQueriesEnd := time.Time(cfg.UserQueriesEnd)
	fmt.Printf("Querying user queries from %s to %s (namespace: %s, tenant: %s)\n",
		userQueriesStart.Format(time.RFC3339), userQueriesEnd.Format(time.RFC3339), cfg.Namespace, cfg.TenantID)

	err = lokiClient.QueryQueryStats(ctx, cfg.Namespace, cfg.TenantID, "query-frontend", userQueriesStart, userQueriesEnd, func(entry QueryStatsEntry) error {
		analyzer.ProcessQuery(entry.Query, UserQuery)
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to query Loki for user queries: %w", err)
	}

	// Query rule queries from ruler-query-frontend.
	ruleQueriesStart := time.Time(cfg.RuleQueriesStart)
	ruleQueriesEnd := time.Time(cfg.RuleQueriesEnd)
	fmt.Printf("Querying rule queries from %s to %s (namespace: %s, tenant: %s)\n",
		ruleQueriesStart.Format(time.RFC3339), ruleQueriesEnd.Format(time.RFC3339), cfg.Namespace, cfg.TenantID)

	err = lokiClient.QueryQueryStats(ctx, cfg.Namespace, cfg.TenantID, "ruler-query-frontend", ruleQueriesStart, ruleQueriesEnd, func(entry QueryStatsEntry) error {
		analyzer.ProcessQuery(entry.Query, RuleQuery)
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to query Loki for rule queries: %w", err)
	}

	fmt.Printf("\nTotal queries analyzed: %d (user: %d, rules: %d)\n",
		analyzer.TotalQueries(), analyzer.TotalUserQueries(), analyzer.TotalRuleQueries())

	// Step 4: Combine Mimir and Loki analysis and output results.
	fmt.Printf("\n--- Segmentation Label Analysis Results ---\n")
	fmt.Printf("Total series: %d\n", totalSeriesCount)
	fmt.Printf("Total queries analyzed: %d (user: %d, rules: %d)\n\n",
		analyzer.TotalQueries(), analyzer.TotalUserQueries(), analyzer.TotalRuleQueries())

	userQueryDuration := userQueriesEnd.Sub(userQueriesStart)
	ruleQueryDuration := ruleQueriesEnd.Sub(ruleQueriesStart)
	labelStats := analyzer.GetLabelStats(allLabelStats, totalSeriesCount, userQueryDuration, ruleQueryDuration)

	// Sort by series coverage descending.
	sort.Slice(labelStats, func(i, j int) bool {
		return labelStats[i].SeriesCoverage > labelStats[j].SeriesCoverage
	})

	fmt.Println("Top labels by series coverage:")
	printLabelStatsTable(labelStats, 30)

	// Sort by query coverage descending.
	sort.Slice(labelStats, func(i, j int) bool {
		return labelStats[i].QueryCoverage > labelStats[j].QueryCoverage
	})

	fmt.Println("\nTop labels by query coverage:")
	printLabelStatsTable(labelStats, 30)

	// Identify good segmentation candidates: score > 0.5.
	fmt.Printf("\n--- Segmentation Label Candidates ---\n")
	fmt.Printf("Labels with score > 0.5:\n\n")

	var candidates []LabelStats
	for _, ls := range labelStats {
		if ls.Score > 0.5 {
			candidates = append(candidates, ls)
		}
	}

	// Sort candidates by score descending.
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Score > candidates[j].Score
	})

	if len(candidates) == 0 {
		fmt.Println("  No labels meet both criteria.")
	} else {
		printCandidatesTable(candidates)
	}

	return nil
}

// fetchLabelValuesStats fetches label values cardinality for all labels in parallel.
// It returns a map of label name to stats, total series count, and any labels that failed to fetch.
func fetchLabelValuesStats(ctx context.Context, mimirClient *CachedMimirClient, labelNames []string) (map[string]LabelSeriesStats, uint64, []string) {
	// Query label values in batches to get series counts.
	// We use limit=20 to get per-value cardinality for distribution uniformity calculation.
	// The API returns values sorted by series count (highest first), so the top 20 values
	// capture the most significant portion of the distribution. If a label is skewed
	// (e.g., 99% of series in one value), this will be evident in the top values.
	// If series are evenly distributed, the top 20 will show similar counts.
	// The long tail of small values contributes minimally to the entropy calculation.
	const batchSize = 20
	const cardinalityLimit = 20
	const fetchConcurrency = 4

	// Create batches of label names.
	var batches [][]string
	for i := 0; i < len(labelNames); i += batchSize {
		end := min(i+batchSize, len(labelNames))
		batches = append(batches, labelNames[i:end])
	}

	// Track results and failures.
	var (
		resultsMx        sync.Mutex
		allLabelStats    = make(map[string]LabelSeriesStats)
		totalSeriesCount uint64
		failedMx         sync.Mutex
		failedLabels     []string
		completedBatches int
	)

	// Start a ticker to print progress periodically.
	ticker := time.NewTicker(5 * time.Second)
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				resultsMx.Lock()
				completed := completedBatches
				resultsMx.Unlock()
				pct := float64(completed) * 100 / float64(len(batches))
				fmt.Printf("Progress: %.0f%% (%d/%d batches)\n", pct, completed, len(batches))
			case <-done:
				return
			}
		}
	}()

	// processResponse extracts label stats from a cardinality response.
	processResponse := func(resp *api.LabelValuesCardinalityResponse) {
		resultsMx.Lock()
		defer resultsMx.Unlock()

		// Use the total series count from the first successful response (it's the same for all).
		if totalSeriesCount == 0 {
			totalSeriesCount = resp.SeriesCountTotal
		}

		for _, label := range resp.Labels {
			// Extract per-value series counts for entropy calculation.
			seriesCountPerValue := make([]uint64, 0, len(label.Cardinality))
			for _, v := range label.Cardinality {
				seriesCountPerValue = append(seriesCountPerValue, v.SeriesCount)
			}

			allLabelStats[label.LabelName] = LabelSeriesStats{
				SeriesCount:         label.SeriesCount,
				ValuesCount:         label.LabelValuesCount,
				SeriesCountPerValue: seriesCountPerValue,
			}
		}
	}

	// Fetch label values in parallel.
	_ = concurrency.ForEachJob(ctx, len(batches), fetchConcurrency, func(ctx context.Context, idx int) error {
		batch := batches[idx]

		labelValuesResp, err := mimirClient.GetLabelValuesCardinality(ctx, batch, cardinalityLimit)
		if err != nil {
			// If timeout, retry each label individually.
			if isTimeoutError(err) {
				for _, labelName := range batch {
					singleResp, singleErr := mimirClient.GetLabelValuesCardinality(ctx, []string{labelName}, cardinalityLimit)
					if singleErr != nil {
						failedMx.Lock()
						failedLabels = append(failedLabels, labelName)
						failedMx.Unlock()
						continue
					}
					processResponse(singleResp)
				}
			} else {
				// Non-timeout error - mark all labels in batch as failed.
				failedMx.Lock()
				failedLabels = append(failedLabels, batch...)
				failedMx.Unlock()
			}
		} else {
			processResponse(labelValuesResp)
		}

		resultsMx.Lock()
		completedBatches++
		resultsMx.Unlock()

		return nil
	})

	// Stop the ticker.
	ticker.Stop()
	close(done)

	return allLabelStats, totalSeriesCount, failedLabels
}

func printLabelStatsTable(stats []LabelStats, limit int) {
	columns := []TableColumn{
		{Header: "Label name", Align: AlignLeft},
		{Header: "Series", Align: AlignRight},
		{Header: "Unique values", Align: AlignRight},
		{Header: "All queries", Align: AlignRight},
	}

	rows := make([]TableRow, 0, limit)
	for i, ls := range stats {
		if i >= limit {
			break
		}
		rows = append(rows, TableRow{
			ls.Name,
			fmt.Sprintf("%.2f%%", ls.SeriesCoverage),
			fmt.Sprintf("%d", ls.ValuesCount),
			fmt.Sprintf("%.2f%%", ls.QueryCoverage),
		})
	}

	PrintTable(columns, rows)
}

func printCandidatesTable(candidates []LabelStats) {
	columns := []TableColumn{
		{Header: "Label name", Align: AlignLeft},
		{Header: "Score", Align: AlignRight},
		{Header: "Series", Align: AlignRight},
		{Header: "All queries", Align: AlignRight},
		{Header: "User queries", Align: AlignRight},
		{Header: "Rule queries", Align: AlignRight},
		{Header: "Unique values", Align: AlignRight},
		{Header: "Values distribution", Align: AlignRight},
		{Header: "Avg values/query", Align: AlignRight},
	}

	rows := make([]TableRow, 0, len(candidates))
	for _, ls := range candidates {
		rows = append(rows, TableRow{
			ls.Name,
			fmt.Sprintf("%.2f", ls.Score),
			fmt.Sprintf("%.2f%%", ls.SeriesCoverage),
			fmt.Sprintf("%.2f%%", ls.QueryCoverage),
			fmt.Sprintf("%.2f%%", ls.UserQueryCoverage),
			fmt.Sprintf("%.2f%%", ls.RuleQueryCoverage),
			fmt.Sprintf("%d", ls.ValuesCount),
			fmt.Sprintf("%.2f", ls.LabelValuesDistribution),
			fmt.Sprintf("%.2f", ls.AvgDistinctValuesPerQuery),
		})
	}

	PrintTable(columns, rows)
}

// isTimeoutError returns true if the error is a timeout error.
func isTimeoutError(err error) bool {
	// Check for context deadline exceeded.
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	// Check for net.Error timeout (e.g., http.Client.Timeout).
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}

	return false
}
