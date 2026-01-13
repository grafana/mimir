// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"cmp"
	"context"
	"flag"
	"fmt"
	"os"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/flagext"
	"github.com/prometheus/common/model"

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
		if slices.Contains(failedLabels, model.MetricNameLabel) {
			fmt.Println("Error: __name__ label is required but failed to fetch. Exiting.")
			os.Exit(1)
		}
	}

	fmt.Printf("Total series count: %d\n", totalSeriesCount)

	// Step 3: Query Loki for query stats logs and analyze queries.
	lokiClient := NewCachedLokiClient(
		NewLokiClient(cfg.LokiAddress, cfg.LokiAuthType, cfg.TenantID, cfg.LokiUsername, cfg.LokiPassword),
		cache,
	)
	analyzer := NewAnalyzer()

	// Query user queries from query-frontend.
	userQueriesStart := time.Time(cfg.UserQueriesStart)
	userQueriesEnd := time.Time(cfg.UserQueriesEnd)
	fmt.Printf("Querying user queries from %s to %s\n",
		userQueriesStart.Format(time.RFC3339), userQueriesEnd.Format(time.RFC3339))

	userQueriesCount, userSkipped, err := queryLokiStats(ctx, lokiClient, analyzer, cfg.Namespace, cfg.TenantID, "query-frontend", userQueriesStart, userQueriesEnd, UserQuery)
	if err != nil {
		return fmt.Errorf("failed to query Loki for user queries: %w", err)
	}
	fmt.Printf("Processed %d user queries\n", userQueriesCount)
	if userSkipped > 0 {
		fmt.Printf("Warning: skipped %d malformed log entries for user queries\n", userSkipped)
	}

	// Query rule queries from ruler-query-frontend.
	ruleQueriesStart := time.Time(cfg.RuleQueriesStart)
	ruleQueriesEnd := time.Time(cfg.RuleQueriesEnd)
	fmt.Printf("Querying rule queries from %s to %s\n",
		ruleQueriesStart.Format(time.RFC3339), ruleQueriesEnd.Format(time.RFC3339))

	ruleQueriesCount, ruleSkipped, err := queryLokiStats(ctx, lokiClient, analyzer, cfg.Namespace, cfg.TenantID, "ruler-query-frontend", ruleQueriesStart, ruleQueriesEnd, RuleQuery)
	if err != nil {
		return fmt.Errorf("failed to query Loki for rule queries: %w", err)
	}
	fmt.Printf("Processed %d rule queries\n", ruleQueriesCount)
	if ruleSkipped > 0 {
		fmt.Printf("Warning: skipped %d malformed log entries for rule queries\n", ruleSkipped)
	}

	fmt.Printf("\nTotal queries analyzed: %d (user: %d, rules: %d)\n",
		analyzer.TotalQueries(), analyzer.TotalUserQueries(), analyzer.TotalRuleQueries())

	// Step 4: Combine Mimir and Loki analysis and output results.
	printSectionTitle("Segmentation Label Analysis Results")
	fmt.Printf("Tenant ID:  %s\n", cfg.TenantID)
	fmt.Printf("Namespace:  %s\n", cfg.Namespace)
	fmt.Printf("Total series: %s\n", formatNumberWithCommas(totalSeriesCount))
	fmt.Printf("Total queries analyzed: %s (user: %s, rules: %s)\n",
		formatNumberWithCommas(uint64(analyzer.TotalQueries())),
		formatNumberWithCommas(uint64(analyzer.TotalUserQueries())),
		formatNumberWithCommas(uint64(analyzer.TotalRuleQueries())))

	userQueryDuration := userQueriesEnd.Sub(userQueriesStart)
	ruleQueryDuration := ruleQueriesEnd.Sub(ruleQueriesStart)
	labelStats := analyzer.GetLabelStats(allLabelStats, totalSeriesCount, userQueryDuration, ruleQueryDuration)

	// Identify good segmentation candidates: score > 0.5.
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

	fmt.Println()
	if len(candidates) == 0 {
		fmt.Println("No candidates found (score > 0.5).")
	} else {
		printCandidatesTable(candidates)

		// Print top metric names if __name__ is a candidate.
		for _, ls := range candidates {
			if ls.Name == model.MetricNameLabel {
				if len(ls.TopValuesSeries) > 0 {
					fmt.Println("\nTop ingested metric names:")
					for _, v := range ls.TopValuesSeries {
						fmt.Printf("- %s (%.0f%%)\n", v.Value, v.Percent)
					}
				}
				if len(ls.TopValuesQueries) > 0 {
					fmt.Println("\nTop queried metric names:")
					for _, v := range ls.TopValuesQueries {
						fmt.Printf("- %s (%.0f%%)\n", v.Value, v.Percent)
					}
				}
				break
			}
		}

		printSectionTitle("Top Values per Label")
		printTopValuesTable(candidates)
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
	const fetchConcurrency = 8

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
	ticker := time.NewTicker(15 * time.Second)
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
			topValues := make([]LabelValueSeriesCount, 0, len(label.Cardinality))
			for _, v := range label.Cardinality {
				topValues = append(topValues, LabelValueSeriesCount{
					Value:       v.LabelValue,
					SeriesCount: v.SeriesCount,
				})
			}

			// Ensure sorted in descending order (API should return sorted, but verify).
			if !slices.IsSortedFunc(topValues, func(a, b LabelValueSeriesCount) int {
				return cmp.Compare(b.SeriesCount, a.SeriesCount)
			}) {
				slices.SortFunc(topValues, func(a, b LabelValueSeriesCount) int {
					return cmp.Compare(b.SeriesCount, a.SeriesCount)
				})
			}

			allLabelStats[label.LabelName] = LabelSeriesStats{
				SeriesCount: label.SeriesCount,
				ValuesCount: label.LabelValuesCount,
				TopValues:   topValues,
			}
		}
	}

	// Fetch label values in parallel.
	_ = concurrency.ForEachJob(ctx, len(batches), fetchConcurrency, func(ctx context.Context, idx int) error {
		batch := batches[idx]

		labelValuesResp, err := mimirClient.GetLabelValuesCardinality(ctx, batch, cardinalityLimit)
		if err != nil {
			if isTimeoutError(err) {
				// Timeout - retry each label individually as fallback.
				for _, label := range batch {
					singleResp, singleErr := mimirClient.GetLabelValuesCardinality(ctx, []string{label}, cardinalityLimit)
					if singleErr != nil {
						failedMx.Lock()
						failedLabels = append(failedLabels, label)
						failedMx.Unlock()
					} else {
						processResponse(singleResp)
					}
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

// splitTimeRangeUTC splits a time range into UTC-aligned chunks.
// Split duration is dynamic: <=10m range uses 30s chunks, <=60m uses 5m chunks, otherwise 30m chunks.
func splitTimeRangeUTC(start, end time.Time) []struct{ Start, End time.Time } {
	var chunks []struct{ Start, End time.Time }

	// Determine chunk duration based on total time range.
	totalDuration := end.Sub(start)
	var chunkDuration time.Duration
	switch {
	case totalDuration <= 10*time.Minute:
		chunkDuration = 30 * time.Second
	case totalDuration <= 60*time.Minute:
		chunkDuration = 5 * time.Minute
	default:
		chunkDuration = 30 * time.Minute
	}

	current := start
	for current.Before(end) {
		// Next boundary aligned to chunk duration (or end if sooner).
		nextBoundary := current.Truncate(chunkDuration).Add(chunkDuration)
		chunkEnd := nextBoundary
		if end.Before(nextBoundary) {
			chunkEnd = end
		}

		chunks = append(chunks, struct{ Start, End time.Time }{current, chunkEnd})
		current = chunkEnd
	}
	return chunks
}

// queryLokiStats queries Loki for query stats and processes them with the analyzer.
// Returns the number of queries processed, number of skipped entries, and any error.
func queryLokiStats(
	ctx context.Context,
	lokiClient *CachedLokiClient,
	analyzer *Analyzer,
	namespace, tenantID, container string,
	start, end time.Time,
	queryType QueryType,
) (int, int, error) {
	const fetchConcurrency = 8

	// Split time range into UTC-aligned chunks.
	chunks := splitTimeRangeUTC(start, end)

	var (
		resultsMx        sync.Mutex
		queriesProcessed int
		skippedEntries   int
		completedChunks  int
		errorsMx         sync.Mutex
		errs             []error
	)

	// Progress ticker.
	ticker := time.NewTicker(15 * time.Second)
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				resultsMx.Lock()
				completed := completedChunks
				resultsMx.Unlock()
				pct := float64(completed) * 100 / float64(len(chunks))
				fmt.Printf("Progress: %.0f%% (%d/%d chunks)\n", pct, completed, len(chunks))
			case <-done:
				return
			}
		}
	}()

	_ = concurrency.ForEachJob(ctx, len(chunks), fetchConcurrency, func(ctx context.Context, idx int) error {
		chunk := chunks[idx]

		skipped, err := lokiClient.QueryQueryStats(ctx, namespace, tenantID, container, chunk.Start, chunk.End, func(entry QueryStatsEntry) error {
			resultsMx.Lock()
			analyzer.ProcessQuery(entry.Query, queryType)
			queriesProcessed++
			resultsMx.Unlock()
			return nil
		})

		resultsMx.Lock()
		skippedEntries += skipped
		resultsMx.Unlock()

		if err != nil {
			errorsMx.Lock()
			errs = append(errs, err)
			errorsMx.Unlock()
		}

		resultsMx.Lock()
		completedChunks++
		resultsMx.Unlock()

		return nil
	})

	ticker.Stop()
	close(done)

	if len(errs) > 0 {
		return queriesProcessed, skippedEntries, fmt.Errorf("some chunks failed: %v", errs)
	}

	return queriesProcessed, skippedEntries, nil
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
		{Header: "Avg values/query", Align: AlignRight},
		{Header: "Series values dist", Align: AlignRight},
		{Header: "Top values series %", Align: AlignRight},
		{Header: "Query values dist", Align: AlignRight},
		{Header: "Top values queries %", Align: AlignRight},
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
			fmt.Sprintf("%.2f", ls.AvgDistinctValuesPerQuery),
			fmt.Sprintf("%.2f", ls.SeriesValuesDistribution),
			formatTopValuesPercent(ls.TopValuesSeries),
			fmt.Sprintf("%.2f", ls.QueryValuesDistribution),
			formatTopValuesPercent(ls.TopValuesQueries),
		})
	}

	PrintTable(columns, rows)
}

func printTopValuesTable(candidates []LabelStats) {
	columns := []TableColumn{
		{Header: "Label", Align: AlignLeft},
		{Header: "Top values series", Align: AlignLeft},
		{Header: "Top values queries", Align: AlignLeft},
	}

	rows := make([]TableRow, 0, len(candidates))
	for _, ls := range candidates {
		rows = append(rows, TableRow{
			ls.Name,
			formatTopValuesNameAndPercent(ls.TopValuesSeries),
			formatTopValuesNameAndPercent(ls.TopValuesQueries),
		})
	}

	PrintTable(columns, rows)
}

// formatTopValuesPercent formats the top values as "45%, 20%, 10%".
func formatTopValuesPercent(values []LabelValuePercent) string {
	if len(values) == 0 {
		return "-"
	}
	parts := make([]string, len(values))
	for i, v := range values {
		parts[i] = fmt.Sprintf("%.0f%%", v.Percent)
	}
	return strings.Join(parts, ", ")
}

// formatTopValuesNameAndPercent formats the top values with each on a separate line.
func formatTopValuesNameAndPercent(values []LabelValuePercent) string {
	if len(values) == 0 {
		return "-"
	}
	parts := make([]string, len(values))
	for i, v := range values {
		parts[i] = fmt.Sprintf("%s (%.0f%%)", v.Value, v.Percent)
	}
	return strings.Join(parts, "\n")
}

// printSectionTitle prints a styled section title.
func printSectionTitle(title string) {
	fmt.Println()
	fmt.Println(strings.Repeat("=", len(title)+4))
	fmt.Printf("  %s\n", title)
	fmt.Println(strings.Repeat("=", len(title)+4))
	fmt.Println()
}

// formatNumberWithCommas formats a number with thousands separators.
func formatNumberWithCommas(n uint64) string {
	str := fmt.Sprintf("%d", n)
	if len(str) <= 3 {
		return str
	}

	var result strings.Builder
	for i, c := range str {
		if i > 0 && (len(str)-i)%3 == 0 {
			result.WriteRune(',')
		}
		result.WriteRune(c)
	}
	return result.String()
}
