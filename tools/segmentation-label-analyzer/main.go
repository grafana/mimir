// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/grafana/dskit/flagext"
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

	// Create Mimir client.
	mimirClient := NewMimirClient(cfg.MimirAddress, cfg.MimirUsername, cfg.MimirPassword)

	// Step 1: Get all label names.
	fmt.Println("Fetching label names from Mimir...")
	labelNamesResp, err := mimirClient.GetLabelNamesCardinality(ctx, 500)
	if err != nil {
		return fmt.Errorf("failed to get label names cardinality: %w", err)
	}

	fmt.Printf("Found %d label names\n", labelNamesResp.LabelNamesCount)

	// Step 2: Get series count for each label by querying label_values.
	// We query in batches of 100 (API limit).
	fmt.Println("Fetching series counts per label...")

	labelNames := make([]string, 0, len(labelNamesResp.Cardinality))
	for _, item := range labelNamesResp.Cardinality {
		labelNames = append(labelNames, item.LabelName)
	}

	// Query label values in batches to get series counts.
	const batchSize = 100
	var totalSeriesCount uint64
	allLabelStats := make(map[string]struct {
		seriesCount uint64
		valuesCount uint64
	})

	for i := 0; i < len(labelNames); i += batchSize {
		end := i + batchSize
		if end > len(labelNames) {
			end = len(labelNames)
		}
		batch := labelNames[i:end]

		labelValuesResp, err := mimirClient.GetLabelValuesCardinality(ctx, batch, 1)
		if err != nil {
			return fmt.Errorf("failed to get label values cardinality: %w", err)
		}

		// Use the total series count from the first batch (it's the same for all).
		if totalSeriesCount == 0 {
			totalSeriesCount = labelValuesResp.SeriesCountTotal
		}

		for _, label := range labelValuesResp.Labels {
			allLabelStats[label.LabelName] = struct {
				seriesCount uint64
				valuesCount uint64
			}{
				seriesCount: label.SeriesCount,
				valuesCount: label.LabelValuesCount,
			}
		}

		fmt.Printf("  Processed %d/%d labels...\n", end, len(labelNames))
	}

	fmt.Printf("Total series count: %d\n", totalSeriesCount)

	// Step 3: Query Loki for query stats logs and analyze queries.
	fmt.Printf("\n--- Querying Loki for query stats logs ---\n")
	lokiClient := NewLokiClient(cfg.LokiAddress, cfg.LokiUsername, cfg.LokiPassword)
	analyzer := NewAnalyzer()

	now := time.Now()

	// Query user queries from query-frontend (longer time range since they're more varied).
	userQueriesStart := now.Add(-cfg.UserQueriesDuration)
	fmt.Printf("Querying user queries from %s to %s (namespace: %s, tenant: %s)\n",
		userQueriesStart.Format(time.RFC3339), now.Format(time.RFC3339), cfg.Namespace, cfg.TenantID)

	err = lokiClient.QueryQueryStats(ctx, cfg.Namespace, cfg.TenantID, "query-frontend", userQueriesStart, now, func(entry QueryStatsEntry) error {
		analyzer.ProcessQuery(entry.Query, UserQuery)
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to query Loki for user queries: %w", err)
	}

	// Query rule queries from ruler-query-frontend (shorter time range since they're repetitive).
	ruleQueriesStart := now.Add(-cfg.RuleQueriesDuration)
	fmt.Printf("Querying rule queries from %s to %s (namespace: %s, tenant: %s)\n",
		ruleQueriesStart.Format(time.RFC3339), now.Format(time.RFC3339), cfg.Namespace, cfg.TenantID)

	err = lokiClient.QueryQueryStats(ctx, cfg.Namespace, cfg.TenantID, "ruler-query-frontend", ruleQueriesStart, now, func(entry QueryStatsEntry) error {
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

	labelStats := analyzer.GetLabelStats(allLabelStats, totalSeriesCount)

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

	// Identify good segmentation candidates: >50% series coverage AND >10% query coverage.
	fmt.Printf("\n--- Segmentation Label Candidates ---\n")
	fmt.Printf("Labels with >50%% series coverage AND >10%% query coverage:\n\n")

	var candidates []LabelStats
	for _, ls := range labelStats {
		if ls.SeriesCoverage >= 50 && ls.QueryCoverage >= 10 {
			candidates = append(candidates, ls)
		}
	}

	// Sort candidates by query coverage descending.
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].QueryCoverage > candidates[j].QueryCoverage
	})

	if len(candidates) == 0 {
		fmt.Println("  No labels meet both criteria.")
	} else {
		printCandidatesTable(candidates)
	}

	return nil
}

func printLabelStatsTable(stats []LabelStats, limit int) {
	columns := []TableColumn{
		{Header: "Label name", Align: AlignLeft},
		{Header: "Series", Align: AlignRight},
		{Header: "Unique label values", Align: AlignRight},
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
		{Header: "Series", Align: AlignRight},
		{Header: "All queries", Align: AlignRight},
		{Header: "User queries", Align: AlignRight},
		{Header: "Rule queries", Align: AlignRight},
		{Header: "Unique label values", Align: AlignRight},
	}

	rows := make([]TableRow, 0, len(candidates))
	for _, ls := range candidates {
		rows = append(rows, TableRow{
			ls.Name,
			fmt.Sprintf("%.2f%%", ls.SeriesCoverage),
			fmt.Sprintf("%.2f%%", ls.QueryCoverage),
			fmt.Sprintf("%.2f%%", ls.UserQueryCoverage),
			fmt.Sprintf("%.2f%%", ls.RuleQueryCoverage),
			fmt.Sprintf("%d", ls.ValuesCount),
		})
	}

	PrintTable(columns, rows)
}
