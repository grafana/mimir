// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
)

type csvRow struct {
	rank              int
	str               string
	count             uint64
	bytes             uint64
	countAsLabelName  uint64
	countAsLabelValue uint64
	countAsMetricName uint64
}

func runAnalyzeCommand(args []string) {
	fs := flag.NewFlagSet("analyze", flag.ExitOnError)

	var inputFile string
	var topN int
	var amplificationFactor int
	var goFormat bool
	fs.StringVar(&inputFile, "input", "", "Path to the CSV file to analyze")
	fs.IntVar(&topN, "top-n", 10, "Number of top strings to display")
	fs.IntVar(&amplificationFactor, "amplification-factor", 1, "Amplification factor")
	fs.BoolVar(&goFormat, "go-format", false, "Output as Go format")

	if err := fs.Parse(args); err != nil {
		log.Fatalln(err.Error())
	}

	if inputFile == "" {
		log.Fatalln("input file must be specified with -input")
	}

	if topN <= 0 {
		log.Fatalln("top-n must be positive")
	}

	rows, err := readCSV(inputFile)
	if err != nil {
		log.Fatalf("failed to read CSV: %v", err)
	}

	log.Printf("Read %d rows from %s", len(rows), inputFile)

	// Apply amplification factor (only to rows used as label values)
	if amplificationFactor > 1 {
		amplifiedRows := make([]csvRow, 0, len(rows)*amplificationFactor)
		for _, row := range rows {
			// Include original
			amplifiedRows = append(amplifiedRows, row)
			// Only amplify if used as a label value
			if row.countAsLabelValue > 0 {
				for i := 1; i < amplificationFactor; i++ {
					amplifiedRow := row
					amplifiedRow.str = fmt.Sprintf("%s_amp%d", row.str, i)
					amplifiedRows = append(amplifiedRows, amplifiedRow)
				}
			}
		}
		rows = amplifiedRows
		log.Printf("Amplified to %d rows (factor %d)", len(rows), amplificationFactor)
	}

	// Sort by bytes descending (stable sort preserves order of amplified copies)
	sort.SliceStable(rows, func(i, j int) bool {
		return rows[i].bytes > rows[j].bytes
	})

	// Print top N strings by bytes
	for i := 0; i < topN && i < len(rows); i++ {
		if goFormat {
			fmt.Printf("\"%s\",\n", rows[i].str)
		} else {
			fmt.Println(rows[i].str)
		}
	}
}

func readCSV(path string) ([]csvRow, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV: %w", err)
	}

	if len(records) < 1 {
		return nil, fmt.Errorf("CSV file is empty")
	}

	// Skip header row
	rows := make([]csvRow, 0, len(records)-1)
	for i, record := range records[1:] {
		if len(record) < 7 {
			return nil, fmt.Errorf("row %d has insufficient columns: %d", i+2, len(record))
		}

		rank, err := strconv.Atoi(record[0])
		if err != nil {
			return nil, fmt.Errorf("row %d: invalid rank: %w", i+2, err)
		}

		count, err := strconv.ParseUint(record[2], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("row %d: invalid count: %w", i+2, err)
		}

		bytes, err := strconv.ParseUint(record[3], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("row %d: invalid bytes: %w", i+2, err)
		}

		countAsLabelName, err := strconv.ParseUint(record[4], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("row %d: invalid countAsLabelName: %w", i+2, err)
		}

		countAsLabelValue, err := strconv.ParseUint(record[5], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("row %d: invalid countAsLabelValue: %w", i+2, err)
		}

		countAsMetricName, err := strconv.ParseUint(record[6], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("row %d: invalid countAsMetricName: %w", i+2, err)
		}

		rows = append(rows, csvRow{
			rank:              rank,
			str:               record[1],
			count:             count,
			bytes:             bytes,
			countAsLabelName:  countAsLabelName,
			countAsLabelValue: countAsLabelValue,
			countAsMetricName: countAsMetricName,
		})
	}

	return rows, nil
}
