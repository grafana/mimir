// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"fmt"
	"strings"
)

// TableColumn defines a column in an ASCII table.
type TableColumn struct {
	Header string
	Align  Alignment
}

// Alignment specifies text alignment in a table cell.
type Alignment int

const (
	AlignLeft Alignment = iota
	AlignRight
)

// TableRow represents a row of data in the table.
type TableRow []string

// PrintTable prints an ASCII table with the given columns and rows.
// Column widths are automatically computed based on the maximum width of
// the header or any cell value in each column.
func PrintTable(columns []TableColumn, rows []TableRow) {
	// Compute column widths.
	widths := computeColumnWidths(columns, rows)

	// Print header.
	printTableRow(columns, widths, getHeaders(columns))

	// Print separator.
	printTableSeparator(widths)

	// Print data rows.
	for _, row := range rows {
		printTableRow(columns, widths, row)
	}
}

func computeColumnWidths(columns []TableColumn, rows []TableRow) []int {
	widths := make([]int, len(columns))

	// Start with header widths.
	for i, col := range columns {
		widths[i] = len(col.Header)
	}

	// Check all row values.
	for _, row := range rows {
		for i, value := range row {
			if i < len(widths) && len(value) > widths[i] {
				widths[i] = len(value)
			}
		}
	}

	return widths
}

func getHeaders(columns []TableColumn) []string {
	headers := make([]string, len(columns))
	for i, col := range columns {
		headers[i] = col.Header
	}
	return headers
}

func printTableRow(columns []TableColumn, widths []int, values []string) {
	for i, col := range columns {
		value := ""
		if i < len(values) {
			value = values[i]
		}

		width := widths[i]
		isLast := i == len(columns)-1

		switch col.Align {
		case AlignLeft:
			fmt.Printf("%-*s", width, value)
		case AlignRight:
			fmt.Printf("%*s", width, value)
		}

		if !isLast {
			fmt.Print(" ")
		}
	}
	fmt.Println()
}

func printTableSeparator(widths []int) {
	for i, width := range widths {
		fmt.Print(strings.Repeat("-", width))
		if i < len(widths)-1 {
			fmt.Print(" ")
		}
	}
	fmt.Println()
}
