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

	// Check all row values, accounting for multiline cells.
	for _, row := range rows {
		for i, value := range row {
			if i < len(widths) {
				for _, line := range strings.Split(value, "\n") {
					if len(line) > widths[i] {
						widths[i] = len(line)
					}
				}
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
	// Split all cell values into lines.
	cellLines := make([][]string, len(columns))
	maxLines := 1
	for i := range columns {
		if i < len(values) {
			cellLines[i] = strings.Split(values[i], "\n")
		} else {
			cellLines[i] = []string{""}
		}
		if len(cellLines[i]) > maxLines {
			maxLines = len(cellLines[i])
		}
	}

	// Print each physical line.
	for lineIdx := 0; lineIdx < maxLines; lineIdx++ {
		for colIdx, col := range columns {
			// Get the line for this cell, or empty if exhausted.
			line := ""
			if lineIdx < len(cellLines[colIdx]) {
				line = cellLines[colIdx][lineIdx]
			}

			width := widths[colIdx]
			isLast := colIdx == len(columns)-1

			switch col.Align {
			case AlignLeft:
				fmt.Printf("%-*s", width, line)
			case AlignRight:
				fmt.Printf("%*s", width, line)
			}

			if !isLast {
				fmt.Print(" ")
			}
		}
		fmt.Println()
	}
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
