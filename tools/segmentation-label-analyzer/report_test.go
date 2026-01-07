// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPrintTable(t *testing.T) {
	columns := []TableColumn{
		{Header: "Name", Align: AlignLeft},
		{Header: "Count", Align: AlignRight},
		{Header: "Percentage", Align: AlignRight},
	}

	rows := []TableRow{
		{"foo", "123", "45.67%"},
		{"bar_longer", "7", "100.00%"},
	}

	// Capture stdout.
	old := os.Stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = w

	PrintTable(columns, rows)

	require.NoError(t, w.Close())
	os.Stdout = old

	var buf bytes.Buffer
	_, err = io.Copy(&buf, r)
	require.NoError(t, err)
	require.NoError(t, r.Close())
	output := buf.String()

	expected := strings.TrimSpace(`
Name       Count Percentage
---------- ----- ----------
foo          123     45.67%
bar_longer     7    100.00%
`) + "\n"

	require.Equal(t, expected, output)
}
