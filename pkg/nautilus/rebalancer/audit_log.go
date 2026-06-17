// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"fmt"
	"sort"
	"strings"
)

// actionCounts tallies slicer actions by kind for round-summary logs.
type actionCounts struct {
	moves     int
	reassigns int
	splits    int
	merges    int
}

func countActions(actions []Action) actionCounts {
	var c actionCounts
	for _, a := range actions {
		switch a.Kind {
		case ActionMove:
			c.moves++
		case ActionReassign:
			c.reassigns++
		case ActionSplit:
			c.splits++
		case ActionMerge:
			c.merges++
		}
	}
	return c
}

// formatInt32IDs renders a sorted int32 slice for structured logs.
// Long lists are truncated to keep log lines readable.
func formatInt32IDs(ids []int32, maxShown int) string {
	if len(ids) == 0 {
		return ""
	}
	sorted := append([]int32(nil), ids...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	if maxShown <= 0 || len(sorted) <= maxShown {
		return joinInt32(sorted)
	}
	head := joinInt32(sorted[:maxShown])
	return fmt.Sprintf("%s,...+%d", head, len(sorted)-maxShown)
}

func joinInt32(ids []int32) string {
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = fmt.Sprintf("%d", id)
	}
	return strings.Join(parts, ",")
}
