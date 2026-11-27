// SPDX-License-Identifier: AGPL-3.0-only

//go:build stringlabels

package labels

import (
	"strings"
	"testing"

	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

// Series keys hash like the Go ingester's stringlabels.
func TestHashMatchesStringlabels(t *testing.T) {
	for _, pairs := range [][]string{
		{"__name__", "up", "job", "api"},
		{"a", "", "b", strings.Repeat("w", 70000)},
	} {
		require.Equal(t, promlabels.FromStrings(pairs...).Hash(), FromStrings(pairs...).Hash())
	}
}
