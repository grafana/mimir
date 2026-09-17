// SPDX-License-Identifier: AGPL-3.0-only

package querierpb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAnnotations_Decode(t *testing.T) {
	encoded := Annotations{
		Warnings: []string{
			"warning: something isn't quite right",
			"warning: something else isn't quite right",
		},
		Infos: []string{
			"info: you should know about this",
			"info: you should know about this too",
		},
	}

	decoded := encoded.Decode()

	// If these tests fail, then the errors we're adding to the set of annotations likely
	// don't wrap PromQLInfo / PromQLWarning correctly.
	warnings, infos := decoded.AsStrings("", 0, 0)
	require.ElementsMatch(t, []string{"warning: something isn't quite right", "warning: something else isn't quite right"}, warnings)
	require.ElementsMatch(t, []string{"info: you should know about this", "info: you should know about this too"}, infos)
}
