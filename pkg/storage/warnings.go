// SPDX-License-Identifier: AGPL-3.0-only

package storage

import "github.com/prometheus/prometheus/util/annotations"

// WarningsToStrings flattens annotations into a string slice for wire transport.
// Returns nil for empty input so the proto field is omitted on the wire.
func WarningsToStrings(a annotations.Annotations) []string {
	if len(a) == 0 {
		return nil
	}
	out := make([]string, 0, len(a))
	for _, w := range a {
		out = append(out, w.Error())
	}
	return out
}

