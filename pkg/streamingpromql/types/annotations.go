// SPDX-License-Identifier: AGPL-3.0-only

package types

import (
	"maps"

	"github.com/prometheus/prometheus/util/annotations"
)

// CloneAnnotations returns a shallow copy of the provided annotations.
func CloneAnnotations(a annotations.Annotations) annotations.Annotations {
	if a == nil {
		return nil
	}

	return maps.Clone(a)
}
