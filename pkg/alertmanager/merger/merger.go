// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/alertmanager/merger/merger.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package merger

// Merger represents logic for merging response bodies.
type Merger interface {
	MergeResponses([][]byte) ([]byte, error)
}

// Noop is an implementation of the Merger interface which does not actually merge
// responses, but just returns an arbitrary response(the first in the list). It can
// be used for write requests where the response is either empty or inconsequential.
type Noop struct{}

func (Noop) MergeResponses(in [][]byte) ([]byte, error) {
	if len(in) == 0 {
		return nil, nil
	}
	return in[0], nil
}
