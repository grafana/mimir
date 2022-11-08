// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/block/fetcher_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package block

import (
	"testing"

	"github.com/oklog/ulid"
	"github.com/stretchr/testify/require"
)

func ULID(i int) ulid.ULID { return ulid.MustNew(uint64(i), nil) }

func ULIDs(is ...int) []ulid.ULID {
	ret := []ulid.ULID{}
	for _, i := range is {
		ret = append(ret, ulid.MustNew(uint64(i), nil))
	}

	return ret
}

func Test_ParseRelabelConfig(t *testing.T) {
	_, err := ParseRelabelConfig([]byte(`
    - action: drop
      regex: "A"
      source_labels:
      - cluster
    `), SelectorSupportedRelabelActions)
	require.NoError(t, err)

	_, err = ParseRelabelConfig([]byte(`
    - action: labelmap
      regex: "A"
    `), SelectorSupportedRelabelActions)
	require.ErrorContains(t, err, "unsupported relabel action: labelmap")
}
