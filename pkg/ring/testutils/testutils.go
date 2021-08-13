// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ring/testutils/testutils.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package testutils

import (
	"context"

	"github.com/go-kit/log/level"

	"github.com/grafana/mimir/pkg/ring"
	"github.com/grafana/mimir/pkg/ring/kv"
	util_log "github.com/grafana/mimir/pkg/util/log"
)

// NumTokens determines the number of tokens owned by the specified
// address
func NumTokens(c kv.Client, name, ringKey string) int {
	ringDesc, err := c.Get(context.Background(), ringKey)

	// The ringDesc may be null if the lifecycler hasn't stored the ring
	// to the KVStore yet.
	if ringDesc == nil || err != nil {
		level.Error(util_log.Logger).Log("msg", "error reading consul", "err", err)
		return 0
	}
	rd := ringDesc.(*ring.Desc)
	return len(rd.Ingesters[name].Tokens)
}
