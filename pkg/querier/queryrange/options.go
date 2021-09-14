// SPDX-License-Identifier: AGPL-3.0-only
package queryrange

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/go-kit/kit/log/level"

	util_log "github.com/grafana/mimir/pkg/util/log"
)

var (
	CacheControlHeader = "Cache-Control"
	NoStoreValue       = "no-store"

	TotalShardsControlHeader = "Total-Shard-Control"
)

func DecodeOptions(r *http.Request, opts *Options) {
	for _, value := range r.Header.Values(CacheControlHeader) {
		if strings.Contains(value, NoStoreValue) {
			opts.CacheDisabled = true
			break
		}
	}

	for _, value := range r.Header.Values(TotalShardsControlHeader) {
		shards, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			level.Warn(util_log.Logger).Log("msg", fmt.Sprintf("failed to parse '%s' request header with value %s", TotalShardsControlHeader, value), "err", err)
			break
		}
		opts.TotalShards = int32(shards)
		if opts.TotalShards < 1 {
			opts.ShardingDisabled = true
		}
	}
}
