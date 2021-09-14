// SPDX-License-Identifier: AGPL-3.0-only
package queryrange

import (
	"net/http"
	"strconv"
	"strings"
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
			break
		}
		opts.TotalShards = int32(shards)
		if opts.TotalShards < 1 {
			opts.ShardingDisabled = true
		}
	}
}
