// SPDX-License-Identifier: AGPL-3.0-only

package requestoptions

import (
	"context"
	"net/http"
	"strconv"
	"strings"
)

// Options holds per-request flags decoded from HTTP headers.
type Options struct {
	CacheDisabled    bool
	ShardingDisabled bool
	TotalShards      int32
}

const (
	CacheControlHeader = "Cache-Control"

	// NoStoreValue is the Cache-Control value that disables caching.
	NoStoreValue = "no-store"

	// TotalShardsControlHeader is the HTTP header used to override the
	// total number of query shards. A value of "0" disables sharding.
	TotalShardsControlHeader = "Sharding-Control"
)

type contextKey int

const optionsKey contextKey = 0

func ContextWithOptions(ctx context.Context, opts Options) context.Context {
	return context.WithValue(ctx, optionsKey, opts)
}

func OptionsFromContext(ctx context.Context) Options {
	if v := ctx.Value(optionsKey); v != nil {
		return v.(Options)
	}
	return Options{}
}

func DecodeOptions(r *http.Request) Options {
	opts := Options{
		CacheDisabled: DecodeCacheDisabledOption(r),
	}

	for _, value := range r.Header.Values(TotalShardsControlHeader) {
		shards, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			continue
		}
		opts.TotalShards = int32(shards)
		if opts.TotalShards < 1 {
			opts.ShardingDisabled = true
		}
	}
	return opts
}

func DecodeCacheDisabledOption(r *http.Request) bool {
	for _, value := range r.Header.Values(CacheControlHeader) {
		if strings.Contains(value, NoStoreValue) {
			return true
		}
	}
	return false
}

func EncodeOptions(r *http.Request, opts Options) {
	if opts.CacheDisabled {
		r.Header.Set(CacheControlHeader, NoStoreValue)
	}
	if opts.ShardingDisabled {
		r.Header.Set(TotalShardsControlHeader, "0")
	}
	if opts.TotalShards > 0 {
		r.Header.Set(TotalShardsControlHeader, strconv.Itoa(int(opts.TotalShards)))
	}
}
