// SPDX-License-Identifier: AGPL-3.0-only

package queryrange

import (
	"flag"

	"github.com/grafana/dskit/flagext"

	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/chunk/cache"
	util_log "github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/util/log"
)

type ResultsCacheConfig struct {
	CacheConfig cache.Config `yaml:"cache"`
	Compression string       `yaml:"compression"`
}

var (
	// Value that cacheControlHeader has if the response indicates that the results should not be cached.
	noStoreValue = "no-store"

	// ResultsCacheGenNumberHeaderName holds name of the header we want to set in http response
	ResultsCacheGenNumberHeaderName = "Results-Cache-Gen-Number"
)

func (cfg *ResultsCacheConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.CacheConfig.RegisterFlagsWithPrefix("frontend.", "", f)

	f.StringVar(&cfg.Compression, "frontend.compression", "", "Use compression in results cache. Supported values are: 'snappy' and '' (disable compression).")
	//lint:ignore faillint Need to pass the global logger like this for warning on deprecated methods
	flagext.DeprecatedFlag(f, "frontend.cache-split-interval", "Deprecated: The maximum interval expected for each request, results will be cached per single interval. This behavior is now determined by querier.split-queries-by-interval.", util_log.Logger)
}
