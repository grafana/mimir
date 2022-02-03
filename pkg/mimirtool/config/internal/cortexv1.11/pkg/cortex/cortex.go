// SPDX-License-Identifier: AGPL-3.0-only

package cortex

import (
	"flag"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv/memberlist"
	"github.com/grafana/dskit/runtimeconfig"
	"github.com/pkg/errors"
	"github.com/weaveworks/common/server"

	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/alertmanager"
	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/alertmanager/alertstore"
	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/api"
	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/auxiliary/cortexpb"
	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/auxiliary/encoding"
	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/chunk"
	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/chunk/purger"
	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/chunk/storage"
	chunk_util "github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/chunk/util"
	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/compactor"
	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/configs"
	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/distributor"
	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/flusher"
	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/frontend"
	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/ingester"
	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/querier"
	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/querier/queryrange"
	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/querier/tenantfederation"
	querier_worker "github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/querier/worker"
	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/ruler"
	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/ruler/rulestore"
	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/scheduler"
	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/storegateway"
	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/util/validation"
)

type Config struct {
	Target      flagext.StringSliceCSV `yaml:"target"`
	AuthEnabled bool                   `yaml:"auth_enabled"`
	PrintConfig bool                   `yaml:"-"`
	HTTPPrefix  string                 `yaml:"http_prefix"`

	API              api.Config                      `yaml:"api"`
	Server           server.Config                   `yaml:"server"`
	Distributor      distributor.Config              `yaml:"distributor"`
	Querier          querier.Config                  `yaml:"querier"`
	IngesterClient   client.Config                   `yaml:"ingester_client"`
	Ingester         ingester.Config                 `yaml:"ingester"`
	Flusher          flusher.Config                  `yaml:"flusher"`
	Storage          storage.Config                  `yaml:"storage"`
	ChunkStore       chunk.StoreConfig               `yaml:"chunk_store"`
	Schema           chunk.SchemaConfig              `yaml:"schema" doc:"hidden"` // Doc generation tool doesn't support it because part of the SchemaConfig doesn't support CLI flags (needs manual documentation)
	LimitsConfig     validation.Limits               `yaml:"limits"`
	Prealloc         cortexpb.PreallocConfig         `yaml:"prealloc" doc:"hidden"`
	Worker           querier_worker.Config           `yaml:"frontend_worker"`
	Frontend         frontend.CombinedFrontendConfig `yaml:"frontend"`
	QueryRange       queryrange.Config               `yaml:"query_range"`
	TableManager     chunk.TableManagerConfig        `yaml:"table_manager"`
	Encoding         encoding.Config                 `yaml:"-"` // No yaml for this, it only works with flags.
	BlocksStorage    tsdb.BlocksStorageConfig        `yaml:"blocks_storage"`
	Compactor        compactor.Config                `yaml:"compactor"`
	StoreGateway     storegateway.Config             `yaml:"store_gateway"`
	PurgerConfig     purger.Config                   `yaml:"purger"`
	TenantFederation tenantfederation.Config         `yaml:"tenant_federation"`

	Ruler               ruler.Config                               `yaml:"ruler"`
	RulerStorage        rulestore.Config                           `yaml:"ruler_storage"`
	Configs             configs.Config                             `yaml:"configs"`
	Alertmanager        alertmanager.MultitenantAlertmanagerConfig `yaml:"alertmanager"`
	AlertmanagerStorage alertstore.Config                          `yaml:"alertmanager_storage"`
	RuntimeConfig       runtimeconfig.Config                       `yaml:"runtime_config"`
	MemberlistKV        memberlist.KVConfig                        `yaml:"memberlist"`
	QueryScheduler      scheduler.Config                           `yaml:"query_scheduler"`
}

var (
	errInvalidHTTPPrefix = errors.New("HTTP prefix should be empty or start with /")
)

func (c *Config) RegisterFlags(f *flag.FlagSet) {
	c.Server.MetricsNamespace = "cortex"
	c.Server.ExcludeRequestInLog = true

	// Set the default module list to 'all'
	c.Target = []string{All}

	f.Var(&c.Target, "target", "Comma-separated list of Cortex modules to load. "+
		"The alias 'all' can be used in the list to load a number of core modules and will enable single-binary mode. "+
		"Use '-modules' command line flag to get a list of available modules, and to see which modules are included in 'all'.")

	f.BoolVar(&c.AuthEnabled, "auth.enabled", true, "Set to false to disable auth.")
	f.BoolVar(&c.PrintConfig, "print.config", false, "Print the config and exit.")
	f.StringVar(&c.HTTPPrefix, "http.prefix", "/api/prom", "HTTP path prefix for Cortex API.")

	c.API.RegisterFlags(f)
	c.registerServerFlagsWithChangedDefaultValues(f)
	c.Distributor.RegisterFlags(f)
	c.Querier.RegisterFlags(f)
	c.IngesterClient.RegisterFlags(f)
	c.Ingester.RegisterFlags(f)
	c.Flusher.RegisterFlags(f)
	c.Storage.RegisterFlags(f)
	c.ChunkStore.RegisterFlags(f)
	c.Schema.RegisterFlags(f)
	c.LimitsConfig.RegisterFlags(f)
	c.Prealloc.RegisterFlags(f)
	c.Worker.RegisterFlags(f)
	c.Frontend.RegisterFlags(f)
	c.QueryRange.RegisterFlags(f)
	c.TableManager.RegisterFlags(f)
	c.Encoding.RegisterFlags(f)
	c.BlocksStorage.RegisterFlags(f)
	c.Compactor.RegisterFlags(f)
	c.StoreGateway.RegisterFlags(f)
	c.PurgerConfig.RegisterFlags(f)
	c.TenantFederation.RegisterFlags(f)

	c.Ruler.RegisterFlags(f)
	c.RulerStorage.RegisterFlags(f)
	c.Configs.RegisterFlags(f)
	c.Alertmanager.RegisterFlags(f)
	c.AlertmanagerStorage.RegisterFlags(f)
	c.RuntimeConfig.RegisterFlags(f)
	c.MemberlistKV.RegisterFlags(f)
	c.QueryScheduler.RegisterFlags(f)

	// These don't seem to have a home.
	f.IntVar(&chunk_util.QueryParallelism, "querier.query-parallelism", 100, "Max subqueries run in parallel per higher-level query.")
}
func (c *Config) registerServerFlagsWithChangedDefaultValues(fs *flag.FlagSet) {
	throwaway := flag.NewFlagSet("throwaway", flag.PanicOnError)

	// Register to throwaway flags first. Default values are remembered during registration and cannot be changed,
	// but we can take values from throwaway flag set and reregister into supplied flags with new default values.
	c.Server.RegisterFlags(throwaway)

	throwaway.VisitAll(func(f *flag.Flag) {
		// Ignore errors when setting new values. We have a test to verify that it works.
		switch f.Name {
		case "server.grpc.keepalive.min-time-between-pings":
			_ = f.Value.Set("10s")

		case "server.grpc.keepalive.ping-without-stream-allowed":
			_ = f.Value.Set("true")
		}

		fs.Var(f.Value, f.Name, f.Usage)
	})
}
