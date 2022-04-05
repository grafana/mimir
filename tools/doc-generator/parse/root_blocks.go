// SPDX-License-Identifier: AGPL-3.0-only

package parse

import (
	"reflect"

	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/kv/etcd"
	"github.com/grafana/dskit/kv/memberlist"
	"github.com/weaveworks/common/server"

	"github.com/grafana/mimir/pkg/alertmanager"
	"github.com/grafana/mimir/pkg/alertmanager/alertstore"
	"github.com/grafana/mimir/pkg/cache"
	"github.com/grafana/mimir/pkg/compactor"
	"github.com/grafana/mimir/pkg/distributor"
	"github.com/grafana/mimir/pkg/flusher"
	"github.com/grafana/mimir/pkg/frontend"
	"github.com/grafana/mimir/pkg/ingester"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/querier"
	querier_worker "github.com/grafana/mimir/pkg/querier/worker"
	"github.com/grafana/mimir/pkg/ruler"
	"github.com/grafana/mimir/pkg/ruler/rulestore"
	"github.com/grafana/mimir/pkg/scheduler"
	"github.com/grafana/mimir/pkg/storage/bucket/s3"
	"github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storegateway"
	"github.com/grafana/mimir/pkg/util/validation"
)

var (
	// RootBlocks is an ordered list of root blocks. The order is the same order that will
	// follow the markdown generation.
	RootBlocks = []RootBlock{
		{
			Name:       "server",
			StructType: reflect.TypeOf(server.Config{}),
			Desc:       "The server block configures the HTTP and gRPC server of the launched service(s).",
		},
		{
			Name:       "distributor",
			StructType: reflect.TypeOf(distributor.Config{}),
			Desc:       "The distributor block configures the distributor.",
		},
		{
			Name:       "ingester",
			StructType: reflect.TypeOf(ingester.Config{}),
			Desc:       "The ingester block configures the ingester.",
		},
		{
			Name:       "querier",
			StructType: reflect.TypeOf(querier.Config{}),
			Desc:       "The querier block configures the querier.",
		},
		{
			Name:       "frontend",
			StructType: reflect.TypeOf(frontend.CombinedFrontendConfig{}),
			Desc:       "The frontend block configures the query-frontend.",
		},
		{
			Name:       "query_scheduler",
			StructType: reflect.TypeOf(scheduler.Config{}),
			Desc:       "The query_scheduler block configures the query-scheduler.",
		},
		{
			Name:       "ruler",
			StructType: reflect.TypeOf(ruler.Config{}),
			Desc:       "The ruler block configures the ruler.",
		},
		{
			Name:       "ruler_storage",
			StructType: reflect.TypeOf(rulestore.Config{}),
			Desc:       "The ruler_storage block configures the ruler storage backend.",
		},
		{
			Name:       "alertmanager",
			StructType: reflect.TypeOf(alertmanager.MultitenantAlertmanagerConfig{}),
			Desc:       "The alertmanager block configures the alertmanager.",
		},
		{
			Name:       "alertmanager_storage",
			StructType: reflect.TypeOf(alertstore.Config{}),
			Desc:       "The alertmanager_storage block configures the alertmanager storage backend.",
		},
		{
			Name:       "flusher",
			StructType: reflect.TypeOf(flusher.Config{}),
			Desc:       "The flusher block configures the WAL flusher target, used to manually run one-time flushes when scaling down ingesters.",
		},
		{
			Name:       "ingester_client",
			StructType: reflect.TypeOf(client.Config{}),
			Desc:       "The ingester_client block configures how the distributors connect to the ingesters.",
		},
		{
			Name:       "frontend_worker",
			StructType: reflect.TypeOf(querier_worker.Config{}),
			Desc:       "The frontend_worker block configures the worker running within the querier, picking up and executing queries enqueued by the query-frontend or the query-scheduler.",
		},
		{
			Name:       "etcd",
			StructType: reflect.TypeOf(etcd.Config{}),
			Desc:       "The etcd block configures the etcd client.",
		},
		{
			Name:       "consul",
			StructType: reflect.TypeOf(consul.Config{}),
			Desc:       "The consul block configures the consul client.",
		},
		{
			Name:       "memberlist",
			StructType: reflect.TypeOf(memberlist.KVConfig{}),
			Desc:       "The memberlist block configures the Gossip memberlist.",
		},
		{
			Name:       "limits",
			StructType: reflect.TypeOf(validation.Limits{}),
			Desc:       "The limits block configures default and per-tenant limits imposed by components.",
		},
		{
			Name:       "blocks_storage",
			StructType: reflect.TypeOf(tsdb.BlocksStorageConfig{}),
			Desc:       "The blocks_storage block configures the blocks storage.",
		},
		{
			Name:       "compactor",
			StructType: reflect.TypeOf(compactor.Config{}),
			Desc:       "The compactor block configures the compactor component.",
		},
		{
			Name:       "store_gateway",
			StructType: reflect.TypeOf(storegateway.Config{}),
			Desc:       "The store_gateway block configures the store-gateway component.",
		},
		{
			Name:       "sse",
			StructType: reflect.TypeOf(s3.SSEConfig{}),
			Desc:       "The sse block configures the S3 server-side encryption.",
		},
		{
			Name:       "memcached",
			StructType: reflect.TypeOf(cache.MemcachedConfig{}),
			Desc:       "The memcached block configures the Memcached-based caching backend.",
		},
	}
)
