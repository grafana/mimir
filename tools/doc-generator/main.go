// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/tools/doc-generator/main.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"text/template"

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
	"github.com/grafana/mimir/pkg/mimir"
	"github.com/grafana/mimir/pkg/querier"
	querier_worker "github.com/grafana/mimir/pkg/querier/worker"
	"github.com/grafana/mimir/pkg/ruler"
	"github.com/grafana/mimir/pkg/ruler/rulestore"
	"github.com/grafana/mimir/pkg/storage/bucket/s3"
	"github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storegateway"
	util_log "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/validation"
)

const (
	maxLineWidth = 80
	tabWidth     = 2
)

var (
	// Ordered list of root blocks. The order is the same order that will
	// follow the markdown generation.
	rootBlocks = []rootBlock{
		{
			name:       "server",
			structType: reflect.TypeOf(server.Config{}),
			desc:       "The server block configures the HTTP and gRPC server of the launched service(s).",
		},
		{
			name:       "distributor",
			structType: reflect.TypeOf(distributor.Config{}),
			desc:       "The distributor block configures the distributor.",
		},
		{
			name:       "ingester",
			structType: reflect.TypeOf(ingester.Config{}),
			desc:       "The ingester block configures the ingester.",
		},
		{
			name:       "querier",
			structType: reflect.TypeOf(querier.Config{}),
			desc:       "The querier block configures the querier.",
		},
		{
			name:       "frontend",
			structType: reflect.TypeOf(frontend.CombinedFrontendConfig{}),
			desc:       "The frontend block configures the query-frontend.",
		},
		{
			name:       "ruler",
			structType: reflect.TypeOf(ruler.Config{}),
			desc:       "The ruler block configures the ruler.",
		},
		{
			name:       "ruler_storage",
			structType: reflect.TypeOf(rulestore.Config{}),
			desc:       "The ruler_storage block configures the ruler storage backend.",
		},
		{
			name:       "alertmanager",
			structType: reflect.TypeOf(alertmanager.MultitenantAlertmanagerConfig{}),
			desc:       "The alertmanager block configures the alertmanager.",
		},
		{
			name:       "alertmanager_storage",
			structType: reflect.TypeOf(alertstore.Config{}),
			desc:       "The alertmanager_storage block configures the alertmanager storage backend.",
		},
		{
			name:       "flusher",
			structType: reflect.TypeOf(flusher.Config{}),
			desc:       "The flusher block configures the WAL flusher target, used to manually run one-time flushes when scaling down ingesters.",
		},
		{
			name:       "ingester_client",
			structType: reflect.TypeOf(client.Config{}),
			desc:       "The ingester_client block configures how the distributors connect to the ingesters.",
		},
		{
			name:       "frontend_worker",
			structType: reflect.TypeOf(querier_worker.Config{}),
			desc:       "The frontend_worker block configures the worker running within the querier, picking up and executing queries enqueued by the query-frontend or the query-scheduler.",
		},
		{
			name:       "etcd",
			structType: reflect.TypeOf(etcd.Config{}),
			desc:       "The etcd block configures the etcd client.",
		},
		{
			name:       "consul",
			structType: reflect.TypeOf(consul.Config{}),
			desc:       "The consul block configures the consul client.",
		},
		{
			name:       "memberlist",
			structType: reflect.TypeOf(memberlist.KVConfig{}),
			desc:       "The memberlist block configures the Gossip memberlist.",
		},
		{
			name:       "limits",
			structType: reflect.TypeOf(validation.Limits{}),
			desc:       "The limits block configures default and per-tenant limits imposed by components.",
		},
		{
			name:       "blocks_storage",
			structType: reflect.TypeOf(tsdb.BlocksStorageConfig{}),
			desc:       "The blocks_storage block configures the blocks storage.",
		},
		{
			name:       "compactor",
			structType: reflect.TypeOf(compactor.Config{}),
			desc:       "The compactor block configures the compactor component.",
		},
		{
			name:       "store_gateway",
			structType: reflect.TypeOf(storegateway.Config{}),
			desc:       "The store_gateway block configures the store-gateway component.",
		},
		{
			name:       "sse",
			structType: reflect.TypeOf(s3.SSEConfig{}),
			desc:       "The sse block configures the S3 server-side encryption.",
		},
		{
			name:       "memcached",
			structType: reflect.TypeOf(cache.MemcachedConfig{}),
			desc:       "The memcached block configures the Memcached-based caching backend.",
		},
	}
)

func removeFlagPrefix(block *configBlock, prefix string) {
	for _, entry := range block.entries {
		switch entry.kind {
		case "block":
			// Skip root blocks
			if !entry.root {
				removeFlagPrefix(entry.block, prefix)
			}
		case "field":
			if strings.HasPrefix(entry.fieldFlag, prefix) {
				entry.fieldFlag = "<prefix>" + entry.fieldFlag[len(prefix):]
			}
		}
	}
}

func annotateFlagPrefix(blocks []*configBlock) {
	// Find duplicated blocks
	groups := map[string][]*configBlock{}
	for _, block := range blocks {
		groups[block.name] = append(groups[block.name], block)
	}

	// For each duplicated block, we need to fix the CLI flags, because
	// in the documentation each block will be displayed only once but
	// since they're duplicated they will have a different CLI flag
	// prefix, which we want to correctly document.
	for _, group := range groups {
		if len(group) == 1 {
			continue
		}

		// We need to find the CLI flags prefix of each config block. To do it,
		// we pick the first entry from each config block and then find the
		// different prefix across all of them.
		flags := []string{}
		for _, block := range group {
			for _, entry := range block.entries {
				if entry.kind == "field" {
					flags = append(flags, entry.fieldFlag)
					break
				}
			}
		}

		allPrefixes := []string{}
		for i, prefix := range findFlagsPrefix(flags) {
			group[i].flagsPrefix = prefix
			allPrefixes = append(allPrefixes, prefix)
		}

		// Store all found prefixes into each block so that when we generate the
		// markdown we also know which are all the prefixes for each root block.
		for _, block := range group {
			block.flagsPrefixes = allPrefixes
		}
	}

	// Finally, we can remove the CLI flags prefix from the blocks
	// which have one annotated.
	for _, block := range blocks {
		if block.flagsPrefix != "" {
			removeFlagPrefix(block, block.flagsPrefix)
		}
	}
}

func generateBlocksMarkdown(blocks []*configBlock) string {
	md := &markdownWriter{}
	md.writeConfigDoc(blocks)
	return md.string()
}

func generateBlockMarkdown(blocks []*configBlock, blockName, fieldName string) string {
	// Look for the requested block.
	for _, block := range blocks {
		if block.name != blockName {
			continue
		}

		md := &markdownWriter{}

		// Wrap the root block with another block, so that we can show the name of the
		// root field containing the block specs.
		md.writeConfigBlock(&configBlock{
			name: blockName,
			desc: block.desc,
			entries: []*configEntry{
				{
					kind:      "block",
					name:      fieldName,
					required:  true,
					block:     block,
					blockDesc: "",
					root:      false,
				},
			},
		})

		return md.string()
	}

	// If the block has not been found, we return an empty string.
	return ""
}

func main() {
	// Parse the generator flags.
	flag.Parse()
	if flag.NArg() != 1 {
		fmt.Fprintf(os.Stderr, "Usage: doc-generator template-file")
		os.Exit(1)
	}

	templatePath := flag.Arg(0)

	// In order to match YAML config fields with CLI flags, we map
	// the memory address of the CLI flag variables and match them with
	// the config struct fields' addresses.
	cfg := &mimir.Config{}
	flags := parseFlags(cfg, util_log.Logger)

	// Parse the config, mapping each config field with the related CLI flag.
	blocks, err := parseConfig(nil, cfg, flags)
	if err != nil {
		fmt.Fprintf(os.Stderr, "An error occurred while generating the doc: %s\n", err.Error())
		os.Exit(1)
	}

	// Annotate the flags prefix for each root block, and remove the
	// prefix wherever encountered in the config blocks.
	annotateFlagPrefix(blocks)

	// Generate documentation markdown.
	data := struct {
		ConfigFile               string
		BlocksStorageConfigBlock string
		StoreGatewayConfigBlock  string
		CompactorConfigBlock     string
		QuerierConfigBlock       string
		S3SSEConfigBlock         string
		GeneratedFileWarning     string
	}{
		ConfigFile:               generateBlocksMarkdown(blocks),
		BlocksStorageConfigBlock: generateBlockMarkdown(blocks, "blocks_storage_config", "blocks_storage"),
		StoreGatewayConfigBlock:  generateBlockMarkdown(blocks, "store_gateway_config", "store_gateway"),
		CompactorConfigBlock:     generateBlockMarkdown(blocks, "compactor_config", "compactor"),
		QuerierConfigBlock:       generateBlockMarkdown(blocks, "querier_config", "querier"),
		S3SSEConfigBlock:         generateBlockMarkdown(blocks, "s3_sse_config", "sse"),
		GeneratedFileWarning:     "<!-- DO NOT EDIT THIS FILE - This file has been automatically generated from its .template -->",
	}

	// Load the template file.
	tpl := template.New(filepath.Base(templatePath))
	tpl, err = tpl.ParseFiles(templatePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "An error occurred while loading the template %s: %s\n", templatePath, err.Error())
		os.Exit(1)
	}

	// Execute the template to inject generated doc.
	if err := tpl.Execute(os.Stdout, data); err != nil {
		fmt.Fprintf(os.Stderr, "An error occurred while executing the template %s: %s\n", templatePath, err.Error())
		os.Exit(1)
	}
}
