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
	"strings"
	"text/template"

	"github.com/grafana/dskit/flagext"

	"github.com/grafana/mimir/pkg/mimir"
	util_log "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/tools/doc-generator/parse"
	"github.com/grafana/mimir/tools/doc-generator/write"
)

func removeFlagPrefix(block *parse.ConfigBlock, prefix string) {
	for _, entry := range block.Entries {
		switch entry.Kind {
		case parse.KindBlock:
			// Skip root blocks
			if !entry.Root {
				removeFlagPrefix(entry.Block, prefix)
			}
		case parse.KindField:
			if strings.HasPrefix(entry.FieldFlag, prefix) {
				entry.FieldFlag = "<prefix>" + entry.FieldFlag[len(prefix):]
			}
		}
	}
}

func annotateFlagPrefix(blocks []*parse.ConfigBlock) {
	// Find duplicated blocks
	groups := map[string][]*parse.ConfigBlock{}
	for _, block := range blocks {
		groups[block.Name] = append(groups[block.Name], block)
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
			for _, entry := range block.Entries {
				if entry.Kind == parse.KindField {
					flags = append(flags, entry.FieldFlag)
					break
				}
			}
		}

		allPrefixes := []string{}
		for i, prefix := range parse.FindFlagsPrefix(flags) {
			group[i].FlagsPrefix = prefix
			allPrefixes = append(allPrefixes, prefix)
		}

		// Store all found prefixes into each block so that when we generate the
		// markdown we also know which are all the prefixes for each root block.
		for _, block := range group {
			block.FlagsPrefixes = allPrefixes
		}
	}

	// Finally, we can remove the CLI flags prefix from the blocks
	// which have one annotated.
	for _, block := range blocks {
		if block.FlagsPrefix != "" {
			removeFlagPrefix(block, block.FlagsPrefix)
		}
	}
}

func generateBlocksMarkdown(blocks []*parse.ConfigBlock) string {
	md := &write.MarkdownWriter{}
	md.WriteConfigDoc(blocks, parse.RootBlocks)
	return md.String()
}

func generateBlockMarkdown(blocks []*parse.ConfigBlock, blockName, fieldName string) string {
	// Look for the requested block.
	for _, block := range blocks {
		if block.Name != blockName {
			continue
		}

		md := &write.MarkdownWriter{}

		// Wrap the root block with another block, so that we can show the name of the
		// root field containing the block specs.
		md.WriteConfigBlock(&parse.ConfigBlock{
			Name: blockName,
			Desc: block.Desc,
			Entries: []*parse.ConfigEntry{
				{
					Kind:      parse.KindBlock,
					Name:      fieldName,
					Required:  true,
					Block:     block,
					BlockDesc: "",
					Root:      false,
				},
			},
		})

		return md.String()
	}

	// If the block has not been found, we return an empty string.
	return ""
}

func main() {
	// Clean up all flags registered via init() methods of 3rd-party libraries.
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	// Parse CLI arguments.
	args, err := flagext.ParseFlagsAndArguments(flag.CommandLine)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "Usage: doc-generator template-file")
		os.Exit(1)
	}

	templatePath := args[0]

	// In order to match YAML config fields with CLI flags, we map
	// the memory address of the CLI flag variables and match them with
	// the config struct fields' addresses.
	cfg := &mimir.Config{}
	flags := parse.Flags(cfg, util_log.Logger)

	// Parse the config, mapping each config field with the related CLI flag.
	blocks, err := parse.Config(cfg, flags, parse.RootBlocks)
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
