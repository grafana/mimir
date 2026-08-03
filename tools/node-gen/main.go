// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/grafana/dskit/flagext"
)

type config struct {
	generateSourceFile string
}

func (c *config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&c.generateSourceFile, "gen-source-file", "node_gen.go", "Name of the generated source file written into each package.")
}

func main() {
	cfg := config{}
	cfg.RegisterFlags(flag.CommandLine)

	args, err := flagext.ParseFlagsAndArguments(flag.CommandLine)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to parse flags: %v\n", err)
		os.Exit(1)
	}

	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "Usage: node-gen <package-path>")
		os.Exit(1)
	}
	packagePath := args[0]

	pkg, err := ParsePackage(packagePath, cfg.generateSourceFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to parse package: %v\n", err)
		os.Exit(1)
	}

	if len(pkg.NodeStructs) == 0 {
		return
	}

	generators := CreateGenerators()

	out, err := RenderGenerators(pkg, generators, cfg.generateSourceFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to render generated source: %v\n", err)
		os.Exit(1)
	}

	if len(out) == 0 {
		fmt.Fprintf(os.Stdout, "No files generated for package: %s\n", packagePath)
		os.Exit(0)
	}

	outPath := filepath.Join(packagePath, cfg.generateSourceFile)
	if err := os.WriteFile(outPath, out, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "failed to write %s: %v\n", outPath, err)
		os.Exit(1)
	}
}
