// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/grafana/dskit/flagext"

	"github.com/grafana/mimir/pkg/mimirtool/commands/blocks/undelete"
)

const deprecationNotice = `DEPRECATED: the standalone "undelete-blocks" tool is deprecated and will be removed in a future release.
Use "mimirtool blocks undelete" instead.
`

func main() {
	// Clean up all flags registered via init() methods of 3rd-party libraries.
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	fmt.Fprint(os.Stderr, deprecationNotice)

	var cmd undelete.Command
	cmd.RegisterFlags(flag.CommandLine)

	// Parse CLI arguments.
	if err := flagext.ParseFlagsWithoutArguments(flag.CommandLine); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	if err := cmd.Run(); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}
