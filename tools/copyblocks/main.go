// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/flagext"

	blockscopy "github.com/grafana/mimir/pkg/mimirtool/commands/blocks/copy"
)

const deprecationNotice = `DEPRECATED: the standalone "copyblocks" tool is deprecated and will be removed in a future release.
Use "mimirtool blocks copy" instead.
`

func main() {
	// Clean up all flags registered via init() methods of 3rd-party libraries.
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	fmt.Fprint(os.Stderr, deprecationNotice)

	var cmd blockscopy.Command
	cmd.RegisterFlags(flag.CommandLine)

	// Parse CLI arguments.
	if err := flagext.ParseFlagsWithoutArguments(flag.CommandLine); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	logger := log.NewLogfmtLogger(os.Stdout)
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)

	if err := cmd.Run(logger); err != nil {
		level.Error(logger).Log("msg", "copyblocks failed", "err", err)
		os.Exit(1)
	}
}
