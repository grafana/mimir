// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	gokitlog "github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"

	"github.com/grafana/mimir/pkg/mimirtool/commands/blocks/list"
)

const deprecationNotice = `DEPRECATED: the standalone "listblocks" tool is deprecated and will be removed in a future release.
Use "mimirtool blocks list" instead.
`

func main() {
	// Clean up all flags registered via init() methods of 3rd-party libraries.
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	fmt.Fprint(os.Stderr, deprecationNotice)

	var cmd list.Command
	cmd.RegisterFlags(flag.CommandLine)

	// Parse CLI flags.
	if err := flagext.ParseFlagsWithoutArguments(flag.CommandLine); err != nil {
		log.Fatalln(err.Error())
	}

	if err := cmd.Run(gokitlog.NewNopLogger()); err != nil {
		log.Fatalln(err.Error())
	}
}
