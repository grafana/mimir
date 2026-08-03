// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/flagext"

	"github.com/grafana/mimir/pkg/mimirtool/commands/blocks/mark"
)

const deprecationNotice = `DEPRECATED: the standalone "mark-blocks" tool is deprecated and will be removed in a future release.
Use "mimirtool blocks mark" instead.
`

func main() {
	fmt.Fprint(os.Stderr, deprecationNotice)

	var cmd mark.Command
	cmd.RegisterFlags(flag.CommandLine)

	logger := log.NewLogfmtLogger(os.Stdout)
	logger = log.With(logger, "time", log.DefaultTimestampUTC)

	if err := flagext.ParseFlagsWithoutArguments(flag.CommandLine); err != nil {
		level.Error(logger).Log("msg", "failed to parse flags", "err", err)
		os.Exit(1)
	}

	if err := cmd.Run(logger); err != nil {
		level.Error(logger).Log("msg", "mark-blocks failed", "err", err)
		os.Exit(1)
	}
}
