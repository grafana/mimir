// SPDX-License-Identifier: AGPL-3.0-only

package commands

import (
	"fmt"
	"os"

	"github.com/alecthomas/kingpin/v2"

	"github.com/grafana/mimir/pkg/mimirtool/config"
)

type PromQLCommand struct {
	query                       string
	prettyPrint                 bool
	enableExperimentalFunctions bool
}

func (c *PromQLCommand) Register(app *kingpin.Application, _ EnvVarNames) {
	promqlCmd := app.Command("promql", "PromQL formatting and editing for Grafana Mimir.")
	promqlCmd.Flag("enable-experimental-functions", "If set, enables parsing experimental PromQL functions.").BoolVar(&c.enableExperimentalFunctions)

	// PromQL Format Query Command
	promqlFormatCmd := promqlCmd.Command("format", "Format PromQL query with Prometheus' string formatter; wrap query in quotes for CLI parsing.").Action(c.formatQuery)
	promqlFormatCmd.Flag("pretty", "use Prometheus' pretty-print formatter").BoolVar(&c.prettyPrint)
	promqlFormatCmd.Arg("query", "query to format").Required().StringVar(&c.query)
}

func (c *PromQLCommand) formatQuery(_ *kingpin.ParseContext) error {
	config.ParserOptions.EnableExperimentalFunctions = c.enableExperimentalFunctions
	p := config.CreateParser()
	queryExpr, err := p.ParseExpr(c.query)
	if err != nil {
		return err
	}
	var formattedQuery string
	if c.prettyPrint {
		formattedQuery = queryExpr.Pretty(0)
	} else {
		formattedQuery = queryExpr.String()
	}

	fmt.Fprintln(os.Stdout, formattedQuery)
	return nil
}
