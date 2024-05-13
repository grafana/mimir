// SPDX-License-Identifier: AGPL-3.0-only

package commands

import (
	"fmt"
	"os"

	"github.com/alecthomas/kingpin/v2"
	"github.com/prometheus/prometheus/promql/parser"
)

type PromQLCommand struct {
	query       string
	prettyPrint bool
}

func (c *PromQLCommand) Register(app *kingpin.Application, _ EnvVarNames) {
	promqlCmd := app.Command("promql", "PromQL formatting and editing for Grafana Mimir.")

	// PromQL Format Query Command
	promqlFormatCmd := promqlCmd.Command("format", "Format PromQL query with Prometheus' string formatter; wrap query in quotes for CLI parsing.").Action(c.formatQuery)
	promqlFormatCmd.Flag("pretty", "use Prometheus' pretty-print formatter").BoolVar(&c.prettyPrint)
	promqlFormatCmd.Arg("query", "query to format").Required().StringVar(&c.query)
}

func (c *PromQLCommand) formatQuery(_ *kingpin.ParseContext) error {
	queryExpr, err := parser.ParseExpr(c.query)
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
