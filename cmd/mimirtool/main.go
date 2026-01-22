// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/cmd/cortextool/main.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package main

import (
	"fmt"
	"os"

	"github.com/alecthomas/kingpin/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/mimirtool/commands"
	"github.com/grafana/mimir/pkg/mimirtool/version"
	mimirversion "github.com/grafana/mimir/pkg/util/version"
)

var (
	aclCommand            commands.AccessControlCommand
	alertCommand          commands.AlertCommand
	alertmanagerCommand   commands.AlertmanagerCommand
	analyzeCommand        commands.AnalyzeCommand
	bucketValidateCommand commands.BucketValidationCommand
	configCommand         commands.ConfigCommand
	loadgenCommand        commands.LoadgenCommand
	logConfig             commands.LoggerConfig
	promQLCommand         commands.PromQLCommand
	pushGateway           commands.PushGatewayConfig
	remoteReadCommand     commands.RemoteReadCommand
	ruleCommand           commands.RuleCommand
	backfillCommand       commands.BackfillCommand
	runtimeConfigCommand  commands.RuntimeConfigCommand
	validateCommand       commands.ValidateCommand
)

func main() {
	parser.ExperimentalDurationExpr = true

	app := kingpin.New("mimirtool", "A command-line tool to manage Mimir and GEM.")

	envVars := commands.NewEnvVarsWithPrefix("MIMIR")

	// Register logger first so its PreAction runs before others
	logConfig.Register(app, envVars)

	aclCommand.Register(app, envVars)
	alertCommand.Register(app, envVars, &logConfig, prometheus.DefaultRegisterer)
	alertmanagerCommand.Register(app, envVars, &logConfig)
	analyzeCommand.Register(app, envVars, &logConfig)
	backfillCommand.Register(app, envVars, &logConfig)
	bucketValidateCommand.Register(app, envVars, &logConfig)
	configCommand.Register(app, envVars)
	loadgenCommand.Register(app, envVars, &logConfig, prometheus.DefaultRegisterer)
	promQLCommand.Register(app, envVars)
	pushGateway.Register(app, envVars, &logConfig)
	remoteReadCommand.Register(app, envVars, &logConfig)
	ruleCommand.Register(app, envVars, &logConfig, prometheus.DefaultRegisterer)
	runtimeConfigCommand.Register(app)
	validateCommand.Register(app, envVars, &logConfig)

	app.Command("version", "Get the version of the mimirtool CLI").Action(func(*kingpin.ParseContext) error {
		fmt.Fprintln(os.Stdout, mimirversion.Print("Mimirtool"))
		version.CheckLatest(mimirversion.Version, logConfig.Logger())
		return nil
	})

	kingpin.MustParse(app.Parse(os.Args[1:]))

	pushGateway.Stop()
}
