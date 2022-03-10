// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/cmd/cortextool/main.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package main

import (
	"fmt"
	"os"

	"gopkg.in/alecthomas/kingpin.v2"

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
	pushGateway           commands.PushGatewayConfig
	remoteReadCommand     commands.RemoteReadCommand
	ruleCommand           commands.RuleCommand
	backfillCommand       commands.BackfillCommand
)

func main() {
	app := kingpin.New("mimirtool", "A command-line tool to manage Mimir and GEM.")

	envVars := commands.NewEnvVarsWithPrefix("MIMIR")
	aclCommand.Register(app, envVars)
	alertCommand.Register(app, envVars)
	alertmanagerCommand.Register(app, envVars)
	analyzeCommand.Register(app, envVars)
	bucketValidateCommand.Register(app, envVars)
	configCommand.Register(app, envVars)
	loadgenCommand.Register(app, envVars)
	logConfig.Register(app, envVars)
	pushGateway.Register(app, envVars)
	remoteReadCommand.Register(app, envVars)
	ruleCommand.Register(app, envVars)
	backfillCommand.Register(app, envVars)

	app.Command("version", "Get the version of the mimirtool CLI").Action(func(k *kingpin.ParseContext) error {
		fmt.Fprintln(os.Stdout, mimirversion.Print("Mimirtool"))
		version.CheckLatest(mimirversion.Version)
		return nil
	})

	kingpin.MustParse(app.Parse(os.Args[1:]))

	pushGateway.Stop()
}
