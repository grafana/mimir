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
)

var (
	aclCommand            commands.AccessControlCommand
	alertCommand          commands.AlertCommand
	alertmanagerCommand   commands.AlertmanagerCommand
	analyseCommand        commands.AnalyseCommand
	bucketValidateCommand commands.BucketValidationCommand
	configCommand         commands.ConfigCommand
	loadgenCommand        commands.LoadgenCommand
	logConfig             commands.LoggerConfig
	pushGateway           commands.PushGatewayConfig
	remoteReadCommand     commands.RemoteReadCommand
	ruleCommand           commands.RuleCommand
)

func main() {
	app := kingpin.New("mimirtool", "A command-line tool to manage mimir and GEM.")

	envVars := commands.NewEnvVarsWithPrefix("MIMIR")
	aclCommand.Register(app, envVars)
	alertCommand.Register(app, envVars)
	alertmanagerCommand.Register(app, envVars)
	analyseCommand.Register(app, envVars)
	bucketValidateCommand.Register(app, envVars)
	configCommand.Register(app, envVars)
	loadgenCommand.Register(app, envVars)
	logConfig.Register(app, envVars)
	pushGateway.Register(app, envVars)
	remoteReadCommand.Register(app, envVars)
	ruleCommand.Register(app, envVars)

	app.Command("version", "Get the version of the mimirtool CLI").Action(func(k *kingpin.ParseContext) error {
		fmt.Print(version.Template)
		version.CheckLatest()

		return nil
	})

	kingpin.MustParse(app.Parse(os.Args[1:]))

	pushGateway.Stop()
}
