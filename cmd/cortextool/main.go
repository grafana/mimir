// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/cmd/cortextool/main.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package main

import (
	"fmt"
	"os"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/grafana/mimir/cmd/cortextool/pkg/commands"
	"github.com/grafana/mimir/cmd/cortextool/pkg/version"
)

var (
	ruleCommand           commands.RuleCommand
	alertCommand          commands.AlertCommand
	alertmanagerCommand   commands.AlertmanagerCommand
	logConfig             commands.LoggerConfig
	pushGateway           commands.PushGatewayConfig
	loadgenCommand        commands.LoadgenCommand
	remoteReadCommand     commands.RemoteReadCommand
	aclCommand            commands.AccessControlCommand
	analyseCommand        commands.AnalyseCommand
	bucketValidateCommand commands.BucketValidationCommand
)

func main() {
	app := kingpin.New("cortextool", "A command-line tool to manage cortex.")
	logConfig.Register(app)
	alertCommand.Register(app)
	alertmanagerCommand.Register(app)
	ruleCommand.Register(app)
	pushGateway.Register(app)
	loadgenCommand.Register(app)
	remoteReadCommand.Register(app)
	aclCommand.Register(app)
	analyseCommand.Register(app)
	bucketValidateCommand.Register(app)

	app.Command("version", "Get the version of the cortextool CLI").Action(func(k *kingpin.ParseContext) error {
		fmt.Print(version.Template)
		version.CheckLatest()

		return nil
	})

	kingpin.MustParse(app.Parse(os.Args[1:]))

	pushGateway.Stop()
}
