package main

import (
	"os"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/grafana/cortex-tools/pkg/commands"
)

var (
	ruleCommand    commands.RuleCommand
	alertCommand   commands.AlertCommand
	logConfig      commands.LoggerConfig
	pushGateway    commands.PushGatewayConfig
	loadgenCommand commands.LoadgenCommand
)

func main() {
	kingpin.Version("0.1.3")
	app := kingpin.New("cortextool", "A command-line tool to manage cortex.")
	logConfig.Register(app)
	alertCommand.Register(app)
	ruleCommand.Register(app)
	pushGateway.Register(app)
	loadgenCommand.Register(app)
	kingpin.MustParse(app.Parse(os.Args[1:]))

	pushGateway.Stop()
}
