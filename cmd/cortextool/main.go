package main

import (
	"os"

	"github.com/grafana/cortextool/pkg/commands"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	ruleCommand  commands.RuleCommand
	alertCommand commands.AlertCommand
	logConfig    commands.LoggerConfig
	pushGateway  commands.PushGatewayConfig
)

func main() {
	kingpin.Version("0.0.1")
	app := kingpin.New("cortextool", "A command-line tool to manage cortex.")
	logConfig.Register(app)
	commands.RegisterChunkCommands(app)
	alertCommand.Register(app)
	ruleCommand.Register(app)
	pushGateway.Register(app)
	kingpin.MustParse(app.Parse(os.Args[1:]))

	pushGateway.Stop()
}
