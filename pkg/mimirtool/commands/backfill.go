package commands

import (
	"os"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"gopkg.in/alecthomas/kingpin.v2"
)

const agentString = "mimir-upload"

type BackfillCommand struct {
	backend     string
	source      string
	svcAccount  string
	srcAddress  string
	tgtAddress  string
	tenantID    string
	limitsFPath string
	dryRun      bool
	logger      log.Logger
}

func (c *BackfillCommand) Register(app *kingpin.Application, envVars EnvVarNames) {
	cmd := app.Command("backfill", "Backfill metrics into Grafana Mimir.")
	cmd.Action(c.backfill)
	cmd.Arg("source", "Path to directory to source metrics blocks from.").Required().StringVar(&c.source)

	c.logger = log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
}

func (c *BackfillCommand) backfill(k *kingpin.ParseContext) error {
	level.Info(c.logger).Log("msg", "Backfilling!", "source", c.source)
	return nil
}
