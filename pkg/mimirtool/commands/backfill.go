package commands

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/mimir/pkg/mimirtool/client"
	"github.com/pkg/errors"
	"gopkg.in/alecthomas/kingpin.v2"
)

const agentString = "mimir-upload"

type BackfillCommand struct {
	logger       log.Logger
	clientConfig client.Config
	backend      string
	source       string
	dryRun       bool
}

func (c *BackfillCommand) Register(app *kingpin.Application, envVars EnvVarNames) {
	cmd := app.Command("backfill", "Backfill metrics into Grafana Mimir.")
	cmd.Action(c.backfill)
	cmd.Arg("source", "Path to directory to source metrics blocks from.").Required().StringVar(&c.source)
	cmd.Flag("address", "Address of the Grafana Mimir cluster").Required().StringVar(&c.clientConfig.Address)
	cmd.Flag("id", "Grafana Mimir tenant ID").Required().StringVar(&c.clientConfig.ID)
	cmd.Flag("user", fmt.Sprintf("API user to use when contacting Grafana Mimir")).Default("").StringVar(&c.clientConfig.User)
	cmd.Flag("key", "API key to use when contacting Grafana Mimir").Default("").StringVar(&c.clientConfig.Key)
	cmd.Flag("tls-ca-path", "TLS CA certificate to verify Grafana Mimir API as part of mTLS").Default("").StringVar(&c.clientConfig.TLS.CAPath)
	cmd.Flag("tls-cert-path", "TLS client certificate to authenticate with the Grafana Mimir API as part of mTLS").Default("").StringVar(&c.clientConfig.TLS.CertPath)
	cmd.Flag("tls-key-path", "TLS client certificate private key to authenticate with the Grafana Mimir API as part of mTLS").
		Default("").StringVar(&c.clientConfig.TLS.KeyPath)

	c.logger = log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
}

func (c *BackfillCommand) backfill(k *kingpin.ParseContext) error {
	ctx := context.Background()
	level.Info(c.logger).Log("msg", "Backfilling", "source", c.source)
	cli, err := client.New(c.clientConfig)
	if err != nil {
		return err
	}

	tenantID, err := strconv.Atoi(c.clientConfig.ID)
	if err != nil {
		return errors.Wrap(err, "failed to convert tenant ID to int")
	}
	return cli.Backfill(ctx, c.source, tenantID, c.logger)
}
