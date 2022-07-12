// SPDX-License-Identifier: AGPL-3.0-only

package commands

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/grafana/mimir/pkg/mimirtool/client"
)

type BackfillCommand struct {
	logger       log.Logger
	clientConfig client.Config
	blocks       blockList
}

type blockList []string

func (l *blockList) Set(value string) error {
	st, err := os.Stat(value)
	if err != nil {
		return fmt.Errorf("directory %q doesn't exist", value)
	}
	if !st.IsDir() {
		return fmt.Errorf("%q must be a directory", value)
	}
	*l = append(*l, value)
	return nil
}

func (l blockList) String() string {
	return strings.Join(l, ",")
}

func (l blockList) IsCumulative() bool {
	return true
}

func (c *BackfillCommand) Register(app *kingpin.Application, envVars EnvVarNames) {
	cmd := app.Command("backfill", "Upload metrics blocks to Grafana Mimir.")
	cmd.Action(c.backfill)
	cmd.Arg("block", "block to upload").Required().SetValue(&c.blocks)
	cmd.Flag("address", "Address of the Grafana Mimir cluster").Required().StringVar(&c.clientConfig.Address)
	cmd.Flag("id", "Grafana Mimir tenant ID").Required().StringVar(&c.clientConfig.ID)
	cmd.Flag("key", "API key to use when contacting Grafana Mimir").Default("").StringVar(&c.clientConfig.Key)
	cmd.Flag("tls-ca-path", "TLS CA certificate to verify Grafana Mimir API as part of mTLS").Default("").StringVar(&c.clientConfig.TLS.CAPath)
	cmd.Flag("tls-cert-path", "TLS client certificate to authenticate with the Grafana Mimir API as part of mTLS").Default("").StringVar(&c.clientConfig.TLS.CertPath)
	cmd.Flag("tls-key-path", "TLS client certificate private key to authenticate with the Grafana Mimir API as part of mTLS").
		Default("").StringVar(&c.clientConfig.TLS.KeyPath)

	c.logger = log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
}

func (c *BackfillCommand) backfill(k *kingpin.ParseContext) error {
	ctx := context.Background()
	level.Info(c.logger).Log("msg", "Backfilling", "blocks", c.blocks.String(), "user", c.clientConfig.ID)
	cli, err := client.New(c.clientConfig)
	if err != nil {
		return err
	}

	return cli.Backfill(ctx, c.blocks, c.logger)
}
