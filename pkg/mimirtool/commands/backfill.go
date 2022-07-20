// SPDX-License-Identifier: AGPL-3.0-only

package commands

import (
	"fmt"
	"os"
	"strings"

	"github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/grafana/mimir/pkg/mimirtool/client"
)

type BackfillCommand struct {
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
	cmd := app.Command("backfill", "Upload Prometheus TSDB blocks to Grafana Mimir compactor.")
	cmd.Action(c.backfill)
	cmd.Arg("block-dir", "block to upload").Required().SetValue(&c.blocks)

	cmd.Flag("address", "Address of the Grafana Mimir cluster; alternatively, set "+envVars.Address+".").
		Envar(envVars.Address).
		Required().
		StringVar(&c.clientConfig.Address)

	cmd.Flag("user",
		fmt.Sprintf("API user to use when contacting Grafana Mimir; alternatively, set %s. If empty, %s is used instead.", envVars.APIUser, envVars.TenantID)).
		Default("").
		Envar(envVars.APIUser).
		StringVar(&c.clientConfig.User)

	cmd.Flag("id", "Grafana Mimir tenant ID; alternatively, set "+envVars.TenantID+".").
		Envar(envVars.TenantID).
		Required().
		StringVar(&c.clientConfig.ID)

	cmd.Flag("key", "API key to use when contacting Grafana Mimir; alternatively, set "+envVars.APIKey+".").
		Default("").
		Envar(envVars.APIKey).
		StringVar(&c.clientConfig.Key)

	cmd.Flag("tls-ca-path", "TLS CA certificate to verify Grafana Mimir API as part of mTLS; alternatively, set "+envVars.TLSCAPath+".").
		Default("").
		Envar(envVars.TLSCAPath).
		StringVar(&c.clientConfig.TLS.CAPath)

	cmd.Flag("tls-cert-path", "TLS client certificate to authenticate with the Grafana Mimir API as part of mTLS; alternatively, set "+envVars.TLSCertPath+".").
		Default("").
		Envar(envVars.TLSCertPath).
		StringVar(&c.clientConfig.TLS.CertPath)

	cmd.Flag("tls-key-path", "TLS client certificate private key to authenticate with the Grafana Mimir API as part of mTLS; alternatively, set "+envVars.TLSKeyPath+".").
		Default("").
		Envar(envVars.TLSKeyPath).
		StringVar(&c.clientConfig.TLS.KeyPath)
}

func (c *BackfillCommand) backfill(k *kingpin.ParseContext) error {
	logrus.WithFields(logrus.Fields{
		"blocks": c.blocks.String(),
		"user":   c.clientConfig.ID,
	}).Println("Backfilling")

	cli, err := client.New(c.clientConfig)
	if err != nil {
		return err
	}

	return cli.Backfill(c.blocks)
}
