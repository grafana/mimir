// SPDX-License-Identifier: AGPL-3.0-only

package commands

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/grafana/mimir/pkg/mimirtool/backfill/verify"
	"github.com/grafana/mimir/pkg/mimirtool/client"
)

type BackfillCommand struct {
	clientConfig          client.Config
	blocks                blockList
	sleepTime             time.Duration
	dryRun                bool
	skipChunkVerification bool
	fullReport            bool
	verifyConcurrency     int
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

func (c *BackfillCommand) Register(app *kingpin.Application, envVars EnvVarNames, logConfig *LoggerConfig) {
	cmd := app.Command("backfill", "Upload Prometheus TSDB blocks to Grafana Mimir compactor.")
	cmd.Action(func(_ *kingpin.ParseContext) error {
		return c.backfill(logConfig.Logger())
	})
	cmd.Arg("block-dir", "block to upload").Required().SetValue(&c.blocks)

	cmd.Flag("address", "Address of the Grafana Mimir cluster; alternatively, set "+envVars.Address+".").
		Envar(envVars.Address).
		Required().
		StringVar(&c.clientConfig.Address)

	cmd.Flag("user",
		fmt.Sprintf("Basic auth username to use when contacting Grafana Mimir; alternatively, set %s. If empty, %s is used instead.", envVars.APIUser, envVars.TenantID)).
		Default("").
		Envar(envVars.APIUser).
		StringVar(&c.clientConfig.User)

	cmd.Flag("id", "Grafana Mimir tenant ID. Used for X-Scope-OrgID HTTP header. Also used for basic auth if --user is not provided. Alternatively, set "+envVars.TenantID+".").
		Envar(envVars.TenantID).
		Required().
		StringVar(&c.clientConfig.ID)

	cmd.Flag("key", "Basic auth password to use when contacting Grafana Mimir; alternatively, set "+envVars.APIKey+".").
		Default("").
		Envar(envVars.APIKey).
		StringVar(&c.clientConfig.Key)

	c.clientConfig.ExtraHeaders = map[string]string{}
	cmd.Flag("extra-headers", "Extra headers to add to the requests in header=value format, alternatively set newline separated "+envVars.ExtraHeaders+".").
		Envar(envVars.ExtraHeaders).
		StringMapVar(&c.clientConfig.ExtraHeaders)

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

	cmd.Flag("tls-insecure-skip-verify", "Skip TLS certificate verification; alternatively, set "+envVars.TLSInsecureSkipVerify+".").
		Default("false").
		Envar(envVars.TLSInsecureSkipVerify).
		BoolVar(&c.clientConfig.TLS.InsecureSkipVerify)

	cmd.Flag("sleep-time", "How long to sleep between checking state of block upload after uploading all files for the block.").
		Default("20s").
		DurationVar(&c.sleepTime)

	cmd.Flag("dry-run", "Run verification without uploading any blocks. Exits 0 if all blocks pass verification, non-zero otherwise.").
		Default("false").
		BoolVar(&c.dryRun)

	cmd.Flag("skip-chunk-verification", "Reduce verification depth: skip the per-chunk CRC32 walk. Use only when you trust your block producer.").
		Default("false").
		BoolVar(&c.skipChunkVerification)

	cmd.Flag("full-report", "Aggregate verification failures across all blocks instead of stopping at the first failure.").
		Default("false").
		BoolVar(&c.fullReport)

	cmd.Flag("verify-concurrency", "Number of blocks to verify in parallel. 0 selects min(GOMAXPROCS, 4); 1 forces serial execution.").
		Default("0").
		IntVar(&c.verifyConcurrency)

	cmd.Validate(func(_ *kingpin.CmdClause) error {
		if c.skipChunkVerification && c.fullReport {
			return fmt.Errorf("--full-report requires deep analysis and cannot be combined with --skip-chunk-verification")
		}
		return nil
	})
}

func (c *BackfillCommand) backfill(logger log.Logger) error {
	level.Info(logger).Log("msg", "Backfilling", "blocks", c.blocks.String(), "user", c.clientConfig.ID)

	cli, err := client.New(c.clientConfig, logger)
	if err != nil {
		return err
	}

	mode := verify.Deep
	if c.skipChunkVerification {
		mode = verify.Medium
	}

	verifier := verify.NewVerifier(logger,
		verify.WithMode(mode),
		verify.WithFailFast(!c.fullReport),
		verify.WithConcurrency(c.verifyConcurrency),
		verify.WithBlockCheck(verify.NewWellFormedVerifier(logger, mode)),
		verify.WithBlockCheck(verify.NewSingleUTCDayVerifier(logger, mode)),
	)

	return cli.BackfillWithOptions(context.Background(), c.blocks, c.sleepTime, verifier, c.dryRun)
}
