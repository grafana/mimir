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
	clientConfig      client.Config
	blocks            blockList
	sleepTime         time.Duration
	verifyBlocks      bool
	dryRun            bool
	failFast          bool
	deepVerification  bool
	singleBlockPerDay bool
	verifyConcurrency int
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

	cmd.Flag("address", "Address of the Grafana Mimir cluster; alternatively, set "+envVars.Address+". Required unless --dry-run is set.").
		Envar(envVars.Address).
		Default("").
		StringVar(&c.clientConfig.Address)

	cmd.Flag("user",
		fmt.Sprintf("Basic auth username to use when contacting Grafana Mimir; alternatively, set %s. If empty, %s is used instead.", envVars.APIUser, envVars.TenantID)).
		Default("").
		Envar(envVars.APIUser).
		StringVar(&c.clientConfig.User)

	cmd.Flag("id", "Grafana Mimir tenant ID. Used for X-Scope-OrgID HTTP header. Also used for basic auth if --user is not provided. Alternatively, set "+envVars.TenantID+". Required unless --dry-run is set.").
		Envar(envVars.TenantID).
		Default("").
		StringVar(&c.clientConfig.ID)

	cmd.Flag("key", "Basic auth password to use when contacting Grafana Mimir; alternatively, set "+envVars.APIKey+".").
		Default("").
		Envar(envVars.APIKey).
		StringVar(&c.clientConfig.Key)

	registerSigV4Flags(cmd, envVars, &c.clientConfig.SigV4)

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

	// Verification is opt-in while it is experimental. Eventually this flag's
	// default becomes true, and later the flag goes away entirely so that
	// blocks must pass verification in order to be backfilled.
	cmd.Flag("verify", "Verify blocks before uploading them. Experimental, and disabled by default for now.").
		Default("false").
		BoolVar(&c.verifyBlocks)

	cmd.Flag("dry-run", "Verify blocks without uploading any of them; implies --verify. Exits 0 if all blocks pass verification, non-zero otherwise.").
		Default("false").
		BoolVar(&c.dryRun)

	cmd.Flag("fail-fast", "Aborts verification after the first failure.").
		Default("true").
		BoolVar(&c.failFast)

	cmd.Flag("deep-verification", "Use high verification depth, including slow per-chunk CRC32 walks.").
		Default("true").
		BoolVar(&c.deepVerification)

	cmd.Flag("single-block-per-day", "Enforce at most one block per UTC day. If false, allow multiple blocks per day as long as they don't overlap. Either way, no block may span two UTC days.").
		Default("false").
		BoolVar(&c.singleBlockPerDay)

	cmd.Flag("verify-concurrency", "Number of blocks to verify in parallel. 0 selects min(GOMAXPROCS, 4); 1 forces serial execution.").
		Default("0").
		IntVar(&c.verifyConcurrency)

	cmd.Validate(func(_ *kingpin.CmdClause) error {
		if !c.dryRun {
			var missing []string
			if c.clientConfig.Address == "" {
				missing = append(missing, "--address")
			}
			if c.clientConfig.ID == "" {
				missing = append(missing, "--id")
			}
			if len(missing) > 0 {
				return fmt.Errorf("%s required unless --dry-run is set", strings.Join(missing, " and "))
			}
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

	// A dry run's only purpose is verification, so it turns verification on
	// regardless of --verify. That also makes "neither verify nor upload"
	// impossible to ask for.
	var verifier *verify.Verifier
	if c.verifyBlocks || c.dryRun {
		mode := verify.Medium
		if c.deepVerification {
			mode = verify.Deep
		}

		// Block-level checks run in registration order, so run cheap checks first
		// so fail-fast skips expensive walks when the meta is already bad.
		opts := []verify.Option{
			verify.WithMode(mode),
			verify.WithFailFast(c.failFast),
			verify.WithConcurrency(c.verifyConcurrency),
			verify.WithBlockCheck(verify.NewMetaCheckVerifier(logger)),
			// The compactor rejects a block whose range crosses a boundary of its
			// largest configured block range, so no block may span two UTC days
			// regardless of how many blocks per day we allow.
			verify.WithBlockCheck(verify.NewSingleUTCDayVerifier(logger)),
		}
		if c.singleBlockPerDay {
			opts = append(opts, verify.WithBatchCheck(verify.NewDuplicateDayVerifier(logger)))
		} else {
			// More expensive than single-block-per-day, but necessary for the shape
			// of blocks some tools produce.
			opts = append(opts, verify.WithBatchCheck(verify.NewOverlappingBlockVerifier(logger)))
		}
		opts = append(opts, verify.WithBlockCheck(verify.NewWellFormedVerifier(logger, mode)))
		verifier = verify.NewVerifier(logger, opts...)
	}

	return cli.BackfillWithOptions(context.Background(), c.blocks, c.sleepTime, verifier, c.dryRun)
}
