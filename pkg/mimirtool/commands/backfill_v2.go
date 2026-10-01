// SPDX-License-Identifier: AGPL-3.0-only

package commands

import (
	"context"
	"fmt"
	"os/signal"
	"syscall"

	"github.com/alecthomas/kingpin/v2"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/grafana/mimir/pkg/mimirtool/client"
)

type BackfillV2Command struct {
	clientConfig client.Config
	jobID        string
	blocks       blockList
}

func (c *BackfillV2Command) Register(app *kingpin.Application, envVars EnvVarNames, logConfig *LoggerConfig) {
	cmd := app.Command("backfill-v2", "Upload Prometheus TSDB blocks to Grafana Mimir with the v2 backfill API. Blocks are uploaded into a backfill job, and finishing the job hands it off for asynchronous processing.")

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

	cmd.Command("start", "Start a new backfill job and print its ID to stdout.").
		Action(c.action(logConfig, c.start))

	uploadCmd := cmd.Command("upload", "Upload blocks into an existing backfill job.").
		Action(c.action(logConfig, c.upload))
	uploadCmd.Arg("block-dir", "block to upload").Required().SetValue(&c.blocks)
	uploadCmd.Flag("job", "ID of the backfill job to upload into.").
		Required().
		StringVar(&c.jobID)

	runCmd := cmd.Command("run", "Start a backfill job, upload blocks into it, and finish it if every block uploads successfully.").
		Action(c.action(logConfig, c.run))
	runCmd.Arg("block-dir", "block to upload").Required().SetValue(&c.blocks)

	// TODO: status and cancel subcommands
	// TODO: a way to fetch the ID of an existing job
	finishCmd := cmd.Command("finish", "Finish a backfill job and hand it off for asynchronous processing.").
		Action(c.action(logConfig, c.finishJob))
	finishCmd.Flag("job", "ID of the backfill job to finish.").
		Required().
		StringVar(&c.jobID)
}

func (c *BackfillV2Command) start(ctx context.Context, cli *client.MimirClient, _ log.Logger) error {
	jobID, err := cli.StartBackfillJob(ctx)
	if err != nil {
		return err
	}

	fmt.Println(jobID)
	return nil
}

func (c *BackfillV2Command) upload(ctx context.Context, cli *client.MimirClient, _ log.Logger) error {
	return cli.UploadBackfillBlocks(ctx, c.jobID, c.blocks)
}

func (c *BackfillV2Command) run(ctx context.Context, cli *client.MimirClient, logger log.Logger) error {
	jobID, err := cli.StartBackfillJob(ctx)
	if err != nil {
		return err
	}
	level.Info(logger).Log("msg", "started backfill job", "job", jobID)

	if err := cli.UploadBackfillBlocks(ctx, jobID, c.blocks); err != nil {
		return err
	}

	return finishBackfillJob(ctx, cli, logger, jobID)
}

func (c *BackfillV2Command) finishJob(ctx context.Context, cli *client.MimirClient, logger log.Logger) error {
	return finishBackfillJob(ctx, cli, logger, c.jobID)
}

func finishBackfillJob(ctx context.Context, cli *client.MimirClient, logger log.Logger, jobID string) error {
	if err := cli.FinishBackfillJob(ctx, jobID); err != nil {
		return err
	}
	level.Info(logger).Log("msg", "backfill job finished", "job", jobID)
	return nil
}

func (c *BackfillV2Command) action(logConfig *LoggerConfig, action func(context.Context, *client.MimirClient, log.Logger) error) kingpin.Action {
	return func(_ *kingpin.ParseContext) error {
		logger := logConfig.Logger()
		cli, err := client.New(c.clientConfig, logger)
		if err != nil {
			return err
		}

		ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer cancel()
		return action(ctx, cli, logger)
	}
}
