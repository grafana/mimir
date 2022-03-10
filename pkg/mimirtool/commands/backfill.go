package commands

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/objstore/gcs"
	"github.com/thanos-io/thanos/pkg/shipper"
	"gopkg.in/alecthomas/kingpin.v2"
	"gopkg.in/yaml.v2"

	mimirtsdb "github.com/grafana/mimir/pkg/storage/tsdb"
)

const agentString = "mimir-upload"

type BackfillCommand struct {
	backend     string
	dataDir     string
	svcAccount  string
	srcAddress  string
	tgtAddress  string
	tenantID    string
	limitsFPath string
	dryRun      bool
	logger      log.Logger
}

func (c *BackfillCommand) Register(app *kingpin.Application, envVars EnvVarNames) {
	backfillCmd := app.Command("backfill", "Backfill data into Grafana Mimir.")

	uploadCmd := backfillCmd.Command("upload", "Upload blocks into an object storage bucket.").Action(c.upload)
	uploadCmd.Arg("data-dir", "Path to directory to source data files from.").Required().StringVar(&c.dataDir)
	uploadCmd.Flag("backend", "Object storage backend to use. One of: gcs.").Required().EnumVar(&c.backend, "gcs")
	uploadCmd.Flag("gcs.service-account", "Path to Google Cloud service account YAML file.").Envar("GOOGLE_SERVICE_ACCOUNT").StringVar(&c.svcAccount)
	uploadCmd.Flag("gcs.address", "Google Cloud Storage address.").StringVar(&c.srcAddress)

	execCmd := backfillCmd.Command("exec", "Execute the backfilling process.").Action(c.execute)
	execCmd.Flag("backend", "Object storage backend to use. One of: gcs.").Required().EnumVar(&c.backend, "gcs")
	execCmd.Flag("tenant-id", "Tenant ID.").Required().StringVar(&c.tenantID)
	execCmd.Flag("limits-file", "Path to file containing limits to be enforced on the original data.").Required().StringVar(&c.limitsFPath)
	execCmd.Flag("dry-run", "Enable dry-run mode?").BoolVar(&c.dryRun)
	execCmd.Flag("gcs.service-account", "Path to Google Cloud service account YAML file.").Envar("GOOGLE_SERVICE_ACCOUNT").StringVar(&c.svcAccount)
	execCmd.Flag("gcs.source-address", "Source Google Cloud Storage address.").StringVar(&c.srcAddress)
	execCmd.Flag("gcs.target-address", "Target Google Cloud Storage address.").StringVar(&c.tgtAddress)

	c.logger = log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
}

func (c *BackfillCommand) upload(k *kingpin.ParseContext) error {
	ctx := context.Background()
	switch c.backend {
	case "gcs":
		return c.uploadToGCS(ctx)
	default:
		return fmt.Errorf("unrecognized backend: %q", c.backend)
	}
}

func (c *BackfillCommand) uploadToGCS(ctx context.Context) error {
	if c.srcAddress == "" {
		return fmt.Errorf("--gcs.address is required")
	}
	if c.svcAccount == "" {
		return fmt.Errorf("--gcs.service-account is required")
	}

	start := time.Now()

	uri := fmt.Sprintf("gs://%s", c.srcAddress)
	level.Info(c.logger).Log("msg", "Uploading blocks", "dataDir", c.dataDir, "uri", uri)

	sa, err := os.ReadFile(c.svcAccount)
	if err != nil {
		return fmt.Errorf("failed to read %q: %w", c.svcAccount, err)
	}

	bucketCfg := gcs.Config{
		Bucket:         c.srcAddress,
		ServiceAccount: string(sa),
	}

	gcsBucket, err := gcs.NewBucketWithConfig(ctx, c.logger, bucketCfg, agentString)
	if err != nil {
		return fmt.Errorf("failed to open bucket from config: %w", err)
	}
	defer gcsBucket.Close()

	s := shipper.New(
		c.logger,
		nil, // TODO: Add Prometheus registry
		c.dataDir,
		gcsBucket,
		func() labels.Labels { return labels.Labels{{Name: "foo", Value: "bar"}} }, // TODO: Fix external labels.
		metadata.SourceType(agentString),
		true,
		true,
		metadata.SHA256Func,
	)
	uploaded, err := s.Sync(ctx)
	if err != nil {
		return fmt.Errorf("failed while shipping local files to bucket: %w", err)
	}

	if uploaded == 0 {
		level.Info(c.logger).Log("msg", fmt.Sprintf("0 files were uploaded to bucket. Double check the data dir. Also check if file '%s/thanos.shipper.json' is present since it could be preventing the file upload.", c.dataDir))
	}

	level.Info(c.logger).Log("msg", fmt.Sprintf("Uploaded: %d files, Duration: %f seconds", uploaded, time.Since(start).Seconds()))

	return nil
}

// execute copies blocks, after validating them, from source to target bucket.
//
// Any necessary conversions to meta.json files are also performed.
func (c *BackfillCommand) execute(k *kingpin.ParseContext) error {
	start := time.Now()
	ctx := context.Background()

	limitsYAML, err := os.ReadFile(c.limitsFPath)
	if err != nil {
		return fmt.Errorf("failed to read limits file %q: %w", c.limitsFPath, err)
	}

	var limits limits
	if err := yaml.Unmarshal(limitsYAML, &limits); err != nil {
		return fmt.Errorf("failed to decode limits file %q: %w", c.limitsFPath, err)
	}

	sa, err := os.ReadFile(c.svcAccount)
	if err != nil {
		return fmt.Errorf("failed to read service account file %q: %w", c.svcAccount, err)
	}

	srcBucket, err := gcs.NewBucketWithConfig(ctx, c.logger, gcs.Config{
		Bucket:         c.srcAddress,
		ServiceAccount: string(sa),
	}, agentString)
	if err != nil {
		return fmt.Errorf("failed to open bucket from config: %w", err)
	}
	defer srcBucket.Close()

	if err := c.assessSrcBucket(ctx, srcBucket, limits); err != nil {
		return err
	}

	if err := c.copyObjects(ctx, srcBucket, sa); err != nil {
		return err
	}

	level.Info(c.logger).Log("msg", fmt.Sprintf("Successfully uploaded source bucket contents into target bucket, duration: %f seconds", time.Since(start).Seconds()))
	return nil
}

// assessSrcBucket assesses the contents of the source bucket wrt. meta.json files and limits.
//
// If necessary, and not in dry-run mode, converted meta.json files get uploaded back to srcBucket.
func (c *BackfillCommand) assessSrcBucket(ctx context.Context, srcBucket *gcs.Bucket, limits limits) error {
	var totalBytes uint64
	err := srcBucket.Iter(ctx, "", func(op string) error {
		blockID, ok := block.IsBlockDir(op)
		if !ok {
			// not a block
			return nil
		}

		meta, err := block.DownloadMeta(ctx, c.logger, srcBucket, blockID)
		if err != nil {
			level.Error(c.logger).Log("msg", "Downloading meta.json failed", "block", blockID.String(), "err", err.Error())
			return fmt.Errorf("failed to download meta.json for source block %s: %w", blockID.String(), err)
		}

		// Convert and upload if necessary
		meta, changesRequired := convertMetadata(meta, c.tenantID)
		if err := c.uploadNewMeta(ctx, srcBucket, blockID.String(), meta, changesRequired); err != nil {
			return err
		}

		if limits.MaxSeriesPerBlock > 0 && meta.Stats.NumSeries > limits.MaxSeriesPerBlock {
			return fmt.Errorf("block %s has %d series, exceeding the limit: %d",
				blockID.String(), meta.Stats.NumSeries, limits.MaxSeriesPerBlock)
		}

		// Count bytes of this object
		if err := srcBucket.Iter(ctx, op, func(op string) error {
			attrs, err := srcBucket.Attributes(ctx, op)
			if err != nil {
				return fmt.Errorf("failed to get attributes of source bucket object %s: %w", op, err)
			}

			totalBytes += uint64(attrs.Size)
			return nil
		}, objstore.WithRecursiveIter); err != nil {
			return err
		}

		if limits.TotalBucketSizeBytes > 0 && totalBytes > limits.TotalBucketSizeBytes {
			return fmt.Errorf("the bucket has at least %d bytes, exceeding the limit: %d", totalBytes,
				limits.TotalBucketSizeBytes)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to iterate source bucket for assessment: %w", err)
	}

	return nil
}

// uploadNewMeta uploads, if necessary, a newly formed meta.json file to a block in a bucket.
//
// If no changes are required or in dry-run mode, nothing is done.
func (c *BackfillCommand) uploadNewMeta(ctx context.Context, bucket objstore.Bucket, blockID string,
	meta metadata.Meta, changesRequired []string) error {
	if len(changesRequired) == 0 {
		level.Info(c.logger).Log("msg", "Block doesn't need changes", "block", blockID, "tenantID", c.tenantID)
		return nil
	}

	if c.dryRun {
		level.Info(c.logger).Log("msg", "Block requires changes, but in dry run mode", "block", blockID,
			"tenantID", c.tenantID, "changes_required", strings.Join(changesRequired, ","))
		return nil
	}

	level.Info(c.logger).Log("msg", "Block requires changes, uploading new meta.json", "block",
		blockID, "tenantID", c.tenantID, "changes_required",
		strings.Join(changesRequired, ","))

	var body bytes.Buffer
	if err := meta.Write(&body); err != nil {
		return fmt.Errorf("writing meta failed: %w", err)
	}

	pth := path.Join(blockID, block.MetaFilename)
	if err := bucket.Upload(ctx, pth, &body); err != nil {
		return fmt.Errorf("uploading %s to bucket failed: %w", pth, err)
	}

	return nil
}

// copyObjects copies objects from source bucket to target bucket.
func (c *BackfillCommand) copyObjects(ctx context.Context, srcBucket *gcs.Bucket, sa []byte) error {
	tgtBucket, err := gcs.NewBucketWithConfig(ctx, c.logger, gcs.Config{
		Bucket:         c.tgtAddress,
		ServiceAccount: string(sa),
	}, agentString)
	if err != nil {
		return fmt.Errorf("failed to open target bucket: %w", err)
	}
	defer tgtBucket.Close()

	err = srcBucket.Iter(ctx, "", func(op string) error {
		if strings.HasPrefix(op, "debug") {
			// Not an object we want to upload.
			return nil
		}

		// Upload to target bucket with prefix of c.tenantID
		obj, err := srcBucket.Get(ctx, op)
		if err != nil {
			return fmt.Errorf("failed to get bucket object %s: %w", op, err)
		}
		defer obj.Close()

		pth := path.Join(c.tenantID, op)
		if err := tgtBucket.Upload(ctx, pth, obj); err != nil {
			return fmt.Errorf("failed to upload %s to target bucket: %w", op, err)
		}

		return nil
	}, objstore.WithRecursiveIter)
	if err != nil {
		return fmt.Errorf("failed to iterate source bucket for copying objects: %w", err)
	}

	return nil
}

func convertMetadata(meta metadata.Meta, tenantID string) (metadata.Meta, []string) {
	var changesRequired []string

	org, ok := meta.Thanos.Labels[mimirtsdb.TenantIDExternalLabel]
	if !ok {
		changesRequired = append(changesRequired, fmt.Sprintf("add %s label", mimirtsdb.TenantIDExternalLabel))
	} else if org != tenantID {
		changesRequired = append(changesRequired, fmt.Sprintf("change %s from %s to %s",
			mimirtsdb.TenantIDExternalLabel, org, tenantID))
	}

	// remove __org_id__ so that we can see if there are any other labels
	delete(meta.Thanos.Labels, mimirtsdb.TenantIDExternalLabel)
	if len(meta.Thanos.Labels) > 0 {
		changesRequired = append(changesRequired, "remove extra Thanos labels")
	}

	meta.Thanos.Labels = map[string]string{
		mimirtsdb.TenantIDExternalLabel: tenantID,
	}

	return meta, changesRequired
}

type limits struct {
	TotalBucketSizeBytes uint64 `yaml:"total_bucket_size_bytes"`
	MaxSeriesPerBlock    uint64 `yaml:"max_series_per_block"`
}
