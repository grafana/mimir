// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/commands/bucket_validation.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package commands

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/thanos-io/objstore"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/grafana/mimir/pkg/storage/bucket"
)

// BucketValidationCommand is the kingpin command for bucket validation.
type BucketValidationCommand struct {
	cfg              bucket.Config
	objectCount      int
	testRuns         int
	reportEvery      int
	prefix           string
	retriesOnError   int
	bucketConfig     string
	bucketConfigHelp bool
	bucketClient     objstore.Bucket
	objectNames      map[string]string
	objectContent    string
	logger           log.Logger
}

// retryingBucketClient wraps around a bucket client and wraps some
// of its methods in retrying logic which retries if a call has
// resulted in an error. The wrapped methods are:
// * Upload
// * Exists
// * Iter
// * Get
// * Delete
type retryingBucketClient struct {
	objstore.Bucket
	retries int
}

func (c *retryingBucketClient) withRetries(f func() error) error {
	var tries int
	for {
		err := f()
		if err == nil {
			return nil
		}
		tries++
		if tries >= c.retries {
			return err
		}
	}
}

func (c *retryingBucketClient) Upload(ctx context.Context, name string, r io.Reader) error {
	return c.withRetries(func() error { return c.Bucket.Upload(ctx, name, r) })
}

func (c *retryingBucketClient) Exists(ctx context.Context, name string) (bool, error) {
	var res bool
	var err error
	err = c.withRetries(func() error { res, err = c.Bucket.Exists(ctx, name); return err })
	return res, err
}

func (c *retryingBucketClient) Iter(ctx context.Context, dir string, f func(string) error, opts ...objstore.IterOption) error {
	return c.withRetries(func() error { return c.Bucket.Iter(ctx, dir, f, opts...) })
}

func (c *retryingBucketClient) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	var res io.ReadCloser
	var err error
	err = c.withRetries(func() error { res, err = c.Bucket.Get(ctx, name); return err })
	return res, err
}

func (c *retryingBucketClient) Delete(ctx context.Context, name string) error {
	return c.withRetries(func() error { return c.Bucket.Delete(ctx, name) })
}

// Register is used to register the command to a parent command.
func (b *BucketValidationCommand) Register(app *kingpin.Application, _ EnvVarNames) {
	bvCmd := app.Command("bucket-validation", "Validate that object store bucket works correctly.").Action(b.validate)

	bvCmd.Flag("object-count", "Number of objects to create & delete").Default("2000").IntVar(&b.objectCount)
	bvCmd.Flag("report-every", "Every X operations a progress report gets printed").Default("100").IntVar(&b.reportEvery)
	bvCmd.Flag("test-runs", "Number of times we want to run the whole test").Default("1").IntVar(&b.testRuns)
	bvCmd.Flag("prefix", "Path prefix to use for test objects in object store").Default("tenant").StringVar(&b.prefix)
	bvCmd.Flag("retries-on-error", "Number of times we want to retry if object store returns error").Default("3").IntVar(&b.retriesOnError)
	bvCmd.Flag("bucket-config", "The CLI args to configure a storage bucket").StringVar(&b.bucketConfig)
	bvCmd.Flag("bucket-config-help", "Help text explaining how to use the -bucket-config parameter").BoolVar(&b.bucketConfigHelp)
}

func (b *BucketValidationCommand) validate(k *kingpin.ParseContext) error {
	if b.bucketConfigHelp {
		b.printBucketConfigHelp()
		return nil
	}

	err := b.parseBucketConfig()
	if err != nil {
		return errors.Wrap(err, "error when parsing bucket config")
	}

	b.setObjectNames()
	b.objectContent = "testData"
	b.logger = log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	ctx := context.Background()

	bucketClient, err := bucket.NewClient(ctx, b.cfg, "testClient", b.logger, nil)
	if err != nil {
		return errors.Wrap(err, "failed to create the bucket client")
	}

	b.bucketClient = &retryingBucketClient{
		Bucket:  bucketClient,
		retries: b.retriesOnError,
	}

	for testRun := 0; testRun < b.testRuns; testRun++ {
		// Initially create the objects with an empty string of content. They will be
		// overwritten next with the expected content. This helps ensure that uploads
		// can overwrite existing files and their contents reflect that.
		err = b.uploadTestObjects(ctx, "", "creating test objects")
		if err != nil {
			return errors.Wrap(err, "error when uploading test data")
		}

		// Run the upload test again to verify that we can write to objects that
		// already exist. Some object storage compatibility APIs don't actually let
		// objects be overwritten via uploads if they already exist.
		err = b.uploadTestObjects(ctx, b.objectContent, "overwriting test objects")
		if err != nil {
			return errors.Wrap(err, "error when overwriting test data")
		}

		err = b.validateTestObjects(ctx)
		if err != nil {
			return errors.Wrap(err, "error when validating test data")
		}

		err = b.deleteTestObjects(ctx)
		if err != nil {
			return errors.Wrap(err, "error when deleting test data")
		}

		level.Info(b.logger).Log("testrun_successful", testRun+1)
	}

	return nil
}

func (b *BucketValidationCommand) printBucketConfigHelp() {
	fs := flag.NewFlagSet("bucket-config", flag.ContinueOnError)
	b.cfg.RegisterFlags(fs)

	fmt.Fprintf(fs.Output(), `
The following help text describes the arguments
which may be specified in the string that gets
passed to "-bucket-config".

Example:
mimirtool bucket-validation --bucket-config='-backend=s3 -s3.endpoint=localhost:9000 -s3.bucket-name=example-bucket'

`)
	fs.Usage()
}

func (b *BucketValidationCommand) parseBucketConfig() error {
	fs := flag.NewFlagSet("bucket-config", flag.ContinueOnError)
	b.cfg.RegisterFlags(fs)
	err := fs.Parse(strings.Split(b.bucketConfig, " "))
	if err != nil {
		return err
	}

	return b.cfg.Validate()
}

func (b *BucketValidationCommand) report(phase string, completed int) {
	if completed == 0 || completed%b.reportEvery == 0 {
		level.Info(b.logger).Log("phase", phase, "completed", completed, "total", b.objectCount)
	}
}

func (b *BucketValidationCommand) setObjectNames() {
	b.objectNames = make(map[string]string, b.objectCount)
	for objectIdx := 0; objectIdx < b.objectCount; objectIdx++ {
		b.objectNames[fmt.Sprintf("%s/%05X/", b.prefix, objectIdx)] = "testfile"
	}
}

func (b *BucketValidationCommand) uploadTestObjects(ctx context.Context, content string, phase string) error {
	iteration := 0
	for dirName, objectName := range b.objectNames {
		b.report(phase, iteration)
		iteration++

		objectPath := dirName + objectName
		err := b.bucketClient.Upload(ctx, objectPath, strings.NewReader(content))
		if err != nil {
			return errors.Wrapf(err, "failed to upload object (%s)", objectPath)
		}

		exists, err := b.bucketClient.Exists(ctx, objectPath)
		if err != nil {
			return errors.Wrapf(err, "failed to check if obj exists (%s)", objectPath)
		}
		if !exists {
			return errors.Errorf("Expected obj %s to exist, but it did not", objectPath)
		}
	}
	b.report(phase, iteration)

	return nil
}

func (b *BucketValidationCommand) validateTestObjects(ctx context.Context) error {
	foundDirs := make(map[string]struct{}, b.objectCount)

	level.Info(b.logger).Log("phase", "listing test objects")

	err := b.bucketClient.Iter(ctx, b.prefix, func(dirName string) error {
		foundDirs[dirName] = struct{}{}
		return nil
	})
	if err != nil {
		return errors.Wrapf(err, "failed to list objects")
	}

	if len(foundDirs) != len(b.objectNames) {
		return fmt.Errorf("expected list to return %d directories, but it returned %d", len(b.objectNames), len(foundDirs))
	}

	iteration := 0
	for dirName, objectName := range b.objectNames {
		b.report("validating test objects", iteration)
		iteration++

		if _, ok := foundDirs[dirName]; !ok {
			return fmt.Errorf("expected directory did not exist (%s)", dirName)
		}

		objectPath := dirName + objectName
		reader, err := b.bucketClient.Get(ctx, objectPath)
		if err != nil {
			return errors.Wrapf(err, "failed to get object (%s)", objectPath)
		}

		content, err := io.ReadAll(reader)
		if err != nil {
			return errors.Wrapf(err, "failed to read object (%s)", objectPath)
		}

		_ = reader.Close()
		if string(content) != b.objectContent {
			return errors.Wrapf(err, "got invalid object content (%s)", objectPath)
		}
	}
	b.report("validating test objects", iteration)

	return nil
}

func (b *BucketValidationCommand) deleteTestObjects(ctx context.Context) error {
	iteration := 0
	for dirName, objectName := range b.objectNames {
		b.report("deleting test objects", iteration)
		iteration++

		objectPath := dirName + objectName

		exists, err := b.bucketClient.Exists(ctx, objectPath)
		if err != nil {
			return errors.Wrapf(err, "failed to check if obj exists (%s)", objectPath)
		}
		if !exists {
			return errors.Errorf("Expected obj %s to exist, but it did not", objectPath)
		}

		err = b.bucketClient.Delete(ctx, objectPath)
		if err != nil {
			return errors.Wrapf(err, "failed to delete obj (%s)", objectPath)
		}
		exists, err = b.bucketClient.Exists(ctx, objectPath)
		if err != nil {
			return errors.Wrapf(err, "failed to check if obj exists (%s)", objectPath)
		}
		if exists {
			return errors.Errorf("Expected obj %s to not exist, but it did", objectPath)
		}

		var foundDirCount int
		foundDeletedDir := false
		err = b.bucketClient.Iter(ctx, b.prefix, func(dirName string) error {
			foundDirCount++
			if objectName == dirName {
				foundDeletedDir = true
			}
			return nil
		})
		if err != nil {
			return errors.Wrapf(err, "failed to list objects")
		}
		if foundDeletedDir {
			return errors.Errorf("List returned directory which is supposed to be deleted.")
		}
		expectedDirCount := len(b.objectNames) - iteration
		if foundDirCount != expectedDirCount {
			return fmt.Errorf("expected list to return %d directories, but it returned %d", expectedDirCount, foundDirCount)
		}
	}
	b.report("deleting test objects", iteration)

	return nil
}
