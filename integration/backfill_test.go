// SPDX-License-Identifier: AGPL-3.0-only
//go:build requires_docker
// +build requires_docker

package integration

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/test"
	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/runutil"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/grafana/mimir/integration/e2emimir"
	"github.com/grafana/mimir/pkg/mimirtool/commands"
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/bucket/s3"
	"github.com/grafana/mimir/pkg/storegateway/testhelper"
	util_test "github.com/grafana/mimir/pkg/util/test"
)

func TestMimirtoolBackfill(t *testing.T) {
	const (
		overridesFile = "overrides.yaml"
	)

	tmpDir := t.TempDir()
	blockEnd := time.Now().Truncate(2 * time.Hour)

	block1, err := testhelper.CreateBlock(context.Background(), tmpDir, []labels.Labels{labels.FromStrings("test", "test1")}, 100, blockEnd.Add(-2*time.Hour).UnixMilli(), blockEnd.UnixMilli(), nil, 0, metadata.NoneFunc)
	require.NoError(t, err)
	block1Path := filepath.Join(tmpDir, block1.String())

	block2, err := testhelper.CreateBlock(context.Background(), tmpDir, []labels.Labels{labels.FromStrings("test", "test2")}, 100, blockEnd.Add(-2*time.Hour).UnixMilli(), blockEnd.UnixMilli(), nil, 0, metadata.NoneFunc)
	require.NoError(t, err)
	block2Path := filepath.Join(tmpDir, block2.String())

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Write blank overrides file, so we can check if they are updated later
	require.NoError(t, writeFileToSharedDir(s, overridesFile, []byte{}))

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, mimirBucketName)
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	// Configure the compactor with runtime config (for overrides)
	flags := mergeFlags(CommonStorageBackendFlags(), BlocksStorageFlags(), map[string]string{
		"-runtime-config.reload-period": "100ms",
		"-runtime-config.file":          filepath.Join(e2e.ContainerSharedDir, overridesFile),
	})

	// Start Mimir compactor.
	compactor := e2emimir.NewCompactor("compactor", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.StartAndWaitReady(compactor))

	compactorURL := fmt.Sprintf("http://%s/", compactor.HTTPEndpoint())

	{
		// Try to upload block using mimirtool. Should fail because upload is not enabled for user.
		output, err := runMimirtoolBackfill(t, "--address", compactorURL, "--id", "anonymous", block1Path)
		require.Contains(t, output, "server returned HTTP status 400 Bad Request: block upload is disabled")
		require.Error(t, err)
	}

	{
		runtimeConfigURL := fmt.Sprintf("http://%s/runtime_config?mode=diff", compactor.HTTPEndpoint())
		out, err := getURL(runtimeConfigURL)
		require.NoError(t, err)

		// Enable block upload for anonymous.
		require.NoError(t, writeFileToSharedDir(s, overridesFile, []byte(`
overrides:
  anonymous:
    compactor_block_upload_enabled: true
`)))

		// Wait until compactor has reloaded runtime config.
		test.Poll(t, 1*time.Second, true, func() interface{} {
			newOut, err := getURL(runtimeConfigURL)
			require.NoError(t, err)
			return out != newOut
		})

		// Try to upload block using mimirtool. Should fail because upload is not enabled for user.
		output, err := runMimirtoolBackfill(t, "--address", compactorURL, "--id", "anonymous", block1Path)
		require.Contains(t, output, fmt.Sprintf("msg=\"block uploaded successfully\" block=%s", block1))
		require.NoError(t, err)
	}

	{
		// Upload block1 and block2. Block 1 already exists, but block2 should be uploaded without problem.
		output, err := runMimirtoolBackfill(t, "--address", compactorURL, "--id", "anonymous", block1Path, block2Path)
		require.Contains(t, output, fmt.Sprintf("msg=\"block already exists on the server\" path=%s", block1Path))
		require.Contains(t, output, fmt.Sprintf("msg=\"block uploaded successfully\" block=%s", block2))

		// If blocks exist, it's not an error.
		require.NoError(t, err)
	}

	{
		// Verify that blocks exist in the bucket.

		client, err := s3.NewBucketClient(s3.Config{
			Endpoint:        minio.HTTPEndpoint(),
			Insecure:        true,
			BucketName:      mimirBucketName,
			AccessKeyID:     e2edb.MinioAccessKey,
			SecretAccessKey: flagext.SecretWithValue(e2edb.MinioSecretKey),
		}, "test", log.NewNopLogger())
		require.NoError(t, err)

		verifyBlock(t, client, block1, block1Path)
		verifyBlock(t, client, block2, block2Path)
	}

	{
		// Let's try to upload broken block (no meta.json)
		badBlock, err := testhelper.CreateBlock(context.Background(), tmpDir, []labels.Labels{labels.FromStrings("test", "bad")}, 100, blockEnd.Add(-2*time.Hour).UnixMilli(), blockEnd.UnixMilli(), nil, 0, metadata.NoneFunc)
		require.NoError(t, err)
		badBlockPath := filepath.Join(tmpDir, badBlock.String())

		require.NoError(t, os.Remove(filepath.Join(badBlockPath, block.MetaFilename)))

		output, err := runMimirtoolBackfill(t, "--address", compactorURL, "--id", "anonymous", badBlockPath)
		require.Regexp(t, fmt.Sprintf("msg=\"failed uploading block\"[^\n]+path=%s", badBlockPath), output)
		require.Error(t, err)
	}

	{
		// Let's try block with external labels. Mimir currently rejects those.
		extLabels := labels.FromStrings("ext", "labels")

		badBlock, err := testhelper.CreateBlock(context.Background(), tmpDir, []labels.Labels{labels.FromStrings("test", "bad")}, 100, blockEnd.Add(-2*time.Hour).UnixMilli(), blockEnd.UnixMilli(), extLabels, 0, metadata.NoneFunc)
		require.NoError(t, err)
		badBlockPath := filepath.Join(tmpDir, badBlock.String())

		output, err := runMimirtoolBackfill(t, "--address", compactorURL, "--id", "anonymous", badBlockPath)
		require.Contains(t, output, "unsupported external label: ext")
		require.Error(t, err)
	}

}

func verifyBlock(t *testing.T, client objstore.Bucket, ulid ulid.ULID, localPath string) {
	for _, fn := range []string{"index", "chunks/000001"} {
		a, err := client.Attributes(context.Background(), path.Join("blocks/anonymous", ulid.String(), fn))
		require.NoError(t, err)

		fi, err := os.Stat(filepath.Join(localPath, filepath.FromSlash(fn)))
		require.NoError(t, err)

		require.Equal(t, a.Size, fi.Size())
	}

	localMeta, err := metadata.ReadFromDir(localPath)
	require.NoError(t, err)

	remoteMeta, err := block.DownloadMeta(context.Background(), log.NewNopLogger(), bucket.NewPrefixedBucketClient(client, "blocks/anonymous"), ulid)
	require.NoError(t, err)

	require.Equal(t, localMeta.ULID, remoteMeta.ULID)
	require.Equal(t, localMeta.Stats, remoteMeta.Stats)
}

func runMimirtoolBackfill(t *testing.T, args ...string) (string, error) {
	oldStdout, oldStderr := os.Stdout, os.Stderr
	defer func() {
		os.Stdout = oldStdout
		os.Stderr = oldStderr

		logrus.SetOutput(os.Stderr)
	}()

	co := util_test.CaptureOutput(t)
	// mimirtool uses logrus, which has global logger. We reinitialize it to use our captured Stderr.
	logrus.SetOutput(os.Stderr)

	app := kingpin.New("mimirtool", "help")
	envVars := commands.NewEnvVarsWithPrefix("MIMIR")

	// Only register mimirtool backfill command.
	backfill := commands.BackfillCommand{}
	backfill.Register(app, envVars)

	// Run mimirtool (Parse calls any action).
	_, err := app.Parse(append([]string{"backfill"}, args...))

	stdout, stderr := co.Done()
	return stdout + stderr, err
}

func getURL(url string) (string, error) {
	res, err := e2e.DoGet(url)
	if err != nil {
		return "", err
	}

	// Check the status code.
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return "", fmt.Errorf("unexpected status code %d %s", res.StatusCode, res.Status)
	}

	defer runutil.ExhaustCloseWithErrCapture(&err, res.Body, "reading body")
	body, err := ioutil.ReadAll(res.Body)

	return string(body), err
}
