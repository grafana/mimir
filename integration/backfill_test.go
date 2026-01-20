// SPDX-License-Identifier: AGPL-3.0-only
//go:build requires_docker

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/runutil"
	"github.com/grafana/dskit/test"
	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"golang.org/x/time/rate"

	"github.com/grafana/mimir/integration/e2emimir"
	"github.com/grafana/mimir/pkg/mimirtool/client"
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/bucket/s3"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

func TestMimirtoolBackfill(t *testing.T) {
	const (
		overridesFile = "overrides.yaml"
	)

	tmpDir := t.TempDir()
	blockEnd := time.Now().Truncate(2 * time.Hour)

	block1, err := block.CreateBlock(
		context.Background(), tmpDir,
		[]labels.Labels{
			labels.FromStrings("test", "test1", "a", "1"),
			labels.FromStrings("test", "test1", "a", "2"),
			labels.FromStrings("test", "test1", "a", "3"),
		},
		100, blockEnd.Add(-2*time.Hour).UnixMilli(), blockEnd.UnixMilli(), labels.EmptyLabels())
	require.NoError(t, err)

	block2, err := block.CreateBlock(
		context.Background(), tmpDir,
		[]labels.Labels{
			labels.FromStrings("test", "test2", "a", "1"),
			labels.FromStrings("test", "test2", "a", "2"),
			labels.FromStrings("test", "test2", "a", "3"),
		},
		100, blockEnd.Add(-2*time.Hour).UnixMilli(), blockEnd.UnixMilli(), labels.EmptyLabels())
	require.NoError(t, err)

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Write blank overrides file, so we can check if they are updated later
	require.NoError(t, writeFileToSharedDir(s, overridesFile, []byte{}))

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, mimirBucketName)
	require.NoError(t, s.StartAndWaitReady(consul, minio))
	dataDir := filepath.Join(s.SharedDir(), "data")
	err = os.Mkdir(dataDir, os.ModePerm)
	require.NoError(t, err)

	// Configure the compactor with a data directory and runtime config (for overrides)
	flags := mergeFlags(CommonStorageBackendFlags(), BlocksStorageFlags(), map[string]string{
		"-compactor.data-dir":           filepath.Join(e2e.ContainerSharedDir, "data"),
		"-runtime-config.reload-period": "100ms",
		"-runtime-config.file":          filepath.Join(e2e.ContainerSharedDir, overridesFile),
	})

	// Start Mimir compactor.
	compactor := e2emimir.NewCompactor("compactor", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.StartAndWaitReady(compactor))

	{
		// Try to upload block using mimirtool. Should fail because upload is not enabled for user.
		output, err := runMimirtoolBackfill(tmpDir, compactor, block1)
		require.Contains(t, output, "400 Bad Request")
		require.Contains(t, output, "block upload is disabled")
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

		// Upload block using mimirtool, should work since upload is enabled for user.
		output, err := runMimirtoolBackfill(tmpDir, compactor, block1)
		requireLineContaining(t, output, "msg=\"block uploaded successfully\"", fmt.Sprintf("block=%s", block1))
		require.NoError(t, err)
	}

	{
		// Upload block1 and block2. Block 1 already exists, but block2 should be uploaded without problems.
		output, err := runMimirtoolBackfill(tmpDir, compactor, block1, block2)
		requireLineContaining(t, output, "msg=\"block already exists on the server\"", fmt.Sprintf("path=%s", path.Join(e2e.ContainerSharedDir, block1.String())))
		requireLineContaining(t, output, "msg=\"block uploaded successfully\"", fmt.Sprintf("block=%s", block2))

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

		verifyBlock(t, client, block1, filepath.Join(tmpDir, block1.String()))
		verifyBlock(t, client, block2, filepath.Join(tmpDir, block2.String()))
	}

	{
		// Let's try to upload a block without meta.json.
		b, err := block.CreateBlock(
			context.Background(), tmpDir,
			[]labels.Labels{
				labels.FromStrings("test", "bad", "a", "1"),
				labels.FromStrings("test", "bad", "a", "2"),
				labels.FromStrings("test", "bad", "a", "3"),
			},
			100, blockEnd.Add(-2*time.Hour).UnixMilli(), blockEnd.UnixMilli(), labels.EmptyLabels())
		require.NoError(t, err)
		require.NoError(t, os.Remove(filepath.Join(tmpDir, b.String(), block.MetaFilename)))

		output, err := runMimirtoolBackfill(tmpDir, compactor, b)
		requireLineContaining(t, output, "msg=\"failed uploading block\"", fmt.Sprintf("path=%s", path.Join(e2e.ContainerSharedDir, b.String())))
		require.Error(t, err)
	}

	{
		// Let's try block with external labels. Mimir currently rejects those.
		extLabels := labels.FromStrings("ext", "labels")

		b, err := block.CreateBlock(
			context.Background(), tmpDir,
			[]labels.Labels{
				labels.FromStrings("test", "bad", "a", "1"),
				labels.FromStrings("test", "bad", "a", "2"),
				labels.FromStrings("test", "bad", "a", "3"),
			},
			100, blockEnd.Add(-2*time.Hour).UnixMilli(), blockEnd.UnixMilli(), extLabels)
		require.NoError(t, err)

		output, err := runMimirtoolBackfill(tmpDir, compactor, b)
		require.Contains(t, output, "unsupported external label: ext")
		require.Error(t, err)
	}
}

func TestBackfillSlowUploadSpeed(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, mimirBucketName)
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	dataDir := filepath.Join(s.SharedDir(), "data")
	require.NoError(t, os.Mkdir(dataDir, os.ModePerm))

	const httpServerTimeout = 2 * time.Second

	flags := mergeFlags(CommonStorageBackendFlags(), BlocksStorageFlags(), map[string]string{
		"-compactor.data-dir": filepath.Join(e2e.ContainerSharedDir, "data"),

		// Enable uploads for all tenants
		"-compactor.block-upload-enabled": "true",

		// Use short timeouts for HTTP endpoints.
		// This timeout will not be applied to file upload endpoint.
		"-server.http-read-timeout":  httpServerTimeout.String(),
		"-server.http-write-timeout": httpServerTimeout.String(),
		"-querier.timeout":           httpServerTimeout.String(), // must be set too, otherwise validation check fails because default value is too high compared to -server.http-write-timeout.
		"-log.level":                 "debug",
	})

	// Start Mimir compactor.
	compactor := e2emimir.NewCompactor("compactor", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.StartAndWaitReady(compactor))

	// Create block that we will try to upload.
	tmpDir := t.TempDir()
	blockEnd := time.Now().Truncate(2 * time.Hour)

	block, err := block.CreateBlock(
		context.Background(), tmpDir,
		[]labels.Labels{
			labels.FromStrings("test", "test1", "a", "1"),
			labels.FromStrings("test", "test1", "a", "2"),
			labels.FromStrings("test", "test1", "a", "3"),
		},
		100, blockEnd.Add(-2*time.Hour).UnixMilli(), blockEnd.UnixMilli(), labels.EmptyLabels())
	require.NoError(t, err)

	httpClient := &http.Client{}

	// Initiate new block upload.
	{
		// client.GetBlockMeta will generate correct meta.json file ready for initiating upload of our block.
		meta, err := client.GetBlockMeta(fmt.Sprintf("%s/%s", tmpDir, block.String()))
		require.NoError(t, err)

		buf := bytes.NewBuffer(nil)
		require.NoError(t, json.NewEncoder(buf).Encode(meta))

		req, err := http.NewRequest("POST", fmt.Sprintf("http://%s/api/v1/upload/block/%s/start", compactor.HTTPEndpoint(), block.String()), buf)
		req.Header.Set("X-Scope-OrgID", userID)
		require.NoError(t, err)

		resp, err := httpClient.Do(req)
		require.NoError(t, err)

		body := readAndCloseBody(t, resp.Body)

		require.Equal(t, http.StatusOK, resp.StatusCode, string(body))
	}

	// Now start uploading block data, slowly, so that it takes longer than http server timeout.
	// This will only succeed if timeout is overridden for the endpoint.
	{
		const targetDuration = httpServerTimeout * 2
		chunksFilename := fmt.Sprintf("%s/%s/chunks/000001", tmpDir, block.String())
		chunksFiledata, err := os.ReadFile(chunksFilename)
		require.NoError(t, err)

		// We add one second, because rate-limited reader already allows reading in time 0.
		rateLimit := float64(len(chunksFiledata)) / (targetDuration.Seconds() + 1)
		require.True(t, rateLimit > 0) // sanity check

		rr := newRateLimitedReader(bytes.NewReader(chunksFiledata), rateLimit, int(rateLimit))

		req, err := http.NewRequest("POST", fmt.Sprintf("http://%s/api/v1/upload/block/%s/files?path=chunks/000001", compactor.HTTPEndpoint(), block.String()), rr)
		req.Header.Set("X-Scope-OrgID", userID)
		require.NoError(t, err)

		start := time.Now()
		resp, err := httpClient.Do(req)
		elapsed := time.Since(start)

		require.NoError(t, err)

		body := readAndCloseBody(t, resp.Body)

		require.Equal(t, http.StatusOK, resp.StatusCode, string(body))

		// Verify that it actually took as long as we expected.
		require.True(t, elapsed > targetDuration)
	}
}

func readAndCloseBody(t *testing.T, r io.ReadCloser) []byte {
	b, err := io.ReadAll(r)
	require.NoError(t, err)
	require.NoError(t, r.Close())
	return b
}

func verifyBlock(t *testing.T, client objstore.Bucket, ulid ulid.ULID, localPath string) {
	for _, fn := range []string{"index", "chunks/000001"} {
		a, err := client.Attributes(context.Background(), path.Join("blocks/anonymous", ulid.String(), fn))
		require.NoError(t, err)

		fi, err := os.Stat(filepath.Join(localPath, filepath.FromSlash(fn)))
		require.NoError(t, err)

		require.Equal(t, fi.Size(), a.Size)
	}

	localMeta, err := block.ReadMetaFromDir(localPath)
	require.NoError(t, err)

	remoteMeta, err := block.DownloadMeta(context.Background(), log.NewNopLogger(), bucket.NewPrefixedBucketClient(client, "blocks/anonymous"), ulid)
	require.NoError(t, err)

	require.Equal(t, localMeta.ULID, remoteMeta.ULID)
	require.Equal(t, localMeta.Stats, remoteMeta.Stats)
}

func runMimirtoolBackfill(sharedDir string, compactor *e2emimir.MimirService, blocksToUpload ...ulid.ULID) (string, error) {
	dockerArgs := []string{
		"run",
		"--rm",
		"--net=" + networkName,
		"--name=" + networkName + "-mimirtool-backfill",
	}
	dockerArgs = append(dockerArgs, "--volume", fmt.Sprintf("%s:%s:z", sharedDir, e2e.ContainerSharedDir))
	dockerArgs = append(dockerArgs, e2emimir.GetMimirtoolImage())

	// Mimirtool args.
	dockerArgs = append(dockerArgs, "backfill")
	dockerArgs = append(dockerArgs, "--address", fmt.Sprintf("http://%s:%d/", compactor.Name(), compactor.HTTPPort()))
	dockerArgs = append(dockerArgs, "--id", "anonymous")

	for _, b := range blocksToUpload {
		dockerArgs = append(dockerArgs, path.Join(e2e.ContainerSharedDir, b.String()))
	}

	out, err := e2e.RunCommandAndGetOutput("docker", dockerArgs...)
	return string(out), err
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
	body, err := io.ReadAll(res.Body)

	return string(body), err
}

func newRateLimitedReader(r io.Reader, bytesPerSec float64, burst int) io.Reader {
	return &rateLimitedReader{
		r:      r,
		burst:  burst,
		bucket: rate.NewLimiter(rate.Limit(bytesPerSec), burst),
	}
}

type rateLimitedReader struct {
	r      io.Reader
	burst  int
	bucket *rate.Limiter
}

func (r *rateLimitedReader) Read(buf []byte) (int, error) {
	// We can't wait for more bytes than our burst size.
	if len(buf) > r.burst {
		buf = buf[:r.burst]
	}

	n, err := r.r.Read(buf)
	if n <= 0 {
		return n, err
	}

	err1 := r.bucket.WaitN(context.Background(), n)
	if err == nil {
		err = err1
	}
	return n, err
}

func TestRateLimitedReader(t *testing.T) {
	t.Skip("slow test, checking implementation of rate-limited reader.")

	const dataLen = 16384
	const dataRate = 4096

	data := make([]byte, dataLen)

	start := time.Now()
	n, err := io.Copy(io.Discard, newRateLimitedReader(bytes.NewReader(data), dataRate, dataRate))
	elapsed := time.Since(start)

	require.NoError(t, err)
	require.Equal(t, int64(len(data)), n)
	// rate limiter starts "full", hence "dataLen - dataRate".
	require.InDelta(t, elapsed.Seconds(), (dataLen-dataRate)/dataRate, 1)
}

// requireLineContaining requires that some line in the output contains all the given substrings.
func requireLineContaining(t *testing.T, output string, substrs ...string) {
	lines := strings.Split(output, "\n")
linesLoop:
	for _, line := range lines {
		for _, substr := range substrs {
			if !strings.Contains(line, substr) {
				continue linesLoop
			}
		}
		return
	}
	require.FailNow(t, "line not found", "no line in output contains all of: %q", substrs)
}
