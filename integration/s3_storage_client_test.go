// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/integration/s3_storage_client_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.
//go:build requires_docker

package integration

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/test"
	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
	miniosdk "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/e2emimir"
	s3 "github.com/grafana/mimir/pkg/storage/bucket/s3"
)

func TestS3Client(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// We use KES to emulate a Key Management Store for use with Minio
	kesDNSName := networkName + "-kes"
	require.NoError(t, writeCerts(s.SharedDir(), kesDNSName))
	// Start dependencies.
	kes, err := e2emimir.NewKES(7373, kesDNSName, serverKeyFile, serverCertFile, clientKeyFile, clientCertFile, caCertFile, s.SharedDir())
	require.NoError(t, err)
	require.NoError(t, s.StartAndWaitReady(kes))
	minio := e2emimir.NewMinioWithKES(9000, "https://"+kesDNSName+":7373", clientKeyFile, clientCertFile, caCertFile, blocksBucketName)
	require.NoError(t, s.StartAndWaitReady(minio))

	tests := []struct {
		name string
		cfg  s3.Config
	}{
		{
			name: "expanded-config",
			cfg: s3.Config{
				Endpoint:        minio.HTTPEndpoint(),
				BucketName:      blocksBucketName,
				Insecure:        true,
				AccessKeyID:     e2edb.MinioAccessKey,
				SecretAccessKey: flagext.SecretWithValue(e2edb.MinioSecretKey),
			},
		},
		{
			name: "config-with-sse-s3",
			cfg: s3.Config{
				Endpoint:        minio.HTTPEndpoint(),
				BucketName:      blocksBucketName,
				Insecure:        true,
				AccessKeyID:     e2edb.MinioAccessKey,
				SecretAccessKey: flagext.SecretWithValue(e2edb.MinioSecretKey),
				SSE: s3.SSEConfig{
					Type: "SSE-S3",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := s3.NewBucketClient(tt.cfg, "test", log.NewNopLogger())

			require.NoError(t, err)

			ctx := context.Background()
			objectKey := "key-" + tt.name
			obj := []byte{0x01, 0x02, 0x03, 0x04}

			// The upload may fail if Minio is not ready yet, so we retry until succeed.
			test.Poll(t, 5*time.Second, nil, func() interface{} {
				return client.Upload(ctx, objectKey, bytes.NewReader(obj))
			})

			readCloser, err := client.Get(ctx, objectKey)
			require.NoError(t, err)

			read := make([]byte, 4)
			_, err = readCloser.Read(read)
			if !errors.Is(err, io.EOF) {
				require.NoError(t, err)
			}

			require.Equal(t, obj, read)
		})
	}
}

func TestSiloRejectsUnsignedCopyHeader(t *testing.T) {
	scenario, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer scenario.Close()
	store := e2emimir.NewMinio(9000, "security-test")
	require.NoError(t, scenario.StartAndWaitReady(store))
	client, err := miniosdk.New(store.HTTPEndpoint(), &miniosdk.Options{Creds: credentials.NewStaticV4(e2emimir.MinioAccessKey, e2emimir.MinioSecretKey, "")})
	require.NoError(t, err)
	ctx := t.Context()
	_, err = client.PutObject(ctx, "security-test", "source", bytes.NewReader([]byte("private content")), 15, miniosdk.PutObjectOptions{})
	require.NoError(t, err)
	for _, copyHeader := range []bool{false, true} {
		name := "ordinary-upload"
		if copyHeader {
			name = "unauthorized-copy"
		}
		url, err := client.PresignedPutObject(ctx, "security-test", name, time.Minute)
		require.NoError(t, err)
		request, err := http.NewRequestWithContext(ctx, http.MethodPut, url.String(), http.NoBody)
		require.NoError(t, err)
		if copyHeader {
			request.Header.Set("x-amz-copy-source", "/security-test/source")
		}
		response, err := http.DefaultClient.Do(request)
		require.NoError(t, err)
		require.NoError(t, response.Body.Close())
		if copyHeader {
			require.Contains(t, []int{http.StatusBadRequest, http.StatusForbidden}, response.StatusCode)
			_, err = client.StatObject(ctx, "security-test", name, miniosdk.StatObjectOptions{})
			require.Equal(t, "NoSuchKey", miniosdk.ToErrorResponse(err).Code)
		} else {
			require.Equal(t, http.StatusOK, response.StatusCode)
		}
	}
}
