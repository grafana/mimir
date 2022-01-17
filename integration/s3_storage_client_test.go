// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/integration/s3_storage_client_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.
// +build requires_docker

package integration

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
	"github.com/stretchr/testify/require"

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
	kes, err := e2edb.NewKES(7373, kesDNSName, serverKeyFile, serverCertFile, clientKeyFile, clientCertFile, caCertFile, s.SharedDir())
	require.NoError(t, err)
	require.NoError(t, s.StartAndWaitReady(kes))
	minio := e2edb.NewMinioWithKES(9000, "https://"+kesDNSName+":7373", clientKeyFile, clientCertFile, caCertFile, bucketName)
	require.NoError(t, s.StartAndWaitReady(minio))

	tests := []struct {
		name string
		cfg  s3.Config
	}{
		{
			name: "expanded-config",
			cfg: s3.Config{
				Endpoint:        minio.HTTPEndpoint(),
				BucketName:      bucketName,
				Insecure:        true,
				AccessKeyID:     e2edb.MinioAccessKey,
				SecretAccessKey: flagext.Secret{Value: e2edb.MinioSecretKey},
			},
		},
		{
			name: "config-with-sse-s3",
			cfg: s3.Config{
				Endpoint:        minio.HTTPEndpoint(),
				BucketName:      bucketName,
				Insecure:        true,
				AccessKeyID:     e2edb.MinioAccessKey,
				SecretAccessKey: flagext.Secret{Value: e2edb.MinioSecretKey},
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

			err = client.Upload(ctx, objectKey, bytes.NewReader(obj))
			require.NoError(t, err)

			readCloser, err := client.Get(ctx, objectKey)
			require.NoError(t, err)

			read := make([]byte, 4)
			_, err = readCloser.Read(read)
			if err != io.EOF {
				require.NoError(t, err)
			}

			require.Equal(t, obj, read)
		})
	}
}
