// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storage/bucket/s3/config_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package s3

import (
	"crypto/md5"
	"encoding/base64"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/grafana/dskit/flagext"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSSEConfig_Validate(t *testing.T) {
	tests := map[string]struct {
		setup    func() *SSEConfig
		expected error
	}{
		"should pass with default config": {
			setup: func() *SSEConfig {
				cfg := &SSEConfig{}
				flagext.DefaultValues(cfg)

				return cfg
			},
		},
		"should fail on invalid SSE type": {
			setup: func() *SSEConfig {
				return &SSEConfig{
					Type: "unknown",
				}
			},
			expected: errUnsupportedSSEType,
		},
		"should fail on invalid SSE KMS encryption context": {
			setup: func() *SSEConfig {
				return &SSEConfig{
					Type:                 SSEKMS,
					KMSEncryptionContext: "!{}!",
				}
			},
			expected: errInvalidSSEContext,
		},
		"should pass on valid SSE KMS encryption context": {
			setup: func() *SSEConfig {
				return &SSEConfig{
					Type:                 SSEKMS,
					KMSEncryptionContext: `{"department": "10103.0"}`,
				}
			},
		},
		"should pass on SSE-C encryption type with an encryption key path provided": {
			setup: func() *SSEConfig {
				return &SSEConfig{
					Type:              SSEC,
					EncryptionKeyPath: "/ignore",
				}
			},
		},
		"should fail on SSE-C with no encryption key path provided": {
			setup: func() *SSEConfig {
				return &SSEConfig{
					Type: SSEC,
				}
			},
			expected: errMissingSSECEncryptionKeyPath,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testData.expected, testData.setup().Validate())
		})
	}
}

func TestSSEKMSConfig_BuildMinioConfig(t *testing.T) {
	tests := map[string]struct {
		cfg             *SSEConfig
		expectedType    string
		expectedKeyID   string
		expectedContext string
	}{
		"SSE KMS without encryption context": {
			cfg: &SSEConfig{
				Type:     SSEKMS,
				KMSKeyID: "test-key",
			},
			expectedType:    "aws:kms",
			expectedKeyID:   "test-key",
			expectedContext: "",
		},
		"SSE KMS with encryption context": {
			cfg: &SSEConfig{
				Type:                 SSEKMS,
				KMSKeyID:             "test-key",
				KMSEncryptionContext: "{\"department\":\"10103.0\"}",
			},
			expectedType:    "aws:kms",
			expectedKeyID:   "test-key",
			expectedContext: "{\"department\":\"10103.0\"}",
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			sse, err := testData.cfg.BuildMinioConfig()
			require.NoError(t, err)

			headers := http.Header{}
			sse.Marshal(headers)

			assert.Equal(t, testData.expectedType, headers.Get("x-amz-server-side-encryption"))
			assert.Equal(t, testData.expectedKeyID, headers.Get("x-amz-server-side-encryption-aws-kms-key-id"))
			assert.Equal(t, base64.StdEncoding.EncodeToString([]byte(testData.expectedContext)), headers.Get("x-amz-server-side-encryption-context"))
		})
	}
}
func TestSSEC_BuildMinioConfig(t *testing.T) {
	dir := t.TempDir()

	tests := map[string]struct {
		setup func() (string, []byte)
		valid bool
	}{
		"Missing key": {
			setup: func() (string, []byte) {
				path := filepath.Join(dir, "missing")
				return path, []byte{}
			},
			valid: false,
		},
		"Smaller key": {
			setup: func() (string, []byte) {
				path := filepath.Join(dir, "smaller")
				key := []byte{1} // non 256 bit key
				assert.NoError(t, os.WriteFile(path, key, 0600))
				return path, key
			},
			valid: false,
		},
		"Larger key": {
			setup: func() (string, []byte) {
				path := filepath.Join(dir, "larger")
				key := []byte{33} // non 256 bit key
				assert.NoError(t, os.WriteFile(path, key, 0600))
				return path, key
			},
			valid: false,
		},
		"Valid key": {
			setup: func() (string, []byte) {
				path := filepath.Join(dir, "valid")
				key := make([]byte, 32)
				rand.Read(key)
				assert.NoError(t, os.WriteFile(path, key, 0600))
				return path, key
			},
			valid: true,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			path, key := testData.setup()
			cfg := &SSEConfig{
				Type:              SSEC,
				EncryptionKeyPath: path,
			}

			sse, err := cfg.BuildMinioConfig()
			if !testData.valid {
				assert.Error(t, err)
				assert.Nil(t, sse)
				return
			}

			assert.NoError(t, err)

			hash := md5.New()
			_, err = hash.Write(key)
			assert.NoError(t, err)

			headers := http.Header{}
			sse.Marshal(headers)

			assert.Equal(t, "AES256", headers.Get("x-amz-server-side-encryption-customer-algorithm"))
			assert.Equal(t, base64.StdEncoding.EncodeToString(key), headers.Get("x-amz-server-side-encryption-customer-key"))
			assert.Equal(t, base64.StdEncoding.EncodeToString(hash.Sum(nil)), headers.Get("x-amz-server-side-encryption-customer-key-MD5"))
		})
	}
}

func TestParseKMSEncryptionContext(t *testing.T) {
	actual, err := parseKMSEncryptionContext("")
	assert.NoError(t, err)
	assert.Equal(t, map[string]string(nil), actual)

	expected := map[string]string{
		"department": "10103.0",
	}
	actual, err = parseKMSEncryptionContext(`{"department": "10103.0"}`)
	assert.NoError(t, err)
	assert.Equal(t, expected, actual)
}
