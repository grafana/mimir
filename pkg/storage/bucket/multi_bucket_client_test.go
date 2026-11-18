// SPDX-License-Identifier: AGPL-3.0-only

package bucket

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
)

type multiBucketClientTest struct {
	client    *MultiBucketClient
	primary   *objstore.InMemBucket
	secondary *objstore.InMemBucket

	primaryInjector   *ErrorInjectedBucketClient
	secondaryInjector *ErrorInjectedBucketClient
}

func newMultiBucketClientTest(t *testing.T, reg prometheus.Registerer) *multiBucketClientTest {
	test := &multiBucketClientTest{
		primary:   objstore.NewInMemBucket(),
		secondary: objstore.NewInMemBucket(),
	}
	test.primaryInjector = &ErrorInjectedBucketClient{Bucket: test.primary}
	test.secondaryInjector = &ErrorInjectedBucketClient{Bucket: test.secondary}
	test.client = NewMultiBucketClient(
		objstore.WrapWithMetrics(test.primaryInjector, reg, "primary"),
		objstore.WrapWithMetrics(test.secondaryInjector, reg, "secondary"),
	)
	t.Cleanup(func() { require.NoError(t, test.client.Close()) })
	return test
}

func uploadObject(t *testing.T, bkt objstore.Bucket, name, content string) {
	require.NoError(t, bkt.Upload(context.Background(), name, strings.NewReader(content)))
}

func readObject(t *testing.T, reader io.ReadCloser) string {
	defer func() { require.NoError(t, reader.Close()) }()
	content, err := io.ReadAll(reader)
	require.NoError(t, err)
	return string(content)
}

func TestMultiBucketClient_Reads(t *testing.T) {
	tests := map[string]struct {
		primaryContent   string
		secondaryContent string
		expectedContent  string
	}{
		"object only in the primary bucket": {
			primaryContent:  "primary",
			expectedContent: "primary",
		},
		"object only in the secondary bucket": {
			secondaryContent: "secondary",
			expectedContent:  "secondary",
		},
		"object in the two buckets": {
			primaryContent:   "primary",
			secondaryContent: "secondary",
			expectedContent:  "primary",
		},
		"object in neither bucket": {},
	}

	for name, testData := range tests {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			test := newMultiBucketClientTest(t, prometheus.NewPedanticRegistry())
			if testData.primaryContent != "" {
				uploadObject(t, test.primary, "dir/object", testData.primaryContent)
			}
			if testData.secondaryContent != "" {
				uploadObject(t, test.secondary, "dir/object", testData.secondaryContent)
			}
			expectFound := testData.expectedContent != ""

			exists, err := test.client.Exists(ctx, "dir/object")
			require.NoError(t, err)
			require.Equal(t, expectFound, exists)

			reader, err := test.client.Get(ctx, "dir/object")
			if expectFound {
				require.NoError(t, err)
				require.Equal(t, testData.expectedContent, readObject(t, reader))
			} else {
				require.True(t, test.client.IsObjNotFoundErr(err))
			}

			reader, err = test.client.GetRange(ctx, "dir/object", 1, 3)
			if expectFound {
				require.NoError(t, err)
				require.Equal(t, testData.expectedContent[1:4], readObject(t, reader))
			} else {
				require.True(t, test.client.IsObjNotFoundErr(err))
			}

			attrs, err := test.client.Attributes(ctx, "dir/object")
			if expectFound {
				require.NoError(t, err)
				require.Equal(t, int64(len(testData.expectedContent)), attrs.Size)
			} else {
				require.True(t, test.client.IsObjNotFoundErr(err))
			}
		})
	}
}

func TestMultiBucketClient_ReadReturnsPrimaryErrorThatIsNotNotFound(t *testing.T) {
	ctx := context.Background()
	test := newMultiBucketClientTest(t, prometheus.NewPedanticRegistry())
	uploadObject(t, test.secondary, "object", "secondary")

	injectedErr := errors.New("injected error")
	test.primaryInjector.Injector = InjectErrorOn(OpGet|OpAttributes, "object", injectedErr)

	_, err := test.client.Get(ctx, "object")
	require.ErrorIs(t, err, injectedErr)

	_, err = test.client.Attributes(ctx, "object")
	require.ErrorIs(t, err, injectedErr)
}

func TestMultiBucketClient_Upload(t *testing.T) {
	ctx := context.Background()
	test := newMultiBucketClientTest(t, prometheus.NewPedanticRegistry())

	require.NoError(t, test.client.Upload(ctx, "object", bytes.NewReader([]byte("content"))))

	exists, err := test.primary.Exists(ctx, "object")
	require.NoError(t, err)
	require.True(t, exists)

	exists, err = test.secondary.Exists(ctx, "object")
	require.NoError(t, err)
	require.False(t, exists)
}

func TestMultiBucketClient_Iter(t *testing.T) {
	ctx := context.Background()
	test := newMultiBucketClientTest(t, prometheus.NewPedanticRegistry())
	for _, name := range []string{"tenant/a/meta.json", "tenant/b/meta.json", "tenant/bucket-index.json.gz", "tenant/markers/a-deletion-mark.json"} {
		uploadObject(t, test.primary, name, "primary")
	}
	for _, name := range []string{"tenant/0/meta.json", "tenant/a/meta.json", "tenant/bucket-index.json.gz", "tenant/c/meta.json", "tenant/markers/c-no-compact-mark.json", "tenant/z/meta.json"} {
		uploadObject(t, test.secondary, name, "secondary")
	}

	tests := map[string]struct {
		options  []objstore.IterOption
		expected []string
	}{
		"non-recursive": {
			expected: []string{"tenant/0/", "tenant/a/", "tenant/b/", "tenant/bucket-index.json.gz", "tenant/c/", "tenant/markers/", "tenant/z/"},
		},
		"recursive": {
			options: []objstore.IterOption{objstore.WithRecursiveIter()},
			expected: []string{
				"tenant/0/meta.json", "tenant/a/meta.json", "tenant/b/meta.json", "tenant/bucket-index.json.gz", "tenant/c/meta.json",
				"tenant/markers/a-deletion-mark.json", "tenant/markers/c-no-compact-mark.json", "tenant/z/meta.json",
			},
		},
	}

	for name, testData := range tests {
		t.Run(name, func(t *testing.T) {
			var names []string
			require.NoError(t, test.client.Iter(ctx, "tenant/", func(name string) error {
				names = append(names, name)
				return nil
			}, testData.options...))
			require.Equal(t, testData.expected, names)

			names = nil
			require.NoError(t, test.client.IterWithAttributes(ctx, "tenant/", func(attrs objstore.IterObjectAttributes) error {
				names = append(names, attrs.Name)
				return nil
			}, testData.options...))
			require.Equal(t, testData.expected, names)
		})
	}

	t.Run("callback error stops the iteration", func(t *testing.T) {
		callbackErr := errors.New("callback error")
		var names []string
		err := test.client.Iter(ctx, "tenant/", func(name string) error {
			names = append(names, name)
			if len(names) == 3 {
				return callbackErr
			}
			return nil
		})
		require.ErrorIs(t, err, callbackErr)
		require.Equal(t, []string{"tenant/0/", "tenant/a/", "tenant/b/"}, names)
	})

	t.Run("secondary bucket error", func(t *testing.T) {
		injectedErr := errors.New("injected error")
		test.secondaryInjector.Injector = InjectErrorOn(OpIter, "tenant/", injectedErr)
		t.Cleanup(func() { test.secondaryInjector.Injector = nil })

		err := test.client.Iter(ctx, "tenant/", func(string) error { return nil })
		require.ErrorIs(t, err, injectedErr)
	})
}

func TestMultiBucketClient_Delete(t *testing.T) {
	injectedErr := errors.New("injected error")

	tests := map[string]struct {
		inPrimary         bool
		inSecondary       bool
		primaryErr        error
		secondaryErr      error
		expectNotFound    bool
		expectErr         error
		expectInPrimary   bool
		expectInSecondary bool
	}{
		"object only in the primary bucket": {
			inPrimary: true,
		},
		"object only in the secondary bucket": {
			inSecondary: true,
		},
		"object in the two buckets": {
			inPrimary:   true,
			inSecondary: true,
		},
		"object in neither bucket": {
			expectNotFound: true,
		},
		"secondary bucket fails, primary bucket succeeds": {
			inPrimary:         true,
			inSecondary:       true,
			secondaryErr:      injectedErr,
			expectErr:         injectedErr,
			expectInSecondary: true,
		},
		"primary bucket fails, secondary bucket succeeds": {
			inPrimary:       true,
			inSecondary:     true,
			primaryErr:      injectedErr,
			expectErr:       injectedErr,
			expectInPrimary: true,
		},
	}

	for name, testData := range tests {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			test := newMultiBucketClientTest(t, prometheus.NewPedanticRegistry())
			if testData.inPrimary {
				uploadObject(t, test.primary, "object", "primary")
			}
			if testData.inSecondary {
				uploadObject(t, test.secondary, "object", "secondary")
			}
			if testData.primaryErr != nil {
				test.primaryInjector.Injector = InjectErrorOn(OpDelete, "object", testData.primaryErr)
			}
			if testData.secondaryErr != nil {
				test.secondaryInjector.Injector = InjectErrorOn(OpDelete, "object", testData.secondaryErr)
			}

			err := test.client.Delete(ctx, "object")
			switch {
			case testData.expectErr != nil:
				require.ErrorIs(t, err, testData.expectErr)
			case testData.expectNotFound:
				require.True(t, test.client.IsObjNotFoundErr(err))
			default:
				require.NoError(t, err)
			}

			exists, err := test.primary.Exists(ctx, "object")
			require.NoError(t, err)
			require.Equal(t, testData.expectInPrimary, exists)

			exists, err = test.secondary.Exists(ctx, "object")
			require.NoError(t, err)
			require.Equal(t, testData.expectInSecondary, exists)
		})
	}
}

func TestMultiBucketClient_FailureMetrics(t *testing.T) {
	ctx := context.Background()

	assertFailures := func(t *testing.T, reg prometheus.Gatherer, primaryGetFailures, secondaryGetFailures int) {
		t.Helper()
		var expected bytes.Buffer
		expected.WriteString(`
			# HELP objstore_bucket_operation_failures_total Total number of operations against a bucket that failed, but were not expected to fail in certain way from caller perspective. Those errors have to be investigated.
			# TYPE objstore_bucket_operation_failures_total counter
		`)
		for _, bucketName := range []string{"primary", "secondary"} {
			for _, op := range []string{objstore.OpAttributes, objstore.OpDelete, objstore.OpExists, objstore.OpGet, objstore.OpGetRange, objstore.OpIter, objstore.OpUpload} {
				value := 0
				if op == objstore.OpGet && bucketName == "primary" {
					value = primaryGetFailures
				}
				if op == objstore.OpGet && bucketName == "secondary" {
					value = secondaryGetFailures
				}
				expected.WriteString(`objstore_bucket_operation_failures_total{bucket="` + bucketName + `",operation="` + op + `"} ` + strconv.Itoa(value) + "\n")
			}
		}
		require.NoError(t, testutil.GatherAndCompare(reg, &expected, "objstore_bucket_operation_failures_total"))
	}

	t.Run("a read of an object in the secondary bucket is not a failure", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		test := newMultiBucketClientTest(t, reg)
		uploadObject(t, test.secondary, "object", "secondary")

		reader, err := test.client.Get(ctx, "object")
		require.NoError(t, err)
		require.Equal(t, "secondary", readObject(t, reader))

		assertFailures(t, reg, 0, 0)
	})

	t.Run("a read of a missing object is a failure of the secondary bucket", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		test := newMultiBucketClientTest(t, reg)

		_, err := test.client.Get(ctx, "object")
		require.True(t, test.client.IsObjNotFoundErr(err))

		assertFailures(t, reg, 0, 1)
	})

	t.Run("errors the caller expects are not failures", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		test := newMultiBucketClientTest(t, reg)

		_, err := test.client.ReaderWithExpectedErrs(test.client.IsObjNotFoundErr).Get(ctx, "object")
		require.True(t, test.client.IsObjNotFoundErr(err))

		assertFailures(t, reg, 0, 0)
	})
}

func TestNewClientWithSecondary(t *testing.T) {
	ctx := context.Background()
	primaryDir := t.TempDir()
	secondaryDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(secondaryDir, "tenant"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(secondaryDir, "tenant", "object"), []byte("secondary"), 0o644))

	cfg := Config{StorageBackendConfig: StorageBackendConfig{Backend: Filesystem}}
	cfg.Filesystem.Directory = primaryDir

	t.Run("without a secondary bucket name", func(t *testing.T) {
		client, err := NewClientWithSecondary(ctx, cfg, "", "test", log.NewNopLogger(), nil)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, client.Close()) })

		_, isMultiBucketClient := client.(*MultiBucketClient)
		require.False(t, isMultiBucketClient)

		_, err = client.Get(ctx, "tenant/object")
		require.True(t, client.IsObjNotFoundErr(err))
	})

	t.Run("with a secondary bucket name", func(t *testing.T) {
		client, err := NewClientWithSecondary(ctx, cfg, secondaryDir, "test", log.NewNopLogger(), prometheus.NewPedanticRegistry())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, client.Close()) })

		reader, err := client.Get(ctx, "tenant/object")
		require.NoError(t, err)
		require.Equal(t, "secondary", readObject(t, reader))

		require.NoError(t, client.Upload(ctx, "tenant/new-object", strings.NewReader("new")))
		_, err = os.Stat(filepath.Join(primaryDir, "tenant", "new-object"))
		require.NoError(t, err)
		_, err = os.Stat(filepath.Join(secondaryDir, "tenant", "new-object"))
		require.True(t, os.IsNotExist(err))
	})
}
