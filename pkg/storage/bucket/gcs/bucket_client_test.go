// SPDX-License-Identifier: AGPL-3.0-only

package gcs

import (
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"google.golang.org/api/option"
)

func TestRetryAlwaysBucketUpload(t *testing.T) {
	t.Run("applies conditional options", func(t *testing.T) {
		tests := map[string]struct {
			option  objstore.ObjectUploadOption
			wantURI string
		}{
			"if not exists": {
				option:  objstore.WithIfNotExists(),
				wantURI: "/upload/storage/v1/b/buck/o?alt=json&ifGenerationMatch=0&name=obj&prettyPrint=false&projection=full&uploadType=multipart",
			},
			"if match": {
				option:  objstore.WithIfMatch(&objstore.ObjectVersion{Type: objstore.Generation, Value: "1234"}),
				wantURI: "/upload/storage/v1/b/buck/o?alt=json&ifGenerationMatch=1234&name=obj&prettyPrint=false&projection=full&uploadType=multipart",
			},
			"if not match": {
				option:  objstore.WithIfNotMatch(&objstore.ObjectVersion{Type: objstore.Generation, Value: "1234"}),
				wantURI: "/upload/storage/v1/b/buck/o?alt=json&ifGenerationNotMatch=1234&name=obj&prettyPrint=false&projection=full&uploadType=multipart",
			},
		}
		for name, tc := range tests {
			t.Run(name, func(t *testing.T) {
				requests := make(chan string, 1)
				handlerErrs := make(chan error, 1)
				bkt := newRetryAlwaysBucketWithHandler(t, func(w http.ResponseWriter, r *http.Request) {
					if _, err := io.Copy(io.Discard, r.Body); err != nil {
						handlerErrs <- err
						http.Error(w, err.Error(), http.StatusInternalServerError)
						return
					}
					requests <- r.Method + " " + r.RequestURI

					w.Header().Set("Content-Type", "application/json")
					if _, err := w.Write([]byte(`{"bucket":"buck","name":"obj","generation":"1","metageneration":"1"}`)); err != nil {
						handlerErrs <- err
					}
				})

				uploadErr := bkt.Upload(t.Context(), "obj", strings.NewReader("data"), tc.option)
				select {
				case err := <-handlerErrs:
					require.NoError(t, err)
				default:
				}
				require.NoError(t, uploadErr)

				select {
				case actual := <-requests:
					require.Equal(t, "POST "+tc.wantURI, actual)
				case <-time.After(5 * time.Second):
					t.Fatal("timed out waiting for GCS upload request")
				}
			})
		}
	})

	t.Run("applies content type with default chunk size", func(t *testing.T) {
		metadataContentTypes := make(chan string, 1)
		handlerErrs := make(chan error, 1)
		bkt := newRetryAlwaysBucketWithHandler(t, func(w http.ResponseWriter, r *http.Request) {
			contentType, err := gcsUploadMetadataContentType(r)
			if err != nil {
				handlerErrs <- err
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			metadataContentTypes <- contentType

			w.Header().Set("Content-Type", "application/json")
			if _, err := w.Write([]byte(`{"bucket":"buck","name":"obj","generation":"1","metageneration":"1"}`)); err != nil {
				handlerErrs <- err
			}
		})
		require.Zero(t, bkt.chunkSize)

		uploadErr := bkt.Upload(t.Context(), "obj", strings.NewReader("data"), objstore.WithContentType("application/custom"))
		select {
		case err := <-handlerErrs:
			require.NoError(t, err)
		default:
		}
		require.NoError(t, uploadErr)

		select {
		case actual := <-metadataContentTypes:
			require.Equal(t, "application/custom", actual)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for GCS upload metadata")
		}
	})

	t.Run("rejects invalid conditions", func(t *testing.T) {
		tests := map[string]struct {
			option objstore.ObjectUploadOption
		}{
			"non-generation condition": {
				option: objstore.WithIfMatch(&objstore.ObjectVersion{Type: objstore.ETag, Value: "etag"}),
			},
			"malformed generation": {
				option: objstore.WithIfMatch(&objstore.ObjectVersion{Type: objstore.Generation, Value: "not-int"}),
			},
		}
		for name, tc := range tests {
			t.Run(name, func(t *testing.T) {
				requests := make(chan string, 1)
				bkt := newRetryAlwaysBucketWithHandler(t, func(_ http.ResponseWriter, r *http.Request) {
					requests <- r.Method + " " + r.RequestURI
				})

				err := bkt.Upload(t.Context(), "obj", strings.NewReader("data"), tc.option)
				require.ErrorIs(t, err, objstore.ErrUploadOptionInvalid)

				select {
				case actual := <-requests:
					t.Fatalf("unexpected GCS upload request: %s", actual)
				default:
				}
			})
		}
	})
}

func newRetryAlwaysBucketWithHandler(t *testing.T, handler http.HandlerFunc) *retryAlwaysBucket {
	t.Helper()

	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	client, err := storage.NewClient(
		t.Context(),
		option.WithEndpoint(server.URL+"/storage/v1/"),
		option.WithoutAuthentication(),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, client.Close())
	})

	return &retryAlwaysBucket{
		bkt: client.Bucket("buck").Retryer(storage.WithPolicy(storage.RetryAlways)),
	}
}

func gcsUploadMetadataContentType(r *http.Request) (string, error) {
	mediaType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil {
		return "", err
	}
	if mediaType != "multipart/related" {
		return "", fmt.Errorf("unexpected upload content type %q", mediaType)
	}

	boundary := params["boundary"]
	if boundary == "" {
		return "", fmt.Errorf("upload content type missing multipart boundary")
	}

	reader := multipart.NewReader(r.Body, boundary)
	part, err := reader.NextPart()
	if err != nil {
		return "", err
	}

	var metadata struct {
		ContentType string `json:"contentType"`
	}
	if err := json.NewDecoder(part).Decode(&metadata); err != nil {
		_ = part.Close()
		return "", err
	}
	if _, err := io.Copy(io.Discard, part); err != nil {
		_ = part.Close()
		return "", err
	}
	if err := part.Close(); err != nil {
		return "", err
	}

	for {
		part, err := reader.NextPart()
		if err == io.EOF {
			return metadata.ContentType, nil
		}
		if err != nil {
			return "", err
		}
		if _, err := io.Copy(io.Discard, part); err != nil {
			_ = part.Close()
			return "", err
		}
		if err := part.Close(); err != nil {
			return "", err
		}
	}
}
