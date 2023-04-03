package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/go-kit/log"
	"github.com/grafana/dskit/concurrency"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/thanos-io/objstore"
	"google.golang.org/api/iterator"

	"github.com/grafana/mimir/pkg/storegateway/indexheader"
)

func main() {
	ctx := context.Background()
	logger := log.NewLogfmtLogger(os.Stdout)

	bucketName := flag.String("bucket-name", "", "The bucket name")
	concurrencyLimit := 10
	flag.Parse()

	if bucketName == nil {
		logger.Log("msg", "no bucket name provided")
		os.Exit(1)
	}

	fmt.Println("Block index headers are going to be downloaded to:", os.TempDir())
	fmt.Println("Remember to clean it up after the tool runs.")
	fmt.Println("")

	// Parse blocks to analyse.
	blockRefs := flag.Args()

	// Create the GCS bucket client.
	gcsClient, err := storage.NewClient(ctx)
	if err != nil {
		logger.Log("msg", "unable to create GCS client", "err", err)
		os.Exit(1)
	}

	bucketClient := gcsClient.Bucket(*bucketName)

	concurrency.ForEachJob(ctx, len(blockRefs), concurrencyLimit, func(ctx context.Context, idx int) error {
		blockRef := blockRefs[idx]
		logger.Log("msg", "checking block", "ref", blockRef)

		if isValid, err := analyzeBlock(ctx, bucketClient, blockRef, logger); err != nil {
			logger.Log("msg", "unable to analyze block", "ref", blockRef, "err", err)
		} else if !isValid {
			logger.Log("msg", "invalid block", "ref", blockRef)
		} else {
			logger.Log("msg", "valid block", "ref", blockRef)
		}

		return nil
	})
}

func analyzeBlock(ctx context.Context, bucketClient *storage.BucketHandle, blockRef string, logger log.Logger) (bool, error) {
	// Parse the block ref.
	parts := strings.Split(blockRef, "/")
	if len(parts) != 2 {
		return false, fmt.Errorf("invalid block ref %s", blockRef)
	}

	userID := parts[0]
	blockID, err := ulid.Parse(parts[1])
	if err != nil {
		return false, errors.Wrapf(err, "unable to parse block ID from block ref %s", blockRef)
	}

	// Download index header and get a reader.
	reader, err := indexheader.NewStreamBinaryReader(ctx, logger, newIndexBucketReader(bucketClient, userID), os.TempDir(), blockID, 32, indexheader.NewStreamBinaryReaderMetrics(nil), indexheader.Config{MaxIdleFileHandles: 1})
	if err != nil {
		return false, errors.Wrapf(err, "unable to download index header for block ref %s", blockRef)
	}

	labelNames, err := reader.LabelNames()
	if err != nil {
		return false, errors.Wrapf(err, "unable to get label names for block ref %s", blockRef)
	}

	// Validate all label names.
	var invalidLabelNames []string

	for _, labelName := range labelNames {
		if !model.LabelName(labelName).IsValid() {
			invalidLabelNames = append(invalidLabelNames, labelName)
		}
	}

	if len(invalidLabelNames) > 0 {
		logger.Log("msg", "found invalid label names", "names", strings.Join(invalidLabelNames, " "))
		return false, nil
	}

	return true, nil
}

func downloadIndexHeader(ctx context.Context, bucketClient *storage.BucketHandle, userID string, blockID ulid.ULID) (string, error) {
	filename := filepath.Join(os.TempDir(), blockID.String()+"-index-header")

	err := indexheader.WriteBinary(ctx, newIndexBucketReader(bucketClient, userID), blockID, filename)
	if err != nil {
		return "", err
	}

	return filename, nil
}

type indexBucketReader struct {
	bucketClient *storage.BucketHandle
	userID       string
}

func newIndexBucketReader(bucketClient *storage.BucketHandle, userID string) *indexBucketReader {
	return &indexBucketReader{
		bucketClient: bucketClient,
		userID:       userID,
	}
}

func (r *indexBucketReader) Iter(ctx context.Context, dir string, f func(string) error, options ...objstore.IterOption) error {
	return errors.New("Iter is not implemented")
}

func (r *indexBucketReader) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	return nil, errors.New("Get is not implemented")
}

func (r *indexBucketReader) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	name = path.Join(r.userID, name)

	generation, err := getObjectGeneration(ctx, r.bucketClient, name)
	if err != nil {
		return nil, err
	}

	return r.bucketClient.Object(name).Generation(generation).NewRangeReader(ctx, off, length)
}

func (r *indexBucketReader) Exists(ctx context.Context, name string) (bool, error) {
	return false, errors.New("Exists is not implemented")
}

func (r *indexBucketReader) IsObjNotFoundErr(err error) bool {
	return false
}

func (r *indexBucketReader) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	name = path.Join(r.userID, name)
	attrs := objstore.ObjectAttributes{}

	generation, err := getObjectGeneration(ctx, r.bucketClient, name)
	if err != nil {
		return attrs, err
	}

	attrsRaw, err := r.bucketClient.Object(name).Generation(generation).Attrs(ctx)
	if err != nil {
		return attrs, err
	}

	attrs.Size = attrsRaw.Size
	return attrs, nil
}

func getObjectGeneration(ctx context.Context, bucketClient *storage.BucketHandle, objectPath string) (int64, error) {
	it := bucketClient.Objects(ctx, &storage.Query{
		Prefix:   objectPath,
		Versions: true,
	})

	// Returns the 1st generation.
	attrs, err := it.Next()
	if errors.Is(err, iterator.Done) {
		return 0, errors.New("object not found")
	}

	return attrs.Generation, nil
}
