package gcp

import (
	"context"
	"fmt"
	"io/ioutil"

	"cloud.google.com/go/storage"
	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/gcp"
	"github.com/grafana/cortex-tool/pkg/chunk/filter"
	"github.com/grafana/cortex-tool/pkg/chunk/tool"
	"github.com/pkg/errors"
	"google.golang.org/api/iterator"
)

type gcsScanner struct {
	config gcp.GCSConfig
	client *storage.Client
	bucket *storage.BucketHandle
}

// NewGcsScanner returns a bigtable scanner
func NewGcsScanner(ctx context.Context, cfg gcp.GCSConfig) (tool.Scanner, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}

	bucket := client.Bucket(cfg.BucketName)

	return &gcsScanner{
		config: cfg,
		client: client,
		bucket: bucket,
	}, nil
}

// Scan forwards metrics to a golang channel, forwarded chunks must have the same
// user ID
func (s *gcsScanner) Scan(ctx context.Context, tbl string, mFilter filter.MetricFilter, out chan chunk.Chunk) error {
	decodeContext := chunk.NewDecodeContext()

	it := s.bucket.Objects(ctx, &storage.Query{
		Prefix: mFilter.User + "/",
	})

	for {
		objAttrs, err := it.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			return fmt.Errorf("unable to iterate chunks, err: %v, user: %v", err, mFilter.User)
		}

		c, err := chunk.ParseExternalKey(mFilter.User, objAttrs.Name)
		if err != nil {
			return errors.WithStack(err)
		}

		reader, err := s.bucket.Object(objAttrs.Name).NewReader(ctx)
		if err != nil {
			return errors.WithStack(err)
		}
		defer reader.Close()

		buf, err := ioutil.ReadAll(reader)
		if err != nil {
			return errors.WithStack(err)
		}

		if err := c.Decode(decodeContext, buf); err != nil {
			return err
		}

		if mFilter.Filter(c) {
			out <- c
		}
	}

	return nil
}
