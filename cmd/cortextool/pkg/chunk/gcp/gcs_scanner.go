package gcp

import (
	"context"
	"fmt"
	"io/ioutil"

	"cloud.google.com/go/storage"
	"github.com/grafana/mimir/pkg/chunk"
	"github.com/grafana/mimir/pkg/chunk/gcp"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/api/iterator"

	chunkTool "github.com/grafana/mimir/cmd/cortextool/pkg/chunk"
)

type gcsScanner struct {
	config gcp.GCSConfig
	client *storage.Client
	bucket *storage.BucketHandle
}

// NewGcsScanner returns a bigtable scanner
func NewGcsScanner(ctx context.Context, cfg gcp.GCSConfig) (chunkTool.Scanner, error) {
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
func (s *gcsScanner) Scan(ctx context.Context, req chunkTool.ScanRequest, filterFunc chunkTool.FilterFunc, out chan chunk.Chunk) error {
	decodeContext := chunk.NewDecodeContext()

	it := s.bucket.Objects(ctx, &storage.Query{
		Prefix: req.User + "/" + req.Prefix,
	})

	for {
		objAttrs, err := it.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			return fmt.Errorf("unable to iterate chunks, err: %v, user: %v", err, req.User)
		}

		c, err := chunk.ParseExternalKey(req.User, objAttrs.Name)
		if err != nil {
			return errors.WithStack(err)
		}

		if !req.CheckTime(c.From, c.Through) {
			fmt.Println(*req.Interval, c.From, c.Through)
			logrus.Debugln("skipping chunk updated at timestamp outside filters range")
			continue
		}

		reader, err := s.bucket.Object(objAttrs.Name).NewReader(ctx)
		if err != nil {
			return errors.WithStack(err)
		}

		buf, err := ioutil.ReadAll(reader)
		reader.Close()

		if err != nil {
			return errors.WithStack(err)
		}

		if err := c.Decode(decodeContext, buf); err != nil {
			return err
		}

		if filterFunc(c) {
			out <- c
		}
	}

	return nil
}
