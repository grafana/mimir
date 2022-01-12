package gcp

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigtable"
	"github.com/grafana/mimir/pkg/chunk"
	"github.com/sirupsen/logrus"

	chunkTool "github.com/grafana/mimir/cmd/cortextool/pkg/chunk"
)

type bigtableScanner struct {
	client *bigtable.Client
}

// NewBigtableScanner returns a bigtable scanner
func NewBigtableScanner(ctx context.Context, project, instance string) (chunkTool.Scanner, error) {
	client, err := bigtable.NewClient(ctx, project, instance)
	if err != nil {
		return nil, err
	}

	return &bigtableScanner{
		client: client,
	}, nil
}

// Scan forwards metrics to a golang channel, forwarded chunks must have the same
// user ID
func (s *bigtableScanner) Scan(ctx context.Context, req chunkTool.ScanRequest, filterFunc chunkTool.FilterFunc, out chan chunk.Chunk) error {
	var processingErr error

	table := s.client.Open(req.Table)
	decodeContext := chunk.NewDecodeContext()

	rr := bigtable.PrefixRange(req.User + "/" + req.Prefix)

	// Read through rows and forward slices of chunks with the same metrics
	// fingerprint
	err := table.ReadRows(ctx, rr, func(row bigtable.Row) bool {
		c, err := chunk.ParseExternalKey(req.User, row.Key())
		if err != nil {
			processingErr = err
			return false
		}

		if !req.CheckTime(c.From, c.Through) {
			logrus.Debugln("skipping chunk updated at timestamp outside filters range")
			return true
		}

		err = c.Decode(decodeContext, row[columnFamily][0].Value)
		if err != nil {
			processingErr = err
			return false
		}

		if filterFunc(c) {
			out <- c
		}
		return true
	})

	if err != nil {
		return fmt.Errorf("stream canceled, err: %v, table: %v, user: %v", err, req.Table, req.User)
	}
	if processingErr != nil {
		return fmt.Errorf("stream canceled, err: %v, table: %v, user: %v", processingErr, req.Table, req.User)
	}

	return nil
}
