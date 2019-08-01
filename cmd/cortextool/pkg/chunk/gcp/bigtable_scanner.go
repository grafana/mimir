package gcp

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigtable"
	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/grafana/cortex-tool/pkg/chunk/filter"
	"github.com/grafana/cortex-tool/pkg/chunk/tool"
)

type bigtableScanner struct {
	client *bigtable.Client
}

// NewBigtableScanner returns a bigtable scanner
func NewBigtableScanner(ctx context.Context, project, instance string) (tool.Scanner, error) {
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
func (s *bigtableScanner) Scan(ctx context.Context, tbl string, mFilter filter.MetricFilter, out chan chunk.Chunk) error {
	var processingErr error

	table := s.client.Open(tbl)
	decodeContext := chunk.NewDecodeContext()
	rr := bigtable.PrefixRange(mFilter.User + "/")

	// Read through rows and forward slices of chunks with the same metrics
	// fingerprint
	err := table.ReadRows(ctx, rr, func(row bigtable.Row) bool {
		c, err := chunk.ParseExternalKey(mFilter.User, row.Key())
		if err != nil {
			processingErr = err
			return false
		}
		err = c.Decode(decodeContext, row[columnFamily][0].Value)
		if err != nil {
			processingErr = err
			return false
		}
		if mFilter.Filter(c) {
			out <- c
		}
		return true
	})

	if err != nil {
		return fmt.Errorf("stream canceled, err: %v, table: %v, user: %v", err, tbl, mFilter.User)
	}
	if processingErr != nil {
		return fmt.Errorf("stream canceled, err: %v, table: %v, user: %v", processingErr, tbl, mFilter.User)
	}

	return nil
}
