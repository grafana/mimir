package chunk

import (
	"context"

	"github.com/grafana/mimir/pkg/chunk"
	"github.com/prometheus/common/model"
)

type FilterFunc func(chunk.Chunk) bool

// Scanner scans an
type Scanner interface {
	Scan(ctx context.Context, req ScanRequest, filterFunc FilterFunc, out chan chunk.Chunk) error
}

// ScannerProvider allows creating a new Scanner
type ScannerProvider interface {
	NewScanner() Scanner
}

// ScanRequest is used to designate the scope of a chunk table scan
// if Prefix is not set, scan all shards
// If Interval is not set, consider all chunks
type ScanRequest struct {
	Table    string
	User     string
	Prefix   string
	Interval *model.Interval
}

func (s *ScanRequest) CheckTime(from, through model.Time) bool {
	if s.Interval == nil {
		return true
	}

	if s.Interval.Start > through || from > s.Interval.End {
		return false
	}

	return true
}
