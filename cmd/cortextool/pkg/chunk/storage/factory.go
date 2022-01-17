package storage

import (
	"context"
	"fmt"

	"github.com/grafana/mimir/pkg/chunk/storage"

	"github.com/grafana/mimir/cmd/cortextool/pkg/chunk"
	"github.com/grafana/mimir/cmd/cortextool/pkg/chunk/gcp"
)

// NewChunkScanner makes a new table client based on the configuration.
func NewChunkScanner(name string, cfg storage.Config) (chunk.Scanner, error) {
	switch name {
	case "gcp", "gcp-columnkey":
		return gcp.NewBigtableScanner(context.Background(), cfg.GCPStorageConfig.Project, cfg.GCPStorageConfig.Instance)
	case "gcs":
		return gcp.NewGcsScanner(context.Background(), cfg.GCSConfig)
	default:
		return nil, fmt.Errorf("unrecognized storage client %v, choose one of: gcp, gcp-columnkey, gcs", name)
	}
}
