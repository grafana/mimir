package commands

import (
	"context"
	"fmt"
	"sync"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/gcp"
	"github.com/grafana/cortex-tool/pkg/chunk/filter"
	toolGCP "github.com/grafana/cortex-tool/pkg/chunk/gcp"
	"github.com/grafana/cortex-tool/pkg/chunk/tool"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"
)

// ChunkCommand configures and executes chunk related cortex operations
type ChunkCommand struct {
	Table          string
	ChunkStore     string
	ChunkGCPConfig gcp.Config
	ChunkGCSConfig gcp.GCSConfig
	IndexGCPConfig gcp.Config
	FilterConfig   filter.Config
	SchemaFile     string
}

// Register chunk related commands and flags with the kingpin application
func (c *ChunkCommand) Register(app *kingpin.Application) {
	cmd := app.Command("chunk", "View & edit chunks stored in cortex.").Action(c.run)
	deleteCmd := cmd.Command("delete", "Delete specified chunks from cortex")
	deleteCmd.Flag("chunkstore.type", "specify which object store backend to utilize").StringVar(&c.ChunkStore)
	deleteCmd.Flag("chunk.bigtable.project", "bigtable project to use").StringVar(&c.ChunkGCPConfig.Project)
	deleteCmd.Flag("chunk.bigtable.instance", "bigtable instance to use").StringVar(&c.ChunkGCPConfig.Instance)
	deleteCmd.Flag("chunk.bigtable.table", "bigtable table to use").StringVar(&c.Table)
	deleteCmd.Flag("chunk.gcs.bucketname", "name of the gcs bucket to use").StringVar(&c.ChunkGCSConfig.BucketName)
	deleteCmd.Flag("index.bigtable.project", "bigtable project to use").StringVar(&c.IndexGCPConfig.Project)
	deleteCmd.Flag("index.bigtable.instance", "bigtable instance to use").StringVar(&c.IndexGCPConfig.Instance)
	deleteCmd.Flag("index.bigtable.table", "bigtable table to use").StringVar(&c.Table)
	deleteCmd.Flag("index.bigtable.column-key", "enable column key for bigtable index").BoolVar(&c.IndexGCPConfig.ColumnKey)
	deleteCmd.Flag("index.bigtable.distribute-keys", "enable distributes for bigtable index").BoolVar(&c.IndexGCPConfig.DistributeKeys)
	deleteCmd.Flag("schema.config-yaml", "path to file containing cortex schema config").StringVar(&c.SchemaFile)
	c.FilterConfig.Register(deleteCmd)
}

func (c *ChunkCommand) run(k *kingpin.ParseContext) error {
	var (
		scanner tool.Scanner
		err     error
	)

	ctx := context.Background()
	fltr := filter.NewMetricFilter(c.FilterConfig)

	switch c.ChunkStore {
	case "bigtable":
		logrus.Infof("bigtable object store, project=%v, instance=%v", c.ChunkGCPConfig.Project, c.ChunkGCPConfig.Instance)
		scanner, err = toolGCP.NewBigtableScanner(ctx, c.ChunkGCPConfig.Project, c.ChunkGCPConfig.Instance)
		if err != nil {
			return errors.Wrap(err, "unable to initialize scanner")
		}
	case "gcs":
		logrus.Infof("gcs object store, bucket=%v", c.ChunkGCSConfig.BucketName)
		scanner, err = toolGCP.NewGcsScanner(ctx, c.ChunkGCSConfig)
		if err != nil {
			return errors.Wrap(err, "unable to initialize scanner")
		}
	default:
		return errors.New("no object store specified")
	}

	outChan := make(chan chunk.Chunk, 100)

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go readChan(outChan, wg)

	err = scanner.Scan(ctx, c.Table, fltr, outChan)
	close(outChan)
	if err != nil {
		return errors.Wrap(err, "scan failed")
	}

	wg.Wait()
	return nil
}

func readChan(chunkChan chan chunk.Chunk, wg *sync.WaitGroup) {
	for c := range chunkChan {
		fmt.Printf("chunk: %s, from: %s, to: %s\n", c.ExternalKey(), c.From.Time().String(), c.Through.Time().String())
	}
	wg.Done()
}
