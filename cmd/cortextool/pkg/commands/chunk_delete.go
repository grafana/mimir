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

type DeleteChunkCommand struct {
	Table          string
	ChunkStore     string
	ChunkGCPConfig gcp.Config
	ChunkGCSConfig gcp.GCSConfig
	IndexGCPConfig gcp.Config
	FilterConfig   filter.Config
	SchemaFile     string
}

func (c *DeleteChunkCommand) Register(app *kingpin.Application) {
	cmd := app.Command("delete", "Deletes the specified chunks").Action(c.run)
	cmd.Flag("chunkstore.type", "specify which object store backend to utilize").StringVar(&c.ChunkStore)
	cmd.Flag("chunk.bigtable.project", "bigtable project to use").StringVar(&c.ChunkGCPConfig.Project)
	cmd.Flag("chunk.bigtable.instance", "bigtable instance to use").StringVar(&c.ChunkGCPConfig.Instance)
	cmd.Flag("chunk.bigtable.table", "bigtable table to use").StringVar(&c.Table)
	cmd.Flag("chunk.gcs.bucketname", "name of the gcs bucket to use").StringVar(&c.ChunkGCSConfig.BucketName)
	cmd.Flag("index.bigtable.project", "bigtable project to use").StringVar(&c.IndexGCPConfig.Project)
	cmd.Flag("index.bigtable.instance", "bigtable instance to use").StringVar(&c.IndexGCPConfig.Instance)
	cmd.Flag("index.bigtable.table", "bigtable table to use").StringVar(&c.Table)
	cmd.Flag("index.bigtable.column-key", "enable column key for bigtable index").BoolVar(&c.IndexGCPConfig.ColumnKey)
	cmd.Flag("index.bigtable.distribute-keys", "enable distributes for bigtable index").BoolVar(&c.IndexGCPConfig.DistributeKeys)
	cmd.Flag("schema.config-yaml", "path to file containing cortex schema config").StringVar(&c.SchemaFile)
	c.FilterConfig.Register(cmd)
}

func (c *DeleteChunkCommand) run(k *kingpin.ParseContext) error {
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
