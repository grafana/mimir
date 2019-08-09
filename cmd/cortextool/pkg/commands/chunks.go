package commands

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/gcp"
	"github.com/grafana/cortex-tool/pkg/chunk/filter"
	toolGCP "github.com/grafana/cortex-tool/pkg/chunk/gcp"
	"github.com/grafana/cortex-tool/pkg/chunk/tool"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"
	"gopkg.in/yaml.v2"
)

// SchemaConfig contains the config for our chunk index schemas
type SchemaConfig struct {
	Config   *chunk.PeriodConfig `yaml:"configs"`
	FileName string
}

// Load the yaml file, or build the config from legacy command-line flags
func (cfg *SchemaConfig) Load() error {
	if cfg.Config != nil {
		return nil
	}

	f, err := os.Open(cfg.FileName)
	if err != nil {
		return err
	}

	decoder := yaml.NewDecoder(f)
	decoder.SetStrict(true)
	return decoder.Decode(&cfg)
}

type chunkCommandOptions struct {
	Bigtable     gcp.Config
	DryRun       bool
	Schema       SchemaConfig
	FilterConfig filter.Config
}

type deleteChunkCommandOptions struct {
	chunkCommandOptions
	GCS gcp.GCSConfig
}

type deleteSeriesCommandOptions struct {
	chunkCommandOptions
}

func registerDeleteChunkCommandOptions(cmd *kingpin.CmdClause) {
	deleteChunkCommandOptions := &deleteChunkCommandOptions{}
	deleteChunkCommand := cmd.Command("delete", "Deletes the specified chunk references from the index").Action(deleteChunkCommandOptions.run)
	deleteChunkCommand.Flag("dryrun", "if enabled, no delete action will be taken").BoolVar(&deleteChunkCommandOptions.DryRun)
	deleteChunkCommand.Flag("bigtable.project", "bigtable project to use").StringVar(&deleteChunkCommandOptions.Bigtable.Project)
	deleteChunkCommand.Flag("bigtable.instance", "bigtable instance to use").StringVar(&deleteChunkCommandOptions.Bigtable.Instance)
	deleteChunkCommand.Flag("chunk.gcs.bucketname", "specify gcs bucket to scan for chunks").StringVar(&deleteChunkCommandOptions.GCS.BucketName)
	deleteChunkCommand.Flag("schema-file", "path to file containing cortex schema config").Required().StringVar(&deleteChunkCommandOptions.Schema.FileName)
	deleteChunkCommandOptions.FilterConfig.Register(deleteChunkCommand)
}

func registerDeleteSeriesCommandOptions(cmd *kingpin.CmdClause) {
	deleteSeriesCommandOptions := &deleteSeriesCommandOptions{}
	deleteSeriesCommand := cmd.Command("delete-series", "Deletes the specified chunk references from the index").Action(deleteSeriesCommandOptions.run)
	deleteSeriesCommand.Flag("dryrun", "if enabled, no delete action will be taken").BoolVar(&deleteSeriesCommandOptions.DryRun)
	deleteSeriesCommand.Flag("bigtable.project", "bigtable project to use").StringVar(&deleteSeriesCommandOptions.Bigtable.Project)
	deleteSeriesCommand.Flag("bigtable.instance", "bigtable instance to use").StringVar(&deleteSeriesCommandOptions.Bigtable.Instance)
	deleteSeriesCommand.Flag("schema-file", "path to file containing cortex schema config").Required().StringVar(&deleteSeriesCommandOptions.Schema.FileName)
	deleteSeriesCommandOptions.FilterConfig.Register(deleteSeriesCommand)
}

// RegisterChunkCommands registers the ChunkCommand flags with the kingpin applicattion
func RegisterChunkCommands(app *kingpin.Application) {
	chunkCommand := app.Command("chunk", "Chunk related operations")
	registerDeleteChunkCommandOptions(chunkCommand)
	registerDeleteSeriesCommandOptions(chunkCommand)
}

func (c *deleteChunkCommandOptions) run(k *kingpin.ParseContext) error {
	err := c.Schema.Load()
	if err != nil {
		return errors.Wrap(err, "unable to load schema")
	}

	ctx := context.Background()
	fltr := filter.NewMetricFilter(c.FilterConfig)

	var (
		scanner tool.Scanner
		deleter tool.Deleter
	)

	switch c.Schema.Config.ObjectType {
	case "bigtable":
		logrus.Infof("bigtable object store, project=%v, instance=%v", c.Bigtable.Project, c.Bigtable.Instance)
		scanner, err = toolGCP.NewBigtableScanner(ctx, c.Bigtable.Project, c.Bigtable.Instance)
		if err != nil {
			return errors.Wrap(err, "unable to initialize scanner")
		}
	case "gcs":
		logrus.Infof("gcs object store, bucket=%v", c.GCS.BucketName)
		scanner, err = toolGCP.NewGcsScanner(ctx, c.GCS)
		if err != nil {
			return errors.Wrap(err, "unable to initialize scanner")
		}
	default:
		return fmt.Errorf("object store type %v not supported for deletes", c.Schema.Config.ObjectType)
	}

	switch c.Schema.Config.IndexType {
	case "bigtable":
		logrus.Infof("bigtable deleter, project=%v, instance=%v", c.Bigtable.Project, c.Bigtable.Instance)
		deleter, err = toolGCP.NewStorageIndexDeleter(ctx, c.Bigtable)
		if err != nil {
			return errors.Wrap(err, "unable to initialize deleter")
		}
	case "bigtable-hashed":
		logrus.Infof("bigtable deleter, project=%v, instance=%v", c.Bigtable.Project, c.Bigtable.Instance)
		c.Bigtable.DistributeKeys = true
		deleter, err = toolGCP.NewStorageIndexDeleter(ctx, c.Bigtable)
		if err != nil {
			return errors.Wrap(err, "unable to initialize deleter")
		}
	default:
		return fmt.Errorf("index store type %v not supported for deletes", c.Schema.Config.IndexType)
	}

	outChan := make(chan chunk.Chunk, 100)

	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		schema := c.Schema.Config.CreateSchema()
		for chk := range outChan {
			logrus.Infof("found chunk eligible for deletion: %s, from: %s, to: %s\n", chk.ExternalKey(), chk.From.Time().String(), chk.Through.Time().String())
			entries, err := schema.GetChunkWriteEntries(chk.From, chk.Through, chk.UserID, chk.Metric.Get(labels.MetricName), chk.Metric, chk.ExternalKey())
			if err != nil {
				logrus.Errorln(err)
			}
			for _, e := range entries {
				if !c.DryRun {
					err := deleter.DeleteEntry(ctx, e)
					if err != nil {
						logrus.Errorln(err)
					}
				}
			}

		}
		wg.Done()
	}()

	table := c.Schema.Config.ChunkTables.TableFor(fltr.From)
	err = scanner.Scan(ctx, table, fltr, outChan)
	close(outChan)
	if err != nil {
		return errors.Wrap(err, "scan failed")
	}

	wg.Wait()
	return nil
}

func (c *deleteSeriesCommandOptions) run(k *kingpin.ParseContext) error {
	err := c.Schema.Load()
	if err != nil {
		return errors.Wrap(err, "unable to load schema")
	}

	ctx := context.Background()

	fltr := filter.NewMetricFilter(c.FilterConfig)

	var deleter tool.Deleter

	switch c.Schema.Config.IndexType {
	case "bigtable":
		logrus.Infof("bigtable deleter, project=%v, instance=%v", c.Bigtable.Project, c.Bigtable.Instance)
		deleter, err = toolGCP.NewStorageIndexDeleter(ctx, c.Bigtable)
		if err != nil {
			return errors.Wrap(err, "unable to initialize deleter")
		}
	case "bigtable-hashed":
		logrus.Infof("bigtable deleter, project=%v, instance=%v", c.Bigtable.Project, c.Bigtable.Instance)
		c.Bigtable.DistributeKeys = true
		deleter, err = toolGCP.NewStorageIndexDeleter(ctx, c.Bigtable)
		if err != nil {
			return errors.Wrap(err, "unable to initialize deleter")
		}
	default:
		return fmt.Errorf("index store type %v not supported for deletes", c.Schema.Config.IndexType)
	}

	schema := c.Schema.Config.CreateSchema()

	deleteMetricNameRows, err := schema.GetReadQueriesForMetric(fltr.From, fltr.To, fltr.User, fltr.Name)
	if err != nil {
		logrus.Errorln(err)
	}

	for _, query := range deleteMetricNameRows {
		logrus.WithFields(logrus.Fields{
			"table":     query.TableName,
			"hashvalue": query.HashValue,
			"dryrun":    c.DryRun,
		}).Debugln("deleting series from index")
		if !c.DryRun {
			errs, err := deleter.DeleteSeries(ctx, query)
			for _, e := range errs {
				logrus.WithError(e).Errorln("series deletion error")
			}
			if err != nil {
				return err
			}
		}
	}

	return nil
}
