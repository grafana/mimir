package commands

import (
	"bufio"
	"context"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/cassandra"
	"github.com/cortexproject/cortex/pkg/chunk/gcp"
	"github.com/cortexproject/cortex/pkg/cortex"
	"github.com/grafana/dskit/flagext"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"gopkg.in/alecthomas/kingpin.v2"
	yamlV2 "gopkg.in/yaml.v2"
	"gopkg.in/yaml.v3"

	chunkTool "github.com/grafana/cortex-tools/pkg/chunk"
	toolCassandra "github.com/grafana/cortex-tools/pkg/chunk/cassandra"
	"github.com/grafana/cortex-tools/pkg/chunk/filter"
	toolGCP "github.com/grafana/cortex-tools/pkg/chunk/gcp"
	"github.com/grafana/cortex-tools/pkg/chunk/migrate"
)

var (
	chunkRefsDeleted = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "cortex",
		Name:      "chunk_entries_deleted_total",
		Help:      "Total count of entries deleted from the cortex index",
	})

	seriesEntriesDeleted = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "cortex",
		Name:      "series_entries_deleted_total",
		Help:      "Total count of entries deleted from the cortex index",
	})

	labelEntriesDeleted = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "cortex",
		Name:      "series_label_entries_deleted_total",
		Help:      "Total count of label entries deleted from the cortex index",
	})

	deletionDuration = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "cortex",
		Name:      "delete_operation_seconds",
		Help:      "The duration of the chunk deletion operation.",
	})
)

// SchemaConfig contains the config for our chunk index schemas
type SchemaConfig struct {
	Configs  []*chunk.PeriodConfig `yaml:"configs"`
	FileName string
}

// Load the yaml file, or build the config from legacy command-line flags
func (cfg *SchemaConfig) Load() error {
	if len(cfg.Configs) > 0 {
		return nil
	}

	f, err := os.Open(cfg.FileName)
	if err != nil {
		return err
	}

	decoder := yaml.NewDecoder(f)
	decoder.KnownFields(true)
	return decoder.Decode(&cfg)
}

type chunkCommandOptions struct {
	Bigtable     gcp.Config
	DryRun       bool
	Schema       SchemaConfig
	FilterConfig filter.Config
	DeleteSeries bool
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
	deleteChunkCommand.Flag("delete-series", "if enabled, the entire series will be deleted, not just the chunkID column").BoolVar(&deleteChunkCommandOptions.DeleteSeries)
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

type chunkCleanCommandOptions struct {
	CortexConfigFile      string
	InvalidIndexEntryFile string
	Table                 string
	BatchSize             int
	Concurrency           int
}

func registerChunkCleanCommandOptions(cmd *kingpin.CmdClause) {
	opts := &chunkCleanCommandOptions{}
	chunkCleanCommand := cmd.Command("clean-index", "Deletes the index entries specified in the provided file from the specified index table.").Action(opts.run)
	chunkCleanCommand.Flag("invalid-entry-file", "File with list of index entries to delete. This file is generated using the 'chunk validate-index` command.").Required().StringVar(&opts.InvalidIndexEntryFile)
	chunkCleanCommand.Flag("table", "Cortex index table to delete index entries from").Required().StringVar(&opts.Table)
	chunkCleanCommand.Flag("cortex-config-file", "Path to Cortex config file containing the Cassandra config").Required().StringVar(&opts.CortexConfigFile)
	chunkCleanCommand.Flag("batch-size", "How many deletes to submit in one batch").Default("100").IntVar(&opts.BatchSize)
	chunkCleanCommand.Flag("concurrency", "How many concurrent threads to run").Default("8").IntVar(&opts.Concurrency)
}

type validateIndexCommandOptions struct {
	CortexConfigFile      string
	Table                 string
	FromTimestamp         int64
	ToTimestamp           int64
	InvalidIndexEntryFile string
	TenantID              string
}

func registerValidateIndexCommandOptions(cmd *kingpin.CmdClause) {
	opts := &validateIndexCommandOptions{}
	validateIndexCommand := cmd.Command("validate-index", "Scans the provided Cortex index for invalid entries. Currently, only Cassandra is supported.").Action(opts.run)
	validateIndexCommand.Flag("cortex-config-file", "Path to a valid Cortex config file.").Required().StringVar(&opts.CortexConfigFile)
	validateIndexCommand.Flag("invalid-entry-file", "Path to file where the hash and range values of invalid index entries will be written.").Default("invalid-entries.txt").StringVar(&opts.InvalidIndexEntryFile)
	validateIndexCommand.Flag("table", "Cortex index table to scan for invalid index entries").Required().StringVar(&opts.Table)
	validateIndexCommand.Flag("from-unix-timestamp", "Set a valid unix timestamp in seconds to configure a minimum timestamp to scan for invalid entries.").Default("0").Int64Var(&opts.FromTimestamp)
	validateIndexCommand.Flag("to-unix-timestamp", "Set a valid unix timestamp in seconds to configure a maximum timestamp to scan for invalid entries.").Int64Var(&opts.ToTimestamp)
	validateIndexCommand.Flag("tenant-id", "Tenant ID to scan entries for.").Default("fake").StringVar(&opts.TenantID)
}

// RegisterChunkCommands registers the ChunkCommand flags with the kingpin applicattion
func RegisterChunkCommands(app *kingpin.Application) {
	chunkCommand := app.Command("chunk", "Chunk related operations").PreAction(setup)
	registerDeleteChunkCommandOptions(chunkCommand)
	registerDeleteSeriesCommandOptions(chunkCommand)
	registerMigrateChunksCommandOptions(chunkCommand)
	registerChunkCleanCommandOptions(chunkCommand)
	registerValidateIndexCommandOptions(chunkCommand)
}

func setup(k *kingpin.ParseContext) error {
	if strings.HasPrefix(k.String(), "chunk migrate") {
		return migrate.Setup()
	}
	prometheus.MustRegister(
		chunkRefsDeleted,
		seriesEntriesDeleted,
		labelEntriesDeleted,
	)
	return nil
}

func (c *chunkCleanCommandOptions) run(k *kingpin.ParseContext) error {
	cortexCfg := &cortex.Config{}
	flagext.RegisterFlags(cortexCfg)
	err := LoadConfig(c.CortexConfigFile, true, cortexCfg)
	if err != nil {
		return errors.Wrap(err, "failed to parse Cortex config")
	}

	err = cortexCfg.Schema.Load()
	if err != nil {
		return errors.Wrap(err, "failed to load schemas")
	}

	logrus.Debug("Connecting to Cassandra")
	client, err := cassandra.NewStorageClient(cortexCfg.Storage.CassandraStorageConfig, cortexCfg.Schema, nil)
	if err != nil {
		return errors.Wrap(err, "failed to connect to Cassandra")
	}

	logrus.Debug("Connected")

	inputFile, err := os.Open(c.InvalidIndexEntryFile)
	if err != nil {
		return errors.Wrap(err, "failed opening input file")
	}
	scanner := bufio.NewScanner(inputFile)
	scanner.Split(bufio.ScanLines)

	// One channel message per input line.
	lineCh := make(chan string, c.Concurrency)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var totalLineCnt uint32

	g, ctx := errgroup.WithContext(ctx)
	for i := 0; i < c.Concurrency; i++ {
		g.Go(func() error {
			batch := client.NewWriteBatch()
			lineCnt := 0
			for line := range lineCh {
				select {
				case <-ctx.Done():
					return nil
				default:
				}

				logrus.Debugf("processing line: %s", line)
				parts := strings.SplitN(line, ",", 2)
				if len(parts) != 2 {
					logrus.WithFields(logrus.Fields{
						"line": line,
					}).Errorln("invalid input format")
					continue
				}

				parts[0], parts[1] = strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
				if parts[1][:2] == "0x" {
					parts[1] = parts[1][2:]
				}

				rangeVal, err := hex.DecodeString(parts[1])
				if err != nil {
					logrus.WithFields(logrus.Fields{
						"hash":  parts[0],
						"range": parts[1],
					}).WithError(err).Errorln("invalid range value")
					continue
				}

				batch.Delete(c.Table, parts[0], rangeVal)
				lineCnt++

				if lineCnt >= c.BatchSize {
					writeBatch(ctx, client, batch)
					batch = client.NewWriteBatch()
					lineCnt = 0
				}

				newTotalLineCnt := atomic.AddUint32(&totalLineCnt, 1)
				if newTotalLineCnt%25000 == 0 {
					logrus.WithFields(logrus.Fields{
						"entries_cleaned_up": newTotalLineCnt,
					}).Infoln("cleanup progress")
				}
			}

			writeBatch(ctx, client, batch)
			return nil
		})
	}

	go func() {
		for scanner.Scan() {
			lineCh <- scanner.Text()
		}
		close(lineCh)
	}()

	err = g.Wait()
	if err != nil {
		return errors.Wrap(err, "failed to delete invalid index entries")
	}

	logrus.WithFields(logrus.Fields{
		"entries_cleaned_up": totalLineCnt,
	}).Infoln("cleanup complete")

	return nil
}

func writeBatch(ctx context.Context, client *cassandra.StorageClient, batch chunk.WriteBatch) {
	logrus.Debugf("applying batch")
	for retries := 5; retries > 0; retries-- {
		err := client.BatchWrite(ctx, batch)
		if err != nil {
			if retries > 1 {
				logrus.WithError(err).Warnln("failed to apply batch write, retrying")
			} else {
				logrus.WithError(err).Errorln("failed to apply batch write, giving up")
			}
		}
	}
}

// LoadConfig read YAML-formatted config from filename into cfg.
func LoadConfig(filename string, expandENV bool, cfg *cortex.Config) error {
	buf, err := ioutil.ReadFile(filename)
	if err != nil {
		return errors.Wrap(err, "Error reading config file")
	}

	if expandENV {
		buf = expandEnv(buf)
	}

	err = yamlV2.Unmarshal(buf, cfg)
	if err != nil {
		return errors.Wrap(err, "Error parsing config file")
	}

	return nil
}

// expandEnv replaces ${var} or $var in config according to the values of the current environment variables.
// The replacement is case-sensitive. References to undefined variables are replaced by the empty string.
// A default value can be given by using the form ${var:default value}.
func expandEnv(config []byte) []byte {
	return []byte(os.Expand(string(config), func(key string) string {
		keyAndDefault := strings.SplitN(key, ":", 2)
		key = keyAndDefault[0]

		v := os.Getenv(key)
		if v == "" && len(keyAndDefault) == 2 {
			v = keyAndDefault[1] // Set value to the default.
		}
		return v
	}))
}

func (c *deleteChunkCommandOptions) run(k *kingpin.ParseContext) error {
	err := c.Schema.Load()
	if err != nil {
		return errors.Wrap(err, "unable to load schema")
	}

	var schemaConfig *chunk.PeriodConfig
	for i := len(c.Schema.Configs) - 1; i >= 0; i-- {
		if c.Schema.Configs[i].From.Unix() < c.FilterConfig.From {
			schemaConfig = c.Schema.Configs[i]
			break
		}
	}
	if schemaConfig == nil {
		return fmt.Errorf("no schema found for provided from timestamp")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fltr := filter.NewMetricFilter(c.FilterConfig)

	var (
		scanner chunkTool.Scanner
		deleter chunkTool.Deleter
	)

	switch schemaConfig.ObjectType {
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
		return fmt.Errorf("object store type %v not supported for deletes", schemaConfig.ObjectType)
	}

	switch schemaConfig.IndexType {
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
		return fmt.Errorf("index store type %v not supported for deletes", schemaConfig.IndexType)
	}

	outChan := make(chan chunk.Chunk, 100)

	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer func() {
			cancel()
			wg.Done()
		}()

		baseSchema, err := schemaConfig.CreateSchema()
		if err != nil {
			logrus.WithError(err).Errorln("unable to create schema")
			return
		}

		schema, ok := baseSchema.(chunk.SeriesStoreSchema)
		if !ok {
			logrus.Errorln("unable to cast BaseSchema as SeriesStoreSchema")
			return
		}

		for chk := range outChan {
			logrus.WithFields(logrus.Fields{
				"chunkID": chk.ExternalKey(),
				"from":    chk.From.Time().String(),
				"through": chk.Through.Time().String(),
				"dryrun":  c.DryRun,
			}).Infoln("found chunk eligible for deletion")
			entries, err := schema.GetChunkWriteEntries(chk.From, chk.Through, chk.UserID, chk.Metric.Get(labels.MetricName), chk.Metric, chk.ExternalKey())
			if err != nil {
				logrus.WithFields(logrus.Fields{
					"chunkID": chk.ExternalKey(),
					"from":    chk.From.Time().String(),
					"through": chk.Through.Time().String(),
					"dryrun":  c.DryRun,
				}).Errorln(err)
			}

			_, labelEntries, err := schema.GetCacheKeysAndLabelWriteEntries(chk.From, chk.Through, chk.UserID, chk.Metric.Get(labels.MetricName), chk.Metric, chk.ExternalKey())
			if err != nil {
				logrus.WithFields(logrus.Fields{
					"chunkID": chk.ExternalKey(),
					"from":    chk.From.Time().String(),
					"through": chk.Through.Time().String(),
					"dryrun":  c.DryRun,
					"message": "GetCacheKeysAndLabelWriteEntries",
				}).Errorln(err)
			}

			if c.DeleteSeries {
				expandedLabelEntries := make([]chunk.IndexEntry, 0, len(labelEntries))
				for _, le := range labelEntries {
					expandedLabelEntries = append(expandedLabelEntries, le...)
				}

				// This makes sure the entries for the index are deleted first so that incase we error, we can
				// still get the index entries from the chunk entries.
				entries = append(expandedLabelEntries, entries...)
			}

			for _, e := range entries {
				if !c.DryRun {
					err := deleter.DeleteEntry(ctx, e, c.DeleteSeries)
					if err != nil {
						logrus.Errorln(err)
					} else {
						chunkRefsDeleted.Inc()
					}
				}
			}

		}
	}()

	table := schemaConfig.ChunkTables.TableFor(fltr.From)

	start := time.Now()
	scanRequest := chunkTool.ScanRequest{
		Table:    table,
		User:     fltr.User,
		Interval: &model.Interval{Start: fltr.From, End: fltr.To},
	}
	err = scanner.Scan(ctx, scanRequest, func(c chunk.Chunk) bool {
		return fltr.Filter(c)
	}, outChan)

	close(outChan)
	if err != nil {
		return errors.Wrap(err, "scan failed")
	}
	wg.Wait()
	deletionDuration.Set(time.Since(start).Seconds())
	return nil
}

func (c *deleteSeriesCommandOptions) run(k *kingpin.ParseContext) error {
	err := c.Schema.Load()
	if err != nil {
		return errors.Wrap(err, "unable to load schema")
	}

	var schemaConfig *chunk.PeriodConfig
	for i := len(c.Schema.Configs) - 1; i >= 0; i-- {
		if c.Schema.Configs[i].From.Unix() < c.FilterConfig.From {
			schemaConfig = c.Schema.Configs[i]
			break
		}
	}
	if schemaConfig == nil {
		return fmt.Errorf("no schema found for provided from timestamp")
	}

	ctx := context.Background()

	fltr := filter.NewMetricFilter(c.FilterConfig)

	var deleter chunkTool.Deleter

	switch schemaConfig.IndexType {
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
		return fmt.Errorf("index store type %v not supported for deletes", schemaConfig.IndexType)
	}

	baseSchema, err := schemaConfig.CreateSchema()
	if err != nil {
		logrus.WithError(err).Errorln("unable to create schema")
		return err
	}

	schema, ok := baseSchema.(chunk.SeriesStoreSchema)
	if !ok {
		logrus.Errorln("unable to cast BaseSchema as SeriesStoreSchema")
		return errors.New("unable to cast BaseSchema as SeriesStoreSchema")
	}

	deleteMetricNameRows, err := schema.GetReadQueriesForMetric(fltr.From, fltr.To, fltr.User, fltr.Name)
	if err != nil {
		return err
	}

	start := time.Now()

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
			seriesEntriesDeleted.Inc()
		}
	}

	for _, lbl := range fltr.Labels {
		deleteMetricNameRows, err := schema.GetReadQueriesForMetricLabel(fltr.From, fltr.To, fltr.User, fltr.Name, lbl)
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
				labelEntriesDeleted.Inc()
			}
		}
	}

	deletionDuration.Set(time.Since(start).Seconds())

	return nil
}

func (v *validateIndexCommandOptions) run(k *kingpin.ParseContext) error {
	cortexCfg := &cortex.Config{}
	flagext.RegisterFlags(cortexCfg)
	err := LoadConfig(v.CortexConfigFile, true, cortexCfg)
	if err != nil {
		return errors.Wrap(err, "failed to parse Cortex config")
	}

	err = cortexCfg.Schema.Load()
	if err != nil {
		return errors.Wrap(err, "failed to load schemas")
	}

	indexValidator, err := toolCassandra.NewIndexValidator(cortexCfg.Storage.CassandraStorageConfig, cortexCfg.Schema, v.TenantID)
	if err != nil {
		return err
	}
	defer indexValidator.Stop()

	from := model.TimeFromUnix(v.FromTimestamp)
	to := model.TimeFromUnix(v.ToTimestamp)

	outputFile, err := os.Create(v.InvalidIndexEntryFile)
	if err != nil {
		return err
	}
	defer outputFile.Close()

	outChan := make(chan string)
	go func() {
		defer close(outChan)
		err = indexValidator.IndexScan(context.Background(), v.Table, from, to, outChan)
		if err != nil {
			logrus.WithError(err).Errorln("index validation scan terminated")
		}
	}()

	foundInvalidEntriesTotal := 0
	for s := range outChan {
		_, err := outputFile.WriteString(s)
		if err != nil {
			logrus.WithField("entry", s).WithError(err).Errorln("unable to write invalid index entry to file")
		}
		foundInvalidEntriesTotal++
	}

	logrus.WithField("invalid_entries_total", foundInvalidEntriesTotal).Infoln("index-validation scan complete")

	return nil
}
