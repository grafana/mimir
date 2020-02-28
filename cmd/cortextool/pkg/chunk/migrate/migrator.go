package migrate

import (
	"context"
	"os"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/storage"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"
	"gopkg.in/yaml.v2"

	"github.com/grafana/cortextool/pkg/chunk/migrate/reader"
	"github.com/grafana/cortextool/pkg/chunk/migrate/writer"
)

const chunkBufferSize = 1000

type MigratorConfig struct {
	ReaderConfig reader.ReaderConfig
	WriterConfig writer.WriterConfig
}

func (cfg *MigratorConfig) Register(cmd *kingpin.CmdClause) {
	cfg.ReaderConfig.Register(cmd)
	cfg.WriterConfig.Register(cmd)
}

type Migrator struct {
	cfg         MigratorConfig
	reader      *reader.Reader
	writer      *writer.Writer
	chunkBuffer chan chunk.Chunk
}

func NewMigrator(cfg MigratorConfig) (*Migrator, error) {
	storageConfig, err := decodeStorageConfig(cfg.ReaderConfig.StorageConfigFile)
	if err != nil {
		return nil, err
	}

	cfg.ReaderConfig.StorageConfig = *storageConfig
	chunkReader, err := reader.NewReader(cfg.ReaderConfig)
	if err != nil {
		return nil, err
	}

	storageConfig, err = decodeStorageConfig(cfg.WriterConfig.StorageConfigFile)
	if err != nil {
		return nil, err
	}

	schemaConfig, err := decodeSchemaConfig(cfg.WriterConfig.SchemaConfigFile)
	if err != nil {
		return nil, err
	}

	cfg.WriterConfig.StorageConfig = *storageConfig
	cfg.WriterConfig.SchemaConfig = *schemaConfig
	chunkWriter, err := writer.NewWriter(cfg.WriterConfig)
	if err != nil {
		return nil, err
	}

	return &Migrator{
		cfg:         cfg,
		reader:      chunkReader,
		writer:      chunkWriter,
		chunkBuffer: make(chan chunk.Chunk, chunkBufferSize),
	}, nil
}

func (m *Migrator) Run() {
	go m.reader.Run(context.Background(), m.chunkBuffer)
	m.writer.Run(context.Background(), m.chunkBuffer)

	if m.reader.Err() != nil {
		logrus.WithError(m.reader.Err()).Errorln("stopped migrator due to an error in reader")
	}

	if m.writer.Err() != nil {
		logrus.WithError(m.reader.Err()).Errorln("stopped migrator due to an error in writer")
	}
}

func decodeStorageConfig(filename string) (*storage.Config, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	decoder := yaml.NewDecoder(f)
	decoder.SetStrict(true)

	storageConfig := storage.Config{}
	if err := decoder.Decode(&storageConfig); err != nil {
		return nil, err
	}

	return &storageConfig, nil
}

func decodeSchemaConfig(filename string) (*chunk.SchemaConfig, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	decoder := yaml.NewDecoder(f)
	decoder.SetStrict(true)

	schemaConfig := chunk.SchemaConfig{}
	if err := decoder.Decode(&schemaConfig); err != nil {
		return nil, err
	}

	return &schemaConfig, nil
}

func Setup() error {
	prometheus.MustRegister(
		reader.SentChunks,
		writer.ReceivedChunks,
	)

	return nil
}
