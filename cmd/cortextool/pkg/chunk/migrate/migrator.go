package migrate

import (
	"context"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"github.com/grafana/cortextool/pkg/chunk/migrate/reader"
	"github.com/grafana/cortextool/pkg/chunk/migrate/writer"
)

const chunkBufferSize = 1000

type Config struct {
	ReaderConfig reader.Config `yaml:"reader"`
	WriterConfig writer.Config `yaml:"writer"`
	Mapper       writer.Mapper `yaml:"mapper,omitempty"`
}

type Migrator struct {
	cfg         Config
	reader      *reader.Reader
	writer      *writer.Writer
	chunkBuffer chan chunk.Chunk
}

func NewMigrator(cfg Config, plannerCfg reader.PlannerConfig) (*Migrator, error) {
	chunkReader, err := reader.NewReader(cfg.ReaderConfig, plannerCfg)
	if err != nil {
		return nil, err
	}

	chunkWriter, err := writer.NewWriter(cfg.WriterConfig, cfg.Mapper)
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

func Setup() error {
	prometheus.MustRegister(
		reader.SentChunks,
		writer.ReceivedChunks,
	)

	return nil
}
