package writer

import (
	"context"
	"sync"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/storage"
	"github.com/cortexproject/cortex/pkg/util/validation"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	ReceivedChunks = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "migration_writer_received_chunks_total",
		Help:      "The total number of chunks received by this writer",
	}, nil)
)

// WriterConfig configures are Writer objects
type WriterConfig struct {
	StorageConfig     storage.Config
	StorageConfigFile string

	SchemaConfig     chunk.SchemaConfig
	SchemaConfigFile string

	MapperConfig MapperConfig

	NumWorkers int
}

// RegisterFlags adds the flags required to configure this flag set.
func (cfg *WriterConfig) Register(cmd *kingpin.CmdClause) {
	cmd.Flag("writer.num-workers", "number of worker jobs handling backend writes").Default("5").IntVar(&cfg.NumWorkers)
	cmd.Flag("writer.storage-config-file", "Path to config file for storage").Required().StringVar(&cfg.StorageConfigFile)
	cmd.Flag("writer.schema-config-file", "Path to config file for schema").Required().StringVar(&cfg.SchemaConfigFile)
	cfg.MapperConfig.Register(cmd)
}

// Writer receives chunks and stores them in a storage backend
type Writer struct {
	cfg        WriterConfig
	chunkStore chunk.Store

	workerGroup sync.WaitGroup
	mapper      *Mapper

	err  error
	quit chan struct{}
}

// NewWriter returns a Writer object
func NewWriter(cfg WriterConfig) (*Writer, error) {
	mapper, err := NewMapper(cfg.MapperConfig)
	if err != nil {
		return nil, err
	}

	overrides, err := validation.NewOverrides(validation.Limits{})
	if err != nil {
		return nil, err
	}

	chunkStore, err := storage.NewStore(cfg.StorageConfig, chunk.StoreConfig{}, cfg.SchemaConfig, overrides)
	if err != nil {
		return nil, err
	}

	writer := Writer{
		cfg:         cfg,
		chunkStore:  chunkStore,
		workerGroup: sync.WaitGroup{},
		mapper:      mapper,
		quit:        make(chan struct{}),
	}
	return &writer, nil
}

// Run initializes the writer workers
func (w *Writer) Run(ctx context.Context, inChan chan chunk.Chunk) {
	errChan := make(chan error)
	writeCtx, cancel := context.WithCancel(ctx)

	defer func() {
		// lets wait for all workers to finish before we return.
		// An error in errChan would cause all workers to stop because we cancel the context.
		// Otherwise closure of inChan(which is done by writer) should make all workers to stop.
		w.workerGroup.Wait()
		// closing the errChan to let this function return
		close(errChan)
	}()

	go func() {
		// cancel context when an error occurs or errChan is closed
		defer cancel()

		err := <-errChan
		if err != nil {
			w.err = err
			logrus.WithError(err).Errorln("error writing chunk, stopping write operation")
		}
	}()

	for i := 0; i < w.cfg.NumWorkers; i++ {
		w.workerGroup.Add(1)
		go w.writeLoop(writeCtx, i, inChan, errChan)
	}
}

func (w *Writer) writeLoop(ctx context.Context, workerID int, inChan chan chunk.Chunk, errChan chan error) {
	defer w.workerGroup.Done()

	for {
		select {
		case <-ctx.Done():
			logrus.Info("shutting down writer because context was cancelled")
			return
		case c, open := <-inChan:
			if !open {
				return
			}

			ReceivedChunks.WithLabelValues().Add(1)

			remapped, err := w.mapper.MapChunk(c)
			if err != nil {
				logrus.WithError(err).Errorln("failed to remap chunk", "err", err)
				errChan <- err
				return
			}

			// Ensure the chunk has been encoded before persisting in order to avoid
			// bad external keys in the index entry
			if remapped.Encode() != nil {
				errChan <- err
				return
			}

			err = w.chunkStore.PutOne(ctx, remapped.From, remapped.Through, remapped)
			if err != nil {
				logrus.WithError(err).Errorln("failed to store chunk")
				errChan <- err
				return
			}
		}
	}
}

func (w *Writer) Err() error {
	return w.err
}
