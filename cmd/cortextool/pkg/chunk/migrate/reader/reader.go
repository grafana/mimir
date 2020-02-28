package reader

import (
	"context"
	"fmt"
	"sync"

	cortex_chunk "github.com/cortexproject/cortex/pkg/chunk"
	cortex_storage "github.com/cortexproject/cortex/pkg/chunk/storage"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/grafana/cortextool/pkg/chunk"
	"github.com/grafana/cortextool/pkg/chunk/storage"
)

var (
	SentChunks = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "reader_sent_chunks_total",
		Help:      "The total number of chunks sent by this reader.",
	}, []string{"reader_id"})
)

// ReaderConfig is a config for a Reader
type ReaderConfig struct {
	PlannerConfig     PlannerConfig
	ReaderIDPrefix    string
	StorageClient     string
	StorageConfig     cortex_storage.Config
	StorageConfigFile string

	NumWorkers int
}

// RegisterFlags adds the flags required to configure this flag set.
func (cfg *ReaderConfig) Register(cmd *kingpin.CmdClause) {
	cfg.PlannerConfig.Register(cmd)
	cmd.Flag("reader.storage-client", "Which storage client to use (gcp, gcs).").Default("gcp").StringVar(&cfg.StorageClient)
	cmd.Flag("reader.storage-config-file", "Path to config file for storage").Required().StringVar(&cfg.StorageConfigFile)
	cmd.Flag("reader.prefix", "prefix used to identify reader when forwarding data to writer").StringVar(&cfg.ReaderIDPrefix)
	cmd.Flag("reader.num-workers", "Number of workers to scan tables").Default("1").IntVar(&cfg.NumWorkers)
}

// Reader collects and forwards chunks according to it's planner
type Reader struct {
	cfg ReaderConfig
	id  string // ID is the configured as the reading prefix and the shards assigned to the reader

	scanner          chunk.Scanner
	planner          *Planner
	workerGroup      sync.WaitGroup
	scanRequestsChan chan chunk.ScanRequest
	err              error
	quit             chan struct{}
}

// NewReader returns a Reader struct
func NewReader(cfg ReaderConfig) (*Reader, error) {
	planner, err := NewPlanner(cfg.PlannerConfig)
	if err != nil {
		return nil, err
	}

	scanner, err := storage.NewChunkScanner(cfg.StorageClient, cfg.StorageConfig)
	if err != nil {
		return nil, err
	}

	id := cfg.ReaderIDPrefix + fmt.Sprintf("%d_%d", cfg.PlannerConfig.FirstShard, cfg.PlannerConfig.LastShard)

	return &Reader{
		cfg:              cfg,
		id:               id,
		planner:          planner,
		scanner:          scanner,
		scanRequestsChan: make(chan chunk.ScanRequest),
		quit:             make(chan struct{}),
	}, nil
}

// Run initializes the writer workers
func (r *Reader) Run(ctx context.Context, outChan chan cortex_chunk.Chunk) {
	errChan := make(chan error)
	defer close(outChan)

	readCtx, cancel := context.WithCancel(ctx)

	// starting workers
	for i := 0; i < r.cfg.NumWorkers; i++ {
		r.workerGroup.Add(1)
		go r.readLoop(readCtx, outChan, errChan)
	}

	go func() {
		// cancel context when an error occurs or errChan is closed
		defer cancel()

		err := <-errChan
		if err != nil {
			r.err = err
			logrus.WithError(err).Errorln("error scanning chunks, stopping read operation")
			close(r.quit)
		}
	}()

	scanRequests := r.planner.Plan()
	logrus.Infof("built %d plans for reading", len(scanRequests))

	defer func() {
		// lets wait for all workers to finish before we return.
		// An error in errChan would cause all workers to stop because we cancel the context.
		// Otherwise closure of scanRequestsChan(which is done after sending all the scanRequests) should make all workers to stop.
		r.workerGroup.Wait()
		close(errChan)
	}()

	// feeding scan requests to workers
	for _, req := range scanRequests {
		select {
		case r.scanRequestsChan <- req:
			continue
		case <-r.quit:
			return
		}
	}

	// all scan requests are fed, close the channel
	close(r.scanRequestsChan)
}

func (r *Reader) readLoop(ctx context.Context, outChan chan cortex_chunk.Chunk, errChan chan error) {
	defer r.workerGroup.Done()

	for {
		select {
		case <-ctx.Done():
			logrus.Infoln("shutting down reader because context was cancelled")
			return
		case req, open := <-r.scanRequestsChan:
			if !open {
				return
			}

			logEntry := logrus.WithFields(logrus.Fields{
				"table": req.Table,
				"user":  req.User,
				"shard": req.Prefix})

			logEntry.Infoln("attempting  scan request")
			err := r.scanner.Scan(ctx, req, func(i cortex_chunk.Chunk) bool {
				// while this does not mean chunk is sent by scanner, this is the closest we can get
				SentChunks.WithLabelValues(r.id).Add(1)
				return true
			}, outChan)

			if err != nil {
				logEntry.WithError(err).Errorln("error scanning chunks")
				errChan <- fmt.Errorf("scan request failed, %v", req)
				return
			}

			logEntry.Infoln("completed scan request")
		}
	}
}

func (r *Reader) Stop() {
	close(r.quit)
}

func (r *Reader) Err() error {
	return r.err
}
