package reader

import (
	"context"
	"fmt"
	"sync"

	cortex_chunk "github.com/grafana/mimir/pkg/chunk"
	cortex_storage "github.com/grafana/mimir/pkg/chunk/storage"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"github.com/grafana/mimir/cmd/cortextool/pkg/chunk"
	"github.com/grafana/mimir/cmd/cortextool/pkg/chunk/storage"
)

var (
	SentChunks = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "reader_sent_chunks_total",
		Help:      "The total number of chunks sent by this reader.",
	})
)

// Config is a config for a Reader
type Config struct {
	StorageType   string                `yaml:"storage_type"`
	StorageConfig cortex_storage.Config `yaml:"storage"`
	NumWorkers    int                   `yaml:"num_workers"`
}

// Reader collects and forwards chunks according to it's planner
type Reader struct {
	cfg Config
	id  string // ID is the configured as the reading prefix and the shards assigned to the reader

	scanner          chunk.Scanner
	planner          *Planner
	workerGroup      sync.WaitGroup
	scanRequestsChan chan chunk.ScanRequest
	err              error
	quit             chan struct{}
}

// NewReader returns a Reader struct
func NewReader(cfg Config, plannerCfg PlannerConfig) (*Reader, error) {
	planner, err := NewPlanner(plannerCfg)
	if err != nil {
		return nil, err
	}

	scanner, err := storage.NewChunkScanner(cfg.StorageType, cfg.StorageConfig)
	if err != nil {
		return nil, err
	}

	id := fmt.Sprintf("%d_%d", plannerCfg.FirstShard, plannerCfg.LastShard)

	// Default to one worker if none is set
	if cfg.NumWorkers < 1 {
		cfg.NumWorkers = 1
	}

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
				SentChunks.Inc()
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
