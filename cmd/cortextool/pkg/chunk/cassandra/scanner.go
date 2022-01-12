package cassandra

import (
	"context"
	"fmt"
	"sync"

	"github.com/grafana/mimir/pkg/chunk"
	"github.com/grafana/mimir/pkg/chunk/cassandra"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/sirupsen/logrus"
	"go.uber.org/atomic"
)

// scanBatch represents a batch of rows read from Cassandra.
type scanBatch struct {
	hash       []byte
	rangeValue []byte
	value      []byte
}

type IndexValidator struct {
	schema   chunk.SchemaConfig
	s        *StorageClient
	o        *ObjectClient
	tenantID string

	totalIgnoredTime *atomic.Int64
	totalInvalid     *atomic.Int64
}

func NewIndexValidator(
	cfg cassandra.Config,
	schema chunk.SchemaConfig,
	tenantID string,
) (*IndexValidator, error) {
	logrus.Debug("Connecting to Cassandra")
	o, err := NewObjectClient(
		cfg,
		schema,
		prometheus.NewRegistry(),
	)
	if err != nil {
		return nil, err
	}

	s, err := NewStorageClient(
		cfg,
		schema,
		prometheus.NewRegistry(),
	)
	if err != nil {
		return nil, err
	}

	logrus.Debug("Connected")
	return &IndexValidator{
		schema:           schema,
		s:                s,
		o:                o,
		tenantID:         tenantID,
		totalIgnoredTime: atomic.NewInt64(0),
		totalInvalid:     atomic.NewInt64(0),
	}, nil
}

func (i *IndexValidator) Stop() {
	i.s.Stop()
}

func (i *IndexValidator) IndexScan(ctx context.Context, table string, from model.Time, to model.Time, out chan string) error {
	q := i.s.readSession.Query(fmt.Sprintf("SELECT hash, range, value FROM %s", table))

	iter := q.WithContext(ctx).Iter()
	defer iter.Close()
	scanner := iter.Scanner()

	wg := &sync.WaitGroup{}
	batchChan := make(chan scanBatch, 1000)

	for n := 0; n < 64; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for b := range batchChan {
				i.checkEntry(ctx, from, to, out, b)
			}
		}()
	}

	rowsReadTotal := 0

	logrus.WithFields(logrus.Fields{
		"table":   table,
		"from_ts": from.String(),
		"to_ts":   to.String(),
	}).Infoln("starting scan")

	for scanner.Next() {
		b := scanBatch{}
		if err := scanner.Scan(&b.hash, &b.rangeValue, &b.value); err != nil {
			return errors.WithStack(err)
		}
		batchChan <- b
		rowsReadTotal++
		if rowsReadTotal%25000 == 0 {
			logrus.WithFields(logrus.Fields{
				"entries_scanned":               rowsReadTotal,
				"entries_outside_range_skipped": i.totalIgnoredTime.Load(),
				"entries_invalid_found":         i.totalInvalid.Load(),
			}).Infoln("scan progress")
		}
	}
	close(batchChan)
	wg.Wait()
	return errors.WithStack(scanner.Err())
}

func (i *IndexValidator) checkEntry(
	ctx context.Context,
	from model.Time,
	to model.Time,
	out chan string,
	entry scanBatch,
) {
	chunkID, _, isSeriesID, err := parseChunkTimeRangeValue(entry.rangeValue, entry.value)
	if err != nil {
		logrus.WithField("chunk_id", chunkID).WithError(err).Errorln("unable to parse chunk time range value")
		return
	}

	if isSeriesID {
		logrus.WithField("series_id", chunkID).Debugln("ignoring series id row")
		return
	}

	c, err := chunk.ParseExternalKey(i.tenantID, chunkID)
	if err != nil {
		logrus.WithField("chunk_id", chunkID).WithError(err).Errorln("unable to parse external key")
		return
	}

	if from > c.Through || (c.From > to && to > 0) {
		i.totalIgnoredTime.Inc()
		logrus.WithField("chunk_id", chunkID).Debugln("ignoring chunk outside time range")
		return
	}

	chunkTable, err := i.schema.ChunkTableFor(c.From)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"chunk_id": chunkID,
			"from":     c.From.String(),
			"through":  c.Through.String(),
		}).WithError(err).Errorln("unable to determine chunk table")
		return
	}

	var count int
	err = i.o.readSession.Query(
		fmt.Sprintf("SELECT count(*) FROM %s WHERE hash = ?", chunkTable),
		c.ExternalKey(),
	).WithContext(ctx).Scan(&count)

	if err != nil {
		logrus.WithFields(logrus.Fields{
			"chunk_id": chunkID,
		}).WithError(err).Errorln("unable to read chunk table")
		return
	}

	chunkExists := count > 0
	if !chunkExists {
		i.totalInvalid.Inc()
		logrus.WithField("chunk_id", chunkID).Infoln("chunk not found, adding index entry to output file")
		out <- fmt.Sprintf("%s,0x%x\n", string(entry.hash), entry.rangeValue)
	}
}
