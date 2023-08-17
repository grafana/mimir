package main

import (
	"flag"
	"io"
	"log"
	"math"
	"os"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/record"
	"github.com/prometheus/prometheus/tsdb/wlog"

	util_math "github.com/grafana/mimir/pkg/util/math"
)

func main() {
	// Clean up all flags registered via init() methods of 3rd-party libraries.
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	var (
		printSeriesEntries           bool
		printSeriesWithSampleEntries bool
	)

	flag.BoolVar(&printSeriesEntries, "series-entries", true, "Print series entries")
	flag.BoolVar(&printSeriesWithSampleEntries, "print-series-with-samples", false, "Print series information for each sample")
	args, err := flagext.ParseFlagsAndArguments(flag.CommandLine)
	if err != nil {
		log.Fatalln(err.Error())
	}

	log.SetOutput(os.Stdout)

	for _, dir := range args {
		err := printWal(dir, printSeriesEntries, printSeriesWithSampleEntries)
		if err != nil {
			log.Fatalln("failed to print WAL from directory", dir, "due to error:", err)
		}
	}
}

func printWal(walDir string, printSeriesEntries, printSeriesWithSampleEntries bool) error {
	// Backfill the checkpoint first if it exists.
	checkpointDir, startFrom, err := wlog.LastCheckpoint(walDir)
	if err != nil && err != record.ErrNotFound {
		return errors.Wrap(err, "find last checkpoint")
	}

	// Find the last segment.
	_, endAt, e := wlog.Segments(walDir)
	if e != nil {
		return errors.Wrap(e, "finding WAL segments")
	}

	log.Println("Using checkpoint directory:", checkpointDir)

	sr, err := wlog.NewSegmentsReader(checkpointDir)
	if err != nil {
		return errors.Wrap(err, "open checkpoint")
	}
	defer func() {
		if err := sr.Close(); err != nil {
			log.Println("Error while closing the wal segments reader when processing checkpoint", "err", err)
		}
	}()

	var minSampleTime, maxSampleTime int64 = math.MaxInt64, math.MinInt64
	// map series references to labels (as a string)
	series := map[chunks.HeadSeriesRef]string{}
	log.Println("replaying checkpoint")
	if err := printWalEntries(wlog.NewReader(sr), series, printSeriesEntries, printSeriesWithSampleEntries, &minSampleTime, &maxSampleTime); err != nil {
		return errors.Wrap(err, "replaying checkpoint")
	}

	startFrom++
	for i := startFrom; i <= endAt; i++ {
		log.Println("replaying WAL segment", i)
		s, err := wlog.OpenReadSegment(wlog.SegmentName(walDir, i))
		if err != nil {
			return errors.Wrapf(err, "open WAL segment: %d", i)
		}

		sr, err := wlog.NewSegmentBufReaderWithOffset(0, s)
		if errors.Is(err, io.EOF) {
			// File does not exist.
			continue
		}
		if err != nil {
			return errors.Wrapf(err, "reader for segment %d", i)
		}
		err = printWalEntries(wlog.NewReader(sr), series, printSeriesEntries, printSeriesWithSampleEntries, &minSampleTime, &maxSampleTime)
		if err := sr.Close(); err != nil {
			log.Println("Error while closing the wal segments reader:", err)
		}
		if err != nil {
			return errors.Wrapf(err, "replaying segment %d", i)
		}
	}

	log.Println("min sample time:", minSampleTime, formatTimestamp(minSampleTime), "max sample time:", maxSampleTime, formatTimestamp(maxSampleTime))
	return nil
}

const timeFormat = time.RFC3339Nano

func formatTimestamp(ts int64) string {
	return time.UnixMilli(ts).UTC().Format(timeFormat)
}

func printWalEntries(r *wlog.Reader, seriesMap map[chunks.HeadSeriesRef]string, printSeriesEntries, printSeriesWithSampleEntries bool, minSampleTime, maxSampleTime *int64) error {
	var dec record.Decoder

	seriesInfo := func(ref chunks.HeadSeriesRef) string {
		if !printSeriesWithSampleEntries {
			return ""
		}

		ser, found := seriesMap[ref]
		if !found {
			ser = "[series not found]"
		}
		return ser
	}

	for r.Next() {
		seg, off := r.Segment(), r.Offset()

		rec := r.Record()

		switch dec.Type(rec) {
		case record.Series:
			series, err := dec.Series(rec, nil)
			if err != nil {
				return &wlog.CorruptionErr{Err: errors.Wrap(err, "decode series"), Segment: r.Segment(), Offset: r.Offset()}
			}

			for _, s := range series {
				if prev, exists := seriesMap[s.Ref]; exists {
					log.Println("duplicate series entry, previous:", prev, "new:", s.Labels)
				}

				if printSeriesEntries {
					log.Println("seg:", seg, "off:", off, "series record:", s.Ref, s.Labels)
				}

				seriesMap[s.Ref] = s.Labels.String()
			}

		case record.Samples:
			samples, err := dec.Samples(rec, nil)
			if err != nil {
				return &wlog.CorruptionErr{Err: errors.Wrap(err, "decode samples"), Segment: r.Segment(), Offset: r.Offset()}
			}

			for _, s := range samples {
				log.Println("seg:", seg, "off:", off, "samples record:", s.Ref, s.T, formatTimestamp(s.T), s.V, seriesInfo(s.Ref))

				*minSampleTime = util_math.Min(s.T, *minSampleTime)
				*maxSampleTime = util_math.Max(s.T, *maxSampleTime)
			}

		case record.Tombstones:
			tstones, err := dec.Tombstones(rec, nil)
			if err != nil {
				return &wlog.CorruptionErr{Err: errors.Wrap(err, "decode tombstones"), Segment: r.Segment(), Offset: r.Offset()}
			}

			for _, s := range tstones {
				log.Println("seg:", seg, "off:", off, "tombstones record:", s.Ref, s.Intervals)
			}

		case record.Exemplars:
			exemplars, err := dec.Exemplars(rec, nil)
			if err != nil {
				return &wlog.CorruptionErr{Err: errors.Wrap(err, "decode exemplars"), Segment: r.Segment(), Offset: r.Offset()}
			}

			for _, s := range exemplars {
				log.Println("seg:", seg, "off:", off, "exemplars record:", s.Ref, s.Labels, s.T, formatTimestamp(s.T), s.V, seriesInfo(s.Ref))
			}

		case record.HistogramSamples:
			hists, err := dec.HistogramSamples(rec, nil)
			if err != nil {
				return &wlog.CorruptionErr{Err: errors.Wrap(err, "decode histograms"), Segment: r.Segment(), Offset: r.Offset()}
			}

			for _, s := range hists {
				log.Println("seg:", seg, "off:", off, "histograms record:", s.Ref, s.T, formatTimestamp(s.T), seriesInfo(s.Ref))

				*minSampleTime = util_math.Min(s.T, *minSampleTime)
				*maxSampleTime = util_math.Max(s.T, *maxSampleTime)
			}

		case record.FloatHistogramSamples:
			hists, err := dec.FloatHistogramSamples(rec, nil)
			if err != nil {
				return &wlog.CorruptionErr{Err: errors.Wrap(err, "decode float histograms"), Segment: r.Segment(), Offset: r.Offset()}
			}

			for _, s := range hists {
				log.Println("seg:", seg, "off:", off, "float histograms record:", s.Ref, s.T, formatTimestamp(s.T), seriesInfo(s.Ref))

				*minSampleTime = util_math.Min(s.T, *minSampleTime)
				*maxSampleTime = util_math.Max(s.T, *maxSampleTime)
			}

		case record.Metadata:
			meta, err := dec.Metadata(rec, nil)
			if err != nil {
				return &wlog.CorruptionErr{Err: errors.Wrap(err, "decode metadata"), Segment: r.Segment(), Offset: r.Offset()}
			}

			for _, s := range meta {
				log.Println("seg:", seg, "off:", off, "metadata:", s.Ref)
			}
		default:
			if len(rec) < 1 {
				log.Println("seg:", seg, "off:", off, "unknown record type: no data")
			} else {
				log.Println("seg:", seg, "off:", off, "unknown record type:", rec[0])
			}
		}
	}

	return nil
}
