// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
)

// lastSample is the previous float sample seen for a series, plus the Kafka
// record timestamp it arrived in. Only the most recent sample is kept per
// series, mirroring TSDB's exact-duplicate check (which compares an incoming
// sample against the latest one already stored).
type lastSample struct {
	ts      int64
	valBits uint64
	recTime time.Time
	offset  int64
}

// doFindDuplicates streams over the dump and prints every occurrence where a
// series sends a float sample identical (same timestamp AND value) to the
// previous sample seen for that series. These are silently dropped by the
// ingester at commit time (no discard metric), yet they inflate the
// distributor/ingester "received samples" counters and therefore billed DPM.
func (c *DumpCommand) doFindDuplicates(*kingpin.ParseContext) error {
	last := make(map[uint64]lastSample)
	hashBuf := make([]byte, 0, 1024)
	var dupCount int

	err := c.parseWriteRequestsFromDumpFile(
		func(_ int, record *kgo.Record, req *mimirpb.WriteRequest) {
			if c.dupTenant != "" && string(record.Key) != c.dupTenant {
				return
			}

			for _, ts := range req.Timeseries {
				var h uint64
				hashBuf, h = ingest.LabelAdaptersHash(hashBuf, ts.Labels)

				for _, s := range ts.Samples {
					cur := lastSample{ts: s.TimestampMs, valBits: math.Float64bits(s.Value), recTime: record.Timestamp, offset: record.Offset}

					if prev, ok := last[h]; ok && prev.ts == cur.ts && prev.valBits == cur.valBits {
						dupCount++
						c.printer.PrintLine(fmt.Sprintf(
							"labels: %s, received (ts=%d, val=%s) at %s (offset %d) and then at %s (offset %d)",
							mimirpb.FromLabelAdaptersToLabels(ts.Labels).String(),
							s.TimestampMs,
							strconv.FormatFloat(s.Value, 'g', -1, 64),
							prev.recTime.UTC().Format(time.RFC3339Nano),
							prev.offset,
							record.Timestamp.UTC().Format(time.RFC3339Nano),
							record.Offset,
						))
					}

					// Always update to the latest sample for this series.
					last[h] = cur
				}
			}
		},
		func(recordIdx int, err error) {
			c.printer.PrintLine(fmt.Sprintf("corrupted record %d: %v", recordIdx, err))
		},
	)
	if err != nil {
		return err
	}

	c.printer.PrintLine(fmt.Sprintf("total duplicate occurrences: %d", dupCount))
	return nil
}
