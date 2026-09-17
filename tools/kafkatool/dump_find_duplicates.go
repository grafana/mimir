// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"fmt"
	"math"
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
	// Keyed by tenant ID, then by series hash. Duplicate detection is per-tenant:
	// each tenant has its own TSDB, so the same series across tenants is unrelated.
	last := make(map[string]map[uint64]lastSample)
	hashBuf := make([]byte, 0, 1024)
	var dupCount int

	err := c.parseWriteRequestsFromDumpFile(
		func(_ int, record *kgo.Record, req *mimirpb.WriteRequest) {
			tenantID := string(record.Key)
			if c.dupTenant != "" && tenantID != c.dupTenant {
				return
			}

			tenantLast, ok := last[tenantID]
			if !ok {
				tenantLast = make(map[uint64]lastSample)
				last[tenantID] = tenantLast
			}

			for _, ts := range req.Timeseries {
				var h uint64
				hashBuf, h = ingest.LabelAdaptersHash(hashBuf, ts.Labels)

				for _, s := range ts.Samples {
					cur := lastSample{ts: s.TimestampMs, valBits: math.Float64bits(s.Value), recTime: record.Timestamp, offset: record.Offset}

					if prev, ok := tenantLast[h]; ok && prev.ts == cur.ts && prev.valBits == cur.valBits {
						dupCount++
						c.printer.PrintLine(fmt.Sprintf(
							"labels: %s, timestamp: %s -> %s (%s delta), offset: %d -> %d (%d delta)",
							mimirpb.FromLabelAdaptersToLabels(ts.Labels).String(),
							prev.recTime.UTC().Format(time.RFC3339Nano),
							record.Timestamp.UTC().Format(time.RFC3339Nano),
							record.Timestamp.Sub(prev.recTime).String(),
							prev.offset,
							record.Offset,
							record.Offset-prev.offset,
						))
					}

					// Always update to the latest sample for this series.
					tenantLast[h] = cur
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
