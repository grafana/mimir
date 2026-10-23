// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
)

// Composition is what the records of a partition are made of, which decides what a record's unchanged metadata costs
// an engine: per sample for the TSDB, per record for the seriesstore engine's memo. Each is a ratio of totals over
// the records.
type Composition struct {
	Records int `json:"records"`
	// Series with data per record.
	SeriesPerRecord float64 `json:"series_per_record"`
	// Series with data per distinct metadata value among them, per record.
	SeriesPerValue float64 `json:"series_per_value"`
	// Metric families' metadata entries per record, which the TSDB's batches count as series.
	StandInsPerRecord float64 `json:"stand_ins_per_record"`
}

// compositionCounter adds records up into a Composition. In today's records, series carry no metadata of their own:
// a series' value is its family's, by its name without a classic histogram's suffixes, and each distinct family entry
// is a stand-in the new writer would write. In the new writer's, a value is a series' own, and stand-ins are series
// without data.
type compositionCounter struct {
	today                             bool
	records, series, values, standIns int
}

func (c *compositionCounter) add(req *mimirpb.WriteRequest) {
	c.records++
	values := map[string]struct{}{}
	entries := map[string]struct{}{}
	for _, m := range req.Metadata {
		entries[fmt.Sprint(m.MetricFamilyName, m.Type, m.Help, m.Unit)] = struct{}{}
	}
	for _, ts := range req.Timeseries {
		if len(ts.Samples) == 0 && len(ts.Histograms) == 0 && len(ts.Exemplars) == 0 {
			// Today's stand-ins are among the family entries.
			if !c.today {
				c.standIns++
			}
			continue
		}
		c.series++
		if c.today {
			name := familyName(metricName(ts.Labels))
			values[name] = struct{}{}
			if m := ts.SeriesMetadata; !m.IsEmpty() {
				// The family's entry the old writer copied onto the series named like it.
				entries[fmt.Sprint(name, m.Type, m.Help, m.Unit)] = struct{}{}
			}
			continue
		}
		if m := ts.SeriesMetadata; !m.IsEmpty() {
			values[fmt.Sprint(m.Type, m.Help, m.Unit)] = struct{}{}
		}
	}
	c.values += len(values)
	if c.today {
		c.standIns += len(entries)
	}
}

func (c *compositionCounter) composition() Composition {
	out := Composition{Records: c.records}
	if c.records > 0 {
		out.SeriesPerRecord = float64(c.series) / float64(c.records)
		out.StandInsPerRecord = float64(c.standIns) / float64(c.records)
	}
	if c.values > 0 {
		out.SeriesPerValue = float64(c.series) / float64(c.values)
	}
	return out
}

func metricName(labels []mimirpb.LabelAdapter) string {
	for _, l := range labels {
		if l.Name == "__name__" {
			return l.Value
		}
	}
	return ""
}

// familyName is a series' metric family, as a classic histogram's or summary's series name it with a suffix.
func familyName(name string) string {
	for _, suffix := range []string{"_bucket", "_sum", "_count"} {
		if base, ok := strings.CutSuffix(name, suffix); ok {
			return base
		}
	}
	return name
}

// compositionOfDump is the Composition of the records of a kafkatool dump, a JSON record per line, of today's format.
func compositionOfDump(path string) (Composition, error) {
	f, err := os.Open(path)
	if err != nil {
		return Composition{}, err
	}
	defer f.Close()
	counter := compositionCounter{today: true}
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 1<<20), 64<<20)
	for scanner.Scan() {
		var record kgo.Record
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			return Composition{}, err
		}
		var req mimirpb.PreallocWriteRequest
		if err := ingest.DeserializeRecordContent(record.Value, &req, ingest.ParseRecordVersion(&record)); err != nil {
			return Composition{}, fmt.Errorf("record at offset %d: %w", record.Offset, err)
		}
		counter.add(&req.WriteRequest)
	}
	return counter.composition(), scanner.Err()
}
