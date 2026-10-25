// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strconv"
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
	// Of a kafkatool dump, the distinct metric families with a help, and the length of their helps, as a profile's
	// help_length has it.
	Families   int         `json:"families,omitempty"`
	HelpLength Percentiles `json:"help_length,omitempty"`
}

// compositionCounter adds records up into a Composition. In today's records, series carry no metadata of their own:
// a series' value is its family's, by its name without a classic histogram's suffixes, and each distinct family entry
// is a stand-in the new writer would write. In the new writer's, a value is a series' own, and stand-ins are series
// without data.
type compositionCounter struct {
	today                             bool
	records, series, values, standIns int
	// Of today's records, the help of each family, by family and help.
	helps map[[2]string]int
}

func (c *compositionCounter) add(req *mimirpb.WriteRequest) {
	c.records++
	values := map[string]struct{}{}
	entries := map[string]struct{}{}
	for _, m := range req.Metadata {
		entries[fmt.Sprint(m.MetricFamilyName, m.Type, m.Help, m.Unit)] = struct{}{}
		c.help(m.MetricFamilyName, m.Help)
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
				c.help(name, m.Help)
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

// help counts a family's help, of today's records.
func (c *compositionCounter) help(family, help string) {
	if !c.today || help == "" {
		return
	}
	if c.helps == nil {
		c.helps = map[[2]string]int{}
	}
	c.helps[[2]string{family, help}] = len(help)
}

func (c *compositionCounter) composition() Composition {
	out := Composition{Records: c.records}
	if len(c.helps) > 0 {
		lengths := make([]int, 0, len(c.helps))
		families := map[string]struct{}{}
		for key, n := range c.helps {
			lengths = append(lengths, n)
			families[key[0]] = struct{}{}
		}
		slices.Sort(lengths)
		out.Families, out.HelpLength = len(families), Percentiles{}
		for _, q := range []string{"0", "0.1", "0.5", "0.9", "0.99", "1"} {
			f, _ := strconv.ParseFloat(q, 64)
			out.HelpLength[q] = float64(lengths[min(len(lengths)-1, int(f*float64(len(lengths))))])
		}
	}
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

// compositionOfDumps is the Composition of the records of kafkatool dumps, a JSON record per line, of today's format,
// over them all.
func compositionOfDumps(paths []string) (Composition, error) {
	counter := compositionCounter{today: true}
	for _, path := range paths {
		if err := counter.addDump(path); err != nil {
			return Composition{}, fmt.Errorf("%s: %w", path, err)
		}
	}
	return counter.composition(), nil
}

func (c *compositionCounter) addDump(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 1<<20), 64<<20)
	for scanner.Scan() {
		var record kgo.Record
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			return err
		}
		var req mimirpb.PreallocWriteRequest
		if err := ingest.DeserializeRecordContent(record.Value, &req, ingest.ParseRecordVersion(&record)); err != nil {
			return fmt.Errorf("record at offset %d: %w", record.Offset, err)
		}
		c.add(&req.WriteRequest)
	}
	return scanner.Err()
}
