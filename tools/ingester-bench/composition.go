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
// a series' value is its family's, as families tells it by the series' name, and each distinct family entry is a
// stand-in the new writer would write. In the new writer's, a value is a series' own, and stand-ins are series without
// data.
type compositionCounter struct {
	today                             bool
	records, series, values, standIns int
	// Of today's records, the help of each family, by family and help, and the families the records tell of.
	helps    map[[2]string]int
	families familyNames
}

func (c *compositionCounter) add(req *mimirpb.WriteRequest) {
	c.records++
	if c.today {
		c.families.learn(req)
	}
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
			name := c.families.of(metricName(ts.Labels))
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

// familyNames tells a series' metric family by its name, from what records tell of their families: their metadata,
// and the series of classic histograms and summaries.
type familyNames struct {
	// The families with metadata, by name, and their types.
	types map[string]mimirpb.MetricMetadata_MetricType
	// The families with a classic histogram's buckets, and with a summary's quantiles.
	histograms, summaries map[string]bool
}

func (f *familyNames) learn(req *mimirpb.WriteRequest) {
	if f.types == nil {
		f.types, f.histograms, f.summaries = map[string]mimirpb.MetricMetadata_MetricType{}, map[string]bool{}, map[string]bool{}
	}
	for _, m := range req.Metadata {
		f.types[m.MetricFamilyName] = m.Type
	}
	for _, ts := range req.Timeseries {
		name := metricName(ts.Labels)
		if !ts.SeriesMetadata.IsEmpty() {
			// The family's entry the old writer copied onto the series named like it.
			f.types[name] = ts.SeriesMetadata.Type
		}
		for _, l := range ts.Labels {
			switch base, bucket := strings.CutSuffix(name, "_bucket"); {
			case l.Name == "le" && bucket:
				f.histograms[base] = true
			case l.Name == "quantile":
				f.summaries[name] = true
			}
		}
	}
}

// of is the family of the series named name: its own, when a family has its name, or that of the classic histogram
// or summary whose series its suffix names it as, or else its own. Without them, a metric whose name only ends like
// one of their series, such as a requests_count beside a requests, is a family of its own.
func (f *familyNames) of(name string) string {
	if _, ok := f.types[name]; ok {
		return name
	}
	for _, suffix := range []string{"_bucket", "_sum", "_count", "_gsum", "_gcount"} {
		base, ok := strings.CutSuffix(name, suffix)
		if !ok {
			continue
		}
		t, known := f.types[base]
		histogram := known && (t == mimirpb.HISTOGRAM || t == mimirpb.GAUGEHISTOGRAM) || !known && f.histograms[base]
		summary := known && t == mimirpb.SUMMARY || !known && f.summaries[base]
		if histogram || summary && (suffix == "_sum" || suffix == "_count") {
			return base
		}
	}
	return name
}

// compositionOfDumps is the Composition of the records of kafkatool dumps, a JSON record per line, of today's format,
// over them all: it first learns their families from all of them, then counts them.
func compositionOfDumps(paths []string) (Composition, error) {
	counter := compositionCounter{today: true}
	for _, learning := range []bool{true, false} {
		for _, path := range paths {
			err := readDump(path, func(req *mimirpb.WriteRequest) {
				if learning {
					counter.families.learn(req)
				} else {
					counter.add(req)
				}
			})
			if err != nil {
				return Composition{}, fmt.Errorf("%s: %w", path, err)
			}
		}
	}
	return counter.composition(), nil
}

// readDump calls each with the request of each record of a kafkatool dump.
func readDump(path string, each func(*mimirpb.WriteRequest)) error {
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
		each(&req.WriteRequest)
	}
	return scanner.Err()
}
