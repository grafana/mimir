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
	// Of a kafkatool dump, the distinct metric families with a help, each tenant's its own, and the length of their
	// helps, as a profile's help_length has it, and the families whose tenant's metadata gave them more than one type.
	Families            int         `json:"families,omitempty"`
	HelpLength          Percentiles `json:"help_length,omitempty"`
	ConflictingFamilies int         `json:"conflicting_families,omitempty"`
	// Of the records that came by OTLP: how many, the resources of each, the series with data per resource of each,
	// and the bytes of each one's resource table. A series' resource is its job and instance in today's records, as
	// OTLP's converter derives them, and its entry of the record's resource table in the new writer's.
	OTLPRecords             int     `json:"otlp_records,omitempty"`
	ResourcesPerRecord      float64 `json:"resources_per_record,omitempty"`
	SeriesPerRecordResource float64 `json:"series_per_record_resource,omitempty"`
	TableBytesPerRecord     float64 `json:"table_bytes_per_record,omitempty"`
	// The share of the series with data that came by OTLP, and per target_info series among them: OTLP's converter
	// writes one for each resource of each push, so it's the series per resource of a push.
	OTLPSeriesShare     float64 `json:"otlp_series_share,omitempty"`
	SeriesPerTargetInfo float64 `json:"series_per_target_info,omitempty"`
	// Of a kafkatool dump, the distinct target_info series' attributes besides job and instance, each tenant's its own,
	// the length of their values, and the share of the values that are integers: what a resource profile's
	// descriptive_attributes, value_length and int_share are.
	TargetInfoAttributes  Percentiles `json:"target_info_attributes,omitempty"`
	TargetInfoValueLength Percentiles `json:"target_info_value_length,omitempty"`
	TargetInfoIntShare    float64     `json:"target_info_int_share,omitempty"`
}

// compositionCounter adds records up into a Composition. In today's records, series carry no metadata of their own:
// a series' value is its family's, as families tells it by the series' name, and each distinct family entry is a
// stand-in the new writer would write. In the new writer's, a value is a series' own, and stand-ins are series without
// data.
type compositionCounter struct {
	today                             bool
	records, series, values, standIns int
	// Of today's records, the length of each family's help, by tenant, family and help, and each tenant's families as
	// its records tell of them.
	helps    map[[3]string]int
	families map[string]*familyNames
	// Of the records that came by OTLP, the records, their resources, their series with data with a resource, the
	// bytes of their resource tables, their series with data, and the target_info series among them.
	otlpRecords, resources, resourceSeries, tableBytes, otlpSeries, targetInfos int
	// Of today's records, the distinct target_info series, by tenant and labels, their attributes, and the length of
	// their values and how many are integers.
	targetInfoSeries                        map[string]struct{}
	targetInfoAttributes, targetInfoLengths []int
	targetInfoInts                          int
}

// add counts the tenant's request.
func (c *compositionCounter) add(tenant string, req *mimirpb.WriteRequest) {
	c.records++
	families := c.familiesOf(tenant)
	if c.today {
		families.learn(req)
	}
	values := map[string]struct{}{}
	entries := map[string]struct{}{}
	otlp := req.Source == mimirpb.OTLP
	// Today's resources of the record, by job and instance.
	var resources map[[2]string]struct{}
	if otlp {
		c.otlpRecords++
		if c.today {
			resources = map[[2]string]struct{}{}
		}
		c.resources += len(req.ResourceTable)
		for i := range req.ResourceTable {
			c.tableBytes += req.ResourceTable[i].Size()
		}
	}
	for _, m := range req.Metadata {
		entries[fmt.Sprint(m.MetricFamilyName, m.Type, m.Help, m.Unit)] = struct{}{}
		c.help(tenant, m.MetricFamilyName, m.Help)
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
		if otlp {
			c.otlpResource(tenant, ts.TimeSeries, resources)
		}
		if c.today {
			name := families.of(metricName(ts.Labels))
			values[name] = struct{}{}
			if m := ts.SeriesMetadata; !m.IsEmpty() {
				// The family's entry the old writer copied onto the series named like it.
				entries[fmt.Sprint(name, m.Type, m.Help, m.Unit)] = struct{}{}
				c.help(tenant, name, m.Help)
			}
			continue
		}
		if m := ts.SeriesMetadata; !m.IsEmpty() {
			values[fmt.Sprint(m.Type, m.Help, m.Unit)] = struct{}{}
		}
	}
	c.values += len(values)
	c.resources += len(resources)
	if c.today {
		c.standIns += len(entries)
	}
}

// otlpResource counts the resource of a series with data of a record that came by OTLP, and in today's records, adds
// it to the record's resources.
func (c *compositionCounter) otlpResource(tenant string, ts *mimirpb.TimeSeries, resources map[[2]string]struct{}) {
	c.otlpSeries++
	if metricName(ts.Labels) == "target_info" {
		c.targetInfos++
		if c.today {
			c.targetInfo(tenant, ts.Labels)
		}
		return
	}
	if !c.today {
		if ts.ResourceRef != 0 {
			c.resourceSeries++
		}
		return
	}
	var job, instance string
	for _, l := range ts.Labels {
		switch l.Name {
		case "job":
			job = l.Value
		case "instance":
			instance = l.Value
		}
	}
	if job != "" || instance != "" {
		resources[[2]string{job, instance}] = struct{}{}
		c.resourceSeries++
	}
}

// targetInfo counts a tenant's target_info series' attributes, the first time it's seen.
func (c *compositionCounter) targetInfo(tenant string, labels []mimirpb.LabelAdapter) {
	key := tenant + "\x00" + mimirpb.FromLabelAdaptersToString(labels)
	if _, ok := c.targetInfoSeries[key]; ok {
		return
	}
	if c.targetInfoSeries == nil {
		c.targetInfoSeries = map[string]struct{}{}
	}
	c.targetInfoSeries[key] = struct{}{}
	attributes := 0
	for _, l := range labels {
		if l.Name == "__name__" || l.Name == "job" || l.Name == "instance" {
			continue
		}
		attributes++
		c.targetInfoLengths = append(c.targetInfoLengths, len(l.Value))
		if _, err := strconv.ParseInt(l.Value, 10, 64); err == nil {
			c.targetInfoInts++
		}
	}
	c.targetInfoAttributes = append(c.targetInfoAttributes, attributes)
}

// familiesOf is the tenant's families: a tenant's metadata says nothing of another's metrics.
func (c *compositionCounter) familiesOf(tenant string) *familyNames {
	if c.families == nil {
		c.families = map[string]*familyNames{}
	}
	f, ok := c.families[tenant]
	if !ok {
		f = &familyNames{}
		c.families[tenant] = f
	}
	return f
}

// help counts a tenant's family's help, of today's records.
func (c *compositionCounter) help(tenant, family, help string) {
	if !c.today || help == "" {
		return
	}
	if c.helps == nil {
		c.helps = map[[3]string]int{}
	}
	c.helps[[3]string{tenant, family, help}] = len(help)
}

func (c *compositionCounter) composition() Composition {
	out := Composition{Records: c.records}
	if len(c.helps) > 0 {
		lengths := make([]int, 0, len(c.helps))
		families := map[[2]string]struct{}{}
		for key, n := range c.helps {
			lengths = append(lengths, n)
			families[[2]string{key[0], key[1]}] = struct{}{}
		}
		out.Families, out.HelpLength = len(families), percentilesOf(lengths)
	}
	if c.otlpRecords > 0 {
		out.OTLPRecords = c.otlpRecords
		out.ResourcesPerRecord = float64(c.resources) / float64(c.otlpRecords)
		out.TableBytesPerRecord = float64(c.tableBytes) / float64(c.otlpRecords)
	}
	if c.resources > 0 {
		out.SeriesPerRecordResource = float64(c.resourceSeries) / float64(c.resources)
	}
	if c.series > 0 {
		out.OTLPSeriesShare = float64(c.otlpSeries) / float64(c.series)
	}
	if c.targetInfos > 0 {
		out.SeriesPerTargetInfo = float64(c.otlpSeries-c.targetInfos) / float64(c.targetInfos)
	}
	if len(c.targetInfoAttributes) > 0 {
		out.TargetInfoAttributes = percentilesOf(c.targetInfoAttributes)
	}
	if len(c.targetInfoLengths) > 0 {
		out.TargetInfoValueLength = percentilesOf(c.targetInfoLengths)
		out.TargetInfoIntShare = float64(c.targetInfoInts) / float64(len(c.targetInfoLengths))
	}
	for _, f := range c.families {
		out.ConflictingFamilies += len(f.conflicts)
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

// percentilesOf is the percentiles of values, as a profile has them.
func percentilesOf(values []int) Percentiles {
	sorted := slices.Sorted(slices.Values(values))
	out := Percentiles{}
	for _, q := range []string{"0", "0.1", "0.5", "0.9", "0.99", "1"} {
		f, _ := strconv.ParseFloat(q, 64)
		out[q] = float64(sorted[min(len(sorted)-1, int(f*float64(len(sorted))))])
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

// familyNames tells a series' metric family by its name, from what one tenant's records tell of its families: their
// metadata, and the series of classic histograms and summaries.
type familyNames struct {
	// The families with metadata, by name, and their types.
	types map[string]mimirpb.MetricMetadata_MetricType
	// The families whose metadata gave them more than one type, which are taken as of none.
	conflicts map[string]bool
	// The families with a classic histogram's buckets, and with a summary's quantiles.
	histograms, summaries map[string]bool
}

func (f *familyNames) learn(req *mimirpb.WriteRequest) {
	if f.types == nil {
		f.types, f.conflicts = map[string]mimirpb.MetricMetadata_MetricType{}, map[string]bool{}
		f.histograms, f.summaries = map[string]bool{}, map[string]bool{}
	}
	for _, m := range req.Metadata {
		f.typed(m.MetricFamilyName, m.Type)
	}
	for _, ts := range req.Timeseries {
		name := metricName(ts.Labels)
		if !ts.SeriesMetadata.IsEmpty() {
			// The family's entry the old writer copied onto the series named like it.
			f.typed(name, ts.SeriesMetadata.Type)
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

// typed records that the family's metadata gives it the type, which conflicts with another it gave it.
func (f *familyNames) typed(family string, t mimirpb.MetricMetadata_MetricType) {
	if known, ok := f.types[family]; ok && known != t {
		f.conflicts[family] = true
	}
	f.types[family] = t
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
		known = known && !f.conflicts[base]
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
			err := readDump(path, func(tenant string, req *mimirpb.WriteRequest) {
				if learning {
					counter.familiesOf(tenant).learn(req)
				} else {
					counter.add(tenant, req)
				}
			})
			if err != nil {
				return Composition{}, fmt.Errorf("%s: %w", path, err)
			}
		}
	}
	return counter.composition(), nil
}

// readDump calls each with the tenant, the record's key, and the request of each record of a kafkatool dump.
func readDump(path string, each func(string, *mimirpb.WriteRequest)) error {
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
		each(string(record.Key), &req.WriteRequest)
	}
	return scanner.Err()
}
