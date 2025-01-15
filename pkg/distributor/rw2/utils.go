package rw2

import (
	"fmt"

	"github.com/prometheus/prometheus/model/labels"
	promRW2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"
)

// NewWriteRequest is used in testing Remote Write 2.0 to generate a new write request.
func NewWriteRequest() *promRW2.Request {
	return &promRW2.Request{}
}

// AddFloatSeries is used in testing Remote Write 2.0 to add a float series to a write request.
// Write 0 into createdTimestamp to not use it.
func AddFloatSeries(
	req *promRW2.Request,
	lbls labels.Labels,
	floats []promRW2.Sample,
	metricType promRW2.Metadata_MetricType,
	help string,
	unit string,
	createdTimestamp int64,
	exemplars []promRW2.Exemplar) *promRW2.Request {
	if req == nil {
		req = NewWriteRequest()
	}

	var labelsRefs []uint32
	lbls.Range(func(l labels.Label) {
		labelsRefs = append(labelsRefs, getSymbol(l.Name, &req.Symbols))
		labelsRefs = append(labelsRefs, getSymbol(l.Value, &req.Symbols))
	})

	ts := promRW2.TimeSeries{
		LabelsRefs: labelsRefs,
		Samples:    floats,
		Metadata: promRW2.Metadata{
			Type:    metricType,
			HelpRef: getSymbol(help, &req.Symbols),
			UnitRef: getSymbol(unit, &req.Symbols),
		},
		Exemplars:        exemplars,
		CreatedTimestamp: createdTimestamp,
	}
	fmt.Printf("KRAJO: AddFloatSeries: %v\n", ts.Metadata)
	req.Timeseries = append(req.Timeseries, ts)

	return req
}

// AddHistogramSeries is used in testing Remote Write 2.0 to add a histogram series to a write request.
// Write 0 into createdTimestamp to not use it.
func AddHistogramSeries(
	req *promRW2.Request,
	lbls labels.Labels,
	histograms []promRW2.Histogram,
	help string,
	unit string,
	createdTimestamp int64,
	exemplars []promRW2.Exemplar) *promRW2.Request {
	if req == nil {
		req = NewWriteRequest()
	}

	var labelsRefs []uint32
	lbls.Range(func(l labels.Label) {
		labelsRefs = append(labelsRefs, getSymbol(l.Name, &req.Symbols))
		labelsRefs = append(labelsRefs, getSymbol(l.Value, &req.Symbols))
	})

	metricType := promRW2.Metadata_METRIC_TYPE_HISTOGRAM
	if histograms[0].ResetHint == promRW2.Histogram_RESET_HINT_GAUGE {
		metricType = promRW2.Metadata_METRIC_TYPE_GAUGEHISTOGRAM
	}

	ts := promRW2.TimeSeries{
		LabelsRefs: labelsRefs,
		Histograms: histograms,
		Metadata: promRW2.Metadata{
			Type:    metricType,
			HelpRef: getSymbol(help, &req.Symbols),
			UnitRef: getSymbol(unit, &req.Symbols),
		},
		Exemplars:        exemplars,
		CreatedTimestamp: createdTimestamp,
	}
	req.Timeseries = append(req.Timeseries, ts)

	return req
}

func getSymbol(sym string, symbols *[]string) uint32 {
	for i, s := range *symbols {
		if s == sym {
			return uint32(i)
		}
	}
	*symbols = append(*symbols, sym)
	return uint32(len(*symbols) - 1)
}
