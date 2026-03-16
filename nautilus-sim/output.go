package main

import (
	"fmt"
	"io"
)

type CSVWriter struct {
	w io.Writer
}

func NewCSVWriter(w io.Writer) *CSVWriter {
	return &CSVWriter{w: w}
}

func (c *CSVWriter) WriteHeader() {
	fmt.Fprintln(c.w,
		"step,"+
			"ing_min,ing_max,ing_mean,ing_p1,ing_p5,ing_p50,ing_p99,ing_stddev,ing_max_mean,"+
			"qry_min,qry_max,qry_mean,qry_p1,qry_p5,qry_p50,qry_p99,qry_stddev,qry_max_mean,"+
			"ing_moves,ing_merges,ing_splits,ing_ranges,ing_max_range_load,ing_max_range_width,qry_moves,ing_churn,"+
			"catchup_count,catchup_load,load_change")
}

func (c *CSVWriter) WriteRow(
	step int,
	ing, qry Stats,
	ingMoves, ingMerges, ingSplits, ingRanges int,
	ingMaxRangeLoad float64,
	ingMaxRangeWidth uint64,
	qryMoves int,
	ingChurn float64,
	catchupCount int,
	catchupLoad float64,
	loadChange bool,
) {
	lc := 0
	if loadChange {
		lc = 1
	}
	fmt.Fprintf(c.w,
		"%d,%.4f,%.4f,%.4f,%.4f,%.4f,%.4f,%.4f,%.4f,%.4f,%.4f,%.4f,%.4f,%.4f,%.4f,%.4f,%.4f,%.4f,%.4f,%d,%d,%d,%d,%.4f,%d,%d,%.6f,%d,%.4f,%d\n",
		step,
		ing.Min, ing.Max, ing.Mean, ing.P1, ing.P5, ing.P50, ing.P99, ing.Stddev, ing.MaxMean,
		qry.Min, qry.Max, qry.Mean, qry.P1, qry.P5, qry.P50, qry.P99, qry.Stddev, qry.MaxMean,
		ingMoves, ingMerges, ingSplits, ingRanges, ingMaxRangeLoad, ingMaxRangeWidth, qryMoves, ingChurn,
		catchupCount, catchupLoad, lc,
	)
}
