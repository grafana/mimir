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
			"ing_min,ing_max,ing_mean,ing_p99,ing_stddev,ing_max_mean,"+
			"qry_min,qry_max,qry_mean,qry_p99,qry_stddev,qry_max_mean,"+
			"ing_moves,qry_moves,ing_churn,"+
			"catchup_count,catchup_load")
}

func (c *CSVWriter) WriteRow(
	step int,
	ing, qry Stats,
	ingMoves, qryMoves int,
	ingChurn float64,
	catchupCount int,
	catchupLoad float64,
) {
	fmt.Fprintf(c.w,
		"%d,%.4f,%.4f,%.4f,%.4f,%.4f,%.4f,%.4f,%.4f,%.4f,%.4f,%.4f,%.4f,%d,%d,%.6f,%d,%.4f\n",
		step,
		ing.Min, ing.Max, ing.Mean, ing.P99, ing.Stddev, ing.MaxMean,
		qry.Min, qry.Max, qry.Mean, qry.P99, qry.Stddev, qry.MaxMean,
		ingMoves, qryMoves, ingChurn,
		catchupCount, catchupLoad,
	)
}
