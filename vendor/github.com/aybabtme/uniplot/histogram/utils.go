package histogram

import (
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"text/tabwriter"
)

// FormatFunc formats a float into the proper string form. Used to
// print meaningful axe labels.
type FormatFunc func(v float64) string

var blocks = []string{
	"▏", "▎", "▍", "▌", "▋", "▊", "▉", "█",
}

var barstring = func(v float64) string {
	decimalf := (v - math.Floor(v)) * 10.0
	decimali := math.Floor(decimalf)
	charIdx := int(decimali / 10.0 * 8.0)
	return strings.Repeat("█", int(v)) + blocks[charIdx]
}

// Fprint prints a unicode histogram on the io.Writer, using
// scale s. This code:
//
// 	hist := Hist(9, data)
// 	err := Fprint(os.Stdout, hist, Linear(5))
//
// ... yields the graph:
//
//	0.1-0.2  5%   ▋1
//	0.2-0.3  25%  ██▋5
//	0.3-0.4  0%   ▏
//	0.4-0.5  5%   ▋1
//	0.5-0.6  50%  █████▏10
//	0.6-0.7  0%   ▏
//	0.7-0.8  0%   ▏
//	0.8-0.9  5%   ▋1
//	0.9-1    10%  █▏2
func Fprint(w io.Writer, h Histogram, s ScaleFunc) error {
	return fprintf(w, h, s, func(v float64) string {
		return fmt.Sprintf("%.4g", v)
	})
}

// Fprintf is the same as Fprint, but applies f to the axis labels.
func Fprintf(w io.Writer, h Histogram, s ScaleFunc, f FormatFunc) error {
	return fprintf(w, h, s, f)
}

func fprintf(w io.Writer, h Histogram, s ScaleFunc, f FormatFunc) error {
	tabw := tabwriter.NewWriter(w, 2, 2, 2, byte(' '), 0)

	yfmt := func(y int) string {
		if y > 0 {
			return strconv.Itoa(y)
		}
		return ""
	}

	for i, bkt := range h.Buckets {
		sz := h.Scale(s, i)
		fmt.Fprintf(tabw, "%s-%s\t%.3g%%\t%s\n",
			f(bkt.Min), f(bkt.Max),
			float64(bkt.Count)*100.0/float64(h.Count),
			barstring(sz)+"\t"+yfmt(bkt.Count),
		)
	}

	return tabw.Flush()
}
