package main

import (
	"encoding/csv"
	"fmt"
	"github.com/go-logfmt/logfmt"
	"log/slog"
	"os"
	"slices"
	"strings"
)

func main() {
	if err := run(); err != nil {
		slog.Error("application failed", "err", err)
		os.Exit(1)
	}
}

func run() error {
	logFile := os.Args[1]
	slog.Info("parsing file", "file", logFile)

	f, err := os.Open(logFile)
	if err != nil {
		return err
	}

	d := logfmt.NewDecoderSize(f, 60000)
	w := csv.NewWriter(os.Stdout)
	err = w.Write([]string{
		"Failure time",
		"Trace ID",
		"Expression",
		"Start timestamp",
		"End timestamp",
		"Step",
		"Results cache",
		"Query sample timestamp",
		"Expected value",
		"Actual value",
	})

	if err != nil {
		return err
	}

	for d.ScanRecord() {
		var failureTime string
		var traceID string
		var expression string
		var startT string
		var endT string
		var step string
		var failureDetails string
		var resultsCache string

		for d.ScanKeyval() {
			key := string(d.Key())
			val := string(d.Value())

			switch key {
			case "ts":
				failureTime = val
			case "trace_id":
				traceID = val
			case "query":
				expression = val
			case "start":
				startT = val
			case "end":
				endT = val
			case "step":
				step = val
			case "err":
				failureDetails = val
			case "results_cache":
				resultsCache = val
			}
		}

		failureDetailsLines := strings.Split(failureDetails, "\n")
		firstFailureLineIndex := slices.IndexFunc(failureDetailsLines, valueDiffers)
		lastFailureLineIndex := lastIndex(failureDetailsLines, valueDiffers)
		const failureContextLines = 8
		firstFailureLine := max(0, firstFailureLineIndex-failureContextLines)
		lastFailureLine := min(len(failureDetailsLines)-1, lastFailureLineIndex+failureContextLines)

		for i := firstFailureLine; i <= lastFailureLine; i++ {
			failureDetailsLine := failureDetailsLines[i]
			sampleT, expected, actual := parseFailureDetailsLine(failureDetailsLine)

			err := w.Write([]string{
				failureTime,
				traceID,
				expression,
				startT,
				endT,
				step,
				resultsCache,
				sampleT,
				expected,
				actual,
			})

			if err != nil {
				return err
			}
		}

	}

	w.Flush()

	if d.Err() != nil {
		return fmt.Errorf("failed to parse log file: %w", d.Err())
	}

	return w.Error()
}

func valueDiffers(s string) bool {
	return strings.Contains(s, "value differs!")
}

func lastIndex(s []string, f func(string) bool) int {
	for i := len(s) - 1; i >= 0; i-- {
		if f(s[i]) {
			return i
		}
	}

	return -1
}

func parseFailureDetailsLine(l string) (ts string, expected string, actual string) {
	fields := strings.Split(l, "  ")
	ts, _, _ = strings.Cut(fields[0], " ")
	expected = fields[1]
	actual = fields[2]

	return ts, expected, actual
}
