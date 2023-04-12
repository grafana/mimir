package main

import (
	"os"
	"strings"

	"github.com/pkg/errors"
)

type csvWriter[D any] struct {
	header    string
	data      []D
	formatter func(D) []string
}

func newCSVWriter[D any]() *csvWriter[D] {
	return &csvWriter[D]{}
}

func (w *csvWriter[D]) setHeader(fields []string) {
	w.header = strings.Join(fields, ",")
}

func (w *csvWriter[D]) setData(data []D, formatter func(D) []string) {
	w.data = data
	w.formatter = formatter
}

func (w *csvWriter[D]) writeCSV(filename string) error {
	f, err := os.OpenFile(filename, os.O_TRUNC|os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return errors.Wrap(err, "unable to create output CSV file")
	}

	if _, err := f.WriteString(w.header + "\n"); err != nil {
		return errors.Wrap(err, "error while writing to output CSV file")
	}

	for i := 0; i < len(w.data); i++ {
		if _, err := f.WriteString(strings.Join(w.formatter(w.data[i]), ",") + "\n"); err != nil {
			return errors.Wrap(err, "error while writing to output CSV file")
		}
	}

	if err := f.Close(); err != nil {
		return errors.Wrap(err, "error while closing output CSV file")
	}

	return nil
}
