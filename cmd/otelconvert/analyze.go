package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"slices"

	"github.com/aybabtme/uniplot/histogram"
	gokitlog "github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"

	utillog "github.com/grafana/mimir/pkg/util/log"
)

func validateAnalyzeConfig(cfg config) error {
	if cfg.block == "" {
		return fmt.Errorf("missing --block")
	}
	if cfg.dest == "" {
		return fmt.Errorf("missing --dest")
	}

	return nil
}

func analyzeBlock(ctx context.Context, blockDir, dest string, histScale, histMax, histNumBins int, logger gokitlog.Logger) error {
	b, err := tsdb.OpenBlock(utillog.SlogFromGoKit(logger), blockDir, nil, nil)
	if err != nil {
		return fmt.Errorf("open block: %w", err)
	}

	ir, err := b.Index()
	if err != nil {
		return fmt.Errorf("get index reader: %w", err)
	}
	defer func() { _ = ir.Close() }()

	p := ir.PostingsForAllLabelValues(ctx, "__name__")
	p = ir.SortedPostings(p)

	var builder labels.ScratchBuilder
	chkMetas := []chunks.Meta(nil)

	// Lookup labels by name, then value.
	lbls := make(map[string]map[string]struct{})

	for p.Next() {
		if err = p.Err(); err != nil {
			return fmt.Errorf("iterate postings: %w", err)
		}

		err = ir.Series(p.At(), &builder, &chkMetas)
		if err != nil {
			return fmt.Errorf("populate series chunk metas: %w", err)
		}

		builder.Labels().Range(func(lbl labels.Label) {
			if _, ok := lbls[lbl.Name]; !ok {
				lbls[lbl.Name] = make(map[string]struct{})
			}

			if _, ok := lbls[lbl.Name][lbl.Value]; !ok {
				lbls[lbl.Name][lbl.Value] = struct{}{}
			}
		})

		builder.Reset()
	}

	type labelCardinality struct {
		labelName   string
		cardinality int
	}

	lblCardinalities := make([]labelCardinality, 0, len(lbls))
	cardVals := make([]float64, 0, len(lbls))

	for lblName, lblVals := range lbls {
		c := len(lblVals)

		lblCardinalities = append(lblCardinalities, labelCardinality{
			labelName:   lblName,
			cardinality: c,
		})

		if c > histMax {
			continue
		}
		cardVals = append(cardVals, float64(c))
	}

	log.Printf("distribution of cardinalities over %d labels:\n", len(cardVals))
	h := histogram.Hist(histNumBins, cardVals)
	_ = histogram.Fprint(os.Stdout, h, histogram.Linear(histScale))

	log.Printf("writing cardinalities for %d labels to %s...\n", len(lblCardinalities), dest)

	slices.SortFunc(lblCardinalities, func(a, b labelCardinality) int {
		return a.cardinality - b.cardinality
	})

	f, err := os.OpenFile(dest, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("open file '%s' for writing: %w", dest, err)
	}
	defer func() { _ = f.Close() }()

	for _, lbl := range lblCardinalities {
		if _, err = f.WriteString(fmt.Sprintf("%s: %d\n", lbl.labelName, lbl.cardinality)); err != nil {
			return fmt.Errorf("write label cardinality to '%s': %w", dest, err)
		}
	}

	return nil
}
