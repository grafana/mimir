// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus-community/parquet-common/blob/382b6ec8ae40fb5dcdcabd8019f69a4be1cd8869/schema/schema_builder.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package schema

import (
	"fmt"
	"strconv"

	"github.com/parquet-go/parquet-go"
	"github.com/pkg/errors"
)

type Builder struct {
	g parquet.Group

	metadata          map[string]string
	dataColDurationMs int64
	mint, maxt        int64
}

func NewBuilder(mint, maxt, colDuration int64) *Builder {
	b := &Builder{
		g:                 make(parquet.Group),
		dataColDurationMs: colDuration,
		metadata: map[string]string{
			DataColSizeMd: strconv.FormatInt(colDuration, 10),
			MaxTMd:        strconv.FormatInt(maxt, 10),
			MinTMd:        strconv.FormatInt(mint, 10),
		},
		mint: mint,
		maxt: maxt,
	}

	return b
}

func FromLabelsFile(lf *parquet.File) (*TSDBSchema, error) {
	md := MetadataToMap(lf.Metadata().KeyValueMetadata)
	mint, err := strconv.ParseInt(md[MinTMd], 0, 64)
	if err != nil {
		return nil, errors.Wrap(err, "failed to convert mint to int")
	}

	maxt, err := strconv.ParseInt(md[MaxTMd], 10, 64)
	if err != nil {
		return nil, errors.Wrap(err, "failed to convert max to int")
	}

	dataColDurationMs, err := strconv.ParseInt(md[DataColSizeMd], 10, 64)
	if err != nil {
		return nil, errors.Wrap(err, "failed to convert dataColDurationMs to int")
	}
	g := make(parquet.Group)

	b := &Builder{
		g:                 g,
		metadata:          md,
		mint:              mint,
		maxt:              maxt,
		dataColDurationMs: dataColDurationMs,
	}

	for _, c := range lf.Schema().Columns() {
		lbl, ok := ExtractLabelFromColumn(c[0])
		if !ok {
			continue
		}

		b.AddLabelNameColumn(lbl)
	}

	return b.Build()
}

func (b *Builder) AddLabelNameColumn(lbls ...string) {
	for _, lbl := range lbls {
		b.g[LabelToColumn(lbl)] = parquet.Optional(parquet.Encoded(parquet.String(), &parquet.RLEDictionary))
	}
}

func (b *Builder) Build() (*TSDBSchema, error) {
	colIdx := 0

	b.g[ColIndexes] = parquet.Encoded(parquet.Leaf(parquet.ByteArrayType), &parquet.DeltaByteArray)
	for i := b.mint; i <= b.maxt; i += b.dataColDurationMs {
		b.g[DataColumn(colIdx)] = parquet.Encoded(parquet.Leaf(parquet.ByteArrayType), &parquet.DeltaLengthByteArray)
		colIdx++
	}

	s := parquet.NewSchema("tsdb", b.g)

	dc := make([]int, colIdx)
	for i := range dc {
		lc, ok := s.Lookup(DataColumn(i))
		if !ok {
			return nil, fmt.Errorf("data column %v not found", DataColumn(i))
		}
		dc[i] = lc.ColumnIndex
	}

	return &TSDBSchema{
		Schema:            s,
		Metadata:          b.metadata,
		DataColDurationMs: b.dataColDurationMs,
		DataColsIndexes:   dc,
		MinTs:             b.mint,
		MaxTs:             b.maxt,
	}, nil
}

type TSDBSchema struct {
	Schema   *parquet.Schema
	Metadata map[string]string

	DataColsIndexes   []int
	MinTs, MaxTs      int64
	DataColDurationMs int64
}

type TSDBProjection struct {
	Schema       *parquet.Schema
	ExtraOptions []parquet.WriterOption
}

func (s *TSDBSchema) DataColumIdx(t int64) int {
	colIdx := 0

	for i := s.MinTs + s.DataColDurationMs; i <= t; i += s.DataColDurationMs {
		colIdx++
	}

	return colIdx
}

func (s *TSDBSchema) LabelsProjection() (*TSDBProjection, error) {
	g := make(parquet.Group)

	lc, ok := s.Schema.Lookup(ColIndexes)
	if !ok {
		return nil, fmt.Errorf("column %v not found", ColIndexes)
	}
	g[ColIndexes] = lc.Node

	for _, c := range s.Schema.Columns() {
		if _, ok := ExtractLabelFromColumn(c[0]); !ok {
			continue
		}
		lc, ok := s.Schema.Lookup(c...)
		if !ok {
			return nil, fmt.Errorf("column %v not found", c)
		}
		g[c[0]] = lc.Node
	}
	return &TSDBProjection{
		Schema: WithCompression(parquet.NewSchema("labels-projection", g)),
	}, nil
}

func (s *TSDBSchema) ChunksProjection() (*TSDBProjection, error) {
	g := make(parquet.Group)
	skipPageBoundsOpts := make([]parquet.WriterOption, 0, len(s.DataColsIndexes))

	for _, c := range s.Schema.Columns() {
		if ok := IsDataColumn(c[0]); !ok {
			continue
		}
		lc, ok := s.Schema.Lookup(c...)
		if !ok {
			return nil, fmt.Errorf("column %v not found", c)
		}
		g[c[0]] = lc.Node
		skipPageBoundsOpts = append(skipPageBoundsOpts, parquet.SkipPageBounds(c...))
	}

	return &TSDBProjection{
		Schema:       WithCompression(parquet.NewSchema("chunk-projection", g)),
		ExtraOptions: skipPageBoundsOpts,
	}, nil
}
