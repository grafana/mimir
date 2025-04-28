// Copyright The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package search

import (
	"fmt"
	"slices"
	"sort"

	"github.com/parquet-go/parquet-go"
)

func initialize(s *parquet.Schema, cs ...Constraint) error {
	for i := range cs {
		if err := cs[i].init(s); err != nil {
			return fmt.Errorf("unable to initialize constraint %d: %w", i, err)
		}
	}
	return nil
}

func filter(rg parquet.RowGroup, cs ...Constraint) ([]rowRange, error) {
	// Constraints for sorting columns are cheaper to evaluate, so we sort them first.
	sc := rg.SortingColumns()

	var n int
	for i := range sc {
		if n == len(cs) {
			break
		}
		for j := range cs {
			if cs[j].path() == sc[i].Path()[0] {
				cs[n], cs[j] = cs[j], cs[n]
				n++
			}
		}
	}
	rr := []rowRange{{from: int64(0), count: rg.NumRows()}}
	for i := range cs {
		srr, err := cs[i].filter(rg, i == 0, rr)
		if err != nil {
			return nil, fmt.Errorf("unable to filter with constraint %d: %w", i, err)
		}
		rr = srr
	}
	return rr, nil
}

// symbolTable is a helper that can decode the i-th value of a page.
// Using it we only need to allocate an int32 slice and not a slice of
// string values.
// It only works for optional dictionary encoded columns. All of our label
// columns are that though.
type symbolTable struct {
	dict parquet.Dictionary
	syms []int32
}

func (s *symbolTable) Get(i int) parquet.Value {
	switch s.syms[i] {
	case -1:
		return parquet.NullValue()
	default:
		return s.dict.Index(s.syms[i])
	}
}

func (s *symbolTable) Reset(pg parquet.Page) {
	dict := pg.Dictionary()
	data := pg.Data()
	syms := data.Int32()
	defs := pg.DefinitionLevels()

	if s.syms == nil {
		s.syms = make([]int32, len(defs))
	} else {
		s.syms = slices.Grow(s.syms, len(defs))[:len(defs)]
	}

	sidx := 0
	for i := range defs {
		if defs[i] == 1 {
			s.syms[i] = syms[sidx]
			sidx++
		} else {
			s.syms[i] = -1
		}
	}
	s.dict = dict
}

type equalConstraint struct {
	pth string

	val parquet.Value

	comp func(l, r parquet.Value) int
}

func Equal(path string, value parquet.Value) Constraint {
	return &equalConstraint{pth: path, val: value}
}

func (ec *equalConstraint) filter(rg parquet.RowGroup, primary bool, rr []rowRange) ([]rowRange, error) {
	if len(rr) == 0 {
		return nil, nil
	}
	from, to := rr[0].from, rr[len(rr)-1].from+rr[len(rr)-1].count

	col, ok := rg.Schema().Lookup(ec.path())
	if !ok {
		return nil, nil
	}
	cc := rg.ColumnChunks()[col.ColumnIndex]

	if skip, err := ec.skipByBloomfilter(cc); err != nil {
		return nil, fmt.Errorf("unable to skip by bloomfilter: %w", err)
	} else if skip {
		return nil, nil
	}

	pgs := cc.Pages()
	defer func() { _ = pgs.Close() }()

	oidx, err := cc.OffsetIndex()
	if err != nil {
		return nil, fmt.Errorf("unable to read offset index: %w", err)
	}
	cidx, err := cc.ColumnIndex()
	if err != nil {
		return nil, fmt.Errorf("unable to read column index: %w", err)
	}
	var (
		symbols = new(symbolTable)
		res     = make([]rowRange, 0)
	)
	for i := 0; i < cidx.NumPages(); i++ {
		// If page does not intersect from, to; we can immediately discard it
		pfrom := oidx.FirstRowIndex(i)
		pcount := rg.NumRows() - pfrom
		if i < oidx.NumPages()-1 {
			pcount = oidx.FirstRowIndex(i+1) - pfrom
		}
		pto := pfrom + pcount
		if pfrom > to {
			break
		}
		if pto < from {
			continue
		}
		// Page intersects [from, to] but we might be able to discard it with statistics
		if cidx.NullPage(i) {
			continue
		}
		minv, maxv := cidx.MinValue(i), cidx.MaxValue(i)

		if !ec.val.IsNull() && !maxv.IsNull() && ec.comp(ec.val, maxv) > 0 {
			if cidx.IsDescending() {
				break
			}
			continue
		}
		if !ec.val.IsNull() && !minv.IsNull() && ec.comp(ec.val, minv) < 0 {
			if cidx.IsAscending() {
				break
			}
			continue
		}
		// We cannot discard the page through statistics but we might need to read it to see if it has the value
		if err := pgs.SeekToRow(pfrom); err != nil {
			return nil, fmt.Errorf("unable to seek to row: %w", err)
		}
		pg, err := pgs.ReadPage()
		if err != nil {
			return nil, fmt.Errorf("unable to read page: %w", err)
		}

		if skip := ec.skipByDictionary(pg); skip {
			continue
		}
		symbols.Reset(pg)

		// The page has the value, but the page might consist of many rows
		// ideally we cut it down a little because chunk pages are pretty small
		n := int(pg.NumRows())
		bl := int(max(pfrom, from) - pfrom)
		br := n - int(pto-min(pto, to))
		var l, r int
		switch {
		case cidx.IsAscending() && primary:
			l = sort.Search(n, func(i int) bool { return ec.comp(ec.val, symbols.Get(i)) <= 0 })
			r = sort.Search(n, func(i int) bool { return ec.comp(ec.val, symbols.Get(i)) < 0 })

			if lv, rv := max(bl, l), min(br, r); rv > lv {
				res = append(res, rowRange{pfrom + int64(lv), int64(rv - lv)})
			}
		default:
			off, count := bl, 0
			for j := bl; j < br; j++ {
				if ec.comp(ec.val, symbols.Get(j)) != 0 {
					if count != 0 {
						res = append(res, rowRange{pfrom + int64(off), int64(count)})
					}
					off, count = j, 0
				} else {
					if count == 0 {
						off = j
					}
					count++
				}
			}
			if count != 0 {
				res = append(res, rowRange{pfrom + int64(off), int64(count)})
			}
		}
	}
	if len(res) == 0 {
		return nil, nil
	}
	return simplify(res), nil
}

func (ec *equalConstraint) init(s *parquet.Schema) error {
	c, ok := s.Lookup(ec.path())
	if !ok {
		return fmt.Errorf("schema: must contain path: %s", ec.path())
	}
	if c.Node.Type().Kind() != ec.val.Kind() {
		return fmt.Errorf("schema: cannot search value of kind %s in column of kind %s", ec.val.Kind(), c.Node.Type().Kind())
	}
	ec.comp = c.Node.Type().Compare
	return nil
}

func (ec *equalConstraint) path() string {
	return ec.pth
}

func (ec *equalConstraint) skipByBloomfilter(cc parquet.ColumnChunk) (bool, error) {
	bf := cc.BloomFilter()
	if bf == nil {
		return false, nil
	}
	ok, err := bf.Check(ec.val)
	if err != nil {
		return false, fmt.Errorf("unable to check bloomfilter: %w", err)
	}
	return !ok, nil
}

func (ec *equalConstraint) skipByDictionary(pg parquet.Page) bool {
	d := pg.Dictionary()
	if d == nil {
		return false
	}
	for i := 0; i != d.Len(); i++ {
		if ec.comp(ec.val, d.Index(int32(i))) == 0 {
			return false
		}
	}
	return true
}

func Not(c Constraint) Constraint {
	return &notConstraint{c: c}
}

type notConstraint struct {
	c Constraint
}

func (nc *notConstraint) filter(rg parquet.RowGroup, primary bool, rr []rowRange) ([]rowRange, error) {
	base, err := nc.c.filter(rg, primary, rr)
	if err != nil {
		return nil, fmt.Errorf("unable to compute child constraint: %w", err)
	}
	return complementRowRanges(base, rr), nil
}

func (nc *notConstraint) init(s *parquet.Schema) error {
	return nc.c.init(s)
}

func (nc *notConstraint) path() string {
	return nc.c.path()
}

type nullConstraint struct{}

func Null() Constraint {
	return &nullConstraint{}
}

func (null *nullConstraint) filter(parquet.RowGroup, bool, []rowRange) ([]rowRange, error) {
	return nil, nil
}

func (null *nullConstraint) init(_ *parquet.Schema) error {
	return nil
}

func (null *nullConstraint) path() string {
	return ""
}
