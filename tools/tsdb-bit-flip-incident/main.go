// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
)

var logger = log.NewLogfmtLogger(os.Stderr)

func main() {
	flag.Parse()
	if flag.NArg() == 0 {
		fmt.Println("No block directory specified.")
		return
	}

	for _, blockDir := range flag.Args() {
		checkBlock(blockDir)
	}
}

func checkBlock(blockDir string) {
	block, err := tsdb.OpenBlock(logger, blockDir, nil)
	if err != nil {
		level.Error(logger).Log("msg", "failed to open block", "dir", blockDir, "err", err)
		return
	}
	defer block.Close()

	fmt.Println(fmt.Sprintf("Analyzing %s", block.Meta().ULID.String()))

	idx, err := block.Index()
	if err != nil {
		level.Error(logger).Log("msg", "failed to open block index", "err", err)
		return
	}
	defer idx.Close()

	k, v := index.AllPostingsKey()
	p, err := idx.Postings(k, v)
	if err != nil {
		level.Error(logger).Log("msg", "failed to get postings", "err", err)
		return
	}

	goodSymbols := map[string]struct{}{}
	candidateSymbols := map[string]labels.Labels{}

	var builder labels.ScratchBuilder
	for p.Next() {
		chks := []chunks.Meta(nil)
		err := idx.Series(p.At(), &builder, &chks)
		if err != nil {
			level.Error(logger).Log("msg", "error getting series", "seriesID", p.At(), "err", err)
			continue
		}

		lbls := builder.Labels()
		isGood := false
		if len(chks) > 1 {
			isGood = true
		} else if chks[0].MinTime != chks[0].MaxTime {
			// blindly assume we have at least 1 chunk, otherwise this will panic.
			// mintime != maxtime implies that there's more than one sample
			isGood = true
		}

		for _, lbl := range lbls {
			if isGood {
				goodSymbols[lbl.Name] = struct{}{}
				goodSymbols[lbl.Value] = struct{}{}
			} else {
				candidateSymbols[lbl.Name] = lbls
				candidateSymbols[lbl.Value] = lbls
			}
		}
	}

	if p.Err() != nil {
		level.Error(logger).Log("msg", "error iterating postings", "err", p.Err())
		return
	}

	count := 0
	for candidateSymbol, ls := range candidateSymbols {
		theGood, _, ok := existsExactlyOneSymbolFlippingJustOneBit(goodSymbols, candidateSymbol)
		if !ok {
			// We couldn't find other series flipping just one bit.
			continue
		}

		fmt.Println("Good:", theGood)
		fmt.Println("Bad: ", candidateSymbol)
		fmt.Println("Bad series:", ls.String())
		fmt.Println("")

		count++
	}

	fmt.Printf("Analyzed BLOCK=%s COUNT=%d\n\n", block.Meta().ULID.String(), count)
}

func existsExactlyOneSymbolFlippingJustOneBit(good map[string]struct{}, candidate string) (string, int, bool) {
	cb := []byte(candidate)
	count := 0
	theGood := ""
	pos := 0
	for i := range cb {
		cb[i] = cb[i] ^ 0b10
		if _, has := good[string(cb)]; has {
			count++
			theGood = string(cb)
			pos = i
			if count > 1 {
				return "", 0, false
			}
		}
		cb[i] = cb[i] ^ 0b10
	}
	return theGood, pos, count == 1
}

func flipHappensInLabelName(good string, candidate labels.Labels) bool {
	// This assumes labels are slices, because it's easier for me to do this that way right now.
	for i := range candidate {
		nb := []byte(candidate[i].Name)
		for j := range nb {
			nb[j] = nb[j] ^ 0b10
			candidate[i].Name = string(nb)
			if good == candidate.String() {
				return true
				// return fmt.Sprintf("flip in byte %d of label %d=%s", j, i, orig[i].Name)
			}
			nb[j] = nb[j] ^ 0b10
			candidate[i].Name = string(nb)
		}
	}
	return false
}
