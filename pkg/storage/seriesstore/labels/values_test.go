// SPDX-License-Identifier: AGPL-3.0-only
package labels

import (
	"fmt"
	"slices"
	"sync"
	"testing"
	"unsafe"
)

func TestSharedValuesRemainCanonicalAndOwned(t *testing.T) {
	raw := []byte("dictionary-value-must-outlive-request")
	value := unsafe.String(unsafe.SliceData(raw), len(raw))
	pairs := [][2]string{{"__name__", "dictionary_metric"}, {"namespace", value}, {"pod", "pod-0123456789"}}
	first := FromSorted(pairs)
	hash := HashPairs(pairs)
	canonical := first.AppendCanonical(nil)
	clear(raw)
	if first.Get("namespace") != "dictionary-value-must-outlive-request" {
		t.Fatal("retained mutable request bytes")
	}
	pairs[1][1] = "dictionary-value-must-outlive-request"
	second := FromSorted(pairs)
	if first != second {
		t.Fatal("equal labels have different encodings")
	}
	if first.Hash() != hash || HashPairs(pairs) != hash {
		t.Fatal("dictionary changed the series hash")
	}
	var key []byte
	for _, p := range pairs {
		key = AppendKnownPair(key, Intern(p[0]), p[1])
	}
	if string(first) != string(key) {
		t.Fatal("wire lookup key differs from stored labels")
	}
	var want []byte
	for _, p := range pairs {
		want = PutVarint(want, uint64(Intern(p[0])))
		want = PutVarint(want, uint64(len(p[1])))
		want = append(want, p[1]...)
	}
	if !slices.Equal(canonical, want) {
		t.Fatal("snapshot encoding depends on dictionary IDs")
	}
	if !first.EqPairs(pairs) || Compare(first, second) != 0 || first.Len() != 3 {
		t.Fatal("dictionary changed label semantics")
	}
}

func TestSharedValuesAdmissionStopsAtLimit(t *testing.T) {
	name := Intern("dictionary_limit_test")
	var first Labels
	for i := range maxValuesPerName + 2 {
		value := fmt.Sprintf("dictionary-limit-value-%05d", i)
		stored := FromSorted([][2]string{{"dictionary_limit_test", value}})
		if i == 0 {
			first = stored
		}
		if stored.Get("dictionary_limit_test") != value {
			t.Fatal("value changed")
		}
		if stored != FromSorted([][2]string{{"dictionary_limit_test", value}}) {
			t.Fatal("admission changed canonical encoding")
		}
	}
	table := valueTables.Load().tables[name]
	if len(table.values) != maxValuesPerName {
		t.Fatalf("dictionary has %d values", len(table.values))
	}
	if first.Get("dictionary_limit_test") != "dictionary-limit-value-00000" {
		t.Fatal("old ID changed")
	}
}

func TestSharedValuesConcurrentReaders(t *testing.T) {
	var wg sync.WaitGroup
	failures := make(chan string, 8)
	for worker := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 1000 {
				value := fmt.Sprintf("shared-value-%d", (worker+i)%200)
				stored := FromSorted([][2]string{{"dictionary_concurrent_test", value}})
				if stored.Get("dictionary_concurrent_test") != value || stored.Hash() != HashPairs([][2]string{{"dictionary_concurrent_test", value}}) {
					failures <- value
					return
				}
			}
		}()
	}
	wg.Wait()
	close(failures)
	for value := range failures {
		t.Errorf("changed value %s", value)
	}
}
