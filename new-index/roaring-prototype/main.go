package main

import "fmt"

func main() {
	idx := NewIndex()

	// --- Ingest batch 1: flush into segment 1 ---
	fmt.Println("=== Ingesting batch 1 (will become segment 1) ===")
	batch1 := []struct {
		ref uint32
		ls  []Label
	}{
		{1, []Label{{"__name__", "http_requests_total"}, {"job", "api"}, {"instance", "host1"}}},
		{2, []Label{{"__name__", "http_requests_total"}, {"job", "api"}, {"instance", "host2"}}},
		{3, []Label{{"__name__", "http_requests_total"}, {"job", "web"}, {"instance", "host1"}}},
		{4, []Label{{"__name__", "http_requests_total"}, {"job", "web"}, {"instance", "host3"}}},
		{5, []Label{{"__name__", "cpu_usage"}, {"job", "api"}, {"instance", "host1"}}},
		{6, []Label{{"__name__", "cpu_usage"}, {"job", "web"}, {"instance", "host2"}}},
		{7, []Label{{"__name__", "memory_bytes"}, {"job", "db"}, {"instance", "host4"}}},
	}
	for _, s := range batch1 {
		idx.Add(s.ref, s.ls)
		fmt.Printf("  added ref=%d %s\n", s.ref, FormatLabels(s.ls))
	}

	idx.Flush()
	fmt.Println("  -> flushed to segment 1")

	// --- Ingest batch 2: stays in write buffer ---
	fmt.Println("\n=== Ingesting batch 2 (in write buffer) ===")
	batch2 := []struct {
		ref uint32
		ls  []Label
	}{
		{8, []Label{{"__name__", "http_requests_total"}, {"job", "api"}, {"instance", "host5"}}},
		{9, []Label{{"__name__", "http_requests_total"}, {"job", "batch"}, {"instance", "host6"}}},
		{10, []Label{{"__name__", "cpu_usage"}, {"job", "batch"}, {"instance", "host6"}}},
		{11, []Label{{"__name__", "disk_io"}, {"job", "db"}, {"instance", "host4"}}},
		{12, []Label{{"__name__", "memory_bytes"}, {"job", "api"}, {"instance", "host5"}}},
	}
	for _, s := range batch2 {
		idx.Add(s.ref, s.ls)
		fmt.Printf("  added ref=%d %s\n", s.ref, FormatLabels(s.ls))
	}

	// --- Query 1: exact match ---
	fmt.Println("\n=== Query: job=\"api\" ===")
	printResults(idx, idx.Postings("job", "api"))

	// --- Query 2: AND of two positive matchers ---
	fmt.Println("\n=== Query: __name__=\"http_requests_total\", job=\"api\" ===")
	printResults(idx, idx.PostingsForMatchers(
		Equal("__name__", "http_requests_total"),
		Equal("job", "api"),
	))

	// --- Query 3: positive AND with negative ANDNOT ---
	fmt.Println("\n=== Query: __name__=\"http_requests_total\", job!=\"api\" ===")
	printResults(idx, idx.PostingsForMatchers(
		Equal("__name__", "http_requests_total"),
		NotEqual("job", "api"),
	))

	// --- Drop oldest segment and re-query ---
	fmt.Println("\n=== Dropping oldest segment (segment 1) ===")
	idx.DropOldestSegment()

	fmt.Println("\n=== Re-query: job=\"api\" (after drop) ===")
	printResults(idx, idx.Postings("job", "api"))

	fmt.Println("\n=== Re-query: __name__=\"http_requests_total\" (after drop) ===")
	printResults(idx, idx.Postings("__name__", "http_requests_total"))
}

func printResults(idx *Index, bm interface{ ToArray() []uint32 }) {
	refs := bm.ToArray()
	if len(refs) == 0 {
		fmt.Println("  (no results)")
		return
	}
	fmt.Printf("  matched %d series:\n", len(refs))
	for _, ref := range refs {
		ls, ok := idx.Series(ref)
		if !ok {
			fmt.Printf("    ref=%d  (labels not found)\n", ref)
			continue
		}
		fmt.Printf("    ref=%-3d %s\n", ref, FormatLabels(ls))
	}
}
