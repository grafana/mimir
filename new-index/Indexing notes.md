## How bad is the inverted index? Dec 23, 2025

### How do indexes work today in ingester?

Ingester blocks are bifurcated to two types:

* The newest block is the head block.  
  * The head block’s index (`MemPostings`) is a “total” inverted index. Literally a map\[label\_name\]\[label\_value\] \=\> \[\]refs. So, O(C=cardinality) entries.  
  * The metric `ingester_memory_series` is driven by the number of entries in this map.  
  * Data notes:  
    * The *values* are written to a memory-mapped area with ChunkDiskMapper.  
    * The \[\]refs in Mempostings indirectly point at ChunkDiskMapperRefs but go through the stripeSeries structure. MemPostings (series ref) → stripeSeries → memSeries → memSeries.mmappedChunks\[\].ref.  
* When the head block is compacted it is turned into a regular block.  
  * The regular block on disk gets a Postings Offset Table which is a total index. O(C), but in a file.  
  * But when we read a TSDB block into memory, we create another in-memory index over the postings offset table, but it’s a sparse one: It is a `map[label_name] => []postingOffset`. ([src](https://github.com/grafana/mimir/blob/f5d064968c732ac49f72ac2551b4d98f596d21ed/vendor/github.com/prometheus/prometheus/tsdb/index/index.go#L972-L974).) This scheme skips many of the label values: for each label name, it will include the first, last and [*every 32nd value*](https://github.com/grafana/mimir/blob/f5d064968c732ac49f72ac2551b4d98f596d21ed/vendor/github.com/prometheus/prometheus/tsdb/index/index.go#L1202) in the in-memory structure. (So, the in-memory size is approximately 1/32 of MemPostings.) At lookup time, searchers fall back to memory-mapped file scans of the postings offset table to locate things between the 1-in-32 values.

▶️ So:

* the head (MemPostings) index seems like a bonafide bottleneck in scaling to higher and higher cardinality without using O(C) memory.  
* Regular blocks use 1/32x memory, which I’m guessing is not a problem, and will become less of a problem as `%proj-X-hour-block-something` makes ingesters hold fewer blocks.  
  * ❓How much memory do we use today on these sparse block indexes?

### How does the large block index affect store-gateways?

* When store gateways encounter a block they haven’t seen before, they create an index-header file that is stored on the local filesystem.  
* The index-header contains two things ([ref](https://github.com/grafana/mimir/blob/5a3ab2a3bd204f998edcb349f3e23cba653dee56/docs/sources/mimir/references/architecture/binary-index-header.md)):  
  * An exact copy of the block’s symbol table.  
  * An exact copy of the block’s Postings Offset Table  
* When the store gateway reads the index-header, it (like ingester) builds a sparse in-memory index from the Postings Offset Table:  
  * Sample rate is governed by [postingOffsetsInMemSampling](https://github.com/grafana/mimir/blob/cd67c8dac7504625591f8fe4dc00d305972b7bfe/pkg/storage/indexheader/index/postings.go#L173-L174). (also [32 by default](https://github.com/grafana/mimir/blob/a0eaad506401bcdb1dda176c9fc287b820b6d59d/pkg/storage/tsdb/config.go#L55-L61) and we don’t override it.)  
* There’s also the sparse-index-header which is the above 1-in-32 sampled index but precomputed and written to object storage.  
* In recent times, Compactor itself produces the sparse-index-header. Which should keep store-gateway from *ever* needing to create one from the Postings Offset Table by itself.

### Compactors? Do they need to load O(cardinality) into memory at once?

Not really:

* When they compact {source files} \-\> {dest files}, they do not use the source postings offset tables. Instead, they compute a *new* postings offset table from the series data.  
* This process involves temp files and O(series) conversion work.  
* There *is* an intermediary O(C’) map in this process, but C’ is the cardinality of a “batch” – batches are kept small to keep this memory from blowing up. (See [writePostingsToTmpFiles](https://github.com/grafana/mimir/blob/f5d064968c732ac49f72ac2551b4d98f596d21ed/vendor/github.com/prometheus/prometheus/tsdb/index/index.go#L757-L782).)

## Summary

* My read is the inverted index (MemPostings) is the index to focus on in supporting Big Cardinality. If MemPostings wasn’t O(C), we could do things like:  
  * support cardinality explosions in the ingester without scaling up.  
  * \<type more here\>  
* Questions:  
  * What other index structures have been experimented with in MemPostings?  
  * Why do we need a full-fidelity O(C) map\[\] in the head block but a 1/32 sampled index in a regular block?

## More research Jan 29, 2026

I want to find:

* What keeps a single ingester from running, say, 20M series?  
  * I do believe that memory series is a strong dominator in the memory used by an ingester. [Explore](https://ops.grafana-ops.net/explore?schemaVersion=1&panes=%7B%22x0q%22:%7B%22datasource%22:%22000000134%22,%22queries%22:%5B%7B%22refId%22:%22A%22,%22expr%22:%22cortex_ingester_memory_series%7Bnamespace%3D%5C%22mimir-ops-03%5C%22,%20pod%3D%5C%22ingester-zone-a-10%5C%22%7D%20%2A%202000%22,%22range%22:true,%22instant%22:true,%22datasource%22:%7B%22type%22:%22prometheus%22,%22uid%22:%22000000134%22%7D,%22editorMode%22:%22code%22,%22legendFormat%22:%22__auto%22%7D,%7B%22refId%22:%22B%22,%22expr%22:%22max%28container_memory_rss%7Bnamespace%3D%5C%22mimir-ops-03%5C%22,%20pod%3D%5C%22ingester-zone-a-10%5C%22%7D%29%22,%22range%22:true,%22instant%22:true,%22datasource%22:%7B%22type%22:%22prometheus%22,%22uid%22:%22000000134%22%7D,%22editorMode%22:%22code%22,%22legendFormat%22:%22__auto%22%7D%5D,%22range%22:%7B%22from%22:%221769134890230%22,%22to%22:%221769696176277%22%7D,%22compact%22:false%7D%7D&orgId=1)\- but it’s kind of hard to get a memory profile to show you this.  
* Annoying side quest question: If ingester no longer uploads compacted blocks, then, can we do a cheaper form of head compaction that doesn’t try to produce “real” TSDB blocks? i.e., why not just “roll” the current head to an older one that is still formatted as a head block?  
* Suppose \&proj-ingester-Xh comes to fruition. Then,  
  * Rather than keeping 12 hours of blocks around, it will be much less.  
  * But we will still have a head block.

Shapes of potential head index improvements:

* Make MemPostings smaller by dict-compressing the keys. (So it would be map\[int\]\[int\] rather than map\[str\]\[str\].) FSST recommended by [Franco Posa](mailto:franco.posa@grafana.com) or otherwise.  
* Keep head index a “total” index but spill parts of it to disk. Like a disk-backed B+tree.  
  * Or similarly, stream the “total” index to disk but keep a sparser one in memory that falls back to the on-disk.

## Right, Feb 2, 2026

What kinds of things do I want to prove before starting on prototypes?

* Project a new reality.  
* What would be the benefit if we just dict-encoded the MemPostings.m keys?  
  * How many keys are in there?  
    * There isn’t a metric for it. But it is exposed in the Prometheus “tsdb status” [API endpoint…](https://github.com/grafana/mimir/blob/b282f9baf60d051de242dcf14a3da660192b1465/vendor/github.com/prometheus/prometheus/web/api/v1/api.go#L1899-L1905)  
    * But ingesters don’t expose the Prometheus API.

## Feb 3, 2026

* Project a new reality.  
  * Eventually ingesters will look like:  
    * They’ll hold fewer blocks.  
    * They’ll spend less CPU time doing query evaluations.  
  * We’ll run ingesters with less memory.  
    * This will save money  
  * When someone produces a cardinality explosion, an ingester will be less affected.  
    * Put another way: ingesters will have a higher series limit. They’ll be able to handle more in-mem series with less memory  
  * Sublinear index size would be the game changer, though. A cardinality explosion would increase disk space and not so much memory.  
    * 👊 We’d autoscale on disk space and query performance rather than active series.