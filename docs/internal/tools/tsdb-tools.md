# TSDB Tools

Grafana Mimir has multiple tools useful for inspecting or debugging TSDB blocks.

## tsdb-series

`tsdb-series` lists all series found in the TSDB index of the block. It can optionally only list series matching a PromQL selector (`-select` option), and also show chunk metadata for each series (`-show-chunks` option).

Example with `-select`:

```
tsdb-series -select 'up{instance="compactor:8006"}' ./01FTT67BBYH23T8870BBF77YZX
level=error msg="using matchers" matcher="instance=\"compactor:8006\"" matcher="__name__=\"up\""
series {__name__="up", cluster="docker-compose", instance="compactor:8006", job="tsdb-blocks-storage-s3/compactor", namespace="tsdb-blocks-storage-s3", scraped_by="grafana-agent"}
series {__name__="up", cluster="docker-compose", instance="compactor:8006", job="tsdb-blocks-storage-s3/compactor", namespace="tsdb-blocks-storage-s3", scraped_by="prometheus"}
```

Example with the same `-select` and also `-show-chunks`:

```
tsdb-series -select 'up{instance="compactor:8006"}' -show-chunks ./01FTT67BBYH23T8870BBF77YZX
level=error msg="using matchers" matcher="instance=\"compactor:8006\"" matcher="__name__=\"up\""
series {__name__="up", cluster="docker-compose", instance="compactor:8006", job="tsdb-blocks-storage-s3/compactor", namespace="tsdb-blocks-storage-s3", scraped_by="grafana-agent"}
chunk 29163268 min time: 1640166760891 max time: 1640167195891
chunk 29163418 min time: 1640167200892 max time: 1640168095892
chunk 29163551 min time: 1640168100892 max time: 1640168725891
chunk 29163735 min time: 1640168730892 max time: 1640168845891
chunk 29163785 min time: 1640168850891 max time: 1640169465891
chunk 29163991 min time: 1640169470891 max time: 1640170085891
chunk 29164197 min time: 1640170090892 max time: 1640170705891
chunk 29164379 min time: 1640170710891 max time: 1640171320891
chunk 29164581 min time: 1640171325891 max time: 1640171935891
chunk 29164777 min time: 1640171940891 max time: 1640171965892
series {__name__="up", cluster="docker-compose", instance="compactor:8006", job="tsdb-blocks-storage-s3/compactor", namespace="tsdb-blocks-storage-s3", scraped_by="prometheus"}
chunk 29164808 min time: 1640166759677 max time: 1640166764677
chunk 29164833 min time: 1640166769677 max time: 1640167199682
chunk 29164981 min time: 1640167204683 max time: 1640167224681
chunk 29165012 min time: 1640167254678 max time: 1640168044653
chunk 29165246 min time: 1640168049652 max time: 1640168684662
chunk 29165439 min time: 1640168689662 max time: 1640169504679
chunk 29165703 min time: 1640169509679 max time: 1640170119676
chunk 29165892 min time: 1640170124676 max time: 1640170734658
chunk 29166073 min time: 1640170739658 max time: 1640171349619
chunk 29166296 min time: 1640171354618 max time: 1640171959602
chunk 29166492 min time: 1640171964600 max time: 1640171964600
```

## tsdb-print-chunk

`tsdb-print-chunk` requires path to block and one or more chunk references, and it prints all samples (values and their start and sample timestamps) from given chunk.

Example:

```
$ tsdb-print-chunk ./01FTT67BBYH23T8870BBF77YZX 8 1574
Chunk ref: 8 samples: 13 bytes: 22
1	ST: 0 (1970-01-01T00:00:00Z)	T: 1640167456784 (2021-12-22T10:04:16.784Z)
1	ST: 0 (1970-01-01T00:00:00Z)	T: 1640167516784 (2021-12-22T10:05:16.784Z)
1	ST: 0 (1970-01-01T00:00:00Z)	T: 1640167576784 (2021-12-22T10:06:16.784Z)
1	ST: 0 (1970-01-01T00:00:00Z)	T: 1640167636784 (2021-12-22T10:07:16.784Z)
1	ST: 0 (1970-01-01T00:00:00Z)	T: 1640167696784 (2021-12-22T10:08:16.784Z)
1	ST: 0 (1970-01-01T00:00:00Z)	T: 1640167756784 (2021-12-22T10:09:16.784Z)
1	ST: 0 (1970-01-01T00:00:00Z)	T: 1640167816784 (2021-12-22T10:10:16.784Z)
1	ST: 0 (1970-01-01T00:00:00Z)	T: 1640167876784 (2021-12-22T10:11:16.784Z)
1	ST: 0 (1970-01-01T00:00:00Z)	T: 1640167936784 (2021-12-22T10:12:16.784Z)
1	ST: 0 (1970-01-01T00:00:00Z)	T: 1640167996784 (2021-12-22T10:13:16.784Z)
1	ST: 0 (1970-01-01T00:00:00Z)	T: 1640168056784 (2021-12-22T10:14:16.784Z)
1	ST: 0 (1970-01-01T00:00:00Z)	T: 1640168116784 (2021-12-22T10:15:16.784Z)
1	ST: 0 (1970-01-01T00:00:00Z)	T: 1640168176784 (2021-12-22T10:16:16.784Z)
Chunk ref: 1574 samples: 7 bytes: 22
0	ST: 0 (1970-01-01T00:00:00Z)	T: 1640171937334 (2021-12-22T11:18:57.334Z)
0	ST: 0 (1970-01-01T00:00:00Z)	T: 1640171942331 (2021-12-22T11:19:02.331Z)
0	ST: 0 (1970-01-01T00:00:00Z)	T: 1640171947331 (2021-12-22T11:19:07.331Z)
0	ST: 0 (1970-01-01T00:00:00Z)	T: 1640171952331 (2021-12-22T11:19:12.331Z)
0	ST: 0 (1970-01-01T00:00:00Z)	T: 1640171957331 (2021-12-22T11:19:17.331Z)
0	ST: 0 (1970-01-01T00:00:00Z)	T: 1640171962331 (2021-12-22T11:19:22.331Z)
0	ST: 0 (1970-01-01T00:00:00Z)	T: 1640171967331 (2021-12-22T11:19:27.331Z)
```

## tsdb-chunks

`tsdb-chunks` parses chunks file (eg. chunks/000001) and prints its content: information about individual chunks, and their samples (if `-samples` is used).

Example:

```
$ tsdb-chunks 01FQKXVKF3QG5WQXSY726KKSBP/chunks/000001 | head
01FQKXVKF3QG5WQXSY726KKSBP/chunks/000001
Chunk #0: position: 8 length: 108 encoding: XOR, crc32: 833aa732, samples: 88
Chunk #1: position: 122 length: 17 encoding: XOR, crc32: 40b9c560, samples: 1
Chunk #2: position: 145 length: 137 encoding: XOR, crc32: 33d3e4fc, samples: 87
Chunk #3: position: 289 length: 66 encoding: XOR, crc32: 15c9a62c, samples: 88
Chunk #4: position: 361 length: 149 encoding: XOR, crc32: d8e556c4, samples: 89
Chunk #5: position: 517 length: 113 encoding: XOR, crc32: 42309707, samples: 87
Chunk #6: position: 636 length: 17 encoding: XOR, crc32: f8406014, samples: 1
Chunk #7: position: 659 length: 126 encoding: XOR, crc32: d9631436, samples: 87
Chunk #8: position: 791 length: 108 encoding: XOR, crc32: 204d98a0, samples: 88
```

Example with samples:

```
tsdb-chunks -samples 01FTT67BBYH23T8870BBF77YZX/chunks/000001 | head -n 20
01FTT67BBYH23T8870BBF77YZX/chunks/000001
Chunk #0: position: 8 length: 22 encoding: XOR, crc32: fe445c39, samples: 13
Chunk #0, sample #0: st: 0 (1970-01-01T00:00:00Z), ts: 1640167456784 (2021-12-22T10:04:16.784Z), val: 1
Chunk #0, sample #1: st: 0 (1970-01-01T00:00:00Z), ts: 1640167516784 (2021-12-22T10:05:16.784Z), val: 1
Chunk #0, sample #2: st: 0 (1970-01-01T00:00:00Z), ts: 1640167576784 (2021-12-22T10:06:16.784Z), val: 1
Chunk #0, sample #3: st: 0 (1970-01-01T00:00:00Z), ts: 1640167636784 (2021-12-22T10:07:16.784Z), val: 1
Chunk #0, sample #4: st: 0 (1970-01-01T00:00:00Z), ts: 1640167696784 (2021-12-22T10:08:16.784Z), val: 1
Chunk #0, sample #5: st: 0 (1970-01-01T00:00:00Z), ts: 1640167756784 (2021-12-22T10:09:16.784Z), val: 1
Chunk #0, sample #6: st: 0 (1970-01-01T00:00:00Z), ts: 1640167816784 (2021-12-22T10:10:16.784Z), val: 1
Chunk #0, sample #7: st: 0 (1970-01-01T00:00:00Z), ts: 1640167876784 (2021-12-22T10:11:16.784Z), val: 1
Chunk #0, sample #8: st: 0 (1970-01-01T00:00:00Z), ts: 1640167936784 (2021-12-22T10:12:16.784Z), val: 1
Chunk #0, sample #9: st: 0 (1970-01-01T00:00:00Z), ts: 1640167996784 (2021-12-22T10:13:16.784Z), val: 1
Chunk #0, sample #10: st: 0 (1970-01-01T00:00:00Z), ts: 1640168056784 (2021-12-22T10:14:16.784Z), val: 1
Chunk #0, sample #11: st: 0 (1970-01-01T00:00:00Z), ts: 1640168116784 (2021-12-22T10:15:16.784Z), val: 1
Chunk #0, sample #12: st: 0 (1970-01-01T00:00:00Z), ts: 1640168176784 (2021-12-22T10:16:16.784Z), val: 1
Chunk #0: minTS=1640167456784 (2021-12-22T10:04:16.784Z), maxTS=1640168176784 (2021-12-22T10:16:16.784Z)
Chunk #1: position: 36 length: 44 encoding: XOR, crc32: 1b2e22dd, samples: 56
Chunk #1, sample #0: st: 0 (1970-01-01T00:00:00Z), ts: 1640168356784 (2021-12-22T10:19:16.784Z), val: 1
Chunk #1, sample #1: st: 0 (1970-01-01T00:00:00Z), ts: 1640168416784 (2021-12-22T10:20:16.784Z), val: 1
Chunk #1, sample #2: st: 0 (1970-01-01T00:00:00Z), ts: 1640168476784 (2021-12-22T10:21:16.784Z), val: 1
```

## tsdb-index-health

`tsdb-index-health` inspects TSDB index of a block and generates summary report in JSON form about the health of the index. This is the same index health-check that is used by Grafana Mimir compactor before it compacts the block.

```
$ tsdb-index-health ./01FTT67BBYH23T8870BBF77YZX
{
    "TotalSeries": 19277,
    "OutOfOrderSeries": 0,
    "OutOfOrderChunks": 0,
    "DuplicatedChunks": 0,
    "OutsideChunks": 0,
    "CompleteOutsideChunks": 0,
    "Issue347OutsideChunks": 0,
    "OutOfOrderLabels": 0,
    "SeriesMinLifeDuration": "5s",
    "SeriesAvgLifeDuration": "1h12m11s925ms",
    "SeriesMaxLifeDuration": "1h26m40s1ms",
    "SeriesMinLifeDurationWithoutSingleSampleSeries": "5s",
    "SeriesAvgLifeDurationWithoutSingleSampleSeries": "1h12m11s925ms",
    "SeriesMaxLifeDurationWithoutSingleSampleSeries": "1h26m40s1ms",
    "SeriesMinChunks": 1,
    "SeriesAvgChunks": 7,
    "SeriesMaxChunks": 11,
    "TotalChunks": 152276,
    "ChunkMinDuration": "0s",
    "ChunkAvgDuration": "9m8s389ms",
    "ChunkMaxDuration": "1h19m25s",
    "ChunkMinSize": 23,
    "ChunkAvgSize": 201,
    "ChunkMaxSize": 4651,
    "SingleSampleSeries": 0,
    "SingleSampleChunks": 3462,
    "LabelNamesCount": 56,
    "MetricLabelValuesCount": 541
}
```

## tsdb-compact

`tsdb-compact` compacts specified blocks together into one or more output blocks.
It doesn't do any planning, and simply merges all specified blocks.
If `-shard-count` option is used, multiple output blocks are produced, using the same sharding algorithm as split-and-merge compactor.

Example:

```
$ tsdb-compact -output-dir ./out -shard-count=4 01FPCEFXKRREFKH3MHQFXA9S7G 01FQGS1WN6KVX3ZM39SRE88DBS 01FQKXVKF3QG5WQXSY726KKSBP
level=info msg="compact blocks" count=3 mint=1638874644135 maxt=1640167200000 ulid=01FV22WZ3F4HTE2JEMS5PJX8RC sources="[01FPCEFXKRREFKH3MHQFXA9S7G 01FQGS1WN6KVX3ZM39SRE88DBS 01FQKXVKF3QG5WQXSY726KKSBP]" duration=2.184381417s shard=1_of_4
level=info msg="compact blocks" count=3 mint=1638874644135 maxt=1640167200000 ulid=01FV22WZ3FMCS7WNV49BZ20GQ9 sources="[01FPCEFXKRREFKH3MHQFXA9S7G 01FQGS1WN6KVX3ZM39SRE88DBS 01FQKXVKF3QG5WQXSY726KKSBP]" duration=2.184538792s shard=2_of_4
level=info msg="compact blocks" count=3 mint=1638874644135 maxt=1640167200000 ulid=01FV22WZ3FGRPHAQD9ZNW4N57P sources="[01FPCEFXKRREFKH3MHQFXA9S7G 01FQGS1WN6KVX3ZM39SRE88DBS 01FQKXVKF3QG5WQXSY726KKSBP]" duration=2.184559125s shard=3_of_4
level=info msg="compact blocks" count=3 mint=1638874644135 maxt=1640167200000 ulid=01FV22WZ3FRXGZA8MFPPFB5KXH sources="[01FPCEFXKRREFKH3MHQFXA9S7G 01FQGS1WN6KVX3ZM39SRE88DBS 01FQKXVKF3QG5WQXSY726KKSBP]" duration=2.184576709s shard=4_of_4
```

## tsdb-index-toc

`tsdb-index-toc` prints sizes of individual sections of TSDB Index, using TOC section from the Index. See [TSDB Index Format](https://github.com/prometheus/prometheus/blob/main/tsdb/docs/format/index.md) for more details.

```
$ tsdb-index-toc 01FPCEFXKRREFKH3MHQFXA9S7G/index
Symbols table size:    25721
Series size:           1750593
Label indices:         4177
Postings:              292168
Label offset table:    661
Postings offset table: 35630
```

## tsdb-symbols

`tsdb-symbols` analyses symbols stored in TSDB index, and shows the summary of this analysis. It can also optionally do the series sharding from split-and-merge compactor and compute expected number of symbols per shard.

```
$ tsdb-symbols -shard-count=4 01FTT67BBYH23T8870BBF77YZX
01FTT67BBYH23T8870BBF77YZX: mint=1640166758768 (2021-12-22T09:52:38Z), maxt=1640174400000 (2021-12-22T12:00:00Z), duration: 2h7m21.232s
01FTT67BBYH23T8870BBF77YZX: {__org_id__="anonymous"}
01FTT67BBYH23T8870BBF77YZX: index: symbol table size: 28817 bytes, symbols: 965
01FTT67BBYH23T8870BBF77YZX: symbols iteration: total length of symbols: 27848 bytes, symbols: 965
01FTT67BBYH23T8870BBF77YZX: index structure overhead: 969 bytes
01FTT67BBYH23T8870BBF77YZX: found 953 unique symbols from series in the block

Found 953 unique symbols from series across ALL blocks, with total length 27622 bytes
Shard 0: Found 808 unique symbols from series in the shard (84.78 %), length of symbols in the shard: 22735 bytes (82.31 %)
Shard 1: Found 807 unique symbols from series in the shard (84.68 %), length of symbols in the shard: 22865 bytes (82.78 %)
Shard 2: Found 797 unique symbols from series in the shard (83.63 %), length of symbols in the shard: 22172 bytes (80.27 %)
Shard 3: Found 814 unique symbols from series in the shard (85.41 %), length of symbols in the shard: 23019 bytes (83.34 %)

Analysis complete in 82.33225ms
```

## tsdb-labels

`tsdb-labels` prints the label names, number of label values, and label values for each label in a TSDB block.

Example:

```bash
tsdb-labels ./01HA705M6Y8VNNR7W4C8116P2T
```

```console
AlertRules 2 [false true]
ClusterName 1 [etcd]
Domain 2 [API Controller]
Event 4 [CREATE LIST READ UPDATE]
access_mode 1 [ReadWriteOnce]
acl_operation 1 [OBJECT_ACCESS_REQUIRED_OBJECT_ACL]
```
