`query-bucket-index` can be used to query a bucket index file (either a gzip-compressed JSON file, or an uncompressed JSON file) for a given query time range.

The output is provided in CSV format.

For example, running `go run . -bucket-index bucket-index.json.gz -start 2024-07-24T12:00:00Z -end 2024-07-24T13:00:00Z` will print the blocks that would be
queried for a query from 2024-07-24 12:00:00 UTC to 2024-07-24 13:00:00 UTC based on the gzip-compressed bucket index `bucket-index.json.gz`.

Sample output:

```
Block ID,Min time,Max time,Uploaded at,Shard ID,Source,Compaction level,Deletion marker present?,Deletion time
01J3JW2VVE4HYNAQ5RGRAK54N9,2024-07-24T12:00:00Z,2024-07-24T14:00:00Z,2024-07-24T17:23:12Z,3_of_4,compactor,4,true,2024-07-25T01:41:46Z
01J3JWBDSEJ4K9MABF569E2E3Z,2024-07-24T12:00:00Z,2024-07-24T14:00:00Z,2024-07-24T17:27:40Z,4_of_4,compactor,4,false,
01J3JWMA3FGGB5253479FBCKYB,2024-07-24T12:00:00Z,2024-07-24T14:00:00Z,2024-07-24T17:31:40Z,2_of_4,compactor,4,true,2024-07-25T01:36:18Z
01J3KP1M6VCNY8J56GBT80JBRT,2024-07-24T12:00:00Z,2024-07-25T00:00:00Z,2024-07-25T01:08:56Z,1_of_4,compactor,5,true,2024-07-25T03:18:33Z
01J3KQ28GNX4EN7AG22HV9ZNGB,2024-07-24T12:00:00Z,2024-07-25T00:00:00Z,2024-07-25T01:41:46Z,3_of_4,compactor,5,false,
01J3KQD3C45M9F6WZ6EN5EQ2Q2,2024-07-24T12:00:00Z,2024-07-25T00:00:00Z,2024-07-25T01:36:17Z,2_of_4,compactor,5,false,
01J3KXB2Z1Y5W01MRF3AHM1MN1,2024-07-24T12:00:00Z,2024-07-25T00:00:00Z,2024-07-25T03:18:32Z,1_of_4,compactor,6,true,2024-07-25T03:23:16Z
01J3KXHZEKA00XYXF6JP4BH3QM,2024-07-24T12:00:00Z,2024-07-25T00:00:00Z,2024-07-25T03:23:10Z,1_of_4,compactor,6,false,
```
