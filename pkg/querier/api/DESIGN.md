## Strong read consistency workflow

Strong read consistency is enabled setting the `X-Read-Consistency: strong` HTTP header. When a query request has strong
read consistency enabled, there are two main Mimir components enforcing it:

- Query-frontend
- Ingester

The query-frontend responsibility is to fetch from Kafka the last produced offsets for each in-use partition
(either `ACTIVE` or `INACTIVE`). The last produced offsets are [serialised](#strong-read-consistency-offsets-serialization-format)
and propagated to downstream Mimir components either via `X-Read-Consistency-Offsets` HTTP header or context.

The query-frontend fetches the last produced offsets of each in-use partition on a fixed interval (constant load
pattern). When a new query with strong read consistency is received by the query-frontend, the query-frontend
waits for the result of the **next** last produced offsets periodic request that will be issued. This is done to
guarantee that the fetched offsets are at a point in time which is after when the query was received by the query-frontend.

The offsets are propagated down to ingesters via context. When the ingester receives a read request with strong
read consistency enabled, the ingester looks up from the context the offset for their partition and waits until
that offset has been replayed before proceeding executing the request. The read request fails if the ingester can't
replay the desired offset with `-ingest-storage.kafka.wait-strong-read-consistency-timeout`; in this case the request
may be retried by the querier on other ingester owning the same partition.

Since the query-frontend fetches offsets only for in-use partitions (fetching for all existing partitions would be
too expensive on the Kafka backend), there may be short-time race conditions during which the query-frontend sees
a subset of the in-use partitions. For example, during a partitions scale up event, when the query hits the querier
there may be more partitions than when it was processed by the query-frontend. To overcome these race conditions,
the ingester also periodically fetchs the last produced offset for its partition. If the offset is missing from the
context, the ingester will fallback to the last produced offset fetched by itself. The ingester uses the same fetching
strategy of the query-frontend, which is waiting for the result of the next last produce offset periodic request that
will be issued.

## Strong read consistency offsets serialization format

We experimented with different formats to serialise the partition offsets. The comparison took in account:

- Length (in bytes) of the encoded string
- Encoding / decoding / lookup performance

The encoding formats experiments [code is here](https://gist.github.com/pracucci/c682cf45967a2473b14058630912f381).

### Benchmark scenario

We benchmarked it taking in account what we consider the worst case scenario:

- 1K partitions
- Each partition offset value is `math.MaxInt64`

### Length (in bytes) of the encoded string

| Encoding format                                                                                    | Bytes length |
| -------------------------------------------------------------------------------------------------- | ------------ |
| Comma-separated string: "partition_id:offset_id"                                                   | 23889        |
| Binary-encoded list of partition_id offset_id pairs, encoded with: Varint + base64                 | 15916        |
| Binary-encoded list of partition_id offset_id pairs, encoded with: Varint + snappy + base64        | 7980         |
| Binary-encoded list of offset_id only, encoded with: Varint + snappy + base64                      | 5364         |
| Binary-encoded list of partition_id offset_id pairs, encoded with: Int32 / int64 + base64          | 16000        |
| Binary-encoded list of partition_id offset_id pairs, encoded with: Int32 / int64 + snappy + base64 | 10688        |
| Binary-encoded list of offset_id only, encoded with: Int + snappy + base64                         | 5356         |

### Encoding performance

The encoding is the process to covert a `map[int32]int64` into the HTTP header value string.

| Encoding format                                                                                    | CPU         | Memory     | Allocations |
| -------------------------------------------------------------------------------------------------- | ----------- | ---------- | ----------- |
| Comma-separated string: "partition_id:offset_id"                                                   | 33543 ns/op | 27264 B/op | 1 allocs/op |
| Binary-encoded list of partition_id offset_id pairs, encoded with: Varint + base64                 | 31210 ns/op | 45232 B/op | 8 allocs/op |
| Binary-encoded list of partition_id offset_id pairs, encoded with: Varint + snappy + base64        | 37613 ns/op | 43184 B/op | 8 allocs/op |
| Binary-encoded list of offset_id only, encoded with: Varint + snappy + base64                      | 37453 ns/op | 39088 B/op | 8 allocs/op |
| Binary-encoded list of partition_id offset_id pairs, encoded with: Int32 / int64 + base64          | 16403 ns/op | 45232 B/op | 8 allocs/op |
| Binary-encoded list of partition_id offset_id pairs, encoded with: Int32 / int64 + snappy + base64 | 25639 ns/op | 59568 B/op | 9 allocs/op |
| Binary-encoded list of offset_id only, encoded with: Int + snappy + base64                         | 22443 ns/op | 34224 B/op | 8 allocs/op |

### Decoding performance

The decoding is the process to covert the HTTP header value string into a `map[int32]int64`.

| Encoding format                                                                                    | CPU         | Memory      | Allocations  |
| -------------------------------------------------------------------------------------------------- | ----------- | ----------- | ------------ |
| Comma-separated string: "partition_id:offset_id"                                                   | N/A         | N/A         | N/A          |
| Binary-encoded list of partition_id offset_id pairs, encoded with: Varint + base64                 | 64179 ns/op | 114722 B/op | 49 allocs/op |
| Binary-encoded list of partition_id offset_id pairs, encoded with: Varint + snappy + base64        | 58544 ns/op | 105252 B/op | 48 allocs/op |
| Binary-encoded list of offset_id only, encoded with: Varint + snappy + base64                      | 46693 ns/op | 90072 B/op  | 31 allocs/op |
| Binary-encoded list of partition_id offset_id pairs, encoded with: Int32 / int64 + base64          | 56468 ns/op | 114724 B/op | 49 allocs/op |
| Binary-encoded list of partition_id offset_id pairs, encoded with: Int32 / int64 + snappy + base64 | 54670 ns/op | 114724 B/op | 49 allocs/op |
| Binary-encoded list of offset_id only, encoded with: Int + snappy + base64                         | 40447 ns/op | 88027 B/op  | 31 allocs/op |

### Lookup performance

The lookup is the process to read 1 specific partition offset from the HTTP header value string.

| Encoding format                                                                                    | CPU         | Memory     | Allocations  |
| -------------------------------------------------------------------------------------------------- | ----------- | ---------- | ------------ |
| Comma-separated string: "partition_id:offset_id"                                                   | 4200 ns/op  | 3 B/op     | 1 allocs/op  |
| Binary-encoded list of partition_id offset_id pairs, encoded with: Varint + base64                 | 23399 ns/op | 48193 B/op | 13 allocs/op |
| Binary-encoded list of partition_id offset_id pairs, encoded with: Varint + snappy + base64        | 18063 ns/op | 38720 B/op | 12 allocs/op |
| Binary-encoded list of offset_id only, encoded with: Varint + snappy + base64                      | 13120 ns/op | 24384 B/op | 10 allocs/op |
| Binary-encoded list of partition_id offset_id pairs, encoded with: Int32 / int64 + base64          | 18974 ns/op | 48192 B/op | 13 allocs/op |
| Binary-encoded list of partition_id offset_id pairs, encoded with: Int32 / int64 + snappy + base64 | 17451 ns/op | 48193 B/op | 13 allocs/op |
| Binary-encoded list of offset_id only, encoded with: Int + snappy + base64                         | 9615 ns/op  | 22336 B/op | 10 allocs/op |

### Considerations

- The Mimir use case is encode once (in the query-frontend) and decode / lookup many (in each ingester)
- The comma separated string format is:
  - The longest bytes length (bad)
  - Among the least efficient to encode for CPU, but most efficient for memory (mixed)
  - The most efficient lookup (good)
  - The only encoding format that is easy to debug, because the HTTP header value is human readable (good)
- The "varbit + snappy + base64" format is probably the best alternative among the tested ones, if we accept non-human readable HTTP header value:
  - The shortest bytes length, excluding options without partition_id (good)
  - Encoding / decoding is on average with other formats
