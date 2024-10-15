# Ingest storage design

This document aims to summarise some design principles and technical decisions applied when building the ingest storage.

## Kafka offsets

### Last produced offset

The offset of the **next** record in a partition can be read from Kafka issuing `ListOffsets` request with `timestamp = -1`.
The special value `-1` means "latest" but in practice it's the "last produced offset + 1".

This has been verified both testing it in Apache Kafka 3.5 (Confluent Kafka 7.5) and Warpstream v524. In details:

- Partition is empty: `ListOffsets(timestamp = -1)` returns offset `0`
- Write 1st record: offset of the written record is `0`
- Partition contains 1 record: `ListOffsets(timestamp = -1)` returns offset `1`

For this reason, the offset of the last produced record in a partition is `ListOffsets(timestamp = -1) - 1`.

### Partition start offset

The partition start offset is the offset of the **first** record available for consumption in a partition, if the partition contains some record,
or the offset of the **next** produced offset if the partition is empty (either because no record has ever been produced yet,
or the Kafka retention kicked in and deleted all segments).
The partition start offset can be read from Kafka issuing `ListOffsets` request with `timestamp = -2`.

This has been verified testing it in Apache Kafka 3.6 (Confluent Kafka 7.6). In details, consider the following sequence of events and how `ListOffsets(timestamp = -2)` behaves:

- No record ever produced: `ListOffsets(timestamp = -2)` returns offset `0`
- Write 1st record: offset of the written record is `0`
  - Partition contains 1 record: `ListOffsets(timestamp = -2)` returns offset `0`
- Write 2nd record: offset of the written record is `1`
  - Partition contains 2 records: `ListOffsets(timestamp = -2)` returns offset `0`
- Kafka retention triggers and deletes the segment containing the 1st and 2nd record
  - Partition contains no records: `ListOffsets(timestamp = -2)` returns offset `2`
- Write 3rd record: offset of the written record is `2`
  - Partition contains 1 record: `ListOffsets(timestamp = -2)` returns offset `2`
