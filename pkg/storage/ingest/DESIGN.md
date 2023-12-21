# Ingest storage design

This document aims to summarise some design principles and technical decisions applied when building the ingest storage.

## Kafka offsets

### Last produced offset

The offset of the **next** record in a partition can be read from Kafka issuing `ListOffsets` request with `timestamp = -1`.
The special value `-1` means "latest" but in practice it's the "last produced offset + 1".

This has been verified both testing it in Confluent Kafka 7.5 and Warpstream v524. In details:

- Partition is empty: `ListOffsets(timestamp = -1)` returns offset `0`
- Write 1st record: offset of the written record is `0`
- Partition contains 1 record: `ListOffsets(timestamp = -1)` returns offset `1`

For this reason, the offset of the last produced record in a partition is `ListOffsets(timestamp = -1) - 1`.
