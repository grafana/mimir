# Ingest storage internals notes

## franz-go and ProducerBatchMaxBytes

How it works:

- The client allows to configure the max size of a single batch of records, by setting `kgo.ProducerBatchMaxBytes()`.
- Batches are per topic-partition. Records for a different partition, go to a different batch.
- When a Produce request is created ([code](https://github.com/twmb/franz-go/blob/37eecbb8927fce0aede1d6af39849839f7b0b3cf/pkg/kgo/sink.go#L111-L122)), the client picks at most 1 batch for each topic-partition, up to the max size (bytes) of the Produce request (configurable with `kgo.BrokerMaxWriteBytes()`, defaults to 100MB).

This means that a low value of `kgo.ProducerBatchMaxBytes()` may put a throughput limit to the Kafka client, because Produce requests may be sent even if they're not full (with regards the max request size) and there are still buffered records (in subsequent batches for the same topic-partition). However, there are other conditions that needs to be true to see a throughput limit due to small max batch size in practice:

- `kgo.ProducerBatchMaxBytes()` is low
- The number of partitions records are sharded to is low (batches are per topic-partition)
- `kgo.MaxProduceRequestsInflightPerBroker()` is low and/or the Produce latency is high (e.g. Warpstream)
