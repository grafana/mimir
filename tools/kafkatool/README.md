# kafkatool

A command-line tool to inspect and manage the Kafka cluster backing Mimir's [ingest storage](https://grafana.com/docs/mimir/latest/references/architecture/components/ingester/).

Build it with `go build .` in this directory.

Every command needs `--kafka-address` (e.g. `localhost:9092`) — except `dump print`, `dump analyse`, and `dump find-duplicates`, which only read a local dump file and therefore don't contact Kafka. For authenticated clusters, also pass `--kafka-sasl-username` and `--kafka-sasl-password`. Run any command with `--help` to see its flags.

## Examples

List topics and a topic's partition leaders:

```bash
./kafkatool --kafka-address localhost:9092 list-topics
./kafkatool --kafka-address localhost:9092 brokers list-leaders-by-partition --topic ingest
```

Inspect and edit consumer group offsets:

```bash
./kafkatool --kafka-address localhost:9092 consumer-group list-offsets --group my-group
./kafkatool --kafka-address localhost:9092 consumer-group commit-offset --group my-group --topic ingest --partition 0 --offset 1234
```

Dump a partition's raw records to a file. Useful for offline analysis:

```bash
./kafkatool --kafka-address localhost:9092 dump export --file ./dump.json --topic ingest --partition 0 --offset 0 --export-max-records 100000
```

Analyze a dump file (these subcommands never contact the broker, so `--kafka-address` is not required):

```bash
./kafkatool dump --file ./dump.json analyse
./kafkatool dump --file ./dump.json print --format json
./kafkatool dump --file ./dump.json find-duplicates
```
