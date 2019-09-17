# Cortex-Tool

This tool is designed to interact with the various user facing apis provided by cortex, as well as, interact with various backend storage components containing cortex data.

## Commands

### Chunk Delete

The delete command currently cleans all index entries pointing to chunks in the specified index. Only bigtable and the v10 schema are currently fully supported. This will not delete the entire index entry, only the corresponding chunk entries within the index row.

#### Example

```bash
usage: cortex-cli chunk delete --schema-file=SCHEMA-FILE [<flags>]

Deletes the specified chunks

Flags:
  --help                     Show context-sensitive help (also try --help-long and --help-man).
  --log.level="info"         set level of the logger
  --dryrun                   if enabled, no delete action will be taken
  --bigtable.project=BIGTABLE.PROJECT
                             bigtable project to use
  --bigtable.instance=BIGTABLE.INSTANCE
                             bigtable instance to use
  --chunk.gcs.bucketname=CHUNK.GCS.BUCKETNAME
                             name of the gcs bucket to use
  --schema-file=SCHEMA-FILE  path to file containing cortex schema config
  --filter.name=FILTER.NAME  option to filter metrics by metric name
  --filter.user=FILTER.USER  option to filter metrics by user
  --filter.from=FILTER.FROM  option to filter metrics by from a specific time point
  --filter.to=FILTER.TO      option to filter metrics by from a specific time point
```

### Rules

#### List

#### Get

#### Set

#### Load

## License
Licensed Apache 2.0, see [LICENSE](LICENSE).