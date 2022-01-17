# Chunk Migrator

Chunk Migrator helps with migrating chunks across Cortex clusters while also taking care of setting right index in the destination cluster as per the specified schema.
It also supports mapping chunks to a new user in destination cluster.

Chunk Migrator can be invoked using `chunktool` with `chunk migrate` command.

## Configuration
Migrator comprises of following components, having their own set of configuration:
1. Reader - Runs specified number of workers to read chunks concurrently from the specified storage.
2. Writer - Runs specified number of workers to write chunks and index concurrently to the specified storage.
3. Mapper - Used by writer while writing chunks to map them to a new User ID i.e add chunks and index with a new ID to the destination Cortex cluster.
4. Planner - Used by reader to selectively read the chunks. Selection criteria can be User IDs, Table Names or Shards.

*Note: Configuration for Planner needs to be set using CLI flags while rest of the components are configured using a single YAML config file.*
### Shards in Planner:
```
// When doing migrations each database is discreetly partitioned into 240 shards
// based on aspects of the databases underlying implementation. 240 was chosen due
// to the bigtable implementation sharding on the first two character of the hex encoded
// metric fingerprint. Cassandra is encoded into 240 discreet shards using the Murmur3
// partition tokens.
//
// Shards are an integer between 1 and 240 that map onto 2 hex characters.
// For Example:
// 			Shard | Prefix
//			    1 | 10
//			    2 | 11
//			  ... | ...
//			   16 |
//			  240 | ff
//
// Technically there are 256 combinations of 2 hex character (16^2). However,
// fingerprints will not lead with a 0 character so 00->0f excluded, leading to
// 240
```


### Reader Config:
`storage_type`: Specifies type of the storage which has the chunks. It currently only supports `bigtable` or `gcs`.
`storage`: It is the same config that is used in Cortex for configuring Storage. See [storage_config](https://github.com/cortexproject/cortex/blob/master/docs/configuration/config-file-reference.md#storage_config) 
`num_workers`: Number of workers to perform read operation concurrently.

### Writer Config:
`storage`: It is the same config that is used in Cortex for configuring Storage. See [storage_config](https://github.com/cortexproject/cortex/blob/master/docs/configuration/config-file-reference.md#storage_config)
`schema`: It is the same config that is used in Cortex for configuring schemas.
`num_workers`: Number of workers to perform write operation concurrently.

### Mapper Config:
The map config file is a yaml file structured as:
```
users:
  user_original: user_mapped
  ...
  <user_id_src>: <user_id_dst>

```

### Planner Config:
`firstShard`: First shard in range of shards to be migrated (1-240).
`lastShard`: Last shard in range of shards to be migrated (1-240).
`users`: Comma separated list of user ids, if empty all users will be queried.
`tables`: Comma separated list of tables to migrate.

### Example Usage:

The following example shows how to do migration of chunks belonging to user `old-user-id` from an old cluster having chunks in GCS bucket to
another cluster with different GCS bucket. It maps chunks from `old-user-id` to `new-user-id` and also sets index in specified Bigtable instance with configured Schema.

```yaml
---config.yaml
reader:
  storage_type: gcs
  storage:
    gcs:
      bucket_name: old-gcs-cortex-cluster
  num_workers: 2

writer:
  storage:
    bigtable:
      project: bigtable-project
      instance: bigtable-instance
    gcs:
      bucket_name: new-gcs-cortex-cluster
  schema:
    configs:
    - from: 2019-01-01
      store: bigtable
      object_store: gcs
      schema: v10
      index:
        prefix: index_
        period: 168h
  num_workers: 2

mapper:
  users:
    "old-user-id": new-user-id
```

Command to run the migration:
>chunktool chunk migrate --config-file=config.yaml --plan.users=old-user-id

*Note1: User IDs in mapper are purely for mapping chunks to a new User ID. If mapping entry is missing, chunks would be written using same ID from original chunk.
For migrating chunks of selected users, use `--plan.users`*

*Note2: Since we haven't specified Shard config, it would default to migrating all the chunks of specified users.*



