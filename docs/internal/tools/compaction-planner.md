# Compaction planner tool

You can use the compaction planner tool to troubleshoot the Grafana Mimir compactor.
The compaction planner tool loads a bucket index from the object storage (for example, Google Cloud Storage) and shows compaction jobs that would be generated by the split-and-merge compaction strategy.
Unlike the compactor, which scans the bucket for blocks, the tool informs itself from the bucket index.
This means that the jobs listed by the compaction planner tool might not reflect the current state of the bucket.
The compactor updates the bucket index every `-compactor.cleanup-interval`.

Configure the compaction planner tool with at least the following parameters:

- The tenant ID (via the `-user` flag)
- Which storage backend to use (via the `-backend` flag)
- Those required by the chosen storage backend (for example, the `gcs` backend requires the `-gcs.bucket-name` flag)

In addition, you might need to adjust shard count (via `-shard-count`) and split groups (via `-split-groups`)
to match the corresponding split-and-merge compactor parameters.
The compaction planner tool cannot automatically deduce these parameters.
You can modify the order of the job via the `-sorting` flag.

## Example

The output in the following example lists compaction jobs that the Grafana Mimir compactor would work on, provided that the compactor configuration is equivalent to the compaction planner tool.
The compactor would work on the jobs in the order they are listed.

```
$ ./compaction-planner -backend=gcs -gcs.bucket-name=blocks-bucket -user=10428 -split-groups=1 -shard-count=4
2022-02-04 09:52:05.506428 I | Using index from 2022-02-04T08:38:27Z
2022-02-04 09:52:05.533720 I | Filtering using *compactor.NoCompactionMarkFilter
Job No.   Start Time             End Time               Blocks   Job Key
1         2022-02-04T02:00:00Z   2022-02-04T04:00:00Z   4        0@6672437747845546250-merge-2_of_4-1643940000000-1643947200000
2         2022-02-04T02:00:00Z   2022-02-04T04:00:00Z   4        0@6672437747845546250-merge-4_of_4-1643940000000-1643947200000
3         2022-02-04T04:00:00Z   2022-02-04T06:00:00Z   4        0@6672437747845546250-merge-1_of_4-1643947200000-1643954400000
4         2022-02-04T04:00:00Z   2022-02-04T06:00:00Z   4        0@6672437747845546250-merge-2_of_4-1643947200000-1643954400000
5         2022-02-04T04:00:00Z   2022-02-04T06:00:00Z   4        0@6672437747845546250-merge-3_of_4-1643947200000-1643954400000
6         2022-02-04T04:00:00Z   2022-02-04T06:00:00Z   4        0@6672437747845546250-merge-4_of_4-1643947200000-1643954400000
```
