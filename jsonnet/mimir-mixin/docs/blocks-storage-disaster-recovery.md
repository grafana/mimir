# Cortex blocks storage - Disaster recovery plan

This document assumes that you are running a Cortex blocks storage cluster:

1. Using this mixin config
2. Using GCS as object store (but similar procedures apply to other backends)

## Recovering from a potential data loss incident

The ingested series data that could be lost during an incident can be stored in two places:

1. Ingesters (before blocks are shipped to the bucket)
2. Bucket

There could be several root causes leading to a potential data loss. In this document we're going to share generic procedures that could be used as a guideline during an incident.

### Halt the compactor

The Cortex cluster continues to successfully operate even if the compactor is not running, except that over a long period (12+ hours) this will lead to query performance degrade. The compactor could potentially be the cause of data loss because:

- It marks blocks for deletion (soft deletion)
- It permanently deletes blocks marked for deletion after `-compactor.deletion-delay` (hard deletion)
- It could generate corrupted compacted blocks (eg. due to a bug or if a source block is corrupted and the automatic checks can't detect it)

**If you suspect the compactor could be the cause of data loss, halt it**. It can be restarted anytime later.

When the compactor is **halted**:

- No new blocks will be compacted
- No blocks will be deleted (soft and hard deletion)

### Recover source blocks from ingesters

Ingesters keep, on their persistent disk, the blocks compacted from TSDB head until the `-experimental.tsdb.retention-period` retention expires. The **default retention is 4 days**, in order to give cluster operators enough time to react in case of a data loss incident.

The blocks retained in the ingesters can be used in case the compactor generates corrupted blocks and the source blocks, shipped from ingesters, have already been hard deleted from the bucket.

How to manually blocks from ingesters to the bucket:

1. Ensure [`gsutil`](https://cloud.google.com/storage/docs/gsutil) is installed in the Cortex pod. If not, [install it](#install-gsutil-in-the-cortex-pod)
2. Run `cd /data/tsdb && /path/to/gsutil -m rsync -r -x 'thanos.shipper.json|chunks_head|wal' . gs://<bucket>/recovered/`
   - `-m` enables parallel mode
   - `-r` enables recursive rsync
   - `-x <pattern>` excludes specific patterns from sync

### Freeze ingesters persistent disk

The blocks and WAL stored in the ingester persistent disk are the last fence of defence in case of an incident involving blocks not shipped to the bucket or corrupted blocks in the bucket. If the data integrity in the ingester's disk is at risk (eg. close to hit the TSDB retention period or close to reach max disk utilisation), you should freeze it taking a **disk snapshot**.

To take a **GCP persistent disk snapshot**:

1. Identify the Kubernetes PVC volume name (`kubectl get pvc -n <namespace>`) of the volumes to snapshot
2. For each volume, [create a snapshot](https://console.cloud.google.com/compute/snapshotsAdd) from the GCP console ([documentation](https://cloud.google.com/compute/docs/disks/create-snapshots))

### Halt the ingesters

Halting the ingesters should be the **very last resort** because of the side effects. To halt the ingesters, while preserving their disk and without disrupting the cluster write path, you need to:

1. Create a second pool of ingesters
  - Uses the functions `newIngesterStatefulSet()`, `newIngesterPdb()`
2. Wait until the second pool is up and running
3. Halt existing ingesters (scale down to 0 or delete their statefulset)

However the **queries will return partial data**, due to all the ingested samples which have not been compacted to blocks yet.


## Appendix

### Install `gsutil` in the Cortex pod

1. Install python
   ```
   apk add python3 py3-pip
   ln -s /usr/bin/python3 /usr/bin/python
   pip install google-compute-engine
   ```
2. Download `gsutil`
   ```
   wget https://storage.googleapis.com/pub/gsutil.tar.gz
   tar -zxvf gsutil.tar.gz
   ./gsutil/gsutil --help
   ```
3. Create `/etc/boto.cfg` with the following content:
   ```
   [GoogleCompute]
   service_account = default

   [Plugin]
   plugin_directory = /usr/lib/python3.8/site-packages/google_compute_engine/boto
   ```
