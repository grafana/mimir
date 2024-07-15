---
aliases:
  - migrating-from-thanos-or-prometheus/
  - ../../migrate/migrate-from-thanos-or-prometheus/
  - ../../migrate/migrating-from-thanos-or-prometheus/
description: Learn how to migrate from Thanos or Prometheus to Grafana Mimir.
menuTitle: Migrate from Thanos or Prometheus
title: Migrate from Thanos or Prometheus to Grafana Mimir
weight: 10
---

# Migrate from Thanos or Prometheus to Grafana Mimir

This document guides an operator through the process of migrating a deployment of Thanos or Prometheus to Grafana Mimir.

## Overview

Grafana Mimir stores series in TSDB blocks uploaded in an object storage bucket.
These blocks are the same as those used by Prometheus and Thanos.
Each project stores blocks in different places and uses slightly different block metadata files.

## Configuring remote write to Grafana Mimir

For configuration of remote write to Grafana Mimir, refer to [Configuring Prometheus remote write]({{< relref "../../manage/secure/authentication-and-authorization#configuring-prometheus-remote-write" >}}).

## Uploading historic TSDB blocks to Grafana Mimir

Grafana Mimir supports uploading of historic TSDB blocks, notably from Prometheus.
In order to enable this functionality, either for all tenants or a specific one, refer to
[Configure TSDB block upload]({{< relref "../../configure/configure-tsdb-block-upload" >}}).

Prometheus stores TSDB blocks in the path specified in the `--storage.tsdb.path` flag.

To find all block directories in the TSDB `<STORAGE TSDB PATH>`, run the following command:

```bash
find <STORAGE TSDB PATH> -name chunks -exec dirname {} \;
```

Grafana Mimir supports multiple tenants and stores blocks per tenant. With multi-tenancy disabled, there
is a single tenant called `anonymous`.

Use Grafana mimirtool to upload each block, such as those identified by the previous command, to Grafana Mimir:

```bash
mimirtool backfill --address=http://<mimir-hostname> --id=<tenant> <block1> <block2>...
```

{{< admonition type="note" >}}
If you need to authenticate against Grafana Mimir, you can provide an API key via the `--key` flag, for example `--key=$(cat token.txt)`.
{{< /admonition >}}

Grafana Mimir performs some sanitization and validation of each block's metadata.
As a result, it rejects Thanos blocks due to unsupported labels.
As a workaround, if you need to upload Thanos blocks, upload the blocks directly to the
Grafana Mimir blocks bucket, prefixed by `<tenant>/<block ID>/`.

## Block metadata

Each block has a `meta.json` metadata file that is used by Grafana Mimir, Prometheus, and Thanos to identify the block contents.
Each project has its own metadata conventions.

In the Grafana Mimir 2.1 (or earlier) release, the ingesters added an external label to the `meta.json` file to identify the tenant that owns the block.

In the Grafana Mimir 2.2 (or later) release, blocks no longer have a label that identifies the tenant.

{{< admonition type="note" >}}
Blocks from Prometheus don't have any external labels stored in them.
Only blocks from Thanos use labels.
{{< /admonition >}}

{{< admonition type="note" >}}
If you encounter the HTTP error 413 "Request Entity Too Large" when uploading blocks using mimirtool, and if Nginx is being used as a reverse proxy, the uploaded block size may be exceeding Nginx's default maximum allowed request body size.
To resolve this issue:

Determine the current size of the blocks you are trying to upload by running the following command(by default the blocks are sized around 500MB, but it can vary depending on the configuration of Prometheus or Thanos.)

```bash
find <path/to/blocks> -name 'chunks' -printf '%s\n' | numfmt --to=iec-i
```

This shows the size of each block's chunks directory in a human-readable format.
Increase the client_max_body_size directive in the Nginx configuration:

For manual Nginx deployments, open the Nginx configuration file (e.g., /etc/nginx/nginx.conf) and set the `client_max_body_size` directive inside the server block for the Mimir endpoint to a value about 5% larger than the maximum size of the blocks you are uploading. For example:

```
server {
    ...
    client_max_body_size 540M;
    ...
}
```

```
location / {
    ...
    client_max_body_size 540M;
    ...
}
```

For Helm deployments of Mimir, you can set the `gateway.nginx.config.clientMaxBodySize` value in your Helm values file to a higher value, e.g.:

```yaml
gateway:
  nginx:
    config:
      clientMaxBodySize: 540M
```

Apply the configuration changes:

For manual Nginx deployments, save the configuration file and reload Nginx:

```bash
sudo nginx -s reload
```

For Helm deployments, upgrade your Mimir release with the updated values file:

```bash
helm upgrade <release-name> <chart-name> -f values.yaml
```

After increasing the `client_max_body_size` setting, you can upload the blocks without encountering the 413 error.
{{< /admonition >}}

## Considerations on Thanos specific features

Thanos requires that Prometheus is configured with external labels.
When the Thanos sidecar uploads blocks, it includes the external labels from Prometheus in the `meta.json` file inside the block.
When you query the block, Thanos injects Prometheus’ external labels in the series returned in the query result.
Thanos also uses labels for the deduplication of replicated data.

If you want to use existing blocks from Thanos by Grafana Mimir, there are some considerations:

**Grafana Mimir doesn't inject external labels into query results.**
This means that blocks that were originally created by Thanos will not include their external labels in the results when queried by Grafana Mimir.
If you need to have external labels in your query results, this is currently not possible to achieve in Grafana Mimir.

**Grafana Mimir will not respect deduplication labels configured in Thanos when querying the blocks.**
For best query performance, only upload Thanos blocks from a single Prometheus replica from each HA pair.
If you upload blocks from both replicas, the query results returned by Mimir will include samples from both replicas.

**Grafana Mimir does not support Thanos’ _downsampling_ feature.**
To guarantee query results correctness please only upload original (raw) Thanos blocks into Mimir's storage.
If you also upload blocks with downsampled data (ie. blocks with non-zero `Resolution` field in `meta.json` file), Grafana Mimir will merge raw samples and downsampled samples together at the query time.
This may cause that incorrect results are returned for the query.

## Migrate historic TSDB blocks from Thanos to Grafana Mimir

1. Copy the blocks from Thanos's bucket to an Intermediate bucket.

   Create an intermediate object storage bucket (such as Amazon S3 or GCS) within your cloud provider, where you can copy the historical blocks and work on them before uploading them to the Mimir bucket.

   {{< admonition type="tip" >}}
   Run the commands within a `screen` or `tmux` session, to avoid any interruptions because the steps might take some time depending on the amount of data.
   {{< /admonition >}}

   For Amazon S3, use the `aws` tool:

   ```bash
   aws s3 cp --recursive s3://<THANOS-BUCKET> s3://<INTERMEDIATE-MIMIR-BUCKET>/
   ```

   For Google Cloud Storage (GCS), use the `gsutil` tool:

   ```bash
   gsutil -m cp -r gs://<THANOS-BUCKET>/* gs://<INTERMEDIATE-MIMIR-BUCKET>/
   ```

   After the copy process completes, inspect the blocks in the bucket to make sure that they are valid from a Thanos perspective.

   ```bash
   thanos tools bucket inspect \
       --objstore.config-file bucket.yaml
   ```

1. Remove the downsampled blocks.

   Mimir doesn’t understand the downsampled blocks from Thanos, such as blocks with a non-zero `Resolution` field in the `meta.json` file. Therefore, you need to remove the `5m` and `1h` downsampled blocks from this bucket.

   Mark the downsampled blocks for deletion:

   ```bash
   thanos tools bucket retention \
       --objstore.config-file bucket.yaml \
       --retention.resolution-1h=1s \
       --retention.resolution-5m=1s \
       --retention.resolution-raw=0s
   ```

   Cleanup the blocks marked for deletion.

   ```bash
   thanos tools bucket cleanup \
       --objstore.config-file bucket.yaml \
       --delete-delay=0
   ```

1. Remove the duplicated blocks.

   If two replicas of Prometheus instances are deployed for high-availability, then only upload the blocks from one of the replicas and drop from the other replica.

   ```bash
   # Get list of all blocks in the bucket
   thanos tools bucket inspect \
       --objstore.config-file bucket.yaml \
       --output=tsv > blocks.tsv

   # Find blocks from replica that we will drop
   cat blocks.tsv| grep prometheus_replica=<PROMETHEUS-REPLICA-TO-DROP> \
       | awk '{print $1}' > blocks_to_drop.tsv

   # Mark found blocks for deletion
   for ID in $(cat blocks_to_drop.tsv)
   do
       thanos tools bucket mark \
          --marker="deletion-mark.json" \
          --objstore.config-file bucket.yaml \
          --details="Removed as duplicate" \
          --id $ID
   done
   ```

   {{< admonition type="note" >}}
   You must replace `prometheus_replica` with the unique label that would differentiate Prometheus replicas in your setup.
   {{< /admonition >}}

   Clean up the duplicate blocks marked for deletion again:

   ```bash
   thanos tools bucket cleanup \
       --objstore.config-file bucket.yaml \
       --delete-delay=0
   ```

   {{< admonition type="tip" >}}
   If you want to visualize exactly what's happening in the blocks, with respect to the source of blocks, external labels, compaction levels, and more, you can use the following command to get the output as CSV and import it into a spreadsheet:

   ```bash
   thanos tools bucket inspect \
       --objstore.config-file bucket-prod.yaml \
       --output=csv > thanos-blocks.csv
   ```

   {{< /admonition >}}

1. Relabel the blocks with external labels.

   Mimir doesn’t inject external labels from the `meta.json` file into query results. Therefore, you need to relabel the blocks with the required external labels in the `meta.json` file.

   {{< admonition type="tip" >}}
   You can get the external labels in the `meta.json` file of each block from the CSV file that's imported, and build the rewrite configuration accordingly.
   {{< /admonition >}}

   Create a rewrite configuration that is similar to this:

   ```bash
   # relabel-config.yaml
   - action: replace
     target_label: "<LABEL-KEY>"
     replacement: "<LABEL-VALUE>"
   ```

   Perform the rewrite dry run to confirm all works well.

   ```bash
   # Get list of all blocks in the bucket after removing the depuplicate and downsampled blocks.
   thanos tools bucket inspect \
       --objstore.config-file bucket.yaml \
       --output=tsv > blocks-to-rewrite.tsv

   # Check if rewrite of the blocks with external labels is working as expected.
   for ID in $(cat blocks-to-rewrite.tsv)
   do
       thanos tools bucket rewrite \
           --objstore.config-file bucket.yaml \
           --rewrite.to-relabel-config-file relabel-config.yaml \
           --dry-run \
           --id $ID
   done
   ```

   After you confirm that the rewrite is working as expected via `--dry-run`, apply the changes with the `--no-dry-run` flag. Remember to include `--delete-blocks`, otherwise the original blocks will not be marked for deletion.

   ```bash
   # Rewrite the blocks with external labels and mark the original blocks for deletion.
   for ID in $(cat blocks-to-rewrite.tsv)
   do
       thanos tools bucket rewrite  \
           --objstore.config-file bucket.yaml \
           --rewrite.to-relabel-config-file relabel-config.yaml \
           --no-dry-run \
           --delete-blocks \
           --id $ID
   done
   ```

   The output of relabelling of every block would look like something below.

   ```console
   level=info ts=2022-10-10T13:03:32.032820262Z caller=factory.go:50 msg="loading bucket configuration"
   level=info ts=2022-10-10T13:03:32.516953867Z caller=tools_bucket.go:1160 msg="downloading block" source=01GEGWPME2187SVFH63G8DH7KH
   level=info ts=2022-10-10T13:03:35.825009556Z caller=tools_bucket.go:1197 msg="changelog will be available" file=/tmp/thanos-  rewrite/01GF0ZWPWGEPHG5NV79NH9KMPV/change.log
   level=info ts=2022-10-10T13:03:35.836953593Z caller=tools_bucket.go:1212 msg="starting rewrite for block" source=01GEGWPME2187SVFH63G8DH7KH  new=01GF0ZWPWGEPHG5NV79NH9KMPV toDelete= toRelabel="- action: replace\n  target_label: \"cluster\"\n  replacement: \"prod-cluster\"\n"
   level=info ts=2022-10-10T13:04:47.57624244Z caller=compactor.go:42 msg="processed 10.00% of 701243 series"
   level=info ts=2022-10-10T13:04:53.4046885Z caller=compactor.go:42 msg="processed 20.00% of 701243 series"
   level=info ts=2022-10-10T13:04:59.649337602Z caller=compactor.go:42 msg="processed 30.00% of 701243 series"
   level=info ts=2022-10-10T13:05:02.986219042Z caller=compactor.go:42 msg="processed 40.00% of 701243 series"
   level=info ts=2022-10-10T13:05:05.990498497Z caller=compactor.go:42 msg="processed 50.00% of 701243 series"
   level=info ts=2022-10-10T13:05:09.349918024Z caller=compactor.go:42 msg="processed 60.00% of 701243 series"
   level=info ts=2022-10-10T13:05:12.040895624Z caller=compactor.go:42 msg="processed 70.00% of 701243 series"
   level=info ts=2022-10-10T13:05:15.253899238Z caller=compactor.go:42 msg="processed 80.00% of 701243 series"
   level=info ts=2022-10-10T13:05:18.471471014Z caller=compactor.go:42 msg="processed 90.00% of 701243 series"
   level=info ts=2022-10-10T13:05:21.536267363Z caller=compactor.go:42 msg="processed 100.00% of 701243 series"
   level=info ts=2022-10-10T13:05:21.536466158Z caller=tools_bucket.go:1222 msg="wrote new block after modifications; flushing" source=01GEGWPME2187SVFH63G8DH7KH new=01GF0ZWPWGEPHG5NV79NH9KMPV
   level=info ts=2022-10-10T13:05:28.675240198Z caller=tools_bucket.go:1231 msg="uploading new block" source=01GEGWPME2187SVFH63G8DH7KH new=01GF0ZWPWGEPHG5NV79NH9KMPV
   level=info ts=2022-10-10T13:05:38.922348564Z caller=tools_bucket.go:1241 msg=uploaded source=01GEGWPME2187SVFH63G8DH7KH new=01GF0ZWPWGEPHG5NV79NH9KMPV
   level=info ts=2022-10-10T13:05:38.979696873Z caller=block.go:203 msg="block has been marked for deletion" block=01GEGWPME2187SVFH63G8DH7KH
   level=info ts=2022-10-10T13:05:38.979832767Z caller=tools_bucket.go:1249 msg="rewrite done" IDs=01GEGWPME2187SVFH63G8DH7KH
   level=info ts=2022-10-10T13:05:38.980197796Z caller=main.go:161 msg=exiting
   ```

   Cleanup the original blocks which are marked for deletion.

   ```bash
   thanos tools bucket cleanup \
       --objstore.config-file bucket.yaml \
       --delete-delay=0
   ```

   {{< admonition type="note" >}}
   If there are multiple Prometheus clusters, then relabelling each of them in parallel may speed up the entire process.

   Get the list of blocks that for each cluster, and process it separately.

   ```bash
   thanos tools bucket inspect \
       --objstore.config-file bucket.yaml \
       --output=tsv \
       | grep <PROMETHEUS-CLUSTER-NAME> \
       | awk '{print $1}' > prod-blocks.tsv
   ```

   ```bash
   for ID in `cat prod-blocks.tsv`
   do
       thanos tools bucket rewrite  \
          --objstore.config-file bucket.yaml \
          --rewrite.to-relabel-config-file relabel-config.yaml \
          --delete-blocks \
          --no-dry-run \
          --id $ID
   done
   ```

   {{< /admonition >}}

1. Remove external labels from meta.json.

   Mimir compactor will not be able to compact the blocks having external labels with Mimir's own blocks that don't have any such labels in their meta.json. Therefore these external labels have to be removed before copying them to the Mimir bucket.

   Use the below script to remove the labels from the meta.json.

   For Amazon S3, use the `aws` tool:

   ```bash
   #!/bin/bash

   BUCKET="XXX"

   echo "Fetching list of meta.json files (this can take a while if there are many blocks)"
   aws s3 ls $BUCKET --recursive | awk '{print $4}' | grep meta.json | grep -v meta.json.orig > meta-files.txt

   echo "Processing meta.json files"
   for FILE in $(cat meta-files.txt); do
      echo "Removing Thanos labels from $FILE"
      ORIG_META_JSON=$(aws s3 cp s3://$BUCKET/$FILE -)
      UPDATED_META_JSON=$(echo "$ORIG_META_JSON" | jq "del(.thanos.labels)")

      if ! diff -u <( echo "$ORIG_META_JSON" | jq . ) <( echo "$UPDATED_META_JSON" | jq .) > /dev/null; then
        echo "Backing up $FILE to $FILE.orig"
        aws s3 cp "s3://$BUCKET/$FILE" "s3://$BUCKET/$FILE.orig"
        echo "Uploading modified $FILE"
        echo "$UPDATED_META_JSON" | aws s3 cp - "s3://$BUCKET/$FILE"
      else
        echo "No diff for $FILE"
      fi
   done
   ```

   For Google Cloud Storage (GCS), use the gsutil tool:

   ```bash
   #!/bin/bash

   BUCKET="GCS Bucket name"

   echo "Fetching list of meta.json files (this can take a while if there are many blocks)"
   gsutil ls "gs://$BUCKET/*/meta.json" > meta-files.txt

   echo "Processing meta.json files"
   for FILE in $(cat meta-files.txt); do
      echo "Removing Thanos labels from $FILE"
      ORIG_META_JSON=$(gsutil cat "$FILE")
      UPDATED_META_JSON=$(echo "$ORIG_META_JSON" | jq "del(.thanos.labels)")

      if ! diff -u <( echo "$ORIG_META_JSON" | jq . ) <( echo "$UPDATED_META_JSON" | jq .) > /dev/null; then
         echo "Backing up $FILE to $FILE.orig"
         gsutil cp "$FILE" "$FILE.orig"
         echo "Uploading modified $FILE"
         echo "$UPDATED_META_JSON" | gsutil cp - "$FILE"
      else
         echo "No diff for $FILE"
      fi
   done
   ```

1. Copy the blocks from the intermediate bucket to the Mimir bucket.

   For Amazon S3, use the `aws` tool:

   ```bash
   aws s3 cp --recursive s3://<INTERMEDIATE-MIMIR-BUCKET> s3://<MIMIR-AWS-BUCKET>/<TENANT>/
   ```

   For Google Cloud Storage (GCS), use the gsutil tool:

   ```bash
   gsutil -m cp -r gs://<INTERMEDIATE-MIMIR-BUCKET> gs://<MIMIR-GCS-BUCKET>/<TENANT>/
   ```

   Historical blocks are not available for querying immediately after they are uploaded because the bucket index with the list of all available blocks first needs to be updated by the compactor. The compactor typically perform such an update every 15 minutes. After an update completes, other components such as the querier or store-gateway are able to work with the historical blocks, and the blocks are available for querying through Grafana.

1. Check the `store-gateway` HTTP endpoint at `http://<STORE-GATEWAY-ENDPOINT>/store-gateway/tenant/<TENANT-NAME>/blocks` to verify that the uploaded blocks are there.
