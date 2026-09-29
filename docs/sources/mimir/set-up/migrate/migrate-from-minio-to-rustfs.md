---
aliases:
  - ../../migrate/migrate-from-minio-to-rustfs/
description: Learn how to migrate Grafana Mimir object storage from MinIO to RustFS with rclone and near-zero downtime.
menuTitle: Migrate from MinIO to RustFS
title: Migrate object storage from MinIO to RustFS
weight: 30
---

# Migrate object storage from MinIO to RustFS

Migrate a Grafana Mimir cluster whose blocks, ruler, and Alertmanager state live in the bundled MinIO deployment (`mimir-distributed` Helm chart, `minio.enabled: true`) to [RustFS](https://rustfs.com), an Apache-2.0, S3-compatible object store.

MinIO Community Edition is archived and read-only since April 25, 2026: no new releases, no official binaries, no reviewed security patches. Mimir itself needs no code change for this migration — both MinIO and RustFS are accessed over the S3 protocol (`backend: s3`) — only the endpoint, bucket, and credentials change.

This guide uses [`rclone`](https://rclone.org) (maintained) for a one-time S3-to-S3 copy plus a continuous tail, instead of the archived `mc` client or offline `/data` volume reuse. It works for any S3-compatible target, but the examples target RustFS.

{{< admonition type="note" >}}
Mimir blocks are immutable once an ingester uploads them (roughly every two hours from the in-memory head). There is no in-place dual-write mode: keep a single writer (MinIO), replicate asynchronously to RustFS, then flip the endpoint atomically. Never run two compactor sets against two buckets.
{{< /admonition >}}

## Before you begin

- A Mimir cluster writing to MinIO (`mimir-tsdb` and `mimir-ruler` buckets).
- A running RustFS cluster (see [Install RustFS with Helm](https://docs.rustfs.com/en/installation/cloud-native/helm-chart/installation)), reachable from the cluster, e.g. `http://rustfs.<rustfs-namespace>.svc:9000`, with empty `mimir-tsdb` and `mimir-ruler` buckets.
- `rclone` v1.60+ locally, or the [`migrate-minio-to-rustfs`](https://github.com/grafana/mimir/blob/main/operations/helm/migrate-minio-to-rustfs/) Helm chart (runs the `rclone/rclone` image in-cluster, no local install).
- Enough RustFS capacity for a full copy of both buckets.
- Enough network headroom: rclone streams object data through the rclone pod (there is no cross-endpoint server-side copy), so a multi-TB migration means full transit through that pod.

## Overview

1. [Copy both buckets with rclone](#copy-both-buckets-with-rclone).
2. [Keep a continuous tail running](#keep-a-continuous-tail-running).
3. [Flip Mimir to RustFS](#flip-mimir-to-rustfs).
4. [Verify and decommission MinIO](#verify-and-decommission-minio).

## Copy both buckets with the migration chart

Run the copy inside the cluster with the [`migrate-minio-to-rustfs`](https://github.com/grafana/mimir/blob/main/operations/helm/migrate-minio-to-rustfs/) chart: a one-shot Job plus a continuous-tail Deployment, both running the `rclone/rclone` image. The chart reads credentials from a Secret you create imperatively — never checked into git (or bring your own via ExternalSecrets/Vault: any Secret with the six keys works, see the chart README). The MinIO Service name comes from the `minio.fullname` template, which defaults to `<helm-release>-minio`; adjust the namespace and release name to yours:

```bash
kubectl create secret generic rclone-minio-to-rustfs \
  --from-literal=MINIO_ENDPOINT=http://<mimir-release>-minio.<mimir-namespace>.svc:9000 \
  --from-literal=MINIO_ACCESS_KEY_ID=<minio-user> \
  --from-literal=MINIO_SECRET_ACCESS_KEY=<minio-password> \
  --from-literal=RUSTFS_ENDPOINT=http://rustfs.<rustfs-namespace>.svc:9000 \
  --from-literal=RUSTFS_ACCESS_KEY_ID=<rustfs-key> \
  --from-literal=RUSTFS_SECRET_ACCESS_KEY=<rustfs-secret>
helm install migrator ./operations/helm/migrate-minio-to-rustfs -f migrator-values.yaml
kubectl logs -l app=rclone-minio-to-rustfs --follow
```

Keep `migrator-values.yaml` for the whole flow — Helm `--set` flags do not persist between upgrades, so every `helm upgrade` below must repeat the same `-f` file (even an empty one), or a filtered migration silently resets to a full copy. Start with an empty file for a full copy:

```yaml
# migrator-values.yaml (full copy; add extraSyncArgs to filter, see below)
```

The chart expects exactly those six keys (`MINIO_*`, `RUSTFS_*` for `ENDPOINT`, `ACCESS_KEY_ID`, `SECRET_ACCESS_KEY`) in `secret.name` (default `rclone-minio-to-rustfs`); dev-only `secret.create=true` renders the Secret from values instead.

### Via ArgoCD

Manage the migrator as a **separate** ArgoCD `Application` from Mimir, pointing at the chart path with `migrator-values.yaml` committed in git:

```yaml
# argocd/migrator-app.yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: mimir-storage-migrator
spec:
  source:
    repoURL: <your-mimir-fork-or-config-repo>
    path: operations/helm/migrate-minio-to-rustfs
    helm:
      valueFiles: [migrator-values.yaml]
  destination: { server: https://kubernetes.default.svc, namespace: mimir }
```

- **Secrets:** don't commit the imperative Secret — use an `ExternalSecret` (or Vault/SealedSecrets) that materializes the same six keys under `secret.name`, synced in the same or an earlier wave.
- **Final delta pass:** a values-only sync won't re-run the completed Job. Delete the Job in the ArgoCD UI (or `argocd app sync mimir-storage-migrator --replace`) so the sync recreates it.
- **Tail stop / teardown:** commit `watch.enabled: false`, sync, verify, then delete the Application. The Mimir flip itself is a normal values commit (`rustfs-values.yaml` + `global.extraEnvFrom`).

The Job creates both RustFS buckets and syncs `mimir-tsdb` and `mimir-ruler` with `-v --checksum --fast-list --checkers 16 --transfers 16 --s3-no-check-bucket`. `--checksum` compares size and hash without extra transactions (the recommended comparison for S3-to-S3 copies); note Mimir uploads blocks via multipart, so ETags are not MD5s and `--checksum` falls back to size comparison for those objects. `--fast-list` cuts listing transactions on the bulk copy at the cost of ~1 KiB RAM per object. For large buckets raise `--transfers 32 --checkers 32 --buffer-size 64M` in the Job command and, if the link drops, re-apply the Job — `sync` is idempotent, only the delta is copied.

Prefer running `rclone` locally instead? Define the two S3 remotes via environment (no config file needed):

```bash
export RCLONE_CONFIG_MINIO_TYPE=s3
export RCLONE_CONFIG_MINIO_PROVIDER=Minio
export RCLONE_CONFIG_MINIO_ENDPOINT=http://<mimir-release>-minio.<mimir-namespace>.svc:9000
export RCLONE_CONFIG_MINIO_ACCESS_KEY_ID=<minio-user>
export RCLONE_CONFIG_MINIO_SECRET_ACCESS_KEY=<minio-password>
export RCLONE_CONFIG_MINIO_REGION=us-east-1

export RCLONE_CONFIG_RUSTFS_TYPE=s3
export RCLONE_CONFIG_RUSTFS_PROVIDER=Minio
export RCLONE_CONFIG_RUSTFS_ENDPOINT=http://rustfs.<rustfs-namespace>.svc:9000
export RCLONE_CONFIG_RUSTFS_ACCESS_KEY_ID=<rustfs-key>
export RCLONE_CONFIG_RUSTFS_SECRET_ACCESS_KEY=<rustfs-secret>
export RCLONE_CONFIG_RUSTFS_REGION=us-east-1

rclone mkdir rustfs:mimir-tsdb
rclone mkdir rustfs:mimir-ruler

rclone sync minio:mimir-tsdb rustfs:mimir-tsdb \
  -v --checksum --fast-list --checkers 16 --transfers 16 --s3-no-check-bucket
rclone sync minio:mimir-ruler rustfs:mimir-ruler \
  -v --checksum --fast-list --checkers 16 --transfers 16 --s3-no-check-bucket
```

### Copying a subset (filtered migration)

By default copy everything. If old blocks are expendable — e.g. your retention period is shorter than the data you're skipping — you can copy a subset instead:

```bash
# only objects uploaded in the last 30 days (--max-age = younger than)
rclone sync minio:mimir-tsdb rustfs:mimir-tsdb --max-age 30d --checksum --s3-no-check-bucket

# copy at most 100G per run; re-run to resume (--cutoff-mode SOFT stops at a file boundary)
rclone sync minio:mimir-tsdb rustfs:mimir-tsdb --max-transfer 100G --cutoff-mode SOFT --checksum --s3-no-check-bucket
```

Note `--min-age` is the inverse (older than) — `--min-age 30d` copies everything _except_ the last 30 days. To filter from the chart instead of the CLI, put it in `migrator-values.yaml` — one value feeds both Job and tail, so they can't drift:

```yaml
extraSyncArgs: "--max-age 30d"
```

{{< admonition type="warning" >}}
A filtered copy is data loss by design: the store-gateway only serves what's in the new bucket, so queries outside the copied window come back empty with no error. Only filter when retention covers the gap. Always copy the small `mimir-ruler` bucket whole.
{{< /admonition >}}

## Keep a continuous tail running

The chart installs the tail Deployment alongside the bulk Job. New blocks land every couple of hours; the loop re-syncs roughly every minute (`watch.intervalSeconds`) and copies only the delta. Nothing to do — just leave the release installed until cutover. Prefer the CLI instead?

```bash
while true; do
  rclone sync minio:mimir-tsdb rustfs:mimir-tsdb --checksum --s3-no-check-bucket -q
  rclone sync minio:mimir-ruler rustfs:mimir-ruler --checksum --s3-no-check-bucket -q
  sleep 60
done
```

## Flip Mimir to RustFS

1. Run one final `rclone sync` pass for each bucket (with writes still going to MinIO) so lag is near zero. With the chart, delete the completed Job and upgrade to re-run it — `sync` is idempotent, only the delta is copied (replace `migrator` with your migrator release name):

   ```bash
   kubectl delete job migrator-migrate-minio-to-rustfs-sync
   helm upgrade migrator ./operations/helm/migrate-minio-to-rustfs -f migrator-values.yaml
   kubectl logs -l app=rclone-minio-to-rustfs --follow
   ```

2. Stop the continuous tail **before** flipping, or it will delete the new blocks Mimir writes to RustFS (`sync` makes the destination identical to MinIO, so RustFS-only objects look like extras to remove):

   ```bash
   helm upgrade migrator ./operations/helm/migrate-minio-to-rustfs -f migrator-values.yaml --set watch.enabled=false
   ```

3. Point Mimir at RustFS. With the Helm chart, disable the bundled MinIO and set the S3 endpoint (same pattern as `small.yaml` / `large.yaml`, which already ship with `minio.enabled: false`). Because the credentials use `${...}` expansion, they must come from the pod environment — create a Secret and reference it via `global.extraEnvFrom` (see [`ci/test-oss-values.yaml`](https://github.com/grafana/mimir/blob/main/operations/helm/charts/mimir-distributed/ci/test-oss-values.yaml) for the same pattern with MinIO):

   ```bash
   kubectl create secret generic mimir-rustfs-secret \
     --from-literal=RUSTFS_ACCESS_KEY_ID=<rustfs-key> \
     --from-literal=RUSTFS_SECRET_ACCESS_KEY=<rustfs-secret>
   ```

   ```yaml
   minio:
     enabled: false

   global:
     extraEnvFrom:
       - secretRef:
           name: mimir-rustfs-secret

   mimir:
     structuredConfig:
       common:
         storage:
           backend: s3
           s3:
             endpoint: rustfs.<rustfs-namespace>.svc:9000
             region: us-east-1
             access_key_id: "${RUSTFS_ACCESS_KEY_ID}"
             secret_access_key: "${RUSTFS_SECRET_ACCESS_KEY}"
             insecure: true # only for http:// endpoints; drop for https://
       blocks_storage:
         s3:
           bucket_name: mimir-tsdb
       ruler_storage:
         s3:
           bucket_name: mimir-ruler
       alertmanager_storage:
         s3:
           bucket_name: mimir-ruler
   ```

   Keep the old MinIO release running until you have verified the new path. Note the safe rollback point is before the flip: blocks Mimir uploads to RustFS after the flip do not exist in MinIO, so rolling the values back afterwards loses them.

   Save the snippet above as `rustfs-values.yaml` and upgrade (replace `mimir` with your Helm release name):

   ```bash
   helm upgrade mimir ./operations/helm/charts/mimir-distributed -f rustfs-values.yaml
   ```

   Let components restart. Heads held in ingester memory/WAL (last ~2h) upload to RustFS after the restart; everything already flushed was covered by the mirror.

## Verify and decommission MinIO

1. Check reads against the new store: run [`mimirtool bucket-validation`](https://grafana.com/docs/mimir/<MIMIR_VERSION>/manage/tools/mimirtool/#bucket-validation) against the RustFS bucket and query a wide time range in Grafana.
2. Check ruler/Alertmanager state loads (rules evaluate, no `s3: NoSuchBucket` errors in compactor/store-gateway logs).
3. Uninstall the migration release (the tail was already disabled before the flip), then delete the credential Secret it used:

   ```bash
   helm uninstall migrator
   kubectl delete secret rclone-minio-to-rustfs
   ```

4. After one full retention period with clean reads, delete the leftover MinIO PVCs. The `helm upgrade` with `minio.enabled: false` already removed the MinIO workloads (MinIO is a subchart of the `mimir-distributed` release, not a standalone release) — PVCs are never removed automatically, so delete them explicitly only when you are sure the copy is complete.
