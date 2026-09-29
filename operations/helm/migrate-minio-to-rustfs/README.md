# migrate-minio-to-rustfs

One-shot Helm chart for the [MinIO → RustFS migration](https://github.com/grafana/mimir/blob/main/docs/sources/mimir/set-up/migrate/migrate-from-minio-to-rustfs.md): a bulk-copy Job plus an optional continuous-tail Deployment, both running `rclone` S3-to-S3. Delete the release when the migration is done — it is not part of the steady-state stack. Once MinIO support is dropped, remove this chart from the repo entirely.

This chart is intentionally outside `operations/helm/charts`, so chart-testing (`ct.yaml` scopes `chart-dirs` to `operations/helm/charts`) ignores it.

## Secrets

The chart never takes plaintext credentials except via `secret.create=true` (dev only).
Production patterns all work by pointing `secret.name` at an out-of-band Secret
holding the six keys `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY_ID`,
`MINIO_SECRET_ACCESS_KEY`, `RUSTFS_ENDPOINT`, `RUSTFS_ACCESS_KEY_ID`,
`RUSTFS_SECRET_ACCESS_KEY`:

```bash
# imperative
kubectl create secret generic rclone-minio-to-rustfs \
  --from-literal=MINIO_ENDPOINT=http://mimir-minio.mimir.svc:9000 \
  --from-literal=MINIO_ACCESS_KEY_ID=... \
  --from-literal=MINIO_SECRET_ACCESS_KEY=... \
  --from-literal=RUSTFS_ENDPOINT=http://rustfs.rustfs.svc:9000 \
  --from-literal=RUSTFS_ACCESS_KEY_ID=... \
  --from-literal=RUSTFS_SECRET_ACCESS_KEY=...
helm install migrator ./operations/helm/migrate-minio-to-rustfs
```

```yaml
# ExternalSecrets: materialize the same six keys, then
# helm install migrator ./operations/helm/migrate-minio-to-rustfs \
#   --set secret.name=my-eso-secret
```

## Filtered copies

Keep migration values in a file and pass it to every install/upgrade —
`--set` flags do not persist between `helm upgrade` runs, and a bare upgrade
silently resets to a full copy:

```yaml
# migrator-values.yaml
extraSyncArgs: "--max-age 30d"
```

```bash
helm install migrator ./operations/helm/migrate-minio-to-rustfs -f migrator-values.yaml
```

The value is shared by Job and tail. Changing any Job-affecting value
(`extraSyncArgs`, `checkers`, `transfers`, `buckets`) after install requires
deleting the Job first — Jobs are immutable, so run `kubectl delete job
<release>-migrate-minio-to-rustfs-sync` before `helm upgrade`, or the upgrade
fails.
