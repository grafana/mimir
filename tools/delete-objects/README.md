# delete-objects

This program can concurrently delete objects in a storage backend that is supported by Mimir. The program expects the paths of the objects to be deleted as input.

## Build
Compile `delete-objects` using `go build`:

```bash
go build .
```

In the example usage below, `objects-to-delete.txt` contains the path of the object to delete per line. For example:
```
path/to/object/to/delete/foo1.json
path/to/object/to/delete/foo2.json
path/to/object/to/delete/foo3.json
path/to/object/to/delete/foo4.json
```

### Example GCS Usage
```bash
cat objects-to-delete.txt | ./delete-objects -concurrency 64 -backend gcs --gcs.bucket-name <GCS_BUCKET_NAME>
```


### Example S3 Usage
```bash
cat objects-to-delete.txt | ./delete-objects \
    -concurrency 64 \
    -backend s3 \
    -s3.endpoint <S3_ENDPOINT> \
    -s3.bucket-name <S3_BUCKET_NAME> \
    -s3.access-key-id <S3_ACCESS_KEY_ID> \
    -s3.secret-access-key <S3_SECRET_ACCESS_KEY>
```


### Example Azure Usage
```bash
cat objects-to-delete.txt | ./delete-objects \
    -concurrency 64 \
    -backend azure \
    -azure.container-name <AZURE_CONTAINER_NAME> \
    -azure.account-name <AZURE_ACCOUNT_NAME> \
    -azure.account-key <AZURE_ACCOUNT_KEY>
```
