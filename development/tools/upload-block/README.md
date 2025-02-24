# upload-block

`upload-block.sh` uploads the given block directory to the `anonymous` tenant in any of the development Docker Compose stacks.
This can be used to debug an issue with a block from a real Mimir cluster, for example.

It marks the block as no-compact, to ensure it is not compacted with other blocks.

`upload-block.sh` expects the directory given to have the same name as the ULID of the block, for example:

```shell
./upload-block.sh /path/to/block/01J5240S2R3BE24W8CH4TPTEST
```

The directory given should contain the usual contents of a block, for example:

```shell
$ ls /path/to/block/01J5240S2R3BE24W8CH4TPTEST

chunks/
index
meta.json
```

`upload-block.sh` requires the AWS CLI to be installed.

---

As an alternative to uploading a real block to a local environment, https://gist.github.com/dimitarvdimitrov/f43d09d332b32e715513784888977d4a
describes how to connect a local Mimir instance to a remote object storage bucket.
