# Copyblocks

This program can copy Mimir blocks between two GCS buckets. It is GCS specific and uses special calls to copy
files between buckets directly, without download to a local system first.

copyblocks can run in one-time mode, or as a service and do regular copies.

You can configure it with a minimum block time range to avoid copying blocks that are too small.
It can also be configured with list of users to copy, or avoid copying blocks for.
