# Copyblocks

This program can copy Mimir blocks between two GCS buckets. It is GCS specific and uses special calls to copy
files between buckets directly, without download to a local system first.

The copyblocks program can run a one-time copy job, or it can run continuously as a service that periodically checks to see if there are blocks to copy and if so runs the copy job.

You can configure it with a minimum block time range to avoid copying blocks that are too small.
You can configure an allowlist and a blocklist of users to copy or not to copy.
