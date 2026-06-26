local blockTools = import 'mimir/block-tools.libsonnet';

blockTools {
  _config:: {
    storage_backend: 's3',
    blocks_storage_bucket_name: 'blocks-bucket',
    storage_s3_endpoint: 's3.dualstack.eu-west-1.amazonaws.com',
    // No static credentials: the jobs should fall back to native AWS auth (IAM/IRSA).
    storage_s3_access_key_id: null,
    storage_s3_secret_access_key: null,
  },

  _images:: {
    mimirtool: 'grafana/mimirtool',
  },

  // Mark a couple of blocks for no-compaction. The bucket configuration is picked up automatically.
  mark_blocks_test_job:
    $.newMimirtoolBlocksJob('test-mark-blocks', 'mark', {
      tenant: '10428',
      'mark-type': 'no-compact',
      details: 'Corrupted blocks',
      blocks: '01FSCTA0A4M1YQHZQ4B2VTGS2R,01FSCTA0A4M1YQHZQ4B2VTGS7Z',
    }),

  // List the blocks of a tenant. The bucket configuration is picked up automatically.
  list_blocks_test_job:
    $.newMimirtoolBlocksJob('test-list-blocks', 'list', {
      user: '10428',
    }),
}
