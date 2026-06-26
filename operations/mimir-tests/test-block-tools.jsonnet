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

  mark_blocks_test_job:
    $.newMimirtoolMarkBlocksJob('test-mark-blocks', {
      tenant: '10428',
      'mark-type': 'no-compact',
      details: 'Corrupted blocks',
      blocks: '01FSCTA0A4M1YQHZQ4B2VTGS2R,01FSCTA0A4M1YQHZQ4B2VTGS7Z',
    }),

  list_blocks_test_job:
    $.newMimirtoolListBlocksJob('test-list-blocks', {
      user: '10428',
    }),

  undelete_blocks_test_job:
    $.newMimirtoolUndeleteBlocksJob('test-undelete-blocks', {
      'blocks-from': 'listing',
      'include-tenants': '10428',
    }),

  copy_blocks_test_job:
    $.newMimirtoolBlocksJob('test-copy-blocks', 'copy', {
      'source.backend': 's3',
      's3.source.bucket-name': 'source-bucket',
      'destination.backend': 'gcs',
      'gcs.destination.bucket-name': 'destination-bucket',
    }),
}
