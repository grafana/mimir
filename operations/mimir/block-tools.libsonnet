local k = import 'ksonnet-util/kausal.libsonnet';

// This file provides helpers to run `mimirtool blocks` subcommands as one-off Kubernetes Jobs.
{
  local container = k.core.v1.container,
  local job = k.batch.v1.job,

  local mimirtoolBucketArgs = {
    backend: $._config.storage_backend,
  } + (
    if $._config.storage_backend == 's3' then {
      's3.bucket-name': $._config.blocks_storage_bucket_name,
      's3.endpoint': $._config.storage_s3_endpoint,
    } + (
      if $._config.storage_s3_access_key_id != null && $._config.storage_s3_secret_access_key != null then {
        's3.access-key-id': $._config.storage_s3_access_key_id,
        's3.secret-access-key': $._config.storage_s3_secret_access_key,
      } else {
        's3.native-aws-auth-enabled': true,
      }
    ) else if $._config.storage_backend == 'gcs' then {
      'gcs.bucket-name': $._config.blocks_storage_bucket_name,
      [if $._config.storage_gcs_service_account != null then 'gcs.service-account']: $._config.storage_gcs_service_account,
    } else if $._config.storage_backend == 'azure' then {
      'azure.container-name': $._config.blocks_storage_bucket_name,
      'azure.account-name': $._config.storage_azure_account_name,
      'azure.account-key': $._config.storage_azure_account_key,
    } else {}
  ),

  local mimirtoolUndeleteBucketArgs = {
    backend: $._config.storage_backend,
  } + (
    if $._config.storage_backend == 's3' then {
      's3.bucket-name': $._config.blocks_storage_bucket_name,
      's3.endpoint': $._config.storage_s3_endpoint,
    } + (
      if $._config.storage_s3_access_key_id != null && $._config.storage_s3_secret_access_key != null then {
        's3.access-key-id': $._config.storage_s3_access_key_id,
        's3.secret-access-key': $._config.storage_s3_secret_access_key,
      } else {}
    ) else if $._config.storage_backend == 'gcs' then {
      'gcs.bucket-name': $._config.blocks_storage_bucket_name,
    } else if $._config.storage_backend == 'azure' then {
      'azure.container-name': $._config.blocks_storage_bucket_name,
      'azure.account-name': $._config.storage_azure_account_name,
      'azure.account-key': $._config.storage_azure_account_key,
    } else {}
  ),

  local mapToDoubleDashFlags(map) = [
    '--%s=%s' % [key, std.toString(map[key])]
    for key in std.objectFields(map)
  ],

  // newMimirtoolBlocksJob returns a Job that runs a `mimirtool blocks <subcommand>` command.
  //
  // Prefer the subcommand-specific wrappers below.
  //
  // For example:
  //
  //   $.newMimirtoolBlocksJob('copy-blocks', 'copy', {
  //     'source.backend': 's3',
  //     's3.source.bucket-name': '<source-bucket>',
  //     'destination.backend': 'gcs',
  //     'gcs.destination.bucket-name': '<destination-bucket>',
  //   })
  newMimirtoolBlocksJob(name, subcommand, args={})::
    local blocksContainer =
      container.new(name, $._images.mimirtool) +
      container.withArgsMixin(['blocks', subcommand] + mapToDoubleDashFlags(args)) +
      k.util.resourcesRequests('1', '512Mi') +
      k.util.resourcesLimits(null, '1Gi');

    job.new(name) +
    job.spec.template.spec.withContainers([blocksContainer]) +
    job.spec.template.spec.withRestartPolicy('Never') +
    job.spec.withBackoffLimit(0),

  // newMimirtoolMarkBlocksJob runs `mimirtool blocks mark`, injecting the cluster's bucket config.
  //
  // For example:
  //
  //   $.newMimirtoolMarkBlocksJob('mark-blocks', {
  //     tenant: '<tenant-id>',
  //     'mark-type': 'no-compact',
  //     details: 'Corrupted block',
  //     blocks: '<block-1>,<block-2>',
  //   })
  newMimirtoolMarkBlocksJob(name, args={})::
    self.newMimirtoolBlocksJob(name, 'mark', mimirtoolBucketArgs + args),

  // newMimirtoolListBlocksJob runs `mimirtool blocks list`, injecting the cluster's bucket config.
  //
  // For example:
  //
  //   $.newMimirtoolListBlocksJob('list-blocks', { user: '<tenant-id>' })
  newMimirtoolListBlocksJob(name, args={})::
    self.newMimirtoolBlocksJob(name, 'list', mimirtoolBucketArgs + args),

  // newMimirtoolSplitBlocksJob runs `mimirtool blocks split`, injecting the cluster's bucket config.
  newMimirtoolSplitBlocksJob(name, args={})::
    self.newMimirtoolBlocksJob(name, 'split', mimirtoolBucketArgs + args),

  // newMimirtoolUndeleteBlocksJob runs `mimirtool blocks undelete`, injecting the cluster's bucket
  // config.
  newMimirtoolUndeleteBlocksJob(name, args={})::
    self.newMimirtoolBlocksJob(name, 'undelete', mimirtoolUndeleteBucketArgs + args),
}
