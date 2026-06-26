local k = import 'ksonnet-util/kausal.libsonnet';

// This file provides a helper to run `mimirtool blocks` subcommands as one-off Kubernetes Jobs.
// The object storage (bucket) configuration is picked up automatically from the cluster's storage
// settings.
{
  local container = k.core.v1.container,
  local job = k.batch.v1.job,

  mimirtool_bucket_args:: {
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

  local mapToDoubleDashFlags(map) = [
    '--%s=%s' % [key, std.toString(map[key])]
    for key in std.objectFields(map)
  ],

  // newMimirtoolBlocksJob returns a Job that runs a `mimirtool blocks <subcommand>` command. The
  // `blocks` parent command is implicit. The object storage (bucket) configuration is picked up
  // automatically from the cluster's storage settings. For example:
  //
  //   $.newMimirtoolBlocksJob('mark-blocks', 'mark', {
  //     tenant: '<tenant-id>',
  //     'mark-type': 'no-compact',
  //     details: 'Corrupted block',
  //     blocks: '<block-1>,<block-2>',
  //   })
  //
  //   $.newMimirtoolBlocksJob('list-blocks', 'list', { user: '<tenant-id>' })
  newMimirtoolBlocksJob(name, subcommand, args={})::
    local blocksContainer =
      container.new(name, $._images.mimirtool) +
      container.withArgsMixin(['blocks', subcommand] + mapToDoubleDashFlags($.mimirtool_bucket_args + args)) +
      k.util.resourcesRequests('1', '512Mi') +
      k.util.resourcesLimits(null, '1Gi');

    job.new(name) +
    job.spec.template.spec.withContainers([blocksContainer]) +
    job.spec.template.spec.withRestartPolicy('Never') +
    job.spec.withBackoffLimit(0),
}
