---
title: "Configuring the Grafana Mimir ruler with Jsonnet"
menuTitle: "Configuring ruler"
description: "Learn how to configure the Grafana Mimir ruler when using Jsonnet."
weight: 20
---

# Configuring the Grafana Mimir ruler with Jsonnet

The ruler is an optional component and is therefore not deployed by default when using Jsonnet.
For more information about the ruler, see [Grafana Mimir ruler]({{< relref "../../architecture/components/ruler/index.md" >}}).

It is enabled by adding the following to the `_config` section of the jsonnet:

```jsonnet
  _config+:: {
    ruler_enabled: true
    ruler_client_type: '<type>',
  }
```

The `ruler_client_type` option must be one of: 'local', 'azure', 'aws', 's3'.
For more information about the options available for storing ruler state, see [Grafana Mimir ruler: State]({{< relref "../../architecture/components/ruler/index.md#state" >}}).

To get started, the 'local' can be used for initial testing:

```jsonnet
  _config+:: {
    ruler_enabled: true
    ruler_client_type: 'local',
    ruler_local_directory: '/path/to/local/directory',
  }
```

This type is generally not recommended for production use because:

- If sharding rules over multiple ruler replicas, the same file must be available on all ruler pods and kept in sync.
- It is read-only and therefore does not support rule configuration via the API.

If using object storage, additional configuration options are required:

- Amazon S3 (`s3`)

  - `ruler_storage_bucket_name`
  - `aws_region`

- Google Cloud Storage (`gcs`)

  - `ruler_storage_bucket_name`

- Azure (`azure`)
  - `ruler_storage_bucket_name`
  - `ruler_storage_azure_account_name`
  - `ruler_storage_azure_account_key`

Note: Currently the storage credentials for `s3` and `gcs` must be manually provided using additional command line arguments as necessary.

## Operational modes

The ruler has two operational modes, internal and remote. By default, the jsonnet deploys the ruler using the internal operational mode.
For more information about these modes, see [Operational modes]({{< relref "../../architecture/components/ruler/index.md#operational-modes" >}}).

To enable the remote operational mode, add the following to the jsonnet:

```jsonnet
  _config+:: {
    ruler_remote_evaluation_enabled: true
  }
```

Note that to support this operational mode, a separate query path is deployed to evaluate rules, consisting of three additional Kubernetes Deployments:

- `ruler-query-frontend`
- `ruler-query-scheduler`
- `ruler-querier`

### Migration to remote evaluation

To perform a zero downtime migration from internal to remote rule evaluation, follow these steps:

1. Deploy the following changes to enable remote evaluation in migration mode. This will cause the three new Deployments listed to be started, but will not reconfigure the ruler to use them just yet.

   ```jsonnet
     _config+:: {
       ruler_remote_evaluation_enabled: true
       ruler_remote_evaluation_migration_enabled: true
     }
   ```

   Check that all pods for the following deployments have successfully started before moving to the next step:

   - `ruler-query-frontend`
   - `ruler-query-scheduler`
   - `ruler-querier`

1. Deploy the following changes to reconfigure ruler pods to perform remote evaluation.

   ```jsonnet
     _config+:: {
       ruler_remote_evaluation_enabled: true
     }
   ```
