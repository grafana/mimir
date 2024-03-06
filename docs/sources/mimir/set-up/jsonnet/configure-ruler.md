---
aliases:
  - ../../operators-guide/deploying-grafana-mimir/jsonnet/configuring-ruler/
  - configuring-ruler/
  - ../../operators-guide/deploy-grafana-mimir/jsonnet/configure-ruler/
description: Learn how to configure the Grafana Mimir ruler when using Jsonnet.
menuTitle: Configure ruler
title: Configure the Grafana Mimir ruler with Jsonnet
weight: 20
---

# Configure the Grafana Mimir ruler with Jsonnet

The ruler is an optional component and is therefore not deployed by default when using Jsonnet.
For more information about the ruler, see [Grafana Mimir ruler]({{< relref "../../references/architecture/components/ruler" >}}).

To enable it, add the following Jsonnet code to the `_config` section:

```jsonnet
{
  _config+:: {
    ruler_enabled: true,
    ruler_storage_bucket_name: 'ruler-bucket-name',
  },
}
```

By default, the object storage backend used for the ruler will be the one set by the `$._config.storage_backend` option.
If desired, you can override it by setting the `$._config.ruler_storage_backend` option.
The `ruler_storage_backend` option must be one of either `local`, `azure`, `gcs`, or `s3`.
For more information about the options available for storing ruler state, see [Grafana Mimir ruler: State]({{< relref "../../references/architecture/components/ruler#state" >}}).

To get started, use the `local` client type for initial testing:

```jsonnet
{
  _config+:: {
    ruler_enabled: true,
    ruler_storage_backend: 'local',
    ruler_local_directory: '/path/to/local/directory',
  },
}
```

If you are using object storage, you must set `ruler_storage_bucket_name` to the name of the bucket that you want to use.

{{< admonition type="note" >}}
If ruler object storage credentials differ from the ones defined in the common section, you need to manually provide them by using additional command line arguments.
For more information, refer to [Grafana Mimir configuration parameters: ruler_storage]({{< relref "../../configure/configuration-parameters#ruler_storage" >}}).
{{< /admonition >}}

## Operational modes

The ruler has two operational modes: _internal_ and _remote_. By default, the Jsonnet deploys the ruler by using the internal operational mode.
For more information about these modes, see [Operational modes]({{< relref "../../references/architecture/components/ruler#operational-modes" >}}).

To enable the remote operational mode, add the following code to the Jsonnet:

```jsonnet
{
  _config+:: {n
    ruler_remote_evaluation_enabled: true,
  },
}
```

{{< admonition type="note" >}}
To support the _remote_ operational mode, the configuration includes three additional Kubernetes Deployments as a separate query path.

These are:

- `ruler-query-frontend`
- `ruler-query-scheduler`
- `ruler-querier`

{{< /admonition >}}

### Migrate to remote evaluation

To perform a zero downtime migration from internal to remote rule evaluation, follow these steps:

1. Deploy the following changes to enable remote evaluation in migration mode.
   Doing so causes the three new and previously listed Kubernetes deployments to start. However, they will not reconfigure the ruler to use them just yet.

   ```jsonnet
   {
    _config+:: {
      ruler_remote_evaluation_enabled: true,
      ruler_remote_evaluation_migration_enabled: true,
    },
   }
   ```

1. Check that all of pods for the following deployments have successfully started before moving to the next step:

   - `ruler-query-frontend`
   - `ruler-query-scheduler`
   - `ruler-querier`

1. Reconfigure the ruler pods to perform remote evaluation, by deploying the following changes:

   ```jsonnet
   {
     _config+:: {
       ruler_remote_evaluation_enabled: true,
     },
   }
   ```
