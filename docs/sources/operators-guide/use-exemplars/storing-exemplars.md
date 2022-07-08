---
aliases:
  - /docs/mimir/latest/operators-guide/using-exemplars/storing-exemplars/
description: Learn how to store exemplars in Grafana Mimir.
menuTitle: Storing exemplars
title: Storing exemplars in Grafana Mimir
weight: 30
---

# Storing exemplars in Grafana Mimir

You can enable exemplar storage in Grafana Mimir and view the resulting data in Grafana.
While exemplars can be enabled for all tenants at once or for only specific tenants, we recommend enabling and enforcing limits for exemplars for only specific tenants.

## Enable exemplars globally

1. In the Grafana Mimir configuration file, set the `limits.max_global_exemplars_per_user` value.
   Start with a relatively low number (100,000) and adjust it if needed.

   A partial Grafana Mimir configuration file with `max_global_exemplars_per_user` set globally looks as follows:

   ```yaml
   limits:
     max_global_exemplars_per_user: 100000
   ```

1. Save and deploy the configuration file.
1. Perform a rolling update of Grafana Mimir components.

## Enable exemplars for a specific tenant

1. Ensure Grafana Mimir uses a runtime configuration file by verifying that the flag `-runtime-config.file` is set to a non-null value.
   For more information about supported runtime configuration, refer to [Runtime configuration file]({{< relref "../configure/about-runtime-configuration.md" >}})
1. In the runtime configuration file, set the `overrides.<TENANT>.max_global_exemplars_per_user` value.
   Start with a relatively low number (100,000) and adjust it if needed.

   A partial runtime configuration file with `max_global_exemplars_per_user` set for a tenant called "tenant-a" would look as follows:

   ```yaml
   overrides:
     "tenant-a":
       max_global_exemplars_per_user: 100000
   ```

1. Save and deploy the runtime configuration file.

After the `-runtime-config.reload-period` has elapsed, components reload the runtime configuration file and use the updated configuration.
