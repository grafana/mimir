---
title: "Enable exemplars in Grafana Mimir"
description: ""
weight: 10
---

# Enable exemplars in Grafana Mimir

You can enable exemplar storage in Grafana Mimir and view the resulting data in Grafana. While exemplars can be enabled for all tenants at once or for only specific tenants, we recommend enabling and enforcing limits for exemplars for only specific tenants.

## Adjust per tenant exemplar limits via the UI

1. Open Grafana Mimir in your Grafana instance.
1. In the tenants' view, select the tenant where you want to adjust the exemplar limit.
1. Click the "edit" icon for the `limit max_global_exemplars_per_user` parameter to edit the value. We recommend starting with a relatively low number (10,000) and adjusting it if needed.

The new limit takes effect automatically in all Grafana Mimir components in five (5) minutes.

## Enable exemplars globally

1. Open the Grafana Mimir configuration file.
1. In the limits [config section]({{< relref "../../configuration/reference-configuration-parameters.md#limits_config" >}}), set the `max_global_exemplars_per_user` value. We recommend starting with a relatively low number (10,000) and adjusting it if needed.
1. Restart all Grafana Mimir for the changes to effect.

See also:

- [Before you begin]({{< relref "./prereq-exemplars.md" >}})
- [View exemplar data]({{< relref "./view-exemplars.md" >}})
