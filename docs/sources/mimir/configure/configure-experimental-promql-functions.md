---
title: Configure experimental PromQL functions
description: Control what experimental PromQL functions are enabled for your Mimir installation.
weight: 220
---

# Configure experimental PromQL functions

Experimental PromQL functions are disabled by default in Mimir. You can selectively enable them per-tenant or globally for all users.
You can enable all functions using the special function name `all`, or you can specifically enable individual functions.

You can enable functions globally for all users using the main Mimir configuration file.

```yaml
limits:
  enabled_promql_experimental_functions: info,sort_by_label,sort_by_label_desc
```

You can enable functions for specific tenants using [per-tenant overrides](../about-runtime-configuration/):

```yaml
overrides:
  "tenant-id-1":
    # Enable only specific functions for this tenant
    enabled_promql_experimental_functions: sort_by_label,sort_by_label_desc
  "tenant-id-2":
    # Enable all experimental functions for this tenant
    enabled_promql_experimental_functions: all
```

Blocking or allowing the use of experimental PromQL functions is enforced on instant and range queries.
When blocked an error is returned when running a query that includes the name of the experimental function.

```
 function "sort_by_label" is not enabled
```
