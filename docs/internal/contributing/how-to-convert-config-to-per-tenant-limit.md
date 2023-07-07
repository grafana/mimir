# Converting a Mimir Config Option to a Per-Tenant Limit

When running Mimir with multi-tenancy enabled, usage patterns among tenants may diverge to the point that an option needs to be configured independently for different tenants.

The values set in the `limits` section of the [Mimir config file](https://grafana.com/docs/mimir/latest/references/configuration-parameters/#limits) serve as global config for all tenants.
These global limits can then be overridden for specified tenants via the `overrides` section of the [runtime configuration](https://grafana.com/docs/mimir/latest/configure/about-runtime-configuration/#runtime-configuration-of-per-tenant-limits).

If a config option is not present in the `limits` section, it cannot be overridden on a per-tenant basis.
If the need arises to override an existing config option for individual tenants, we must follow a deprecation process to move the option into the `limits` section.

## Deprecation and Conversion Process

We need to ensure that:

- the existing config option is deprecated
- the new limits config option is added
- the new limits config option can be set and overridden in a backwards-compatible manner until deprecation is complete
- all docs are updated

We will use the [Mimir PR #5312](https://github.com/grafana/mimir/pull/5312) as a reference example, in which we moved the query-frontend option `cache_unaligned_requests` from the `frontend` config section to `limits`.
[PR #4287](https://github.com/grafana/mimir/pull/4287) is also available for reference.

## Deprecating the Existing Config Option

1. Move the config option description JSON object from its previous location to the "limits" block of `cmd/mimir/config-descriptor.json`.

2. Locate the actual config option in code and mark it as deprecated by renaming the variable and marking it as `hidden` and `deprecated`.

   In the example PR, the existing config option was field of the `querymiddleware.Config` struct, declared in `pkg/frontend/querymiddleware/roundtrip.go`.

   The line

   ```go
   CacheUnalignedRequests bool `yaml:"cache_unaligned_requests" category:"advanced"`
   ```

   was updated to

   ```go
   DeprecatedCacheUnalignedRequests bool `yaml:"cache_unaligned_requests" category:"advanced" doc:"hidden"` // Deprecated: Deprecated in Mimir 2.10.0, remove in Mimir 2.12.0 (https://github.com/grafana/mimir/issues/5253)
   ```

3. Remove the binding of the CLI flag from the deprecated config option.
   The same flag will be bound to the new config option in the next step.

   Binding CLI flags to the config option occurs in the respective config struct's `RegisterFlags` method.
   In the example PR, the following line was removed:

   ```go
   f.BoolVar(&cfg.DeprecatedCacheUnalignedRequests, "query-frontend.cache-unaligned-requests", false, "Cache requests that are not step-aligned.")
   ```

## Adding the New Config Option

1. Add the new config option to the `validation.Limits` struct in `pkg/util/validation/limits.go`.

   Add it the section for related configuration options if one exists.
   The new struct field may need a more descriptive name as is now a member of a more generalized config section.

   In the example, the previous field name under `querymiddleware.Config` was `CacheUnalignedRequests`.
   The new field is named `ResultsCacheForUnalignedQueryEnabled`:

   ```go
   ResultsCacheForUnalignedQueryEnabled bool `yaml:"cache_unaligned_requests" json:"cache_unaligned_requests" category:"advanced"`
   ```

2. Add the binding of the CLI flag to the Limits config.

   Again, group it with other related options.
   Ensure the same default value is maintained.

   In `validation.Limits` struct's `RegisterFlags` method:

   ```go
   f.BoolVar(&l.ResultsCacheForUnalignedQueryEnabled, "query-frontend.cache-unaligned-requests", false, "Cache requests that are not step-aligned.")
   ```

3. Expose the tenant-specific overrides for the new config option

   First add the tenant-specifc method and docstring to the _interface_ which uses the config option.
   The interface definition - which may need to be created - should be named `Limits` and define all related per-tenant config options.

   In the example PR, the config option was used in `pkg/frontend/querymiddleware/limits.go`.
   Interface `querymiddleware.Limits` already existed, so we just added a new interface method:

   ```go
    // ResultsCacheForUnalignedQueryEnabled returns whether to cache results for queries that are not step-aligned
    ResultsCacheForUnalignedQueryEnabled(userID string) bool
   ```

   Then add the implementation of the interface method to the `validation.Overrides` struct in `pkg/util/validation/limits.go`:

   ```go
   func (o *Overrides) ResultsCacheForUnalignedQueryEnabled(userID string) bool {
       return o.getOverridesForUser(user).ResultsCacheForUnalignedQueryEnabled
   }
   ```
