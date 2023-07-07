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
   Interface `querymiddleware.Limits` already existed, so we only needed to add a new interface method:

   ```go
    // ResultsCacheForUnalignedQueryEnabled returns whether to cache results for queries that are not step-aligned
    ResultsCacheForUnalignedQueryEnabled(userID string) bool
   ```

   This interface method may also need to be added to test mocks.

   Then add the implementation of the interface method to the `validation.Overrides` struct in `pkg/util/validation/limits.go`:

   ```go
   func (o *Overrides) ResultsCacheForUnalignedQueryEnabled(userID string) bool {
       return o.getOverridesForUser(user).ResultsCacheForUnalignedQueryEnabled
   }
   ```

4. Decide how to consume the option when multi-tenant paths have different limits.

   Multi-tenant codepaths using limits can involve a mix of tenants which are set to default limits and tenants which have varying tenant-specific overrides.
   These codepaths may need to determine whether to take a largest, smallest, or other value from the varied tenant limits.

   In the example PR, we decided to only cache the request if all tenants have the caching enabled.
   In `pkg/util/validation/limits.go`, we added a helper function:

   ```go
    // AllTrueBooleansPerTenant returns true only if limit func is true for all given tenants
    func AllTrueBooleansPerTenant(tenantIDs []string, f func(string) bool) bool {
        for _, tenantID := range tenantIDs {
            if !f(tenantID) {
                return false
            }
        }
        return true
    }
   ```

   Similar helper functions may already exist for similar purposes: `SmallestPositiveNonZeroIntPerTenant`, `LargestPositiveNonZeroDurationPerTenant`, etc.

5. Consume the overrides with the multiple tenant limits.

   Combining our work from steps 4 and 5 allows us to actually use the limits & overrides wherever they are needed in a multi-tenant codepath:

   ```go
   cacheUnalignedRequests := validation.AllTrueBooleansPerTenant(
       tenantIDs, s.limits.ResultsCacheForUnalignedQueryEnabled,
   )
   ```

## Set the New Config Option to be Backwards Compatible with the Deprecated Option

We need to ensure that a user who is still setting the option in the old config section does not have their setting overridden by the default value for the config option now in the `limits` section.

1. Ensure the config option is set to the default by the old config section.

   In the old config struct's `RegisterFlags` method where we had removed the old flag registration, explicitly set the old config option to the default value.
   In the example PR, this was not technically necessary as an unset boolean field defaults to false, but it is much clearer to be explicit when we are working on a migration and deprecation through many layers of config:

   ```go
   // The query-frontend.cache-unaligned-requests flag has been moved to the limits.go file
   // cfg.DeprecatedCacheUnalignedRequests is set to the default here for clarity
   // and consistency with the process for migrating limits to per-tenant config
   // TODO: Remove in Mimir 2.12.0
   cfg.DeprecatedCacheUnalignedRequests = DefaultDeprecatedCacheUnalignedRequests
   ```

2. Ensure a non-default setting for the deprecated option is carried into the `limits` config.

   If the deprecated option is set to a non-default value and the new limits option is set to the default, we assume the user intends to use the deprecated option and has not yet migrated to use the new option under `limits`.
   Any non-default setting in the `limits` config or tenant-specific overrides in the runtime config will still take precedence.

   In `pkg/mimir/modules.go` in `initRuntimeConfig`:

   ```go
   // DeprecatedCacheUnalignedRequests is moving from a global config that can in the frontend yaml to a limit config
   // We need to preserve the option in the frontend yaml for two releases
   // If the frontend config is configured by the user, the default limit is overwritten
   // TODO: Remove in Mimir 2.12.0
   if t.Cfg.Frontend.QueryMiddleware.DeprecatedCacheUnalignedRequests != querymiddleware.DefaultDeprecatedCacheUnalignedRequests {
       t.Cfg.LimitsConfig.ResultsCacheForUnalignedQueryEnabled = t.Cfg.Frontend.QueryMiddleware.DeprecatedCacheUnalignedRequests
   }
   ```

## Updating Mimir Config File Docs

Run `make doc` from repository root.
