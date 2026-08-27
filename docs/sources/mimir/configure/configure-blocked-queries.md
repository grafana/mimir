---
title: Configure queries to block
description: Control what queries are sent to your Mimir installation.
weight: 210
---

# Configure queries to block

In certain situations, you might want to control what queries are being sent to your Mimir installation. These queries
might be intentionally or unintentionally expensive to run, and they might affect the overall stability or cost of running
your service.

Mimir provides two per-tenant mechanisms for this, and you can use both at the same time:

- `blocked_queries` rejects every matching query outright.
- `limited_queries` allows a matching query to run no more often than a configured frequency, and rejects any matching query that arrives sooner.

Blocking is evaluated before rate limiting, so a query that matches both a `blocked_queries` rule and a `limited_queries` rule is reported as blocked.

## Block queries

Each rule has the following attributes:

- `pattern` - required for all rules and is either a complete PromQL query or a regular expression which will be used to match against PromQL queries
- `regex` - a boolean field and must be set to `true` when the above pattern is to be treated as a regular expression. If the above pattern is not a valid pattern a configuration error will occur

For instance, to match all queries, use `pattern: ".*"` with `regex: true`.

Optional filter conditions can be applied to the matching criteria. These will narrow which queries are blocked and note that all configured conditions must be satisfied to block. See `time_range_longer_than`, `step_size_shorter_than`, `unaligned_range_queries` below.

Optional metadata can be set on a block rule. None of these change which queries are blocked, but each one surfaces at runtime, either in the response returned to the caller, in the `query-frontend` logs, or as a metric:

- `reason` - a string which is returned in the HTTP response to the caller. Use it to tell whoever ran the query why it was denied. The reason also appears in the `query-frontend` logs when a query is blocked.
- `expires_at` - a date/time to flag when this rule should be re-evaluated for removal. When set, the [overrides-exporter](../../references/architecture/components/overrides-exporter/) emits the `cortex_blocked_query_rule_expires_at` metric, which you can use to monitor for expired rules. Once the rule is expired it is still enforced, and **it is not automatically removed or de-activated**.
- `id` - a string to give a rule a unique ID. It's logged every time the rule blocks a query, so you can tell which rule matched. When `expires_at` is also set, the `id` is a label on the `cortex_blocked_query_rule_expires_at` metric, where rules that share an `id` are exported as a single series keyed on the earliest `expires_at` among them.

{{< admonition type="note" >}}
The `expires_at` should be used by an operator to mark a rule for future re-consideration. The emitted metric and corresponding warnings serve as the reminder mechanism. Often when a block rule is added it is only temporarily needed. It is important that stale block rules do not accumulate or become forgotten. It is encouraged to always set an `expires_at`.
{{< /admonition >}}

Optional reference fields can be set. These have no functional impact and are for documentation / reference in the configuration file only:

- `created_by` - a string to flag who requested or added this rule
- `created_at` - a date/time to flag when this rule was added
- `note` - a string to track why this rule was added. For instance, it could be set to a support ticket reference or an incident ID

You can block queries using [per-tenant overrides](../about-runtime-configuration/):

```yaml
overrides:
  "tenant-id":
    blocked_queries:
      # exact match
      - pattern: 'sum(rate(node_cpu_seconds_total{env="prod"}[1m]))'

      # regex match with expiry set
      - pattern: '.*env="prod".*'
        regex: true
        expires_at: 2026-12-31T00:00:00Z

      # match all queries longer than 7 days
      - pattern: ".*"
        regex: true
        time_range_longer_than: 7d
        reason: "queries longer than 7 days are not allowed"

      # match expensive queries longer than 1 day
      - pattern: ".*expensive.*"
        regex: true
        time_range_longer_than: 24h
        reason: "expensive queries over 1 day are blocked"

      # match all queries with a step shorter than 1 minute
      - pattern: ".*"
        regex: true
        step_size_shorter_than: 1m
        reason: "step resolution too fine-grained"

      # match this query only when the time range is not a multiple of the step
      - pattern: 'sum(rate(node_cpu_seconds_total{env="prod"}[1m]))'
        unaligned_range_queries: true

      # a rule with additional metadata about why and when it was added
      - pattern: rate(metric[5m])
        regex: false
        reason: High cardinality query - add labels to restrict the scope of this query
        id: fccc06fc73fa9f32fd67f3b37bc82700e8adb3285f77fb5d7df9558a937645e4
        note: "Incident #ABC123"
        created_by: dev@grafana.com
        created_at: 2026-08-26T12:12:18.531385+08:00
        expires_at: 2026-09-26T12:12:18.531385+08:00
```

The blocking is enforced on instant and range queries as well as remote read queries.

For instant and range queries, the pattern is evaluated against the query. For remote read requests, the pattern is evaluated against each set of matchers, as if the matchers formed a vector selector. If any set of matchers is blocked, the whole remote read request is rejected.

Setting `time_range_longer_than` on a rule blocks queries where the time range duration (calculated as `end - start`) exceeds the specified threshold.
`time_range_longer_than` does not apply to instant queries.

Setting `step_size_shorter_than` on a rule blocks queries where the step is shorter than the configured duration.
`step_size_shorter_than` does not apply to instant queries or queries without a step.

Setting `unaligned_range_queries: true` on a rule limits it to range queries where the time range is not a multiple of the step.
Such queries are not eligible for [range query result caching](https://grafana.com/docs/mimir/latest/references/architecture/components/query-frontend/#caching) by default.
This can be useful to discourage unaligned queries without impacting clients that already send aligned requests.
`unaligned_range_queries` does not apply to instant queries, aligned range queries, or remote read requests.

For remote read requests, each set of matchers is evaluated as a vector selector.
For example, a matcher on `__name__` regex-matched to `foo.*` is interpreted as `{__name__=~"foo.*"}`.
To restrict the blocking to such selectors, use a regex pattern with the curly braces escaped, e.g. `pattern: '\{.*foo.*\}'` with `regex: true`.

To set up runtime overrides, refer to [runtime configuration](../about-runtime-configuration/).

{{< admonition type="note" >}}
The order of patterns is preserved, so the first matching pattern will be used.
{{< /admonition >}}

## Rate limit queries

{{< admonition type="note" >}}
Rate limiting queries with `limited_queries` is an experimental feature.
{{< /admonition >}}

Use the `limited_queries` limit when a query should only be allowed to run once per configured time period.

Rate limiting requires the `query-frontend` results cache. Enable it with `-query-frontend.cache-results`.

Each rule has the following attributes:

- `query` - required for all rules and must be a complete PromQL query. Note that there is no support for partial expressions or regular expressions
- `allowed_frequency` - a duration field which defines the minimum time between two runs of this query. A matching query that arrives sooner is rejected. Being rejected doesn't extend the window: the next run is allowed `allowed_frequency` after the last query that was let through, not after the last attempt

As per [block queries](#block-queries), the same optional metadata (`reason`, `expires_at`, `id`) are supported and also the same optional reference fields (`created_by`, `created_at`, `note`).

When `expires_at` is set, the [overrides-exporter](../../references/architecture/components/overrides-exporter/) emits the `cortex_limited_query_rule_expires_at` metric, which you can use to monitor for expired rules.

You can limit queries using [per-tenant overrides](../about-runtime-configuration/):

```yaml
overrides:
  "tenant-id":
    limited_queries:
      # allow this query to run at most once a minute
      - query: 'sum(rate(node_cpu_seconds_total{env="prod"}[1m]))'
        allowed_frequency: 1m
        reason: "this query is expensive and doesn't need to run more than once a minute"
```

Rate limiting is enforced on instant and range queries. Unlike blocking, it isn't enforced on remote read requests.

When a matching query arrives less than `allowed_frequency` after the previous matching query was allowed, Mimir rejects it with HTTP status 429 and the [`err-mimir-query-limited`](../../manage/mimir-runbooks/#err-mimir-query-limited) error.

`limited_queries` matches the query text exactly, ignoring only leading and trailing whitespace. The `query` rule must be in the canonicalized format of the query to be limited.

See [Format queries to block or limit](#format-queries-to-block-or-limit) for using `mimirtool` to get a canonical version of a query.

If more than one rule matches a query, Mimir enforces the matching rule with the longest `allowed_frequency`, and reports that rule's `reason` and `id`.

## Format queries to block or limit

**This section is important when using a regular expression block rule or a limited query rule.**

When you observe a `param_query=...` in the `query-frontend` `query stats` log line, the PromQL is what was sent by the operator. Before the blocking and/or limiting rules are applied, this query will be canonicalized. This may result in the query being transformed.

As such, **a regular expression pattern or limited query rule must be constructed to match the canonicalized version of a query**.

Use Mimirtool's `mimirtool promql format <query>` command to apply the Prometheus formatter to a query that is expected to be blocked or limited, and then check the formatted query will match.

Among other transformations the Prometheus formatter may reorder operators, remove empty selector braces, and eliminate newlines, extraneous whitespace, and comments.

### Formatted query examples

Empty selector braces removed:

```bash
mimirtool promql format 'foo{}'
```

```console
foo
```

Operators reordered:

```bash
mimirtool promql format 'sum(container_memory_rss) by (namespace)'
```

```console
sum by (namespace) (container_memory_rss)
```

Newlines, extra whitespace, and comments eliminated:

```bash
mimirtool promql format '
rate(
  metric_counter[15m] # comment 1
) /
rate(
  other_counter[15m] # comment 2
)
'
```

```console
rate(metric_counter[15m]) / rate(other_counter[15m])
```

## Track expiring rules

`expires_at` is informational only. Mimir never stops enforcing a rule because it has expired: an expired rule keeps blocking or rate limiting matching queries until you remove it from the tenant's configuration.
Use it so that rules added for a temporary reason, such as mitigating an incident, aren't forgotten indefinitely.

The [overrides-exporter](../../references/architecture/components/overrides-exporter/) exports the expiry of each rule as:

- `cortex_blocked_query_rule_expires_at{user, id}`
- `cortex_limited_query_rule_expires_at{user, id}`

The value is the Unix timestamp of the earliest `expires_at` among that tenant's rules that share the same `id`. Rules without an `id` are grouped together under an empty `id`.

The `MimirBlockedQueryRuleExpired` and `MimirLimitedQueryRuleExpired` alerts fire when a tenant has at least one expired rule. Refer to [MimirBlockedQueryRuleExpired](../../manage/mimir-runbooks/#MimirBlockedQueryRuleExpired) and [MimirLimitedQueryRuleExpired](../../manage/mimir-runbooks/#MimirLimitedQueryRuleExpired).
The **Query blocking and rate limiting** row on the **Mimir / Queries** dashboard shows the number of expired rules per tenant.

## Observe blocked and rate limited queries

Blocked queries are logged at info level with `msg="query blocked"`, including the query text (`query`), the query duration (`query_duration_ms`), the step (`step_ms`), the position of the matching rule in the tenant's `blocked_queries` list (`index`), the rule's `id` and `reason`, and whether the rule's `expires_at` has passed (`expired`).

Rate limited queries are logged at info level with `msg="query limited"`, including the query text (`query`), the rule's `id` and `reason`, the `allowed_frequency` that was enforced, and whether the rule's `expires_at` has passed (`expired`).

Use these fields to identify which rule matched and why.

Both are counted in the `cortex_query_frontend_rejected_queries_total` metric per tenant (`user` label), with `reason="blocked"` for blocked queries and `reason="limited"` for rate limited ones.

To see the rate of rejected queries by tenant:

```promql
sum by (user, reason) (rate(cortex_query_frontend_rejected_queries_total[$__interval]))
```

For the errors returned to clients, refer to [`err-mimir-query-blocked`](../../manage/mimir-runbooks/#err-mimir-query-blocked) and [`err-mimir-query-limited`](../../manage/mimir-runbooks/#err-mimir-query-limited).

## Troubleshoot invalid configuration

Invalid rules are rejected when the runtime configuration is loaded. The behaviour differs depending on whether the invalid configuration is present at startup or introduced at runtime.

**At startup**, Mimir logs an error at ERROR level and exits, resulting in a crash loop in Docker Compose or Kubernetes until the configuration is corrected. Search for `msg="module failed"` with `module=runtime-config` in your logs:

Missing `pattern` field:

```
level=error msg="module failed" module=runtime-config err="starting module runtime-config: invalid service state: Failed, expected: Running, failure: failed to load runtime config: load file: tenant \"anonymous\": blocked_queries[0]: pattern is required"
```

Invalid regex pattern with `regex: true`:

```
level=error msg="module failed" module=runtime-config err="starting module runtime-config: invalid service state: Failed, expected: Running, failure: failed to load runtime config: load file: tenant \"anonymous\": blocked_queries[0]: invalid regex pattern \"[a-9}\": error parsing regexp: invalid character class range: `a-9`"
```

**At runtime**, Mimir rejects the updated config and continues running with the previous valid configuration. Search for `msg="failed to load config"` in your logs:

```
level=error msg="failed to load config" err="load file: tenant \"anonymous\": blocked_queries[1]: invalid regex pattern \"[a-9}\": error parsing regexp: invalid character class range: `a-9`"
```

In both cases, the `err` field identifies the tenant, rule index, and the specific problem. Correct or remove the invalid rule.
