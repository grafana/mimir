# Mimirtool Github Action

This action is used to lint, prepare, verify, diff, and sync rules to a [Grafana Mimir](https://github.com/grafana/mimir) cluster.

## Environment Variables

This action is configured using environment variables defined in the workflow. The following variables can be configured.

| Name                         | Description                                                                                                                                                                                                                                                                                        | Required | Default |
| ---------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------- | ------- |
| `MIMIR_ADDRESS`              | URL address for the target Mimir cluster                                                                                                                                                                                                                                                           | `false`  | N/A     |
| `MIMIR_TENANT_ID`            | ID for the desired tenant in the target Mimir cluster. Used as the username under HTTP Basic authentication.                                                                                                                                                                                       | `false`  | N/A     |
| `MIMIR_API_KEY`              | Optional password that is required for password-protected Mimir clusters. An encrypted [github secret](https://help.github.com/en/actions/automating-your-workflow-with-github-actions/creating-and-using-encrypted-secrets) is recommended. Used as the password under HTTP Basic authentication. | `false`  | N/A     |
| `MIMIR_AUTH_TOKEN`           | Optional bearer or JWT token that is required for Mimir clusters authenticating by bearer or JWT token. An encrypted [github secret](https://help.github.com/en/actions/automating-your-workflow-with-github-actions/creating-and-using-encrypted-secrets) is recommended.                         | `false`  | N/A     |
| `MIMIR_EXTRA_HEADERS`        | Newline-separated list of header=value pairs to add to the request. (for `mimirtool rules` command only)                                                                                                                                                                                           | `false`  | N/A     |
| `ACTION`                     | Which action to take. One of `lint`, `prepare`, `check`, `diff` or `sync`                                                                                                                                                                                                                          | `true`   | N/A     |
| `RULES_DIR`                  | Comma-separated list of directories to walk in order to source rules files                                                                                                                                                                                                                         | `false`  | `./`    |
| `LABEL`                      | Label to include as part of the aggregations. This option is supported only by the `prepare` action.                                                                                                                                                                                               | `false`  | N/A     |
| `LABEL_EXCLUDED_RULE_GROUPS` | Comma separated list of rule group names to exclude when including the configured label to aggregations. This option is supported only by the `prepare` action.                                                                                                                                    | `false`  | N/A     |
| `NAMESPACES`                 | Comma-separated list of namespaces to use during a sync or diff.                                                                                                                                                                                                                                   | `false`  | N/A     |
| `NAMESPACES_REGEX`           | Regex matching namespaces to check during a sync or diff.                                                                                                                                                                                                                                          | `false`  | N/A     |
| `IGNORED_NAMESPACES`         | Comma-separated list of namespaces to ignore during a sync of diff.                                                                                                                                                                                                                                | `false`  | N/A     |
| `IGNORED_NAMESPACES_REGEX`   | Regex matching namespaces to ignore during a sync or diff.                                                                                                                                                                                                                                         | `false`  | N/A     |

> NOTE: Only one of the namespace selection inputs (`NAMESPACES`, `NAMESPACES_REGEX`, `IGNORED_NAMESPACES`, `INGORED_NAMESPACES_REGEX`) can be provided.

## Authentication

This GitHub Action uses [`mimirtool`](https://github.com/grafana/mimir) under the hood.
`mimirtool` uses HTTP Basic authentication against a Mimir cluster. The variable `MIMIR_TENANT_ID` is used as the username and `MIMIR_API_KEY` as the password.

## Actions

All actions will crawl the specified `RULES_DIR` for Prometheus rules and alerts files with a `yaml`/`yml` extension.

### `diff`

Outputs the differences in the configured files and the currently configured ruleset in a Mimir cluster. It will output the required operations in order to make the running Mimir cluster match the rules configured in the directory. It will **not create/update/delete any rules** currently running in the Mimir cluster.

### `sync`

Reconcile the differences with the sourced rules and the rules currently running in a configured Mimir cluster. It **will create/update/delete rules** currently running in Mimir to match what is configured in the files in the provided directory.

### `lint`

Lints a rules file(s). The linter's aim is not to verify correctness but to fix YAML and PromQL expression formatting within the rule file(s). The linting happens in-place within the specified file(s). Does not interact with your Mimir cluster.

### `prepare`

Prepares a rules file(s) for upload to Mimir. It lints all your PromQL expressions and adds a `cluster` label to your PromQL query aggregations in the file. Prepare modifies the file(s) in-place. Does not interact with your Mimir cluster.

### `check`

Checks rules file(s) against the recommended [best practices](https://prometheus.io/docs/practices/rules/) for rules. Does not interact with your Mimir cluster.

### `print`

fetch & print rules from the Mimir cluster.

## Outputs

### `summary`

The `summary` output variable is a string denoting the output summary of the action, if there is one.

### `detailed`

The `detailed` output variable returned by this action is the full output of the command executed.

## Example Workflows

### Pull Request Diff

The following workflow will run a diff on every pull request against the repo and print the summary as a comment in the associated pull request:

```yaml
name: diff_rules_pull_request
on: [pull_request]
jobs:
  diff-pr:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout
        uses: actions/checkout@v2
      - name: Diff Rules
        id: diff_rules
        uses: grafana/mimir/operations/mimir-rules-action@main
        env:
          MIMIR_ADDRESS: https://example-cluster.com/
          MIMIR_TENANT_ID: 1
          MIMIR_API_KEY: ${{ secrets.MIMIR_API_KEY }} # Encrypted Github Secret https://help.github.com/en/actions/automating-your-workflow-with-github-actions/creating-and-using-encrypted-secrets
          ACTION: diff
          RULES_DIR: "./rules/" # In this example rules are stored in a rules directory in the repo
      - name: comment PR
        uses: unsplash/comment-on-pr@v1.2.0 # https://github.com/unsplash/comment-on-pr
        env:
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
        with:
          msg: "${{ steps.diff_rules.outputs.summary }}" # summary could be replaced with detailed for a more granular view
```

### Master Sync

The following workflow will sync the rule files in the `master` branch with the configured Mimir cluster.

```yaml
name: sync_rules_master
on:
  push:
    branches:
      - master
jobs:
  sync-master:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout
        uses: actions/checkout@v2
        with:
          ref: master
      - name: sync-rules
        uses: grafana/mimir/operations/mimir-rules-action@main
        env:
          MIMIR_ADDRESS: https://example-cluster.com/
          MIMIR_TENANT_ID: 1
          MIMIR_API_KEY: ${{ secrets.MIMIR_API_KEY }} # Encrypted Github Secret https://help.github.com/en/actions/automating-your-workflow-with-github-actions/creating-and-using-encrypted-secrets
          ACTION: sync
          RULES_DIR: "./rules/" # In this example rules are stored in a rules directory in the repo
```
