# Documentation

## Test Mimir and Helm chart documentation together locally

Testing the documentation accurately requires a copy of Mimir documentation at the target version of the Helm chart.

One way to do this is to use a git worktree to checkout the version branch of Mimir that matches the Helm chart target version.
For example, if the Helm chart target version is `v2.9.x`, checkout the `release-2.9` branch:

```console
$ git worktree add --checkout "$(git rev-parse --show-toplevel)/../mimir-v2.9.x" origin/release-2.9
Preparing worktree (detached HEAD 761114d8b)
HEAD is now at 761114d8b Bump version to 2.9.0 (#5289)
```
