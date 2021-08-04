---
title: "How to cherry-pick a PR from Cortex"
linkTitle: "How to cherry-pick a PR from Cortex"
weight: 4
slug: how-to-cherrypick-from-cortex
---

To cherry-pick a PR from Cortex:

1. Find the [Cortex](https://github.com/cortexproject/cortex) PR in question and note its merge commit
1. In your local clone of [Mimir](https://github.com/grafana/mimir), add a Cortex remote: `git remote add cortex https://github.com/cortexproject/cortex.git`
1. Synchronize your Mimir clone with Cortex: `git fetch --all --prune`
1. Note the remote name corresponding to [Mimir](https://github.com/grafana/mimir) on GitHub, e.g. `origin`, as `$ORIGIN`.
1. Switch to a new branch, f.ex.: `git switch -c chore/cherrypick-cortex-${COMMIT} ${ORIGIN}/main`
1. Cherry-pick the Cortex merge commit: `git cherry-pick -x $COMMIT` (if any merge conflicts, resolve them and commit)
1. Push your branch to GitHub: `git push -u $ORIGIN chore/cherrypick-cortex-${COMMIT}`
1. Make a PR to merge your branch to main, but make sure not to reference neither the related Cortex issue nor PR (since that could cause references back to Mimir in the Cortex GitHub repo)
