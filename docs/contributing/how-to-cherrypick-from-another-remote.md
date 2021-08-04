---
title: "How to cherry-pick from another remote"
linkTitle: "How to cherry-pick from another remote"
weight: 4
slug: how-to-cherrypick-from-another-remote
---

To cherry-pick a commit from another remote, typically related to a PR:

1. Find the commit you want on the other remote, typically the merge commit of a certain PR, and note it as `$COMMIT`
1. In your local clone of [Mimir](https://github.com/grafana/mimir), add the remote: `git remote add other $REMOTE_URL`
1. Synchronize your Mimir clone with the new remote: `git fetch --all --prune`
1. Note the remote name corresponding to [Mimir](https://github.com/grafana/mimir) on GitHub, e.g. `origin`, as `$REMOTE`
1. Switch to a new branch, f.ex.: `git switch -c chore/cherrypick-${COMMIT} ${REMOTE}/main`
1. Cherry-pick the desired commit: `git cherry-pick -x $COMMIT` (if any merge conflicts, resolve them and commit)
1. Push your branch to GitHub: `git push -u $REMOTE chore/cherrypick-${COMMIT}`
1. Make a PR to merge your branch to main
