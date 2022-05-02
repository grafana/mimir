#!/bin/sh
# SPDX-License-Identifier: AGPL-3.0-only
# Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/packaging/fpm/package.sh
# Provenance-includes-license: Apache-2.0
# Provenance-includes-copyright: The Cortex Authors.

set -eu

SRC_PATH=/go/src/github.com/grafana/mimir

# If we run make directly, any files created on the bind mount
# will have awkward ownership.  So we switch to a user with the
# same user and group IDs as source directory.  We have to set a
# few things up so that sudo works without complaining later on.
uid=$(stat -c "%u" $SRC_PATH)
gid=$(stat -c "%g" $SRC_PATH)
echo "mimir:x:$uid:$gid::$SRC_PATH:/bin/sh" >>/etc/passwd
echo "mimir:*:::::::" >>/etc/shadow
echo "mimir	ALL=(ALL)	NOPASSWD: ALL" >>/etc/sudoers

su mimir -c "PATH=$PATH make -C $SRC_PATH PACKAGE_IN_CONTAINER=false $*"
