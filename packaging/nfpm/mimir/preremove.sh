#!/bin/sh
# SPDX-License-Identifier: AGPL-3.0-only
# Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/packaging/deb/control/prerm
# Provenance-includes-license: Apache-2.0
# Provenance-includes-copyright: The Cortex Authors.

if ! command -V systemctl >/dev/null 2>&1; then
  echo "This package only support systemd as init system."
  exit 2
fi

uninstall() {
    systemctl stop mimir.service ||:
}

upgrade() {
    :
}

purge() {
    :
}

# Step 2, check if this is a clean install or an upgrade
action="$1"

case "$action" in
  "0" | "remove")
    uninstall
    ;;
  "1" | "upgrade")
    upgrade
    ;;
  "purge")
    purge
    ;;
  *)
    echo "Unsupported action: ${action}. ignoring."
    ;;
esac
