#!/bin/sh
# SPDX-License-Identifier: AGPL-3.0-only
# Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/packaging/deb/control/postinst
# Provenance-includes-license: Apache-2.0
# Provenance-includes-copyright: The Cortex Authors.

# shellcheck disable=SC1091
. "$OS_ENV_DIR/mimir"

if ! command -V systemctl >/dev/null 2>&1; then
  echo "This package only support systemd as init system."
  exit 2
fi

install() {
    printf "\033[32m Post Install of an clean install\033[0m\n"
    [ -z "$MIMIR_USER" ] && MIMIR_USER="mimir"
    [ -z "$MIMIR_GROUP" ] && MIMIR_GROUP="mimir"
    if ! getent group "$MIMIR_GROUP" >/dev/null 2>&1; then
        groupadd -r "$MIMIR_GROUP"
    fi
    if ! getent passwd "$MIMIR_USER" >/dev/null 2>&1; then
        useradd -m -r -g "$MIMIR_GROUP" -d /var/lib/mimir -s /sbin/nologin -c "mimir user" "$MIMIR_USER"
        # useradd command has sometimes a strange behaviour about home directory
        # owner and rights. Let's ensure those are corrects for mimir.
        chmod 0755 /var/lib/mimir
        chown "$MIMIR_USER:$MIMIR_GROUP" /var/lib/mimir
    fi

    chmod 640 /etc/mimir/config.example.yaml
    chown root:"$MIMIR_GROUP" /etc/mimir/config.example.yaml

    printf "\033[32m Reload the service unit from disk\033[0m\n"
    systemctl daemon-reload ||:
    printf "\033[32m Unmask the service\033[0m\n"
    systemctl unmask mimir.service ||:
    printf "\033[32m Set the preset flag for the service unit\033[0m\n"
    systemctl preset mimir.service ||:
    printf "\033[32m Set the enabled flag for the service unit\033[0m\n"
    systemctl enable mimir.service ||:
    systemctl start mimir.service ||:
}

upgrade() {
    printf "\033[32m Post Install of an upgrade\033[0m\n"
    if [ "$RESTART_ON_UPGRADE" = "true" ]; then
        systemctl restart mimir.service ||:
    fi
}

# Step 2, check if this is a clean install or an upgrade
action="$1"
if  [ "$1" = "configure" ] && [ -z "$2" ]; then
  # Alpine linux does not pass args, and deb passes $1=configure
  action="install"
elif [ "$1" = "configure" ] && [ -n "$2" ]; then
    # deb passes $1=configure $2=<current version>
    action="upgrade"
fi

case "$action" in
  "1" | "install")
    install
    ;;
  "2" | "upgrade")
    printf "\033[32m Post Install of an upgrade\033[0m\n"
    upgrade
    ;;
  *)
    # $1 == version being installed
    printf "\033[32m Alpine\033[0m"
    install
    ;;
esac
