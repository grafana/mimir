#!/bin/sh
set -eu

case "$*" in
    "") check=false ;;
    --check) check=true ;;
    --check-upstream) check=true ;;
    *) echo "usage: sh generate.sh [--check|--check-upstream]" >&2; exit 2 ;;
esac

cd "$(dirname "$0")"
if [ "${1:-}" = --check-upstream ]; then
    upstream_root=$(go env GOROOT)/src/encoding/json
    for upstream in _upstream/*.go; do
        name=${upstream##*/}
        if ! cmp -s "$upstream" "$upstream_root/$name"; then
            echo "$upstream differs from $upstream_root/$name; review the vendored encoder against the selected Go release" >&2
            exit 1
        fi
    done
fi
work=$(mktemp -d "${TMPDIR:-/tmp}/antithesis-json.XXXXXXXX")
trap 'rm -rf "$work"' EXIT HUP INT TERM
cp _upstream/*.go "$work/"
patch --batch --fuzz=0 -s -d "$work" -p1 < sdk.patch

for upstream in _upstream/*.go; do
    name=${upstream##*/}
    if "$check"; then
        if ! cmp -s "$work/$name" "$name"; then
            echo "$name differs from upstream plus sdk.patch; run go generate ./internal/json" >&2
            exit 1
        fi
    else
        cp "$work/$name" "$name"
    fi
done
