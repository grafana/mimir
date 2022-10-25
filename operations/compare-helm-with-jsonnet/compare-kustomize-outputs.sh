#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only

set -e
set -o pipefail

command -v kustomize >/dev/null 2>&1 || { echo "Please install kustomize. Visit https://kubectl.docs.kubernetes.io/installation/kustomize/ for more info."; exit 1; }

lhs=$1
rhs=$2
filter=${3:-'.'}

cd "$(dirname $0)"

[[ -d scratch ]] && rm -r scratch

mkdir -p scratch/$lhs
mkdir -p scratch/$rhs

KUSTOMIZE_BUILD=(kustomize build --enable-alpha-plugins --enable-exec --load-restrictor LoadRestrictionsNone)

${KUSTOMIZE_BUILD[@]} $lhs | yq -s "\"scratch/$lhs/\" + .metadata.name + \"-\" + .kind" "select(.kind != null) | $filter"
${KUSTOMIZE_BUILD[@]} $rhs | yq -s "\"scratch/$rhs/\" + .metadata.name + \"-\" + .kind" "select(.kind != null) | $filter"

# difft --missing-as-empty --skip-unchanged scratch/$lhs scratch/$rhs
diff -r -u -N scratch/$lhs scratch/$rhs
