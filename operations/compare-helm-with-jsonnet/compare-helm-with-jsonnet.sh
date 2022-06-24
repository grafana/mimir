#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only

set -e

cd "$(dirname $0)"

./compare-kustomize-outputs.sh ./helm/08-config ./jsonnet/08-config
