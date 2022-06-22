#!/bin/bash

set -e

cd "$(dirname $0)"

./compare-kustomize-outputs.sh ./helm/09-config ./jsonnet/09-config
