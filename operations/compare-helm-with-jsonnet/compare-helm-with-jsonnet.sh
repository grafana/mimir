#!/bin/bash

set -e

cd "$(dirname $0)"

./compare-kustomize-outputs.sh ./helm/08-config ./jsonnet/08-config
