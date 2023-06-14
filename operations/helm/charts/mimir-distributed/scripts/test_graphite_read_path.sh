#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# vim: ai:ts=8:sw=8:noet
# This script will perform a curl to the graphite tags endpoint to make sure
# that the graphite read path works.
# It requires:
# - Mimir gateway to be port forwarded to port 8080
# - Basic auth header to be previously exported at MIMIR_BASIC_AUTH

export SHELLOPTS        # propagate set to children by default
IFS=$'\t\n'
umask 0077

MIMIR_GATEWAY_HOST="localhost"

if [[ -z "${MIMIR_BASIC_AUTH}" ]]; then
  echo "Environment variable MIMIR_BASIC_AUTH is not set. Exiting..."
  exit 1
fi

echo "# Getting tags"
curl \
  -X GET \
  -H "Authorization: Basic ${MIMIR_BASIC_AUTH}" \
  -H "Content-Type: application/json" \
  --fail-with-body \
  "${MIMIR_GATEWAY_HOST}:8080/graphite/tags"

echo ""
echo ""

echo "# Getting functions"
# this will return 400 when using values.yaml as graphite-web is not enabled there
curl \
  -X GET \
  -H "Authorization: Basic ${MIMIR_BASIC_AUTH}" \
  -H "Content-Type: application/json" \
  --fail-with-body \
  "${MIMIR_GATEWAY_HOST}:8080/graphite/functions"

echo ""

echo "# Running query"
curl \
  -X GET \
  -H "Authorization: Basic ${MIMIR_BASIC_AUTH}" \
  -H "Content-Type: application/json" \
  --fail-with-body \
  "${MIMIR_GATEWAY_HOST}:8080/graphite/render?target=seriesByTag(\"service=foo\")&from=-5m"

echo ""
echo ""
