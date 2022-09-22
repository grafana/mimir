#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# vim: ai:ts=8:sw=8:noet
# This script will perform a curl to the graphite metrics endpoint to make sure
# that the graphite write path works.
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

echo "# Pushing metrics "
timestamp=$(date '+%s')

data='[{
"time": REPLACE_DATE,
"name": "uplink.speed_test.ping_ms",
"interval": 3600,
"value": 58080231,
"tags": ["service=foo", "server=bar", "test=foo"]
}, {
"time": REPLACE_DATE,
"name": "uplink.speed_test.down_bits",
"interval": 3600,
"value": 23,
"tags": ["service=foo", "server=bar"]
}, {
"time": REPLACE_DATE,
"name": "uplink.speed_test.up_bits",
"interval": 3600,
"value": 52058673,
"tags": ["service=foo", "server=bar"]
}]'

data=${data//REPLACE_DATE/${timestamp}}

curl \
  -X POST \
  -H "Authorization: Basic ${MIMIR_BASIC_AUTH}" \
  -H "Content-Type: application/json" \
  --fail-with-body \
  "${MIMIR_GATEWAY_HOST}:8080/graphite/metrics" -d  "${data}"
