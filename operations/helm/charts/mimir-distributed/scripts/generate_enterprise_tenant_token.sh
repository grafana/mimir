#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# vim: ai:ts=8:sw=8:noet
# This script will talk to the admin API and generate an authorization header
# to use in future Mimir API calls.
# It requires:
# - Select the appropiate kubectl context before run
# - The helm chart has already been installed and has enterprise mode enabled
# - Specify the helm installation name in the HELM_INSTALL_NAME variable
# - Port-forward the metric-api service to port 8080 and specify the hostname in
# ADMIN_API_HOST

set -eufo pipefail
export SHELLOPTS
IFS=$'\t\n'
umask 0077

command -v kubectl >/dev/null 2>&1 || { echo "kubectl is not installed"; exit 1; }
command -v awk >/dev/null 2>&1 || { echo "awk is not installed"; exit 1; }
command -v jq >/dev/null 2>&1 || { echo "jq is not installed"; exit 1; }

HELM_INSTALL_NAME="mimir-demo"
ADMIN_API_HOST="localhost"
TENANT="test-tenant"

echo "# Getting API TOKEN from tokengen job"
API_TOKEN=$(kubectl logs -f "job/${HELM_INSTALL_NAME}-tokengen" | grep Token | awk '{print $2}')
echo "Succesfully found token: ${API_TOKEN}"

echo "# Creating Test Tenant"
data=$(cat <<END
{
  "name": "${TENANT}",
  "display_name": "Test Tenant",
  "cluster": "${HELM_INSTALL_NAME}"
}
END
);

curl \
  -X POST \
  -u ":${API_TOKEN}" \
  -H "Content-Type: application/json" \
  "${ADMIN_API_HOST}:8080/admin/api/v2/tenants" -d  "${data}"

echo
echo "# Creating Access policy for reads and writes"
data=$(cat <<END
{
  "name": "metrics-reader-and-writer",
  "display_name": "Metrics Reader and Writer",
  "realms": [
    {
      "tenant": "${TENANT}",
      "cluster": "${HELM_INSTALL_NAME}"
    }
  ],
  "scopes": [
    "metrics:read",
    "metrics:write"
  ]
}
END
);

curl \
  -X POST \
  -u ":${API_TOKEN}" \
  -H "Content-Type: application/json" \
  "${ADMIN_API_HOST}:8080/admin/api/v2/accesspolicies" -d "${data}"

echo
echo "# Creating access token"
data=$(cat <<END
{
  "name": "${TENANT}-token-7",
  "display_name": "Test Tenant Token",
  "access_policy": "metrics-reader-and-writer"
}
END
);

token_response=$(curl -s \
  -X POST \
  -u ":${API_TOKEN}" \
  -H "Content-Type: application/json" \
  "${ADMIN_API_HOST}:8080/admin/api/v2/tokens" -d "${data}")

token=$(jq -r '.token' <(echo "${token_response}"))
echo "# Generated API token '${token}'"

# The authorization header is the base64 string of $TENANT:$TOKEN
encoded_token=$(echo -n "${TENANT}:${token}" | base64 -w0)

echo "# Use the following Authorization header in your calls"
echo "Authorization: Basic ${encoded_token}"
echo "# Exporting environment variable MIMIR_BASIC_AUTH with the token contents"
export MIMIR_BASIC_AUTH="${encoded_token}"
echo "Done"
