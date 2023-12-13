#!/bin/sh
# SPDX-License-Identifier: AGPL-3.0-only

export MIMIR_ADDRESS=http://localhost:8022/
export MIMIR_TENANT_ID=anonymous

mimirtool rules $@
