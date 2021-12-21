#!/bin/sh
# SPDX-License-Identifier: AGPL-3.0-only

export CORTEX_ADDRESS=http://localhost:8021/
export CORTEX_TENANT_ID=fake

cortextool rules $@
