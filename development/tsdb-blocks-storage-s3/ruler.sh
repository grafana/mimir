#!/bin/sh

export CORTEX_ADDRESS=http://localhost:8021/
export CORTEX_TENANT_ID=fake

cortextool rules $@
