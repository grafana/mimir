#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# Provenance-includes-location: https://github.com/cortexproject/cortex/tools/cleanup-white-noise.sh
# Provenance-includes-license: Apache-2.0
# Provenance-includes-copyright: The Cortex Authors.

SED_BIN=${SED_BIN:-sed}

${SED_BIN} -i 's/[ \t]*$//' "$@"
