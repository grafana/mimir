#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only

set -e

# Install GitHub CLI from the official Debian repository
# This follows the official installation instructions: https://github.com/cli/cli/blob/trunk/docs/install_linux.md

# Add GitHub CLI repository
curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg | dd of=/usr/share/keyrings/githubcli-archive-keyring.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" | tee /etc/apt/sources.list.d/github-cli.list > /dev/null

# Update package index and install gh
apt-get update
apt-get install -y gh
