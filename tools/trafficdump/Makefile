# SPDX-License-Identifier: AGPL-3.0-only

# This tool uses requires_libpcap tag to avoid compilation problems on machines that
# don't have libpcap installed (eg. when running "go test ./..." from Mimir root).
build:
	go build -tags requires_libpcap .
