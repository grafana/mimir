# Trafficdump

Trafficdump is a tool that can read packets from captured `tcpdump` output, reassemble them into TCP streams
and parse HTTP requests and responses. It then prints requests and responses as JSON (one request/response per line)
for further processing. Trafficdump can only parse "raw" HTTP requests and responses, and not HTTP requests and responses
wrapped in gRPC, as used by Grafana Mimir between some components. The best place to capture such traffic is on the entrypoint to Grafana Mimir
(e.g. authentication gateway/proxy).

It has some Grafana Mimir-specific and generic HTTP features:

- filter requests based on Tenant (in Basic or X-Scope-OrgId header)
- filter requests based on URL path
- filter requests based on status code of the response
- decode Grafana Mimir push requests
- filter requests based on matching series in push requests

Trafficdump can be used to inspect both remote-write requests and queries.

## Installation

Trafficdump requires that the pcap library be installed prior to tool compilation. The following are examples for
installing the prerequisite pcap library:

- `sudo apt install libpcap-dev` : Ubuntu and its derivatives
- `dnf install libpcap-devel` : Fedora, CentOS, Red Hat

Once libpcap is installed, build the `trafficdump` binary in the `tools/trafficdump` directory:

```shell
cd mimir/tools/trafficdump
make
```

If the build is successful the `trafficdump` binary will be in the same directory. You can list the tool's options with
`./trafficdump -h`.

Note that Trafficdump currently cannot decode `LINUX_SSL2` link type, which is used when doing `tcpdump -i any` on Linux.
Capturing traffic with `tcpdump -i eth0` (and link type ETHERNET / EN10MB) works fine.
