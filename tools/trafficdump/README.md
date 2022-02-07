Trafficdump tool can read packets from captured tcpdump output, reassemble them into TCP streams
and parse HTTP requests and responses. It then prints requests and responses as json (one request/response per line)
for further processing. Trafficdump can only parse "raw" HTTP requests and responses, and not HTTP requests and responses
wrapped in gRPC, as used by Mimir between some components. Best place to capture such traffic is on the entrypoint to Mimir
(eg. authentication gateway/proxy).

It has some Mimir-specific and generic HTTP features:

- filter requests based on Tenant (in Basic or X-Scope-OrgId header)
- filter requests based on URL path
- filter requests based on status code of the response
- decode Mimir push requests
- filter requests based on matching series in push requests

Trafficdump can be used to inspect both remote-write requests and queries.

Note that trafficdump currently cannot decode LINUX_SSL2 link type, which is used when doing `tcpdump -i any` on Linux.
Capturing traffic with `tcpdump -i eth0` (and link type ETHERNET / EN10MB) works fine.
