Trafficdump tool can read packets from device or from captured tcpdump output, reassemble them into TCP streams
and parse HTTP requests and responses. It then prints requests and responses as json (one request/response per line)
for further processing.

It has some Mimir-specific and generic HTTP features:

- filter requests based on Tenant (in Basic or X-Scope-OrgId header)
- filter requests based on URL path
- filter requests based on status code of the response
- decode Mimir push requests
- filter requests based on matching series in push requests

Trafficdump can be used to inspect both remote-write requests and queries.
