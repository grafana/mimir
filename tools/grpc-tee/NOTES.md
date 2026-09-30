Ultimate goal would look something like this:
1. A handful of the proxies start up and create & join their own ring, with a different ring prefix
2. The proxies also join or begin observing the store-gateway ring as readonly members - however the queriers do it today.
3. The queriers can then have a config flipped that will active their proxy/tee mode where they send queries to the proxies randomly, not based on any block sharding because there is none.
4. The proxies will read the actual proto to get the block hints needed to select the correct store-gateways from the sharding ring, and proxy the requests.
5. EVENTUALLY I want the proxy to be able to read TWO store-gateway rings so it can tee the queries and compare results.
 
