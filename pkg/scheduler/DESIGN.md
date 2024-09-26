## Query Request Queue Design: Queue Splitting and Prioritization

The `RequestQueue` subservice embedded into the scheduler process is responsible for
all decisions regarding enqueuing and dequeuing of query requests.
While the `RequestQueue`'s responsibilities are relatively broad, including the domain logic for
querier-worker connection lifecycles and graceful startup/shutdown logic,
the queuing logic is isolated into a "tree queue" structure and its associated queue algorithms.


### Tree Queue: What and Why

The "tree queue" structure serves the purpose of a discrete priority queue.
Rather than a single queue, the requests are split into many queues,
each of which is located at a leaf node in the tree structure.

The tree structure enables some of the specific requirements of our queue selection algorithms:

* we must select a queue to dequeue from based on two independent algorithms, each with their own state
* there is a hierarchy of importance between the two algorithms - one is primary, the other secondary
* one of the algorithms (tenant-querier shuffle shard) can reject all queue options presented to it,
requiring us to return back up to the previous level of queue selection to continue searching.

These requirements lend themselves to a search tree or decision tree structure;
the levels of the tree express a clear hierarchy of decisonmaking between the two algorithms,
and the depth-first traversal provides a familiar pattern for searching for a leaf node to dequeue from.

#### Simplified Diagram
For brevity, we omit the `unknown` query component node and its subtree,
as the algorithm to select query component nodes treats `unknown` the same as `ingester-and-store-gateway`.

```mermaid
%%{init: {"flowchart": {"htmlLabels": false}} }%%

graph TB
    queryComponentAlgo["`
       select query component node by querier-worker ID
    `"]
    style queryComponentAlgo fill:white,stroke:lightgray,stroke-dasharray:5


    root(["`**root**

    `"])

    root~~~tenandShardAlgo["
        select tenant by global tenant rotation
    "]

    style tenandShardAlgo fill:white,stroke:lightgray,stroke-dasharray:5

    ingester(["`**ingester**`"])
    storeGateway(["`**store-gateway**`"])

    both(["`**ingester +

    store-gateway**`"])

    root<-->ingester
    root<-->storeGateway
    root<-->both

    ingester-tenant1(["`**tenant1**

    [queue node]
    `"])
    ingester-tenant2(["`**tenant2**

    [queue node]
    `"])

    storeGateway-tenant1(["`**tenant1**

    [queue node]
    `"])
    storeGateway-tenant2(["`**tenant2**

    [queue node]
    `"])

    both-tenant1(["`**tenant1**

    [queue node]
    `"])
    both-tenant2(["`**tenant2**

    [queue node]
    `"])

    ingester<-->ingester-tenant1
    ingester<-->ingester-tenant2


    storeGateway<-->storeGateway-tenant1
    storeGateway<-->storeGateway-tenant2

    both<-->both-tenant1
    both<-->both-tenant2


```

#### Enqueuing to the Tree Queue
On enqueue, we bin requests into separate queues based on two static properties of the query request:

* the "expected query component"
  * `ingester`, `store-gateway`, `ingester-and-store-gateway`, or `unknown`
* the tenant ID of the request

These properties are used to place the request into a queue at a leaf node.
For example, a request from tenant1 which is expected to only utilize ingesters
will be enqueued at the leaf node reached by the path `root -> ingester -> tenant1`. 

#### Dequeuing from the Tree Queue
On dequeue, we perform a depth-first search of the tree structure to select a leaf node to dequeue from.
Each of the two non-leaf levels of the tree uses a different algorithm to select the next child node.

1. Starting at the root node, one algorithm chooses one of the 4 query component child nodes.
1. At the next level down, the other algorithm attempts to choose a tenant-specific child node.
1. If a tenant node is selected, the search dequeues from it as it has reached a leaf node.
1. If no tenant node is selected, the search returns back up to the root node level
and selects another query component child node to continue the search from.

#### Full Diagram

```mermaid
%%{init: {"flowchart": {"htmlLabels": false}} }%%

graph TB

    request((("`
    **dequeue request**
    querierID
    workerID
    lastTenantIndex
    `")))

    subgraph rootLayer ["**root layer**"]
        queryComponentAlgo["`
            **query component
            node selection algorithm:**
            select starting query component by querier-worker ID;
            move on to next query component node only if DFS exhausted
        `"]
        style queryComponentAlgo fill:white,stroke:lightgray,stroke-dasharray:5

    root(["**root**"])
    end
    request-->root

    subgraph queryComponentLayer ["**query component layer**"]
        tenandShardAlgo["`
            **tenant shuffle shard
            node selection algorithm:**
            select next tenant in global rotation;
            start after lastTenantIndex,
            move on to next tenant node if tenant not sharded to querier
        `"]
        style tenandShardAlgo fill:white,stroke:lightgray,stroke-dasharray:5

        ingester(["`**ingester**`"])
        storeGateway(["`**store-gateway**`"])
        both(["`**ingester + store-gateway**`"])
    end

        %% root~~~tqasDescribe
        root-->|search subtree for next sharded tenant|ingester
        ingester-->|no sharded tenant found|root

        root-->|search subtree for next sharded tenant|storeGateway
        storeGateway-->|no sharded tenant found|root

        root-->|search subtree for next sharded tenant|both
        both-->|no sharded tenant found|root


    subgraph tenantLayer ["**tenant layer (leaf)**"]
        leafAlgo["`
        **dequeue:**
        No further node selection at leaf node level;
        any existing node has a non-empty queue
        `"]
        style leafAlgo fill:white,stroke:lightgray,stroke-dasharray:5

        ingester-tenant1([**tenant-1**])
        ingester-tenant2([**tenant-2**])

        storeGateway-tenant1([**tenant-1**])
        storeGateway-tenant2([**tenant-2**])

        both-tenant1([**tenant-1**])
        both-tenant3([**tenant-3**])
    end

    ingester-->|sharded to querierID?|ingester-tenant1
    ingester-tenant1-->|no|ingester
    ingester-->|sharded to querierID?|ingester-tenant2
    ingester-tenant2-->|no|ingester

    storeGateway-->|sharded to querierID?|storeGateway-tenant1
    storeGateway-tenant1-->|no|storeGateway
    storeGateway-->|sharded to querierID?|storeGateway-tenant2
    storeGateway-tenant2-->|no|storeGateway

    both-->|sharded to querierID?|both-tenant1
    both-tenant1-->|no|both
    both-->|sharded to querierID?|both-tenant3
    both-tenant3-->|no|both

    dequeue-ingester-tenant1[[dequeue for ingester,  tenant1]]
    dequeue-ingester-tenant2[[dequeue for ingester, tenant2]]
    ingester-tenant1-->|yes|dequeue-ingester-tenant1
    ingester-tenant2-->|yes|dequeue-ingester-tenant2

    dequeue-storeGateway-tenant1[[dequeue for store-gateway, tenant1]]
    dequeue-storeGateway-tenant2[[dequeue for store-gateway, tenant2]]
    storeGateway-tenant1-->|yes|dequeue-storeGateway-tenant1
    storeGateway-tenant2-->|yes|dequeue-storeGateway-tenant2

    dequeue-both-tenant1[[dequeue for ingester+store-gateway,  tenant1]]
    dequeue-both-tenant3[[dequeue for ingester+store-gateway, tenant3]]
    both-tenant1-->|yes|dequeue-both-tenant1
    both-tenant3-->|yes|dequeue-both-tenant3
```

### Deep Dive: Queue Selection Algorithms

#### Context & Requirements

The `RequestQueue` originally utilized only a single dimension of queue splitting, by tenant.
The structure and queue selection served to accomplish two purposes:
1. tenant fairness via a simple round-robin between all tenants with non-empty query request queues
1. rudimentary tenant isolation via shuffle-sharding noisy tenants to only a subset of queriers

While this inter-tenant Quality-Of-Service approach has worked well,
we observed QOS issues arising from the varying characteristics of Mimir's two "query components"
used by the queriers to fetch TSDB data for executing queries: ingesters and store-gateways.

<!--The structure had a simple hashmap mapping tenant IDs to a queue,-->
<!--and rotated through a global list of active tenantIDs.-->
<!--to select the next tenant sharded to the waiting querier.-->
