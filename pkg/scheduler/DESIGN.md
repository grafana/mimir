# Query Request Queue Design: Queue Splitting and Prioritization

The `RequestQueue` subservice embedded into the scheduler process is responsible for
all decisions regarding enqueuing and dequeuing of query requests.
While the `RequestQueue`'s responsibilities are relatively broad, including management of
querier-worker connection lifecycles and graceful startup/shutdown,
the queuing logic is isolated into a "tree queue" structure and its associated queue algorithms.

## Tree Queue: What and Why

The "tree queue" structure serves the purpose of a discrete priority queue.
The requests are split into many queues, each of which is located at a leaf node in the tree structure.

The tree structure enables some of the specific requirements of our queue selection algorithms:

- we must select a queue to dequeue from based on two independent algorithms, each with their own state
- there is a hierarchy of importance between the two algorithms - one is primary, the other secondary
- one of the algorithms (tenant-querier shuffle shard) can reject all queue options presented to it,
  requiring us to return back up to the previous level of queue selection to continue searching.

These requirements lend themselves to a search tree or decision tree structure;
the levels of the tree express a clear hierarchy of decisonmaking between the two algorithms,
and the depth-first traversal provides a familiar pattern for searching for a leaf node to dequeue from.

### Diagram: Dequeue Decision Tree (Simplified)

```mermaid
---
title: Dequeue Decision Tree (Simplified)
config:
  theme: default
---

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

    ingester-tenant1(["`**tenant-1**

    [queue node]
    `"])
    ingester-tenant2(["`**tenant-2**

    [queue node]
    `"])

    storeGateway-tenant1(["`**tenant-1**

    [queue node]
    `"])
    storeGateway-tenant2(["`**tenant-2**

    [queue node]
    `"])

    both-tenant1(["`**tenant-1**

    [queue node]
    `"])
    both-tenant2(["`**tenant-2**

    [queue node]
    `"])

    ingester<-->ingester-tenant1
    ingester<-->ingester-tenant2


    storeGateway<-->storeGateway-tenant1
    storeGateway<-->storeGateway-tenant2

    both<-->both-tenant1
    both<-->both-tenant2

```

### Enqueuing to the Tree Queue

On enqueue, we partition requests into separate queues based on two static properties of the query request:

- the "expected query component"
  - `ingester`, `store-gateway`, `ingester-and-store-gateway`, or `unknown`
- the tenant ID of the request

These properties are used to place the request into a queue at a leaf node.
A request from `tenant-1` which is expected to only utilize ingesters
will be enqueued at the leaf node reached by the path `root -> ingester -> tenant-1`.

### Dequeuing from the Tree Queue

On dequeue, we perform a depth-first search of the tree structure to select a leaf node to dequeue from.
Each of the two non-leaf levels of the tree uses a different algorithm to select the next child node.

1. At the root node level, one algorithm selects one of four possible query component child nodes.
1. At query component level, the other algorithm attempts to select a tenant-specific child node.
   1. due to tenant-querier shuffle sharding, it is possible that none of the tenant nodes
      can be selected for dequeuing for the current querier.
1. If a tenant node is selected, the search dequeues from it as it has reached a leaf node.
1. If no tenant node is selected, the search returns back up to the root node level
   and selects the next query component child node to continue the search from.

### Diagram: Dequeue Decision Tree (Full)
```mermaid
---
title: Dequeue Decision Tree (Full)
config:
  theme: default
---

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

## Deep Dive: Queue Selection Algorithms

### Context & Requirements

### Original State: Queue Splitting by Tenant

The `RequestQueue` originally utilized only a single dimension of queue splitting, by tenant.
The structure and queue selection served to accomplish two purposes:

1. tenant fairness via a simple round-robin between all tenants with non-empty query request queues
1. rudimentary tenant isolation via shuffle-sharding noisy tenants to only a subset of queriers

While this inter-tenant Quality-Of-Service approach has worked well,
we observed other QOS issues arising from the varying characteristics of Mimir's two "query components"
utilized by the queriers to fetch TSDB data for executing queries: ingesters and store-gateways.

### New Requirement: Queue Splitting by Query Component

Ingesters serve requests for recent data, and store-gateways serve requests for older data.
While queries can overlap the time periods of data fetched by both query components,
many requests are served by only one of the two components.

Ingesters and store-gateways tend to experience issues independently of each other,
but when one component was in a degraded state, _all_ queries would wait in the queue behind the slow queries,
causing high latency and timeouts for queries which could have been serviced by the non-degraded query component.

### Phase 1: Query Component Selection by Round-Robin

In the first phase, we believed that it would be enough to duplicate the tenant queue splitting approach.
We split the tenant queues further by query component, so that each tenant could have up to four queues.

With the addition of another split, we introduced the "tree queue" structure, inspired by Loki's implementation.
The tree queue allowed more clear management of the two dimensions of queue splitting rather than one.

For simplicity at this stage, the tenant selection algorithm was kept higher in the tree
and therefore took priority over the query component queue selection algorithm.
Additionally, the query component selection algorithm was a simple round-robin.

This phase was a failure due to both of those design decisions.

#### Failure 1: Tenant Selection Priority over Query Component Selection (minor)

The fact that the tenant selection was given priority over query-component selection
meant that a tenant's query traffic profile could override the query component round-robin.

If the query component round-robin was set to dequeue a store-gateway query
but the tenant rotation had selected `tenant-1` which was only sending ingester queries at the time,
the system would dequeue an ingester query in order to prioritize dequeuing for `tenant-1`.

#### Failure 2: Inability to Prevent Processing Time Dominance by Slow Queries (major)

A vanilla round-robin algorithm does not sufficiently guard against a high-latency component
saturating all or nearly all connections with queries stuck in the slow component.
Despite alternating the which query component is dequeued for, utilization of the querier-worker connections
as measured by inflight query processing time will grow asymptotically to be dominated by the slow query component.

### Phase 2: Query Component Selection to Solve Processing Time Dominance by Slow Queries

To demonstrate the issue, we can simplify the system to two query components and four querier connections.
Queries to the "slow" query component take 8 ticks to process while queries to the "fast" query component take 1 tick.
The round-robin selection advances from fast to slow or vice versa with each dequeue.

In 16 ticks each for 4 querier connections (totaling 64 ticks), the system:

- dequeues and starts processing 16 queries: 8 fast, 8 slow
- completes processing 13 queries: 8 fast, 5 slow
- spends 8 ticks processing the fast queries
- spends 56 ticks processing the slow queries

#### Diagram: Query Processing Time Utilization with Round-Robin

```mermaid
---
title: Query Processing Time Utilization with Round-Robin
config:
  gantt:
    displayMode: compact
    numberSectionStyles: 2
  theme: default
---
gantt
    dateFormat ss
    axisFormat %S
    tickInterval 1second

    section consumer-1
        fast        :active, c1-1, 00, 1s
        fast        :active, c1-2, 01, 1s
        fast        :active, c1-3, 02, 1s
        slow        :done, c1-4, 03, 8s
        slow...     :done, c1-5, 11, 5s

    section consumer-2
        slow        :done, c2-1, 00, 8s
        fast        :active, c2-2, 08, 1s
        fast        :active, c2-3, 09, 1s
        fast        :active, c2-4, 10, 1s
        fast        :active, c2-5, 11, 1s
        slow...     :done, c2-6, 12, 4s

    section consumer-3
        fast        :active, c3-1, 00, 1s
        slow        :done, c3-2, 01, 8s
        slow...     :done, c3-3, 09, 7s

    section consumer-4
        slow        :done, c4-1, 00, 8s
        slow        :done, c4-2, 08, 8s

```

<!--The structure had a simple hashmap mapping tenant IDs to a queue,-->
<!--and rotated through a global list of active tenantIDs.-->
<!--to select the next tenant sharded to the waiting querier.-->

... [fill in in stuff about the new algorithm]

In 16 ticks each for 4 querier connections (totaling 64 ticks), the system:

- dequeues and starts processing 36 queries: 32 fast, 4 slow
- completes processing 36 queries: 32 fast, 4 slow
- spends 32 ticks processing the fast queries
- spends 32 ticks processing the slow queries

#### Diagram: Query Processing Time Utilization with Querier-Worker Queue Prioritization

```mermaid
---
title: Query Processing Time Utilization with Querier-Worker Queue Prioritization
config:
  gantt:
    displayMode: compact
    numberSectionStyles: 2
  theme: default
---
gantt
    dateFormat ss
    axisFormat %S
    tickInterval 1second

    section consumer-1
        fast        :active, c1-1, 00, 1s
        fast        :active, c1-2, 01, 1s
        fast        :active, c1-3, 02, 1s
        fast        :active, c1-4, 03, 1s
        fast        :active, c1-5, 04, 1s
        fast        :active, c1-6, 05, 1s
        fast        :active, c1-7, 06, 1s
        fast        :active, c1-8, 07, 1s
        fast        :active, c1-9, 08, 1s
        fast        :active, c1-10, 09, 1s
        fast        :active, c1-11, 10, 1s
        fast        :active, c1-12, 11, 1s
        fast        :active, c1-13, 12, 1s
        fast        :active, c1-14, 13, 1s
        fast        :active, c1-15, 14, 1s
        fast        :active, c1-16, 15, 1s

    section consumer-2
        fast        :active, c2-1, 00, 1s
        fast        :active, c2-2, 01, 1s
        fast        :active, c2-3, 02, 1s
        fast        :active, c2-4, 03, 1s
        fast        :active, c2-5, 04, 1s
        fast        :active, c2-6, 05, 1s
        fast        :active, c2-7, 06, 1s
        fast        :active, c2-8, 07, 1s
        fast        :active, c2-9, 08, 1s
        fast        :active, c2-10, 09, 1s
        fast        :active, c2-11, 10, 1s
        fast        :active, c2-12, 11, 1s
        fast        :active, c2-13, 12, 1s
        fast        :active, c2-14, 13, 1s
        fast        :active, c2-15, 14, 1s
        fast        :active, c2-16, 15, 1s


    section consumer-3
        slow        :done, c3-1, 00, 8s
        slow        :done, c3-2, 08, 8s

    section consumer-4
        slow        :done, c4-1, 00, 8s
        slow        :done, c4-2, 08, 8s

```
