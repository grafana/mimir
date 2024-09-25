## Query Request Queue Design: Queue Splitting and Prioritization

The `RequestQueue` subservice embedded into the scheduler process is responsible for
all decisions regarding enqueuing and dequeuing of query requests.
While the `RequestQueue`'s responsibilities are relatively broad, including the domain logic for
querier-worker connection lifecycles and graceful startup/shutdown logic,
the queuing logic is isolated into a "tree queue" structure and its associated queue algorithms.


### Tree Queue: What and Why?

The "tree queue" structure serves a purpose much like a discrete priority queue.
Rather than a single queue, the requests are split into many queues,
each of which is located at a leaf node in the tree structure.


The tree structure meets the specific requirements of our queue prioritization algorithms, namely:

* we must select a queue to dequeue from based on two separate algorithms, each with independent state
* there is a hierarchy of importance between the two queue selection algorithms - one is primary, the other secondary
* one of the queue selection algorithms (tenant-querier shuffle shard) can reject all queue options presented to it

These requirements lend themselves to a search tree or decision tree structure;
the levels of the tree express a clear hierarchy of decisonmaking
and traversal algorithms provide a familiar pattern for searching through the tree.

#### Tree Queue: Simplified Diagram

Before digging deeper into the specific queue selection algorithms,
we can start with this simplified view of how we traverse the tree
to select the next queue to dequeue a query request from:

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

    both(["`**ingester +store-gateway**`"])

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


